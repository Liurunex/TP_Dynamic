// -*- C++ -*-
//=============================================================================
/**
 *      Copyright (c) Freewheel, 2007-2009. All rights reserved.
 *
 *      @file
 *
 *      @author
 *
 *      @brief
 *
 *      Revision History:
 *              2017/07/06      Neo
 *                      Created.
 *
 */
//=============================================================================

#include "Forecast_Adaptive_TP_Service.h"
#include "Forecast_Task.h"

//==============================================================================
// zxliu modification: TP_Adaptive
//==============================================================================

Ads_Job_Queue_Service_TP_Adaptive::Ads_Job_Queue_Service_TP_Adaptive(const Ads_String& type)
	: Ads_Job_Queue_Service(), type_(type), mutex_map_(), mutex_job_(), mutex_size_()
	, job_done_counter_(0), tp_throughput_(new TP_Throughput())
	{}

Ads_Job_Queue_Service_TP_Adaptive::~Ads_Job_Queue_Service_TP_Adaptive() { thread_ids_map_.clear(); }

size_t
Ads_Job_Queue_Service_TP_Adaptive::count_idle_threads() {
	ads::Guard _map_guard(mutex_map_);
	
	size_t all_idle = 0;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map_.begin();
	while(it != thread_ids_map_.end()) {
		if (!it->second) all_idle ++;
		it ++;
	}
	
	return all_idle;
}

int
Ads_Job_Queue_Service_TP_Adaptive::thread_status_set(pthread_t pid, int set_sta) {
	ads::Guard _map_guard(mutex_map_);
	
	std::unordered_map<pthread_t, int>::iterator got = thread_ids_map_.find(pid);
	if (got != thread_ids_map_.end()) {
		got->second = set_sta;
	}
	else {
		ADS_LOG((LP_ERROR, "----------------------: %s tried thread_status_set(), not found target: %d\n",
			type_.c_str(), pid));
		return -1;
	}

	return 0;
}

int
Ads_Job_Queue_Service_TP_Adaptive::deleteNode(pthread_t target) {
	/* mutex_size(n_threads_) locked by thread itself*/
	ads::Guard _map_guard(mutex_map_);
	
	std::unordered_map<pthread_t, int>::iterator got = thread_ids_map_.find(target);
	if (got != thread_ids_map_.end()) {
		thread_ids_map_.erase(got);
		this->n_threads_ --;
		ADS_LOG((LP_ERROR, "----------------------: %s tried deleteNode(), erased the pid %d\n",
			type_.c_str(), target));
	}
	else {
		ADS_LOG((LP_ERROR, "----------------------: %s tried deleteNode(), not found target: %d\n",
			type_.c_str(), target));
		return -1;
	}
	
	return 0;
}

int
Ads_Job_Queue_Service_TP_Adaptive::extend_threadpool(size_t extend_size) {
	/* mutex_size(n_threads_) locked by supervisor */
	ads::Guard _map_guard(mutex_map_);
	
	size_t start_index         = n_threads_;
	this->n_threads_          += extend_size;
	
	pthread_attr_t _attr;
	pthread_attr_init(&_attr);
	pthread_attr_setstacksize(&_attr, 0x4000000); //64M

	for (size_t i = start_index; i < n_threads_; ++ i) {
		pthread_t pth_id;
		pthread_attr_t *attr = 0;
		attr = &_attr;
		
		int ret = ::pthread_create(&pth_id, attr, &Ads_Job_Queue_Service_TP_Adaptive::svc_run, this);
		if (ret != 0) ADS_LOG((LP_ERROR, "----------------------: %s failed to create thread %d in extend() \n",
			type_.c_str(), pth_id));
		thread_ids_map_[pth_id] = 0;
	}
	
	ADS_LOG((LP_ERROR, "----------------------: %s extended thread_pool size to : %d\n",
		type_.c_str(), thread_ids_map_.size()));
	
	return 0;
}

int
Ads_Job_Queue_Service_TP_Adaptive::curtail_threadpool(size_t curtail_size) {
	size_t curtail_counter = 0, try_time = 0;
	/* try double times to ensure successful message delivery */
	
	while (try_time < (curtail_size + curtail_size)) {
		Ads_Job* msg = new Curtail_TP();
		if(this->mq_return().enqueue(msg) < 0) {
			ADS_LOG((LP_ERROR, "----------------------: %s cannot post curtail message\n",
				type_.c_str()));
			msg->destroy();	
		}
		else {
			curtail_counter ++;
			if (curtail_counter == curtail_size) return 0;
		}
		try_time ++;
	}
	ADS_LOG((LP_ERROR, "----------------------: %s only post curtail message: %d out of %d\n",
		type_.c_str(), curtail_counter, curtail_size));
	
	return 0;
}

int
Ads_Job_Queue_Service_TP_Adaptive::curtail_action() {
	ads::Guard _size_guard(mutex_size_);

	if (this->n_threads_ > TP_MIN_THRESHOLD) {
		if (!this->deleteNode(pthread_self())) {
			ADS_LOG((LP_ERROR, "----------------------: %s curtail thread pool size to: %d\n",
				type_.c_str(), this->n_threads_));
			return 0;
		}
	}
	else ADS_LOG((LP_ERROR, "----------------------: %s curtail action forbidden: thread_pool size reached Min_Limit: %d\n",
		type_.c_str(), this->n_threads_));
	
	return -1;
}

/* override base fucntions */

int
Ads_Job_Queue_Service_TP_Adaptive::open() {
	#if defined(ADS_ENABLE_FORECAST)
	
	if(thread_ids_map_.size() !=0 && thread_ids_map_.begin()->first != 0) {
		ADS_LOG((LP_ERROR, "service is already opened.\n"));
		return 0;
	}
	
	#endif
	
	/* worker thread initial */
	#if defined(ADS_ENABLE_SEARCH) || defined(ADS_ENABLE_MACOSX)
	
	pthread_attr_t _attr;
	pthread_attr_init(&_attr);
	pthread_attr_setstacksize(&_attr, 0x4000000); //64M
	
	#endif
	
	for (size_t i = 0; i < n_threads_; ++ i) {
		pthread_t pth_id;
		pthread_attr_t *attr = 0;
		
		#if defined(ADS_ENABLE_SEARCH) || defined(ADS_ENABLE_MACOSX)
		attr = &_attr;
		#endif
		
		int ret = ::pthread_create(&pth_id, attr, &Ads_Job_Queue_Service_TP_Adaptive::svc_run, this);
		if (ret != 0) ADS_LOG((LP_ERROR, "----------------------: %s failed to create thread in open() %d\n",
			type_.c_str(), pth_id));
		thread_ids_map_[pth_id] = 0;
	}
	
	ADS_LOG((LP_ERROR, "----------------------: %s ORIGINAL thread_pool size: %d\n",
		type_.c_str(), thread_ids_map_.size()));
	
	/* initial tp throughput structure */
	this->init_tp_thrput();
	
	return 0;
}

int
Ads_Job_Queue_Service_TP_Adaptive::wait() {
	ads::Guard _map_guard(mutex_map_);
	
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map_.begin();
	while(it != thread_ids_map_.end()) {
		::pthread_join(it->first, 0);
		ADS_LOG((LP_ERROR, "----------------------: %s joined: %d done\n",
			type_.c_str(), it->first));
		it ++;
	}
	
	return 0;
}

int
Ads_Job_Queue_Service_TP_Adaptive::stop() {
	this->exitting_ = 1;
	{
		ads::Guard _size_guard(mutex_size_);
		size_t exit_counter = 0;
		for (size_t i = 0; i < n_threads_ + n_threads_; ++ i) {
			Ads_Job* msg = new No_Op();
			if(this->mq_return().enqueue(msg) < 0) {
				ADS_LOG((LP_ERROR, "----------------------: %s cannot post exit message\n",
					type_.c_str()));
				msg->destroy();	
			}
			else {
				exit_counter ++;
				if (exit_counter == n_threads_) break;
			}
		}
	}
	
	this->wait();
	size_t count = 0;
	while (!this->mq_return().is_empty()) {
		Ads_Job *job = 0;
		if (this->mq_return().dequeue(job) < 0) 
			break;
		
		if (!job) continue;
		
		bool abandoned = job->abandoned();
		if (job->waitable())
			reinterpret_cast<Ads_Waitable_Job *>(job)->signal();
		
		if (abandoned) {
			job->destroy();
			count ++;
		}
	}
	
	ADS_LOG((LP_ERROR, "----------------------: %s stop(): %d jobs destroyed\n",
		type_.c_str(), count));
	
	thread_ids_map_.clear();
	
	return 0;
}

int
Ads_Job_Queue_Service_TP_Adaptive::svc() {
	ADS_LOG((LP_INFO, "(jobq: %llu) service started.\n", ads::thr_id()));
	
	while (!this->exitting_) {
		Ads_Job *job = 0;
		if (this->mq_return().dequeue(job) < 0 || !job) break;
		
		if (job->type() == TYPE_CURTAIL) {
			if (this->curtail_action() < 0) {
				ADS_LOG((LP_ERROR, "----------------------: %s curtail action failed\n",
					type_.c_str()));
				job->destroy();
				continue;
			}
			else {
				/* terminate current thread */
				job->destroy();
				::pthread_detach(pthread_self());
				return 0;
			}
		}
		/*? the problem here is how to handle the error case where the map didn't contian the current thread's pid:
			if so, we may add the thread id into the map, or we could simply shut the service down, or just ignore it
		?*/
		if (job->type() == TYPE_JOB)
			if(this->thread_status_set(pthread_self(), 1) < 0)
				ADS_LOG((LP_ERROR, "----------------------: %s: %d set thread status to active failed\n",
					type_.c_str(), pthread_self()));
		
		if (job->run() < 0)
			ADS_LOG((LP_ERROR, "----------------------: %s: %d failed to run job\n", type_.c_str(), pthread_self()));
		else this->job_counter_increasement();
		
		if (job->type() == TYPE_JOB)
			if(this->thread_status_set(pthread_self(), 0) < 0)
				ADS_LOG((LP_ERROR, "----------------------: %s: %d set thread status to idle failed\n",
					type_.c_str(), pthread_self()));
		
		bool abandoned = job->abandoned();
		if (job->waitable())
			reinterpret_cast<Ads_Waitable_Job *>(job)->signal();
		
		if (abandoned) job->destroy();
	}
	
	ADS_LOG((LP_INFO, "(jobq: %lu) service stopped.\n", ads::thr_id()));
	
	return 0;
}

void
Ads_Job_Queue_Service_TP_Adaptive::job_counter_increasement() {
	ads::Guard _job_guard(mutex_job_);
	this->job_done_counter_ ++;
	return;
}

void
Ads_Job_Queue_Service_TP_Adaptive::init_tp_thrput() {
	tp_throughput_->cur_thread_size = this->n_threads_;
	tp_throughput_->pre_job_count    = 0;
	tp_throughput_->cur_job_count    = 0;
	tp_throughput_->cur_diff_percent = 0;
	tp_throughput_->pre_thrput       = 0;
}

void
Ads_Job_Queue_Service_TP_Adaptive::get_tp_thrput_diff() {
	/* mutex_size(n_threads_) locked by supervisor */
	tp_throughput_->cur_thread_size = this->n_threads_;
	{
		ads::Guard _job_guard(mutex_job_);
		tp_throughput_->cur_job_count   = this->job_done_counter_;
	}
	
	size_t job_done 	= tp_throughput_->cur_job_count - tp_throughput_->pre_job_count;
	double cur_thrput   = job_done/double(tp_throughput_->cur_thread_size);
	
	/*? NOTICE: cur_diff_percent is in the range [-100, +infinite) */
	if (tp_throughput_->pre_thrput == cur_thrput) {
		if (!cur_thrput)
			tp_throughput_->cur_diff_percent = TP_GR_THRESHOLD;
		else tp_throughput_->cur_diff_percent = 0;
	}
	else if (!tp_throughput_->pre_thrput)
		/* at least make the throughput = 0 case not be the matter of forbiddening threads increasement */
		tp_throughput_->cur_diff_percent  = TP_GR_THRESHOLD;
	else {
		tp_throughput_->cur_diff_percent  = 100 * (cur_thrput - tp_throughput_->pre_thrput);
		tp_throughput_->cur_diff_percent /= tp_throughput_->pre_thrput;
	}
	
	tp_throughput_->pre_job_count   = tp_throughput_->cur_job_count;
	tp_throughput_->pre_thrput      = cur_thrput;
}

//==============================================================================
// zxliu modification: Supervisor
//==============================================================================

Forecast_Manager_Supervisor::Forecast_Manager_Supervisor(const Ads_String& type, 
	const size_t thread_limit, const size_t n_tp)
	: type_(type), exitting_(false), n_tp_(n_tp), thread_limit_(thread_limit)
	, added_counter_(0), res_integration_(new ResultInfo())
	, cur_cpu_(new CpuInfo()), pre_cpu_(new CpuInfo())
	{}

Forecast_Manager_Supervisor::~Forecast_Manager_Supervisor() {
	tp_group_.clear();
	tp_weights_.clear();
	tp_modification_.clear();
}

int
Forecast_Manager_Supervisor::openself() {
	/*? shall we check if the supervisor is already started: check the supervisor pid might not help
		since it already set to a pid in class constructor ?*/
	
	tp_weights_.resize(n_tp_);
	tp_modification_.resize(n_tp_);
	
	pthread_attr_t _sattr;
	pthread_attr_init(&_sattr);
	pthread_attr_t *sattr;
	sattr = &_sattr;
	
	if (::pthread_create(&supervisor_id_, sattr, &Forecast_Manager_Supervisor::supervisor_func_run, this))
		ADS_LOG((LP_ERROR, "======================: Supervisor failed to create its own thread\n"));
	
	ADS_LOG((LP_ERROR, "======================: Supervisor created its own thread\n"));
	
	return 0;
}

int
Forecast_Manager_Supervisor::addworker(Forecast_Task_Manager* worker) {
	tp_group_.push_back(worker);
	added_counter_ ++;

	/*? risk: shall we check whether the pointer really point to a exsiting worker reference ?*/
	if (added_counter_ == n_tp_) {
		ADS_LOG((LP_ERROR, "======================: Supervisor added all worders and got ready to work\n"));
		/* we do nothing here as the supervisor will sleep for 100 seconds once start */
	}
	
	return 0;
}

int
Forecast_Manager_Supervisor::stop() {
	this->exitting_ = true;
	
	::pthread_join(supervisor_id_, 0);
	ADS_LOG((LP_ERROR, "======================: Supervisor join()/stop() done\n"));
	
	tp_modification_.clear();
	tp_weights_.clear();
	tp_group_.clear();
	
	return 0;
}

void *
Forecast_Manager_Supervisor::supervisor_func_run(void *arg) {
	Forecast_Manager_Supervisor *s = reinterpret_cast<Forecast_Manager_Supervisor *>(arg);
	s->supervisor_func();
	return 0;
}

/*? NOTE: thread size is conditional varaible;
	singel service  accessing in curtial_action() --> delete_node(),
	superviosr accessing in get_tp_thrput_diff() --> extend_thread();
	service accessing in open() --> initial() --> stop();
	solution: lock all mutext_size while the supervisor do its job so that
	single service cannot access the thread size varibale;
*/
int
Forecast_Manager_Supervisor::supervisor_func() {
	sleep(100);
	
	init_proc_parameter(this->pre_cpu_);
	get_cpu_number(this->res_integration_);
	
	while (!this->exitting_) {
		sleep(10);
		int cur_curtail_scale = TP_MODIFY_CURTAIL_SCALE;
		int cur_extend_scale  = TP_MODIFY_EXTEND_SCALE;
		
		/* grab cpu/mem/io info */
		init_proc_parameter(this->pre_cpu_);
		sleep(1);
		get_current_value(this->cur_cpu_, this->pre_cpu_, this->res_integration_);
		
		ADS_LOG((LP_ERROR, "======================:cpu_# \t r \t b \t cpu_% \t mem_% \t util_% \t io_load\n"));
		ADS_LOG((LP_ERROR, "======================:%d \t %d \t %d \t %lf \t %lf \t %lf \t %lf\n", res_integration_->num_cpu, 
			res_integration_->r_process, res_integration_->b_process, res_integration_->cpu_percent, 
			res_integration_->mem_percent, res_integration_->util_percent,res_integration_->io_load));
		
		/* determine whether the program was forbidden to create more threads */
		int extend_forbidden = 0;
		if (this->res_integration_->cpu_percent > CPU_LIMIT || this->res_integration_->mem_percent > MEM_LIMIT) {
			ADS_LOG((LP_ERROR, "======================:system resource limit reached, thread_pool extend opeartion was forbiddened\n"));
			extend_forbidden  = 1;
			cur_curtail_scale = 2;
		}
		else {
			if (this->res_integration_->io_load > IO_LIMIT && this->res_integration_->util_percent > IO_LIMIT) {
				ADS_LOG((LP_ERROR, "======================:system io limit reached\n"));
				cur_extend_scale = 2;
			}
			/* do the LIMIT extendtion */
			add_thread_limit();
			/*? the thread limit should be reconsider based on the basic thread size divided by total available  memory ?*/
		}
		
		/* thread pool size adjustment */
		size_t waiting_mq_count   = 0;
		size_t idle_thread_count  = 0;
		size_t all_threads_size   = 0;
		size_t all_extend_tp      = 0;
		double positive_gr_sum    = 0;
		
		std::fill(tp_weights_.begin(), tp_weights_.end(), 1);
		std::fill(tp_modification_.begin(), tp_modification_.end(), 0);
		
		/* determine the raw modification and the weight value of each thread pool */
		for (size_t i = 0; i < n_tp_; ++ i) {
			tp_group_[i]->mutex_size_lock();
			
			Ads_Job_Queue_Service_TP_Adaptive::TP_Throughput *tp_thrput = tp_group_[i]->tp_thrput();
			size_t wait_message      = tp_group_[i]->num_pending_jobs();
			size_t idle_threads      = tp_group_[i]->count_idle_threads();
			all_threads_size        += tp_group_[i]->tp_size();
			
			/*? actually it cannot guarantee the time interval for each thread pool stayed unchanged ?*/
			tp_group_[i]->get_tp_thrput_diff();
				
			if (!extend_forbidden && (wait_message >= MQ_THRESHOLD || idle_threads == 0)) {
				waiting_mq_count   += wait_message;
				
				if (wait_message == 0) wait_message = MQ_THRESHOLD;
				tp_modification_[i] = (int)(wait_message);
				
				if (tp_thrput->cur_diff_percent <= 0)
					tp_weights_[i]   = TP_GR_THRESHOLD;
				tp_weights_[i]      = tp_thrput->cur_diff_percent;
				positive_gr_sum    += tp_weights_[i];
				
				all_extend_tp ++;
			}
			else if (idle_threads  >=  TP_IDLE_THRESHOLD) {
				tp_modification_[i] =  -1 * (int)(idle_threads);
				idle_thread_count  += idle_threads;
				tp_weights_[i]      = 1; /* conservative assignment */
				
				/*? what we should do for thread pool with negative growth rate, have a try: notice there are two cases:
					tp_modification_[i] is negative, or positive, try somthing reasonable
				}*/
			}
			ADS_LOG((LP_ERROR, "======================: worker %d: t_size(%d), wait_mg(%d), idle_t(%d), thrput(%f), modify(%d)\n",
						i, tp_group_[i]->tp_size(), wait_message, idle_threads, tp_thrput->cur_diff_percent, tp_modification_[i]));
		}
		ADS_LOG((LP_ERROR, "======================: Supervisor: all waiting message: %d, all idle threads: %d \n", waiting_mq_count, idle_thread_count));
		size_t extend_sum         = waiting_mq_count  / cur_extend_scale;
		size_t curtail_sum        = idle_thread_count / cur_curtail_scale;
		size_t modify_thread_size = extend_sum - curtail_sum;
		if (all_threads_size >= thread_limit_) {
			ADS_LOG((LP_ERROR, "======================: Supervisor: total threads limit size reached before trying modifying\n"));
			cur_curtail_scale = 1;
			for (size_t i = 0; i < n_tp_; ++ i) {
				if (tp_modification_[i] < 0) {
					tp_weights_[i]    = 1;
					size_t to_curtail = (size_t)(tp_weights_[i] * (size_t)(-1 * tp_modification_[i]) / cur_curtail_scale);
					if (to_curtail > 0 && tp_group_[i]->tp_size() >= (to_curtail + TP_MIN_THRESHOLD)) {
						ADS_LOG((LP_ERROR, "======================: Supervisor (LIMIT reached) informed %s: do curtail ( %d )\n",
									tp_group_[i]->type().c_str(), to_curtail));
						tp_group_[i]->curtail_threadpool(to_curtail);
					}
				}
			}
			continue;
		}
		/* to avoid the total threads number after modification surpassed the LIMIT*/
		if (all_threads_size + modify_thread_size > thread_limit_) {
			extend_sum       = (thread_limit_ - all_threads_size) * extend_sum / modify_thread_size;
			curtail_sum      = extend_sum - (thread_limit_ - all_threads_size);
			all_threads_size = thread_limit_;
		}
		else all_threads_size += modify_thread_size;
		
		ADS_LOG((LP_ERROR, "======================: Supervisor: extending threads sum: %d\n", extend_sum));
		ADS_LOG((LP_ERROR, "======================: Supervisor: curtailing threads sum: %d\n", curtail_sum));
		ADS_LOG((LP_ERROR, "======================: Superviosr: target threads size: %d\n", all_threads_size));
		
		/* individual thread pool modify */
		for (size_t i = 0; i < n_tp_; ++ i) {
			if (tp_modification_[i] > 0) {
				/* re_adjust the weights value */
				tp_weights_[i]   = all_extend_tp * tp_weights_[i] / positive_gr_sum;
				size_t to_extend = (size_t)(tp_weights_[i] * (size_t)(tp_modification_[i]) * extend_sum / waiting_mq_count);
				if (tp_modification_[i] ==  1 || to_extend < 1) to_extend = 1;
				if (to_extend > 0) {
					ADS_LOG((LP_ERROR, "+++++++++++======================: Supervisor informed %s: do extend ( %d )\n",
						tp_group_[i]->type().c_str(), to_extend));
					tp_group_[i]->extend_threadpool(to_extend);
				}
			}
			else if (tp_modification_[i] < 0) {
				/* re_adjust the weights value, do nothing currently, but might do some modification in future version */
				tp_weights_[i]    = 1;
				size_t to_curtail = (size_t)(tp_weights_[i] * (size_t)(-1 * tp_modification_[i]) * curtail_sum / idle_thread_count);
				if (to_curtail > 0 && tp_group_[i]->tp_size() >= (to_curtail + TP_MIN_THRESHOLD)) {
					ADS_LOG((LP_ERROR, "++++++++++======================: Supervisor informed %s: do curtail ( %d )\n",
						tp_group_[i]->type().c_str(), to_curtail));
					/*? shall we set the return value to -1: actually we might dipatch some successful curtail messages,
					but if we return -1 as we didn't dipatch all expected messages, what's the optimal operation to handle it? ?*/
					tp_group_[i]->curtail_threadpool(to_curtail);
				}
			}
			tp_group_[i]->mutex_size_unlock();
		}
	}
	return 0;
}

/* grab info of cpu, mem, io */
void
Forecast_Manager_Supervisor::init_proc_parameter (CpuInfo *pre_cpu) {
	FILE* cpufile = fopen("/proc/stat", "r");
	
	if (!cpufile) {
		ADS_LOG((LP_ERROR, "======================: Supervisor failed to open /proc/stat\n"));
		return;
	}
	fscanf(cpufile, "cpu %llu %llu %llu %llu", &pre_cpu->cpu_totalUser, &pre_cpu->cpu_totalUserLow, 
			&pre_cpu->cpu_totalSys, &pre_cpu->cpu_totalIdle);
	fclose(cpufile);
	
	return;
}

void
Forecast_Manager_Supervisor::get_cpu_number(ResultInfo *res_integration) {
	unsigned int num_cpu = 0;
	FILE *num_cpufile = fopen("/proc/cpuinfo", "r");
	
	if (!num_cpufile) {
		ADS_LOG((LP_ERROR, "======================: Supervisor failed to open /proc/cpuinfo\n"));
		res_integration->num_cpu = 0;
		return;
	}
	while (fgets(buffer_, sizeof(buffer_), num_cpufile))
		if (!strncmp(buffer_, "processor\t:", 11))
			num_cpu ++;
	fclose(num_cpufile);
	res_integration->num_cpu = num_cpu;
	
	return;
}

std::string
Forecast_Manager_Supervisor::popen_usage(std::string command) {
	int skip_line = 0, cur_line = 0;
	if (command == vmstat_) skip_line = 2;
	if (command == iostat_) skip_line = 6;
	
	std::string res;
	FILE *shellin = popen(command.c_str(), "r");
	if (!shellin) {
		ADS_LOG((LP_ERROR, "======================: Supervisor failed to popen command %s\n",
					command.c_str()));
		return res;
	}

	memset(buffer_, 0, sizeof(buffer_));
	while (fgets(buffer_, sizeof(buffer_), shellin)) {
		if (cur_line < skip_line) {
			cur_line ++;
			continue;
		}
		res += std::string(buffer_);
	}
	pclose(shellin);
	
	return res;
}

void
Forecast_Manager_Supervisor::get_current_value (CpuInfo *cur_cpu, CpuInfo *pre_cpu, ResultInfo *res_integration) {
	/* IO util retrive from iostat */
	double util_percent = 0;
	double io_load      = 0;
	double await        = 0;
	double svctim       = 0;
	int io_counter      = 0;
	
	/* here I ignore the case where return string of popen is empty */
	std::string io_string = popen_usage(iostat_);
	if (!io_string.empty()) {
		std::stringstream io_iss(io_string);
		std::string io_line;
		while (getline(io_iss, io_line)) {
			if(!io_line.length()) break;
			io_counter = 0;
			std::size_t prev = io_line.size()-1, pos = io_line.size()-1;
			while ((pos = io_line.find_last_of(" ", prev)) != 0) {
				if (io_counter >= 5)
					break;
				if (pos < prev) {
					io_counter ++;
					switch(io_counter) {
						case 1:
							util_percent = std::max(util_percent, std::stod(io_line.substr(pos, prev-pos+1)));
							break;
						case 2:
							svctim       = std::stod(io_line.substr(pos, prev-pos+1));
							break;
						case 5:
							await        = std::stod(io_line.substr(pos, prev-pos+1));
							break;
					}
				}
				prev = pos - 1;
			}
			io_load = std::max(io_load, (await - svctim)/svctim);
		}
		res_integration->util_percent = util_percent;
		res_integration->io_load      = io_load;
	}
	
	/* r/w retrive from vmstats */
	std::vector<int> tem_res;
	int vm_counter = 0;
	
	std::string vm_string = popen_usage(vmstat_);
	if (!vm_string.empty()) {
		std::stringstream vm_iss(vm_string);
		std::string vm_line;
		while (getline(vm_iss, vm_line)) {
			std::size_t prev = 0, pos = 0;
			while ((pos = vm_line.find_first_of(" ", prev)) != std::string::npos) {
				if (vm_counter == 2) break;
				if (pos > prev) {
					vm_counter ++;
					tem_res.push_back(std::stoi(vm_line.substr(prev, pos-prev)));
				}
				prev = pos + 1;
			}
			break;
		}
		res_integration->r_process = tem_res[0];
		res_integration->b_process = tem_res[1];
	}
	
	/* RAM percent */
	struct sysinfo memInfo;
	sysinfo (&memInfo);
	unsigned long long totalRAM  =  memInfo.totalram;
	totalRAM                    *=  memInfo.mem_unit;
	unsigned long long idleRAM   =  memInfo.freeram;
	idleRAM                     *=  memInfo.mem_unit;
	double ram_percent           =  100 *(totalRAM - idleRAM);
	ram_percent                 /=  totalRAM;
	res_integration->mem_percent =  ram_percent;
	
	/* CPU percent */
	unsigned long long cpu_total = 0;
	double cpu_percent;
	FILE* cpufile = fopen("/proc/stat", "r");
	if (!cpufile) {
		ADS_LOG((LP_ERROR, "======================: Supervisor failed to open /proc/stat\n"));
		return;
	}
	
	fscanf(cpufile, "cpu %llu %llu %llu %llu", &cur_cpu->cpu_totalUser, &cur_cpu->cpu_totalUserLow,
			&cur_cpu->cpu_totalSys, &cur_cpu->cpu_totalIdle);
	fclose(cpufile);
	/* just in case overflow happened */
	if (cur_cpu->cpu_totalUser < pre_cpu->cpu_totalUser || cur_cpu->cpu_totalUserLow < pre_cpu->cpu_totalUserLow ||
			cur_cpu->cpu_totalSys  < pre_cpu->cpu_totalSys  || cur_cpu->cpu_totalIdle < pre_cpu->cpu_totalIdle)
		cpu_percent = -1.0;
	else {
		cpu_total    =  (cur_cpu->cpu_totalUser    - pre_cpu->cpu_totalUser)    + 
			(cur_cpu->cpu_totalUserLow - pre_cpu->cpu_totalUserLow) +
			(cur_cpu->cpu_totalSys     - pre_cpu->cpu_totalSys);
		cpu_percent  =  cpu_total * 100;
		cpu_total   +=  (cur_cpu->cpu_totalIdle - pre_cpu->cpu_totalIdle);
		cpu_percent /=  (cpu_total);
	}
	pre_cpu->cpu_totalUser    = cur_cpu->cpu_totalUser;
	pre_cpu->cpu_totalUserLow = cur_cpu->cpu_totalUserLow;
	pre_cpu->cpu_totalSys     = cur_cpu->cpu_totalSys;
	pre_cpu->cpu_totalIdle    = cur_cpu->cpu_totalIdle;
	res_integration->cpu_percent  = cpu_percent;
	
	return;
}
