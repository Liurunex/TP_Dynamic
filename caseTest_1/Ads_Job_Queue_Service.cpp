#include "Ads_Job_Queue_Service.h"

//==============================================================================
// zxliu modification: TP_Adaptive
//==============================================================================

/* modification start */
Ads_Job_Queue_Service::Ads_Job_Queue_Service()
	: Ads_Service_Base(), mutex_map_()
	, signal_worker_start_(0)
	{}

Ads_Job_Queue_Service::~Ads_Job_Queue_Service() { thread_ids_map_.clear(); }

/* mutex_added_function start */
size_t
Ads_Job_Queue_Service::count_idle_threads() {
	mutex_map_.acquire();

	size_t all_idle = 0;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map_.begin();
	while(it != thread_ids_map_.end()) {
		if (!it->second) all_idle ++;
		it ++;
	}

	mutex_map_.release();
	return all_idle;
}

int
Ads_Job_Queue_Service::thread_status_set(pthread_t pid, int set_sta) {
	mutex_map_.acquire();

	std::unordered_map<pthread_t, int>::iterator got = thread_ids_map_.find(pid);
	if (got != thread_ids_map_.end()) got->second = set_sta;
	else ADS_LOG((LP_ERROR, "thread_status_set() not found target: %d\n", pid));
	mutex_map_.release();
	return 0;
}

int
Ads_Job_Queue_Service::deleteNode(pthread_t target) {
	mutex_map_.acquire();

	std::unordered_map<pthread_t, int>::iterator got = thread_ids_map_.find(target);
	if (got != thread_ids_map_.end()) {
		thread_ids_map_.erase(got);
		this->n_threads_ --;
	}

	int res = thread_ids_map_.find(target) == thread_ids_map_.end() ? 1:0;
	ADS_LOG((LP_INFO, "erase the pid: %d , check success: %d\n", target, res));
	mutex_map_.release();
	return 0;
}

int
Ads_Job_Queue_Service::extend_threadpool(int extend_size) {
	mutex_map_.acquire();

	this->signal_worker_start_ = 0;
	size_t start_index        = n_threads_;
	n_threads_                += extend_size;

	pthread_attr_t _attr;
	pthread_attr_init(&_attr);
	pthread_attr_setstacksize(&_attr, 0x4000000); //64M

	for (size_t i = start_index; i < n_threads_; ++ i) {
		pthread_t pth_id;
		pthread_attr_t *attr = 0;
		attr = &_attr;

		int ret = ::pthread_create(&pth_id, attr, &Ads_Job_Queue_Service::svc_run, this);
		if (ret != 0) ADS_LOG((LP_ERROR, "failed to create thread %d\n", pth_id));
		thread_ids_map_[pth_id] = 0;
	}
	
	ADS_LOG((LP_INFO, "extend thread_pool size: %d\n", (int)n_threads_));

	this->signal_worker_start_ = 1; 

	mutex_map_.release();
	return 0;
}

/* mutex_added_function end */

int
Ads_Job_Queue_Service::curtail_threadpool(int curtail_size) {
	int curtail_counter = 0;
	while (1) {
		Ads_Job *msg = new Curtail_TP();
		if(this->msg_queue_.enqueue(msg) < 0) {
			ADS_LOG((LP_INFO, "cannot post curtail message\n"));
			msg->destroy();
		}
		else {
			curtail_counter ++;
			if (curtail_counter == curtail_size) return 0;
		}
	}
	return 0;
}

int 
Ads_Job_Queue_Service::curtail_action() {
	if (this->n_threads_ > TP_MIN_THRESHOLD) {
		if (!this->deleteNode(pthread_self()))
			return 0;
	}
	else ADS_LOG((LP_ERROR, "curtail action forbidden: thread_pool size is: %d\n", this->n_threads_));
	return 1;
}

/* override fucntions */
int
Ads_Job_Queue_Service::open() {
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

		int ret = ::pthread_create(&pth_id, attr, &Ads_Job_Queue_Service::svc_run, this);
		if (ret != 0) ADS_LOG((LP_ERROR, "failed to create thread %d\n", pth_id));
		thread_ids_map_[pth_id] = 0;
	}

	/* make worker start to work */
	this->signal_worker_start_ = 1;

	return 0;
}

int
Ads_Job_Queue_Service::wait() {
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map_.begin();

	while(it != thread_ids_map_.end()) {
		::pthread_join(it->first, 0);
		ADS_LOG((LP_INFO, "join: %d done\n", it->first));
		it ++;
	}

	return 0;
}

int
Ads_Job_Queue_Service::stop() {
	this->exitting_ = 1;

	/*? potential risk of enqueue failed cuase thread suspend ?*/
	for (int i = 0; i < (int)n_threads_; ++ i)
		this->msg_queue_.enqueue(new No_Op());

	this->wait();
	/*? the 'count' seems not really to count anything ?*/
	size_t count = 0;
	while (! this->msg_queue_.is_empty()) {
		Ads_Job *job = 0;
		if (this->msg_queue_.dequeue(job) < 0)
			break;

		if (! job) continue;

		bool abandoned = job->abandoned();
		if (job->waitable())
			reinterpret_cast<Ads_Waitable_Job *>(job)->signal();

		if (abandoned) job->destroy();
	}

	ADS_LOG((LP_INFO, "%d jobs destroyed\n", count));
	
	thread_ids_map_.clear();
	
	return 0;
}

int
Ads_Job_Queue_Service::svc() {
	while (!this->signal_worker_start_)
		;
	
	ADS_LOG((LP_INFO, "(jobq: %llu) service started.\n", ads::thr_id()));

	while (! this->exitting_) {
		Ads_Job *job = 0;	
		if (this->msg_queue_.dequeue(job) < 0 || ! job) break;

		if (job->type() == TYPE_CURTAIL) {
			if (this->curtail_action()) {
				ADS_LOG(LP_ERROR, "curtail action failed\n");
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

		if (job->type() == TYPE_JOB)
			if(this->thread_status_set(pthread_self(), 1))
				ADS_LOG((LP_ERROR, "set thread status to active failed\n"));
	
		if (job->run(job) < 0)
			ADS_LOG((LP_ERROR, "failed to run job\n"));

		if (job->type() == TYPE_JOB)
			if(this->thread_status_set(pthread_self(), 0))
				ADS_LOG((LP_ERROR, "set thread status to sleep failed\n"));

		bool abandoned = job->abandoned();
		if (job->waitable())
			reinterpret_cast<Ads_Waitable_Job *>(job)->signal();

		if (abandoned) job->destroy();
	}

	ADS_LOG((LP_INFO, "(jobq: %lu) service stopped.\n", ads::thr_id()));

	return 0;
}

