#include "Forecast_Task.h"
#include "Forecast_Aggregator.h"

//==============================================================================
// abstract Manageable_Task
//==============================================================================

Forecast_Manageable_Task::Forecast_Manageable_Task(const Ads_String& job, time_t date) :
	job_(job), date_(date), manager_(nullptr), discarded_(false), created_at_(ads::gettimeofday()), id_(rand())
{
}

int Forecast_Manageable_Task::run()
{
	ads::Time_Value t_run = ads::gettimeofday();
	if (!discarded_)
	{
		manager_->before_run(this);
		run_i();
		manager_->notify_done(this);
	}
	else
		discard();
	ads::Time_Value t_finish = ads::gettimeofday();
	ADS_LOG((LP_INFO, "[FORECAST_TASK] type=%s, pid=%lu, id=%08x, job=%s, date=%s, created=%s.%06ld, wait=%.3f, run=%.3f, queue_size=%d, perf=%s%s\n"
				, manager_->type().c_str(), ads::thr_id(), id_, job_.c_str(), ads::string_date(date_).c_str()
				, ads::time_to_str("%H:%M:%S", created_at_.sec()).c_str(), created_at_.usec()
				, 0.001 * ads::Time_Interval_usec(created_at_, t_run), 0.001 * ads::Time_Interval_usec(t_run, t_finish)
				, manager_->num_pending_jobs()
				, performance_str_.c_str()
				, (discarded_ ? ", discarded" : "")
			));
	return 0;
}

//==============================================================================
// Task_Manager
//==============================================================================

Forecast_Task_Manager::Forecast_Task_Manager(const Ads_String& type, size_t max_days, bool allow_duplicate) :
	type_(type), max_days_(max_days), allow_duplicate_(allow_duplicate),
	job_service_(new Ads_Job_Queue_Service()), stat_wait_cond_(stat_mutex_), stat_full_cond_(stat_mutex_)
{
}

Forecast_Task_Manager::~Forecast_Task_Manager()
{
	if (job_service_) delete job_service_;
}

int Forecast_Task_Manager::open(int n_threads)
{
	job_service_->num_threads(n_threads);
	return job_service_->open();
}

int Forecast_Task_Manager::stop()
{
	return job_service_->stop();
}

int Forecast_Task_Manager::add_task(Forecast_Manageable_Task* task)
{
	if (task == nullptr)
		return -1;
	Ads_String& job = task->job_;
	time_t date = task->date_;

	// check duplicate
	ads::Guard __g(stat_mutex_);

	while (stat_map_[job].size() > max_days_) {
			
		ADS_LOG((LP_INFO, "[FORECAST_TASK] type=%s, id=%08x, job=%s, date=%s: waiting to add\n" , type_.c_str(), task->id_, job.c_str(), ads::string_date(date).c_str()));
		stat_full_cond_.wait();
	}

	if (!allow_duplicate_)
	{
		Stat_Job_Daily_Value::iterator nit = stat_map_.find(job);
		if (nit != stat_map_.end())
		{
			Stat_Daily_Value::iterator dit = nit->second.find(date);
			if (dit != nit->second.end())
			{
				if (dit->second.waiting_tasks_.size() > 0 || dit->second.running_ > 0)
				{
					ADS_LOG((LP_INFO, "[FORECAST_TASK] type=%s, id=%08x, job=%s, date=%s: skipped due to duplication\n" , type_.c_str(), task->id_, job.c_str(), ads::string_date(date).c_str()));
					return 0;
				}
			}
		}
	}

	{
		task->manager_ = this;
		size_t n_queue = num_pending_jobs();
		if (job_service_->enqueue_job(task) < 0)
		{
			ADS_LOG((LP_ERROR, "[FORECAST_TASK] type=%s, id=%08x, job=%s, date=%s: failed to enqueue into job_queue_service\n", type_.c_str(), task->id_, job.c_str(), ads::string_date(date).c_str()));
			return -1;
		}
		stat_map_[job][date].waiting_tasks_.insert(task);
		ADS_LOG((LP_INFO, "[FORECAST_TASK] type=%s, id=%08x, job=%s, date=%s: cnt=%d,%d,%d, queue_size=%d\n", type_.c_str(), task->id_, job.c_str(), ads::string_date(date).c_str(), stat_map_[job][date].waiting_tasks_.size(), stat_map_[job][date].running_, stat_map_[job][date].done_, n_queue));
	}
	return 0;
}

void Forecast_Task_Manager::get_status(const Ads_String& job, time_t date, int& n_waiting, int& n_running, int& n_done)
{
	n_waiting = n_running = n_done = 0;
	ads::Guard __g(stat_mutex_);
	Stat_Job_Daily_Value::iterator nit = stat_map_.find(job);
	if (nit != stat_map_.end())
	{
		Stat_Daily_Value::iterator dit = nit->second.find(date);
		if (dit != nit->second.end())
		{
			n_waiting = dit->second.waiting_tasks_.size();
			n_running = dit->second.running_;
			n_done = dit->second.done_;
		}
	}
}

void Forecast_Task_Manager::wait(const Ads_String& job, time_t date, size_t skip_number)
{
	ads::Guard __g(stat_mutex_);
	while (true)
	{
		Stat_Job_Daily_Value::iterator nit = stat_map_.find(job);
		if (nit == stat_map_.end())
			break;
		Stat_Daily_Value::iterator dit = nit->second.find(date);
		if (dit == nit->second.end() || (dit->second.waiting_tasks_.size() + dit->second.running_) <= skip_number)
			break;
		stat_wait_cond_.wait();
	}
}


void Forecast_Task_Manager::wait(const Ads_String& job)
{
	ads::Guard __g(stat_mutex_);
	while (true)
	{
		Stat_Job_Daily_Value::iterator nit = stat_map_.find(job);
		if (nit == stat_map_.end())
			break;
		stat_wait_cond_.wait();
	}
}

void Forecast_Task_Manager::remove_waiting_tasks(const Ads_String& job)
{
	ads::Guard __g(stat_mutex_);
	Stat_Job_Daily_Value::iterator jit = stat_map_.find(job);
	if (jit == stat_map_.end())
		return;

	size_t n_discarded = 0, n_running = 0;
	std::set<time_t> finished_date;
	for (Stat_Daily_Value::iterator it = jit->second.begin(); it != jit->second.end(); ++it)
	{
		Stat_Value& v = it->second;
		n_discarded += v.waiting_tasks_.size();
		n_running += v.running_;
		v.done_ += v.waiting_tasks_.size();
		for (std::set<Forecast_Manageable_Task*>::iterator t = v.waiting_tasks_.begin(); t != v.waiting_tasks_.end(); ++t)
			(*t)->discarded_ = true;
		v.waiting_tasks_.clear();
		if (v.waiting_tasks_.size() <= 0 && v.running_ <= 0)
			finished_date.insert(it->first);
	}
	for (std::set<time_t>::const_iterator it = finished_date.begin(); it != finished_date.end(); ++it)
		jit->second.erase(*it);
	if (jit->second.empty())
		stat_map_.erase(job);
	stat_wait_cond_.broadcast();
	stat_full_cond_.broadcast();
	ADS_LOG((LP_INFO, "[FORECAST_TASK] type=%s, job=%s: discard %d waiting tasks, still %d running tasks\n", type_.c_str(), job.c_str(), n_discarded, n_running));
}

void Forecast_Task_Manager::before_run(Forecast_Manageable_Task* task)
{
	if (task == nullptr)
		return;
	Ads_String& job = task->job_;
	time_t date = task->date_;

	ads::Guard __g(stat_mutex_);
	Stat_Value& v = stat_map_[job][date];
	v.waiting_tasks_.erase(task);
	v.running_++;
};

void Forecast_Task_Manager::notify_done(Forecast_Manageable_Task* task)
{
	if (task == nullptr)
		return;
	Ads_String& job = task->job_;
	time_t date = task->date_;

	ads::Guard __g(stat_mutex_);
	Stat_Value& v = stat_map_[job][date];
	v.running_--;
	v.done_++;
	if (v.waiting_tasks_.size() <= 0 && v.running_ <= 0)
	{
		stat_map_[job].erase(date);
		if (stat_map_[job].empty())
			stat_map_.erase(job);
	}
	stat_wait_cond_.broadcast();
	stat_full_cond_.broadcast();
};

//==============================================================================
// task for load requests from req.gz or model
//==============================================================================

// sort users by size() (#requests) desc
bool sort_cached_request_list(const Forecast_Batch_Ex& x, const Forecast_Batch_Ex& y)
{
	return x.batch_->size() > y.batch_->size();
}

int Forecast_Load_Requests_Task::run_i()
{
	// check if exists
	bool is_exist = false;
	{
		ads::Guard _g(af_job_ctx_->load_request_mutex_);
		is_exist = af_job_ctx_->load_request_cache_.find(date_) != af_job_ctx_->load_request_cache_.end();
	}
	if (is_exist)
	{
		ADS_LOG((LP_INFO, "[REQUESTS] job=%s, date=%s: skip job since is ready in cache\n", job_.c_str(), ads::string_date(date_).c_str()));
		return 0;
	}

	int ret = 0;
	if (!(af_job_ctx_->flags_ & Forecast_Service::AF_Job_Context::FAST_SIMULATE_OD))
		ret = load_from_plan();
	else if (af_job_ctx_->fast_conf_)
		ret = load_from_model();
	else
	{
		ADS_LOG((LP_ERROR, "[REQUESTS] job=%s, date=%s: unexpected error\n", job_.c_str(), ads::string_date(date_).c_str()));
		ret = -1;
	}

	return ret;
}

int Forecast_Load_Requests_Task::load_from_plan()
{
	Ads_GUID scenario_id = af_job_ctx_->current_scenario_id_;
#if defined(ADS_ENABLE_AB_OFFLINE)
	scenario_id = 0;
#endif
	Forecast_Service::AF_Job_Context::Scenario_Plan_Info_Map::const_iterator it = af_job_ctx_->plan_infos_.find(scenario_id);
	if (it == af_job_ctx_->plan_infos_.end()) return af_job_ctx_->closure_networks_.empty() ? 0 : -1;
	const Forecast_Service::Plan_Info& plan_info = it->second;
	if (plan_info.forecast_networks_.empty()) return -1;
	time_t plan_id = plan_info.plan_id_;
	std::map<Ads_GUID, Network_Cache*>::iterator ncit = af_job_ctx_->network_caches_.find(scenario_id);
	if (ncit == af_job_ctx_->network_caches_.end()) return -1;
	Network_Cache *network_cache = ncit->second;
	std::set<Ads_String>& vnodes = vnodes_;

	size_t n_user = 0, n_req = 0;
	Request_Queue* queue = new Request_Queue();
	Forecast_Service::Plan_Info::Forecast_Networks::const_iterator pit = plan_info.forecast_networks_.find(date_);
	if (pit == plan_info.forecast_networks_.end())
	{
		ADS_LOG((LP_ERROR, "[REQUESTS] job=%s, date=%s: empty plan\n", job_.c_str(), ads::string_date(date_).c_str()));
	}
	else
	{
		Forecast_Batch_Ex_List batch_list;
		for (Ads_GUID_Set::const_iterator nit = pit->second.begin(); nit != pit->second.end(); ++nit)
		{
			Ads_String dir = ads::path_join(Ads_Server::config()->forecast_plan_dir_, ads::i64_to_str(plan_id) + '_' + ads::i64_to_str(af_job_ctx_->timezone_), ads::i64_to_str(af_job_ctx_->current_scenario_id_));
			for(std::set<Ads_String>::iterator vnit = vnodes.begin(); vnit != vnodes.end(); ++vnit)
			{
				File_Sections fs;
				Ads_String key = ads::i64_to_str(*nit) + *vnit + "0" + ads::string_date(date_);
				if(0 == network_cache->get_index(key, fs))
					continue;
				Ads_String vnfile = ads::path_join(dir, ads::entity_id_to_str(*nit), *vnit + ".req.gz");
				Forecast_User_Batch user_batches;
				size_t n = Forecast_Store::read_requests_from_file(vnfile, user_batches, &fs);
				Forecast_Batch_Ex ex;
				for (Forecast_User_Batch::const_iterator it = user_batches.begin(); it != user_batches.end(); ++it)
				{
					ex.batch_ = it->second;
					batch_list.push_back(ex);
				}
				n_user += user_batches.size();
				n_req += size_t(n);
				ADS_LOG((LP_INFO, "[REQUESTS] job=%s, date=%s, vnodeid=%s: plan=%ld, scenario=%ld, network=%s, #user=%d, #req=%d\n", job_.c_str(), ads::string_date(date_).c_str(), (*vnit).c_str(), plan_id, af_job_ctx_->current_scenario_id_, ads::entity_id_to_str(*nit).c_str(), user_batches.size(), n));
			}
		}
		batch_list.sort(sort_cached_request_list);
		for (std::list<Forecast_Batch_Ex>::const_iterator it = batch_list.begin(); it != batch_list.end(); ++it)
			queue->enqueue(it->batch_);
	}

	{
		ads::Guard _g(af_job_ctx_->load_request_mutex_);
		af_job_ctx_->load_request_cache_[date_] = queue;
	}
	ADS_LOG((LP_INFO, "[REQUESTS] job=%s, date=%s: %s, plan=%ld, scenario=%ld, #user=%ld, #req=%ld\n", job_.c_str(), ads::string_date(date_).c_str(), "DONE", plan_id, scenario_id, n_user, n_req));
	return 0;
}

int Forecast_Load_Requests_Task::load_from_model()
{
	Fast_Simulate_Config* fast_conf = reinterpret_cast<Fast_Simulate_Config*>(af_job_ctx_->fast_conf_);
	size_t days = (af_job_ctx_->current_end_date_ - af_job_ctx_->current_start_date_) / 86400;
	if (days == 0) days = 1;
	size_t max_daily_request = (fast_conf->max_daily_request_ > 0) ? fast_conf->max_daily_request_ : Ads_Server::config()->forecast_max_od_request_num_ / days;
	Ads_String model_id = fast_conf->model_id_;
	Ads_GUID network_id = fast_conf->network_id_;
	int64_t version = af_job_ctx_->model_version_;
	size_t n_user = 0, n_req = 0;
	std::map<Ads_GUID, Network_Cache*>::iterator ncit = af_job_ctx_->network_caches_.find(0/*scenario_id*/);
	if (ncit == af_job_ctx_->network_caches_.end()) return -1;
	Network_Cache *network_cache = ncit->second;
	Request_Queue* queue = new Request_Queue();
	Forecast_Batch_Ex_List batch_list;
	for (Ads_GUID_Set::const_iterator nit = af_job_ctx_->closure_networks_.begin(); nit != af_job_ctx_->closure_networks_.end(); ++nit)
	{
		Forecast_Store* store = Forecast_Service::instance()->get_store(*nit);
		Ads_GUID resellable_network = store->network_id();
		size_t num_user = 0, num_req = 0;
		Forecast_User_Batch user_batches;
		store->read_model(date_, network_id/*interested*/, num_req, user_batches, max_daily_request, num_user, job_, model_id, &vnodes_, version, network_cache);
		Forecast_Batch_Ex ex;
		for (Forecast_User_Batch::const_iterator it = user_batches.begin(); it != user_batches.end(); ++it)
		{
			if (it->second && it->second->size() > 0 )
			{
				ex.batch_ = it->second;
				batch_list.push_back(ex);
			}
		}
		n_user += num_user;
		n_req += num_req;
		ADS_LOG((LP_INFO, "[REQUESTS] job=%s, date=%s: od_nw=%s, reseller=%s, #user=%ld, #req=%ld\n",
					job_.c_str(), ads::string_date(date_).c_str(), ADS_ENTITY_ID_CSTR(network_id),
					ADS_ENTITY_ID_CSTR(resellable_network), num_user, num_req));
	}
	batch_list.sort(sort_cached_request_list);
	for (std::list<Forecast_Batch_Ex>::const_iterator it = batch_list.begin(); it != batch_list.end(); ++it)
		queue->enqueue(it->batch_);

	{
		ads::Guard _g(af_job_ctx_->load_request_mutex_);
		af_job_ctx_->load_request_cache_[date_] = queue;
	}
	ADS_LOG((LP_INFO, "[REQUESTS] job=%s, date=%s Done, od_nw=%s, #user=%ld, #req=%ld\n",
				job_.c_str(), ads::string_date(date_).c_str(), ADS_ENTITY_ID_CSTR(network_id), n_user, n_req));
	return 0;
}

//==============================================================================
// task for simulate ad selection process from loaded reqeusts
//==============================================================================

int Forecast_Simulate_Task::run_i()
{
	Forecast_Simulator::instance()->simulate_i(sim_date_, context_, performance_str_);
	af_job_ctx_->merge_wimas_result(context_);
	delete context_;
	return 0;
}
//==============================================================================
// task for calculate metrics from given request binlogs
//==============================================================================

int Forecast_Calculate_Metric_Task::run_i()
{
	Forecast_Service::Perf_Counter* perf = new Forecast_Service::Perf_Counter();

	Meta_Requests_List meta_requests_list_;
	for (Forecast_Simulator::Forecast_Transaction_Vec::iterator it = transaction_vec_->begin(); it != transaction_vec_->end(); ++it)
	{
		ads::Time_Value t0 = ads::gettimeofday();
		Forecast_Transaction* transaction = *it;

		Forecast_Metrics delta_metrics;
		transaction->calculate_metrics(&delta_metrics, af_job_ctx_, perf);
		PERFORMANCE_CALC(calc);

		// divide the metric into its ad's timezone interval (nw timezone for portfolio metric), merge to different days
		/*
		   |<-                             GMT-8 interval1   ->|<-   GMT-8 interval2   ->|
		   |<-   GMT-3 interval1   ->|<-   GMT-3 interval2                             ->|
		   |                         |              *(req)     |                         |
		   |                    GMT+0 03:00  GMT+0 06:00  GMT+0 08:00
		   */
		Forecast_Metrics_Map daily_delta_metrics;
		Forecast_Metrics* tmp_metrics = nullptr;
		if (Ads_Server::config()->forecast_enable_timezone_)
		{
			Interval interval;
			af_job_ctx_->get_interval(int(transaction->meta_->virtual_time_), interval);
			{
				ads::Guard g__(metrics_[index_].second->mutex_);
				tmp_metrics = &((*(metrics_[index_].second))[interval]);
			}
			if (tmp_metrics) tmp_metrics->merge_delta(delta_metrics, true); // only transactional_map will be merged, actually we only need GA, TA
			PERFORMANCE_CALC(merge_hourly);
			// daily metrics
			transaction->reorganize_metrics(int(transaction->meta_->virtual_time_), delta_metrics, daily_delta_metrics);
			metrics_[index_].first->merge_delta(daily_delta_metrics);
		}
		else
		{
			{
				ads::Guard g__(metrics_[index_].first->mutex_);
				tmp_metrics = &((*(metrics_[index_].first))[date_].metrics_);
			}
			if (tmp_metrics) tmp_metrics->merge_delta(delta_metrics);
		}
		PERFORMANCE_CALC(merge_daily);

		// tracer_content
		if (af_job_ctx_->enable_tracer())
		{
			json::ObjectP tracer_content;
			Forecast_Simulator::generate_tracer_content(&delta_metrics, transaction, tracer_content, Ads_Server::config()->forecast_enable_timezone_);
			Forecast_Simulator::output_tracer_content(Ads_Server::config()->logger_working_dir_, af_job_ctx_->fast_conf_, tracer_content);
		}

		// model
		report::Request_Log_Record* model = NULL;
		transaction->generate_model(af_job_ctx_, model);
		Ads_GUID network_id = transaction->meta_->network_id_;
		if (model)
		{
			Forecast_Store* store = Forecast_Service::instance()->get_store(network_id);
			if (!store)
				ADS_LOG((LP_ERROR, "failed to get store for nw %ld write model\n", ads::entity::id(network_id)));
			add_model_to_buffer(store, transaction->meta_, model);
		}
		else if (!(af_job_ctx_->fast_conf_) && af_job_ctx_->current_scenario_id_ == 0)
		{
			ADS_DEBUG((LP_TRACE, "skip write model, meta=%s, network=%ld\n", transaction->meta_->to_csv().c_str(), ads::entity::id(network_id) ));
		}
		PERFORMANCE_CALC(pack_model);

		// request_logs
		if (!af_job_ctx_->fast_conf_ &&
			Ads_Server::config()->forecast_enable_output_simulate_binary_ && 
			transaction->update_request_cookie() == 0 && 
			transaction->update_request_ratios() == 0)
		{
			forecast::Forecast_Metrics* metrics_log = transaction->scenarios_[Forecast_Transaction::REAL]->mutable_forecast_metrics();
			if (delta_metrics.to_proto(metrics_log) < 0)
			{
				transaction->scenarios_[Forecast_Transaction::REAL]->clear_forecast_metrics();
				ADS_DEBUG((LP_TRACE, "fail to transfer metrics to PB meta %s", transaction->meta_->to_csv().c_str() ));
			}
			meta_requests_list_.push_back(std::make_pair(transaction->meta_, transaction->scenarios_));
			transaction->meta_ = NULL;
			transaction->scenarios_ = NULL;
		}
		transaction->destroy_pointers();
		delete transaction;
		PERFORMANCE_CALC(pack_request_logs);
	}
	delete transaction_vec_;
	ADS_ASSERT((metrics_[index_].first->size() == 3));

	// output model and meta_requests
	ads::Time_Value t0 = ads::gettimeofday();
	output_model();
	PERFORMANCE_CALC(output_models);

	// have to call it after all usage of transaction->scenarios_ , ad will be all collect to REAL in output_meta_requests
	Forecast_Simulator::output_meta_requests(af_job_ctx_, &meta_requests_list_/*od or scenatio nt: empty*/, date_);
	PERFORMANCE_CALC(output_meta_requests);

	// performance
	{
		performance_str_ = "";
		ads::Guard __g(af_job_ctx_->calculating_perf_.mu_);
		for (auto& it: perf_keys["calculate"])
		{
			af_job_ctx_->calculating_perf_[it] += (*perf)[it];
			performance_str_ += ads::i64_to_str((*perf)[it] / 1000) + ",";
		}
		delete perf;
	}

	return 0;
}

void Forecast_Calculate_Metric_Task::add_model_to_buffer(Forecast_Store *store, const Forecast_Request_Meta* meta, report::Request_Log_Record* model)
{
	Forecast_Request_Meta* tmp_meta = new Forecast_Request_Meta;
	memcpy(tmp_meta, meta, sizeof(Forecast_Request_Meta));
	//const long uid = tmp_meta->key_.uid_;
	//Ads_String vnodeid = vnodes_[uid % vnodes_.size()];
	Ads_String vnodeid = Ads_String("vnode") + ads::i64_to_str(tmp_meta->get_vnode_id());
	Forecast_Batch*& batch = model_buffer_[store][vnodeid][model->timestamp() / 86400 * 86400];
	if (batch == NULL) batch = new Forecast_Batch();
	batch->push_back(std::make_pair(tmp_meta, model));
}

void Forecast_Calculate_Metric_Task::output_model()
{
	for (const auto& it: model_buffer_)
	{
		Forecast_Store* store = it.first;
		if (!store) continue;
		for (const auto& vnit: it.second)
		{
			size_t cnt = 0;
			// network -> date -> cnt
			std::map<int64_t, std::map<time_t, int64_t> > network_cnt;
			Ads_String vnodeid = vnit.first;
			for(const auto& dit: vnit.second)
			{
				Forecast_User_Batch temp;
				temp[dit.first] = dit.second;
				cnt += dit.second->size();
				store->write_model(vnodeid, cur_version_, temp, dit.first, network_cnt);
			}
			store->write_network_cnt(vnodeid, cur_version_, network_cnt);
			Ads_String tmp;
			for (const auto& nit: network_cnt)
				for (const auto& dit: nit.second)
					tmp += ads::i64_to_str(ads::entity::id(nit.first)) + "-" + ads::string_date(dit.first) + ":" + ads::u64_to_str(dit.second) + ",";
			ADS_LOG((LP_INFO, "[MODEL] pid=%lu, file=%s/%ld/model_w/%s, cnt=%ld, network=[cnt:%d, %s]\n", ads::thr_id(), store->root_dir().c_str(), ads::entity::id(store->network_id()), vnodeid.c_str(), cnt, network_cnt.size(), tmp.c_str()));
		}
	}
}

void Forecast_Calculate_Metric_Task::discard()
{
	for (Forecast_Simulator::Forecast_Transaction_Vec::iterator it = transaction_vec_->begin(); it != transaction_vec_->end(); ++it)
	{
		Forecast_Transaction* transaction = *it;
		transaction->destroy_pointers();
		delete transaction;
	}
	delete transaction_vec_;
}

//==============================================================================
// task for output metrics to pending files
//==============================================================================

int Forecast_Output_Pending_Task::run_i()
{
	Forecast_Service::Perf_Counter* perf = new Forecast_Service::Perf_Counter();
	ads::Time_Value t0 = ads::gettimeofday();

	Forecast_Service::instance()->simulate_manager_->wait(job_, date_);
	delete request_queue_; 
	// wait calculation task
	Forecast_Service::instance()->calculate_metric_manager_->wait(job_, date_);
	PERFORMANCE_CALC(output_wait);

	// merge metrics
	for (size_t i = 1; i < n_metrics_; ++i)
	{
		metrics_[0].first->merge(*(metrics_[i].first));
		delete metrics_[i].first;
		if (Ads_Server::config()->forecast_enable_timezone_)
		{
			metrics_[0].second->merge(*(metrics_[i].second));
			delete metrics_[i].second;
		}
	}
	PERFORMANCE_CALC(output_merge);

	//* for global counter: don't output pending metrics, but need to insert output date
	//* for normal workers:
		//background:
			//* predict_end_date = min(max_forecast_days(enabled_networks), FORECAST_MAX_SIMULATE_DAY)
			//* current_end_date = min(predict_end_date, plan_end_date for this worker this scenario), so current_end_date <= predict_end_date
			//* maxPredictEndDate(scheduler side) = max(current_end_date over all workers), so maxPredictEndDate and predict_end_date are incomparable 
			//* for current_end_date <= maxPredictEndDate situation: still need to output empty file due to full storage of pending metrics 
		//if enable timezone: output all local data when metrics ready then insert output date, data on resumed_start_date will not be regenerated when resume a job since it will never reach merge_count 3		
		//if disable timezone: output pending files, data on resumed_start_date will be regenerated when resume a job

	// output metrics
	if (!(af_job_ctx_->flags_ & Forecast_Service::AF_Job_Context::FAST_SIMULATE_WIMAS) && !Ads_Server::config()->forecast_only_as_counter_)
	{
		if (Ads_Server::config()->forecast_enable_timezone_)
		{
			//hourly metrics	
			if (Forecast_Simulator::output_interval_metrics(af_job_ctx_, metrics_[0].second, date_) < 0)
			{
				af_job_ctx_->pending_lost_ = true;	
				ADS_LOG((LP_ERROR, "output_pending hourly metrics lost on date=%s \n", ads::string_date(date_).c_str()));
			}
			//local daily metrics 
			af_job_ctx_->daily_metrics_map_.merge_delta(*(metrics_[0].first));
			for (auto& it: af_job_ctx_->daily_metrics_map_.get_keys())
			{
				size_t merge_count = 3;
				if (it == af_job_ctx_->resume_start_date_ || it >= af_job_ctx_->current_end_date_ - 86400) //to guarantee data on maxPredictEndDate -86400 could be outputed, since maxPredictEndDate is unknown
				{
					merge_count = 2;
				}

				Forecast_Metrics* output_metrics = nullptr;
				if (af_job_ctx_->daily_metrics_map_.metrics_ready(it, output_metrics, merge_count) && output_metrics)
				{
					if (it == af_job_ctx_->predict_start_date_) //Add outdated metrics back ESC-5918
					{
						Forecast_Metrics& outdated_metrics = af_job_ctx_->daily_metrics_map_.get_metrics(it - 86400);
						output_metrics->merge(outdated_metrics);
						af_job_ctx_->daily_metrics_map_.release_metrics(it - 86400);
					}
					if (Forecast_Simulator::output_metrics(af_job_ctx_, output_metrics, it) < 0)
						af_job_ctx_->pending_lost_ = true;	
					//mutex
					{
						ads::Guard _g(af_job_ctx_->output_mutex_);
						af_job_ctx_->output_pending_dates_.insert(it);
					}
					af_job_ctx_->daily_metrics_map_.release_metrics(it);
				}
			}
		}
		else
		{
			Forecast_Metrics& output_metrics = metrics_[0].first->get_metrics(date_);
			if (Forecast_Simulator::output_metrics(af_job_ctx_, &output_metrics, date_) < 0)
				af_job_ctx_->pending_lost_ = true;	
		}
		sleep(2);
	}

	if (Ads_Server::config()->forecast_only_as_counter_ || !Ads_Server::config()->forecast_enable_timezone_)
	{
		ads::Guard _g(af_job_ctx_->output_mutex_);
		af_job_ctx_->output_pending_dates_.insert(date_);
	}
	PERFORMANCE_CALC(output_metrics);

	delete metrics_[0].first;
	delete metrics_[0].second;
	delete []metrics_;

	// performance
	{
		performance_str_ = "";
		ads::Guard __g(af_job_ctx_->output_perf_.mu_);
		for (auto& it: perf_keys["output"])
		{
			af_job_ctx_->output_perf_[it] += (*perf)[it];
			performance_str_ += ads::i64_to_str((*perf)[it] / 1000) + ","; 
		}
		delete perf;
	}
	return 0;
}

void Forecast_Output_Pending_Task::discard()
{
	Forecast_Service::instance()->simulate_manager_->wait(job_, date_);
	delete request_queue_; 
	Forecast_Service::instance()->calculate_metric_manager_->wait(job_, date_);
	for (size_t i = 0; i < n_metrics_; ++i)
	{
		if (metrics_[i].first) delete metrics_[i].first;
		if (metrics_[i].second) delete metrics_[i].second;
	}
	delete []metrics_;
}

//==============================================================================
// task for pre aggregate
//==============================================================================

int Forecast_Pre_Aggregate_Task::run_i()
{
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	Ads_String root_dir(ads::path_join(Ads_Server::config()->forecast_pre_aggregate_dir_, ads::i64_to_str(af_job_ctx_->plan_id_), ads::u64_to_str(af_job_ctx_->current_scenario_id_)));
	const Ads_Repository* repo = __g.asset_repository();
	ADS_LOG((LP_INFO, "Pre Aggregate: scenario %d job %s for %s, networks %s\n",  af_job_ctx_->current_scenario_id_, af_job_ctx_->name().c_str(), ads::string_date(date_).c_str(), ads::entities_to_str(networks_).c_str()));
	Forecast_Aggregator aggregator(repo, *af_job_ctx_);
	aggregator.pre_aggregate(af_job_ctx_, date_, networks_);
	return 0;
}

//==============================================================================
// task for aggregate
//==============================================================================

int Forecast_Aggregate_Task::run_i()
{
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	Forecast_Aggregator aggregator(repo, *af_job_ctx_, task_id());
	if (af_job_ctx_->flags_ & Forecast_Service::AF_Job_Context::FAST_SIMULATE_OD)
	{
		aggregator.aggregate_on_demand(af_job_ctx_);
	}
	else
		aggregator.aggregate_nightly(af_job_ctx_);
	return 0;
}
