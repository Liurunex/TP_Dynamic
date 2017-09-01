	
// -*- C++ -*-
//=============================================================================
/**
 *	  Copyright (c) Freewheel, 2007-2011. All rights reserved.
 *
 *	  @file
 *
 *	  @author 
 *
 *	  @brief
 *
 *	  Revision History:
 * 
 */
//=============================================================================

#if !defined(FORECAST_SERVICE_H)
#define FORECAST_SERVICE_H

#include "server/Ads_Server.h"
#include "Forecast_Store.h"
#include "Forecast_Performance.h"
//#include "True_Table_Conflict_Detector.h"
#include "Forecast_Metrics.h"
#include "server/Ads_Service_Base.h"
#include "server/Ads_Log_Processor.h"
#include "server/Ads_Job_Queue_Service.h"
#include "server/Ads_Log_Store.h"
#include "cajun/json/elements.h"
#include "server/Ads_Redis_Client.h"
#include "server/Ads_File.h"

/*zxliu added*/
#include "Forecast_Adaptive_TP_Service.h"

#include <zlib.h>
class Ads_Request;
class Ads_Response;
struct Ads_Network;
class Ads_Shared_Data_Service;
struct Fast_Simulate_Config;
namespace json { class Object; };

class Forecast_Task_Manager;
class Forecast_Store;
class Network_Cache;
class Forecast_Pusher;
class Forecast_Planner;
class Forecast_Simulator;
class Forecast_Aggregator;
/* zxliu */
class Forecast_Manager_Supervisor;

namespace{
static std::map<Ads_String, Ads_String_List> perf_keys = {
		{"plan", {"estimate_sample","predict_inventory","load_adjustment","create_output_thread","build_network_plan","output_plan"} }, //plan main steps
		{"build_network_plan", {"prepare_history","parse_meta","load_old_sample","build_plan_helper","release_memory"} }, // build_network_plan breakdown by step by network
		{"build_plan_helper", {"calc_histogram","init_user_freq","new_sample","handle_adjustments","handle_hylda_sample"} }, // build_plan_helper breakdown by step by network, forecast_num_planner_threads_ 
		{"output_plan_helper", {"output_plan_wait","load_csv","load_store","output_plan"} }, //output_plan breakdown by step by network, forecast_num_planner_output_threads_
		{"simulate_daily", {"daily_prepare", "daily_job_create", "daily_merge_request", "daily_simulate_complete", "daily_update_counter", "daily_wait", "daily_simulate_total", "daily_total"} },
		{"simulate_thread", {"request_wait", "thread_wait", "thread_min","thread_max", "thread_ave", "thread_run"} }, //daily_simulate_complete stats	
		{"simulate", {"prepare", "uga", "ga", "na", "real", "post_operation", "update_counter"} }, //daily_simulate_complete breakdown by step
		{"calculate", {"calc", "merge_hourly", "merge_daily", "pack_model", "pack_request_logs", "output_models", "output_meta_requests", "calculate_real", "calculate_custom_portfolio", "calculate_transactional_uga", "calculate_transactional_ga", "calculate_transactional_ta", "calculate_transactional_competing", "calculate_tna", "calculate_transactionl_demo", "calculate_portfolio_demo", "handle_ad_unit"} },
		{"output", {"output_wait", "output_merge", "output_metrics"} },
		{"pre_aggregate", {"load_portfolio_max", "load_portfolio_ave", "load_transactional_max", "load_transactional_ave", "calc_protfolio", "calc_transactional", "export_portfolio_custom_interval"} },
		{"market_ad", {"n_req", "n_req_extra", "n_req_market", "n_ext_ads", "n_ext_ads_openrtb", "n_bids", "n_bids_no_error", "market_req_ratio", "avg_ext_ads", "avg_bids", "avg_bids_no_error", "avg_bids_returned"} },
		};
}

#define PERFORMANCE_CALC(key) if (perf) { ads::Time_Value t1 = ads::gettimeofday(); (*perf)[#key] += ads::Time_Interval_usec(t0, t1); t0 = t1; }

#define PERFORMANCE_CALC_UPDATE_METRICS(key) if (perf) { ads::Time_Value t3 = ads::gettimeofday(); (*perf)[#key] += ads::Time_Interval_usec(t2, t3); t2 = t3; }

#define PERFORMANCE_OUTPUT(key) if (perf) { ads::Time_Value t5 = ads::gettimeofday(); (*perf)[#key] += ads::Time_Interval_usec(t4, t5); t4 = t5; }

#define PERFORMANCE_PLAN(key) if (perf) { ads::Time_Value t7 = ads::gettimeofday(); (*perf)[#key] += ads::Time_Interval_usec(t6, t7); t6 = t7; }

#define PERFORMANCE_PLAN_MUTEX(key) if (perf) {ads::Time_Value t7 = ads::gettimeofday(); ads::Guard _g(perf->mu_); (*perf)[#key] += ads::Time_Interval_usec(t6, t7); t6 = t7; }

class Waitable_Group
{
	public:
		Waitable_Group(int initial_count = 0): count_(initial_count), mutex_(), empty_cond_(mutex_) {}
		void inc(int count = 1)
		{
			ads::Guard __g(mutex_);
			count_ += count;
		}

		void dec()
		{
			ads::Guard __g(mutex_);
			-- count_;
			empty_cond_.signal();
		}

		void wait()
		{
			ads::Guard __g(mutex_);
			while (count_ > 0) empty_cond_.wait();
		}

		bool completed()
		{
			ads::Guard __g(mutex_);
			return count_ <= 0;
		}

	private: 
		int count_;
		ads::Mutex mutex_;
		ads::Condition_Var empty_cond_;
};

class Waitable_Group_Guard
{
	public:
		Waitable_Group_Guard(Waitable_Group& wg): waitable_group_(wg) { wg.inc(); }
		~Waitable_Group_Guard() { waitable_group_.dec(); }

	private:
		Waitable_Group& waitable_group_;

		Waitable_Group_Guard(const Waitable_Group_Guard&);
		const Waitable_Group_Guard& operator = (const Waitable_Group_Guard&);

};

// please be careful to use, it should be put into a queue after wait()
class Waitable_Jobs_Manager: public Ads_Job
{
	public:
		Waitable_Jobs_Manager(int skipped_job_count): wg_(-skipped_job_count) {}

		~Waitable_Jobs_Manager()
		{
		}

		void add_job(Ads_Job_Queue_Service_TP_Adaptive* queue, Ads_Waitable_Job* job)
		{
			ads::Guard __g(job_mutex_);
			Manager_Job* new_job = new Manager_Job(job, this);
			queue->enqueue_job(new_job);
			wg_.inc();
			jobs_.push_back(new_job);
		}

		void wait()
		{
			wg_.wait();
		}

		virtual int run()
		{
			// ads::Guard __g(job_mutex_);
			for (auto& job: jobs_) 
			{
				job->wait();
			}
			when_complet();
			for (auto& job: jobs_) delete job;
			return 0;
		}

		virtual void when_complet() {}
		std::list<Ads_Waitable_Job*>& jobs() { return jobs_; }

	private:
		ads::Mutex job_mutex_;
		std::list<Ads_Waitable_Job*> jobs_;

		Waitable_Group wg_;
		void dec() { this->wg_.dec(); }

	class Manager_Job: public Ads_Waitable_Job
	{
		public:
			Manager_Job(Ads_Waitable_Job* job, Waitable_Jobs_Manager* manager): job_(job), manager_(manager) {}
			~Manager_Job()
			{
				delete job_;
			}

			virtual int run()
			{
				int ret = job_->run();
				manager_->dec();
				return ret;
			}

			Ads_Waitable_Job* job() { return job_; }

		private:
			Ads_Waitable_Job* job_;
			Waitable_Jobs_Manager* manager_;
	};
};

class Cookies{
public:
	Ads_String_Map values_;
	void to_string(Ads_String& s) const{
		s.clear();
		for(Ads_String_Map::const_iterator it = values_.begin(); it != values_.end(); ++it){
			s.append(it->first);
			s.append("=");
			s.append(it->second);
			s.append(";");
		}
	}
	void update(const Ads_String_Map& set_cookies){
		for(Ads_String_Map::const_iterator it = set_cookies.begin(); it != set_cookies.end(); ++it){
			values_[it->first] = it->second;
		}
	}
};

typedef std::pair<time_t, time_t> Date_Pair;
class Forecast_Service: public Ads_Service_Base
{
public:
	struct Forecast_Dispatcher
	{
		size_t n_used_, n_succeeded_;
		bool should_release_;

		Forecast_Dispatcher()
			: n_used_(0), n_succeeded_(0), should_release_(false)
		{}

		virtual ~Forecast_Dispatcher()
		{}

		virtual int handle_request(Ads_Request *req, Ads_Response *res, json::ObjectP &result) = 0;
	};
	struct Perf_Counter: public std::map<Ads_String, suseconds_t> 
	{
		ads::Mutex mu_;
	};
private:
	ads::Mutex	request_mutex_;
	std::map<Ads_String, Forecast_Dispatcher *> dispatchers_;
	int register_forecast_dispatchers();

	int collector_guard_counter_; // 0: default, -1: collecting, >=1: nightly job or stop collect
	ads::Mutex collecting_mutex_;
	ads::Condition_Var collector_cond_;
	ads::Condition_Var nightly_cond_;
	
	std::map<long,Cookies> cookies_map_;
	ads::Mutex cookies_mutex_;

	struct Collect_Guard
	{
		Forecast_Service* service_;
		Collect_Guard(Forecast_Service* service, bool stop_collect = false): service_(service)
		{
			service_->collecting_mutex_.acquire();
			if (stop_collect) // nightly or stop_collect
			{
				Ads_Log_Matcher::instance()->stop_process();
				while (service_->collector_guard_counter_ != 0)
				{
					service_->nightly_cond_.wait();
				}
				service_->collector_guard_counter_ = 1;
			}
			else	//collecting
			{
				while (service_->collector_guard_counter_ != 0)
				{
					service_->collector_cond_.wait();
				}
				service_->collector_guard_counter_ = -1;
			}
			service_->collecting_mutex_.release();
		}
		~Collect_Guard()
		{
			service_->collecting_mutex_.acquire();
			service_->collector_guard_counter_ = 0;
			service_->collector_cond_.signal();
			service_->nightly_cond_.signal();
			Ads_Log_Matcher::instance()->enable_process();
			service_->collecting_mutex_.release();
		}
	};

public:
	struct Plan_Info
	{
		//timestamp -> networks
		typedef std::map<time_t, Ads_GUID_Set> Forecast_Networks;
		//network_id -> (plan_start_date, plan_end_date)
		typedef std::map<Ads_GUID, std::pair<time_t, time_t> > Network_Plan_Infos;

		time_t plan_id_;
		Ads_GUID scenario_id_;
		Forecast_Networks forecast_networks_;
		Network_Plan_Infos network_plan_infos_;
		double sample_ratio_;
		size_t capacity_;
		size_t total_sampled_request_;
		Plan_Info() : plan_id_(0), scenario_id_(0), sample_ratio_(0.0), capacity_(0), total_sampled_request_(0) { }
	};

	struct Simulate_Stats
	{
		ads::Mutex assets_m_;
		// k: <asset_id, section_id>
		// v: the probability to skip the request, calculated by historical slowed times
		std::map<Ads_GUID_Pair, double> slow_pairs_;
		int slow_threshold_; // ms

		Simulate_Stats(): slow_threshold_(0) {}

		void clean_slow_pairs()
		{
			ads::Guard _g(assets_m_);
			slow_pairs_.clear();
		}

		bool check_asset(Ads_GUID asset_id, Ads_GUID section_id, double& p)
		{
			if (slow_threshold_ <= 0) return true; // disabled
			ads::Guard _g(assets_m_);
			auto pair_it = slow_pairs_.find(std::make_pair(asset_id, section_id));
			if (pair_it == slow_pairs_.end()) return true;
			p = pair_it->second;
			double r = 1.0 * rand() / RAND_MAX;
			return r > p;
		}

		void handle_asset(Ads_GUID asset_id, Ads_GUID section_id, size_t duration)
		{
			ads::Guard _g(assets_m_);
			auto it = slow_pairs_.find(std::make_pair(asset_id, section_id));
			if (duration <= (size_t)slow_threshold_) // decrease skip probability
			{
				if (it != slow_pairs_.end())
				{
					it->second /= 2;
					if (it->second < 0.0001) // 0.1%
						slow_pairs_.erase(it);
				}
			}
			else // increse skip probability
			{
				double old = 0.0;
				if (it != slow_pairs_.end())
				{
					old = it->second;
					it->second = (it->second + 1) / 2;
				}
				else
					slow_pairs_[std::make_pair(asset_id, section_id)] = 0.5; // initiali p=0.5
				ADS_LOG((LP_INFO, "slow req <%ld, %ld> found, t=%d, increse p from %.6f to %.6f\n", ads::entity::id(asset_id), ads::entity::id(section_id), duration, old, slow_pairs_[std::make_pair(asset_id, section_id)]));
			}
		}
	};

	Simulate_Stats stats_;

	struct AF_Job_Context;
	Forecast_Task_Manager* load_request_manager_;
	Forecast_Task_Manager* simulate_manager_;
	Forecast_Task_Manager* calculate_metric_manager_;
	Ads_Job_Queue_Service* conflict_detection_job_service_;
	Forecast_Task_Manager* output_pending_manager_;
	Forecast_Task_Manager* pre_aggregate_manager_;
	Forecast_Task_Manager* aggregate_manager_;
	/* zxliu added */
	Forecast_Manager_Supervisor* managers_supervisor_;

	class AF_Daily_Simulate_Jobs: public Waitable_Jobs_Manager
	{
		public:
			AF_Daily_Simulate_Jobs(AF_Job_Context*context, int skipped_job_count): Waitable_Jobs_Manager(skipped_job_count), context_(context) {}
	
			virtual void when_complet(); 
		private:
			AF_Job_Context* context_;
	};

	class Forecast_Conflict_Detection_Job: public Ads_Waitable_Job
	{
	public:
		Ads_String timestamp_;
		void* infos_;
		Forecast_Conflict_Detection_Job(Ads_String timestamp, void* infos): Ads_Waitable_Job(), timestamp_(timestamp), infos_(infos){}
		virtual int run();
		virtual ~Forecast_Conflict_Detection_Job() {}
	};

	typedef std::map<time_t, Request_Queue*> Cached_Requests; // <date, queue>
	struct AF_Job_Context
	{
		enum FLAG {
			NIGHTLY = 1,
			NIGHTLY_TIMEZONE_SPECIFIC = 1 << 1,
			NIGHTLY_RESTARTED = 1 << 2,
			FAST_SIMULATE_OD = 1 << 3,
			FAST_SIMULATE_WIMAS = 1 << 4,
			FAST_SIMULATE_OD_FULL = 1 << 5,

			FLAG_FAST_SIMULATE_OD_FULL = 1 << 15,
			FLAG_COLLECT_REJECT_REASON = 1 << 16,//same as Forecast_Simulator::Context
			FLAG_ENABLE_TRACER = 1 << 17,
			FLAG_DISABLE_UPDATE_COUNTER = 1 << 18,
			FLAG_DRY_RUN = 1 << 19, //only do aggregate
			FLAG_COLLECT_STATS = 1 << 20,
			FLAG_SINGLE_REQUEST = 1 << 21,
			FLAG_PRE_AGGREGATE = 1 << 22,
			FLAG_DELETABLE = 1<< 23,
		};
		ads::Mutex flags_mutex_;
		size_t flags_;
		time_t plan_id_;
		Ads_String job_tag_;
		Ads_GUID current_scenario_id_;
		Ads_GUID_Set scenario_ids_;
		time_t predict_start_date_;
		time_t predict_end_date_;
		time_t resume_start_date_;
		size_t max_forecast_days_;
		Ads_Shared_Data_Service* counter_service_;
		//Fast_Simulate_Config* fast_conf_;
		void* fast_conf_;
		int64_t timezone_;

		//vnode set
		std::set<Ads_String> vnode_set_;
		int64_t vnode_count_;
		int64_t model_version_;
		//scenario -> network cache
		std::map<Ads_GUID, Network_Cache*> network_caches_;

		Ads_GUID_Set enabled_networks_;
		Ads_GUID_Set excluded_networks_;
		Ads_GUID_Set closure_networks_;
		Ads_GUID_Set excluded_placements_;

		Ads_GUID_Set original_enabled_networks_;

		size_t n_daily_simulate_threads_;
		size_t n_metrics_;
		void* global_ctx_;
		ads::Mutex global_ctx_mutex_; //merge wimas result
		json::ObjectP json_result_;
		ads::Mutex output_mutex_;

		ads::Mutex load_request_mutex_;
		Cached_Requests load_request_cache_;

		typedef std::map<Ads_GUID, Plan_Info> Scenario_Plan_Info_Map;
		Scenario_Plan_Info_Map plan_infos_;
		std::set<int> gmt_offsets_;
		Forecast_Metrics_Map daily_metrics_map_; //for output_pending

		time_t current_start_date_;
		time_t current_end_date_;
		Ads_GUID current_network_id_;

		Simulate_Stats* stats_;

		Scenario_Feedback_Map feedback_;
		Scenario_External_Feedback_Map external_feedback_;

		Collect_Guard* collect_guard_;
		Ads_Server::Repository_Guard *repo_guard_;

		std::map<long, Cookies> user_cookies_; // <uid, cookie>
		ads::Mutex user_cookies_mutex_;
		std::map<long, time_t> cookies_clear_time_; // YPP-191
		
		bool fast_nightly_;
		struct Timestamp_Map: public std::map<Ads_String, ads::Time_Value>
		{
			ads::Mutex mu_;
		
		}perf_;
		ads::Mutex perf_mutex_;
		Perf_Counter simulating_perf_;
		Perf_Counter calculating_perf_;
		Perf_Counter output_perf_;
		Perf_Counter pre_aggregate_perf_;

		//OPP-2175
		time_t start_pre_aggregate_date_;
		std::set<time_t> output_pending_dates_;
		bool exit_pre_aggregate_;
		bool resume_error_;
		bool enqueue_aggregate_finish_;
		ads::Mutex pre_aggregate_dates_mu_;
		std::map<Ads_GUID, std::set<time_t> > pre_aggregate_dates_;
		ads::Mutex aggregate_dates_mu_;
		std::map<Ads_GUID, ads::Message_Queue<Date_Pair>* > aggregate_date_ranges_; //scenario_id, dates
		//OPP-5409, write/read redis/NFS error, ENG need to resume nightly or rerun od
		bool pending_lost_;
		Ads_String_Set active_workers_;
		std::map<Ads_String, bool> simulate_output_filename_;  //simulate_file_type -> is transactional_**.csv
		time_t nightly_time_loaded_;
		Waitable_Group wg_;
		AF_Job_Context() : flags_(0), plan_id_(0), current_scenario_id_(0), predict_start_date_(0), predict_end_date_(0), resume_start_date_(0), max_forecast_days_(size_t(-1)),
						counter_service_(0), fast_conf_(0), timezone_(-1), vnode_count_(1024), model_version_(0),
						n_daily_simulate_threads_(0), n_metrics_(0),
						global_ctx_(0),
						current_start_date_(0), current_end_date_(0), current_network_id_(0),
						collect_guard_(0),
						repo_guard_(0)
						, fast_nightly_(false)
						, start_pre_aggregate_date_(0), exit_pre_aggregate_(false)
						, resume_error_(false), enqueue_aggregate_finish_(false)
						, pending_lost_(false)
						, nightly_time_loaded_(0)
						, wg_(1)
						{}


		~AF_Job_Context();

		void to_json(json::ObjectP& o) const;
		Ads_String to_string() const;

		Ads_String name() const
		{
			if (plan_id_ > 0) return ads::i64_to_str(plan_id_);
			return job_tag_;
		}

		/*
		 * update model version
		 * 	1. write model while run nightly
		 * 	2. read model while run ondemand
		 */
		int64_t update_model_version();
		
		//delete items which key >= date in index file
		int reset_index_file(Forecast_Store* store);
		typedef std::map<Ads_GUID, Ads_String> Network_Pending_Path_Map;
		const Ads_String& get_pending_prefix(Network_Pending_Path_Map* pending_paths, Ads_GUID network_id);
		bool filtered(const Ads_Advertisement* ad, time_t fc_date, bool is_competing_or_portfolio = true);
		bool filtered(const Forecast_Metrics::P_Key& p_key, time_t fc_date);
		bool filtered(const Forecast_Metrics::Custom_P_Key& p_key, time_t fc_date);
		int prepare_running(Ads_Request* req);
		int destroy_running();
		void log_performance()
		{
			Ads_String perf = "calculating_perf:";
 			{
				ads::Guard __g(calculating_perf_.mu_);
				for (auto& it: perf_keys["calculate"])
					perf += ads::i64_to_str(calculating_perf_[it] / Ads_Server::config()->forecast_num_calculation_threads_ / 1000) + ",";
			}

			perf += "output_perf:";
 			{
				ads::Guard __g(output_perf_.mu_);
 				for (auto& it: perf_keys["output"])
 					perf += ads::i64_to_str(output_perf_[it] / 1000) + ",";
 			}
 
			perf += "pre_aggregate_perf:";
 			if (flags_ & AF_Job_Context::FLAG_PRE_AGGREGATE)
 			{
				ads::Guard __g(pre_aggregate_perf_.mu_);
 				for (auto &it : perf_keys["pre_aggregate"])
 					perf += ads::i64_to_str(pre_aggregate_perf_[it] / 1000) + ",";
 			}
			ADS_LOG((LP_INFO, "[PERFORMANCE] job=%s, scenario=%d, whole_perf=%s\n", this->name().c_str(), current_scenario_id_, perf.c_str()));
		}
		bool enable_tracer() const
		{
#if defined (ADS_ENABLE_DEBUG)
			return true;
#else
			return flags_ & FLAG_ENABLE_TRACER;
#endif
		}

		bool update_network_cache(std::vector<Ads_String>& vnode_vec);

		int merge_wimas_result(void* simulate_ctx);
//		time_t get_aggregate_end_date();

		bool output_simulate_metrics_files(time_t fc_date, bool is_reload = false); //copy to redis/NFS
		void stop_aggregate();
		void stop_output_pending();
		void get_interval(int virtual_time, Interval& interval);
	};
	std::map<Ads_String, AF_Job_Context*> nightly_simulate_jobs_;//plan_id|fc_job -> ctx
	ads::Mutex nightly_simulate_jobs_mutex_;
	std::map<Ads_String, AF_Job_Context*> od_simulate_jobs_;//plan_id|fc_job -> ctx
	ads::Mutex od_simulate_jobs_mutex_;

	int destroy_job_contexts();

	// nightly/OD procedure
	void prepare_nightly(Ads_Request* req, json::ObjectP& result);//load repa, counter, exchange_feedback, 
	void prepare_ondemand(Ads_Request* req, json::ObjectP& result); //create context, counter
	int plan(AF_Job_Context* job_ctx, size_t event_count, Ads_String& sample_dimensions, bool only_fake_requests, Ads_String& output_networks, int64_t vnode_count, json::ObjectP& result);
	int start_simulate(AF_Job_Context* job_ctx, Ads_Request* req, json::ObjectP& result);
	//OPP-2175
	void get_last_output_pending_date(Ads_Request* req, json::ObjectP& result);
	time_t get_last_output_pending_date(AF_Job_Context* job_ctx);
	void reload_pre_aggregate_status(AF_Job_Context* job_ctx, Ads_GUID scenario_id,  std::map<Ads_GUID, std::set<time_t> >& pre_aggregate_dates);
	void reload_aggregate_status(AF_Job_Context* job_ctx, std::map<Ads_GUID, std::set<time_t> >& finished_dates);
	int add_pre_aggregate_tasks(AF_Job_Context* job_ctx, time_t end_date);
	void stop_output_scenes(AF_Job_Context* job_ctx);
	 
	void end_simulate(Ads_Request* req, json::ObjectP& result);
	void is_busy_simulating(Ads_Request* req, json::ObjectP& result);
	void simulate_daily(Ads_Request* req, json::ObjectP& result);
	void aggregate(Ads_Request* req, json::ObjectP& result);
	int destroy_aggregate(AF_Job_Context* job_ctx);
	static void* scenario_plan_helper(void* ctx); //INK-2668
public:
	AF_Job_Context* initialize_job_context(Ads_Request* req, json::ObjectP& result);
	AF_Job_Context* get_job_context(Ads_Request* req, json::ObjectP& result);
	AF_Job_Context* get_job_context(Ads_String job_name);
	static int generate_response(int ret, const json::UnknownElement* data, json::ObjectP& result);
	int remove_job_context(AF_Job_Context* job_ctx);
	void list_job_contexts(json::ObjectP& result);
private:
	// helper method for nightly/OD procedure
	void exchange_feedback(Ads_Request* req, json::ObjectP& result);

private:
	ads::Mutex store_mutex_;
	std::map<Ads_GUID, Forecast_Store *> stores_; //<network_id, store> root = workdir/store/

	json::Object info_;
	ads::Mutex info_mutex_;
	std::map<Ads_GUID, int> forecast_days_;
	std::unordered_map<time_t, Ads_String> plan_id_to_repa_path_; //INK-12576: plan_id -> repa_path

public:
	int create_load_request_job(AF_Job_Context* job_ctx, time_t date);
	int create_simulate_tasks(AF_Job_Context* job_ctx, time_t fc_date, Request_Queue* request_queue, std::pair<Forecast_Metrics_Map*, Interval_Forecast_Metrics*>*& metrics);
	int merge_from_cached_requests(AF_Job_Context* job_ctx, time_t fc_date, Request_Queue*& request_queue);
public:
	ads::Mutex placements_updated_mutex_;
	Ads_GUID_Set placements_updated_;
	ads::Mutex placements_deleted_mutex_;
	Ads_GUID_Set placements_deleted_;

	ads::Mutex rules_updated_mutex_;
	Ads_GUID_Set rules_updated_;
	ads::Mutex rules_deleted_mutex_;
	Ads_GUID_Set rules_deleted_;

	typedef std::map<Ads_GUID, const Ads_Network*> Network_Map;
	ads::Mutex network_map_mutex_;
	Network_Map enabled_networks_;
	Network_Map closure_networks_;

	void stop_and_wait_collect();

	static int forecast_days(const Ads_Network*);
	static int max_forecast_days_on_r_chains(const Ads_Network*);
	static size_t max_forecast_days(const AF_Job_Context* job_ctx, const Ads_String& enabled_networks);
	int forecast_days(Ads_GUID network_id);

	Scenario_Feedback_Map feedback_;
	ads::Mutex feedback_mutex_;
	ads::Mutex dispatch_mutex_;

	Scenario_External_Feedback_Map external_feedback_;
	ads::Mutex external_feedback_mutex_;

	int reload_feedback(Scenario_Feedback_Map& feedback, time_t start = ::time(NULL) / 86400 * 86400 - 8 * 86400, time_t end = ::time(NULL) / 86400 * 86400 - 1 * 86400, bool network_only = false);
	int reload_external_feedback(Scenario_External_Feedback_Map& scenario_external_feedback, time_t start = ::time(NULL) / 86400 * 86400 - 8 * 86400, time_t end = ::time(NULL) / 86400 * 86400 - 1 * 86400);

	int ingest_feedback(Ads_String& path, json::ObjectP& result);

	void dump_feedback_by_day(Ads_GUID network_id, Ads_GUID scenario_id, Ads_String& type, Ads_GUID id, time_t start, time_t end, bool is_local, json::ObjectP& result);
	void dump_external_feedback_by_day(const Ads_GUID network_id, const Ads_GUID scenario_id, const Ads_GUID ad_id, const time_t start, const time_t end, const bool is_local, json::ObjectP& result);

	void get_inventory(const Ads_String& command, Ads_Request* req, json::ObjectP& result);
	void run_request(Ads_Request* req, json::ObjectP& result); 
	void run_request_in_file(Ads_Request* req, json::ObjectP& result); 
	void check_network_closure(Ads_Request* req, json::ObjectP& result);
	void collect(Ads_Request* req, json::ObjectP& result);
	void simulator__execute_request(Ads_Request* req, json::ObjectP& result);
	void plan_info(Ads_Request* req, json::ObjectP& result);
	void list_fast_simulate_networks(json::ObjectP& result);
	void connect_parse(const Ads_String& connects, Ads_String_Pair_Vector* connect_addresses) const;
	Ads_Shared_Data_Service* get_counter(const Ads_String& name, const Ads_String_Pair_Vector* global_counter_addresses = nullptr);
	int destroy_counter(const Ads_String& name);
	void parse_enabled_networks(Ads_Request* req, Ads_GUID_Set& closure, Ads_GUID_Set& enabled_networks, Ads_GUID_Set& exclude_networks) const;
	void get_timezone_info(std::set<int>& gmt_offsets) const;

	int exchange_feedback(time_t start, time_t end);
	int exchange_external_feedback(time_t start, time_t end);

	void split_collect(Ads_Request* req, json::ObjectP& result);

	int update_conf(Ads_Request* req, json::ObjectP& result);

	ads::Mutex simulate_slow_threshold_mutex_;
	int set_simulate_slow_threshold(int v);
	int get_simulate_slow_threshold();
public:
	Forecast_Service();
	~Forecast_Service();

	static Forecast_Service *instance();

public:
	int open();
	int stop();
	int svc();
	int dispatch_message(Ads_Message_Base *);

	int update_stats(json::ObjectP& o);
	int dispatch(Ads_Request *req, Ads_Response *res);
	int dispatch_others(Ads_Request *req, Ads_Response *res, json::ObjectP& result);

	int dispatch(const json::Object& o);

	Forecast_Store *get_store(Ads_GUID network_id);
	int list_store(std::list<Forecast_Store *>&);
	int list_all_store(const AF_Job_Context* job_ctx, std::list<Forecast_Store *>&);
	int init_closures();

private:
	Forecast_Pusher *pusher_;
	Forecast_Planner *planner_;
	Forecast_Simulator *simulator_;

public:
	Forecast_Pusher *pusher() 		{ return this->pusher_; }
	Forecast_Planner *planner()		{ return this->planner_; }
	Forecast_Simulator *simulator()	{ return this->simulator_; }

	static int remove_directory(const Ads_String & path);
	static int plan_info(const Ads_GUID_Set & networks, Plan_Info & plan_info, time_t& plan_id, int64_t& timezone, const Ads_GUID scenario_id = 0);
	enum UPDATED_ID_TYPE {
		PLACEMENT = 0,
		RULE,
		UPFRONT_PLAN_COMPONENT,
	};
	static int get_updated_ids(const Ads_String& query, Ads_GUID_Set& id_set, UPDATED_ID_TYPE type);
	//INK-12576
	int read_repa_info();  //forecast start
	int write_repa_info(); //forecast stop
	int append_repa_info(const time_t plan_id, const Ads_String& repa_path);
	int load_old_repa(const time_t plan_id, const Ads_String& pattern, Ads_String& file, int& index_out, time_t &time, int& version_out);

private:
	int rotate_virtual_date(Ads_Shared_Data_Service* counter_service, time_t fc_date, Ads_String& error_message);
	int save_counters(AF_Job_Context* context, const Ads_String& filename);
	int rename_counter_files(AF_Job_Context* context);
	int prepare_fast_conf(const Ads_Network* network, const Ads_Repository* repo, const Ads_Request* req, Fast_Simulate_Config& config, size_t& flags, time_t& start_date, time_t& end_date, json::ObjectP& result);
	time_t get_nightly_pusher_timestamp(const Ads_Repository* repo, Ads_String& model_id, Ads_GUID network_id);

private:
// for RPM OD
	int copy_mrm_delivered_repo_to_rpm(const Ads_Repository* repo,  Fast_Simulate_Config& config, Ads_GUID rpm_id);
	int copy_mrm_counter_to_rpm(Fast_Simulate_Config* config, AF_Job_Context* job_ctx, Ads_GUID rpm_id);
// get excluded_placements when od request from RPM
	int get_excluded_placements(AF_Job_Context *job_ctx, const Ads_Repository* repo, Fast_Simulate_Config& conf);
//private:
//	static int build_table_targeting(const Ads_Repository* repo, Forecast_Request_Meta* meta, True_Table_Targeting& targeting);
public:
	void clear_global_cookies(json::ObjectP& result);
	int clear_load_request_cache(AF_Job_Context* job_ctx);

public:
	Ads_Ab_Test_Collection_Map offline_ab_test_collections_;
};

/*
class Forecast_Store_Guard
{
public:
	Forecast_Service* service_;
public:
	Forecast_Store_Guard(Forecast_Service* service)
		: service_(service)
	{
		ads::Guard __g(service_->store_mutex_);
		++ Forecast_Service::store_service_num_;
	}

	virtual ~Forecast_Store_Guard()
	{
		ads::Guard __g(service_->store_mutex_);
		-- Forecast_Service::store_service_num_;
		service_->store_cond_.signal();
	}

};
*/

class Forecast_Placement_Update_Job: public Ads_Job
{
	Forecast_Service* forecast_service_;

public:
	Forecast_Placement_Update_Job(Forecast_Service* service): Ads_Job(), forecast_service_(service) {}

	virtual int run();
};

class Forecast_Rule_Update_Job: public Ads_Job
{
	Forecast_Service* forecast_service_;

public:
	Forecast_Rule_Update_Job(Forecast_Service* service): Ads_Job(), forecast_service_(service) {}

	virtual int run();
};

//INK-2668
typedef ads::Message_Queue<const Ads_Network> Network_Queue;
struct Scenario_Plan_Context
{
	Network_Queue network_queue_;
	Forecast_Planner* planner_;
	Forecast_Service::AF_Job_Context* job_ctx_;
	const Ads_Repository* repo_;
	size_t event_count_;
	Forecast_Service::Network_Map* enabled_networks_;
	Ads_String* sample_dimensions_;
	Ads_String* output_networks_; 
	bool only_fake_requests_;
	int64_t vnode_count_;
	Scenario_Plan_Context(Forecast_Planner* planner, Forecast_Service::AF_Job_Context* job_ctx, const Ads_Repository* repo, size_t event_count,
			Forecast_Service::Network_Map* enable_networks, Ads_String* sample_dimensions, Ads_String* output_networks, bool only_fake_requests, int64_t vnode_count):
		planner_(planner), job_ctx_(job_ctx), repo_(repo), event_count_(event_count), enabled_networks_(enable_networks), sample_dimensions_(sample_dimensions), output_networks_(output_networks), only_fake_requests_(only_fake_requests), vnode_count_(vnode_count){}
};

//OPP-5409: can read/write to redis or NFS
struct Forecast_Pending_Processer
{
	//for redis
	Ads_String_Map redis_contents_;
	char *buf_;
	int rd_pos_;
	int wr_pos_;
	bool eof_;
	int capacity_;
	int64_t last_total_out_;
	z_stream redis_stream_;
	bool is_stream_ready_;
	//NFS
	std::map<Ads_String, Ads_Text_File*> files_;

	Forecast_Pending_Processer(): buf_(NULL), rd_pos_(0), wr_pos_(0), eof_(false), capacity_(Ads_Text_File::DEFAULT_BUFFER_SIZE), last_total_out_(0), is_stream_ready_(false)
	{
		redis_contents_.clear();
		files_.clear();
	}
	~Forecast_Pending_Processer() 
	{
		for (auto it = files_.begin(); it != files_.end(); ++it)
			if (it->second) delete it->second;
		if (buf_) delete []buf_;
		if (is_stream_ready_)
		{
			inflateEnd(&redis_stream_);
			is_stream_ready_ = false;
		}
	}

	//read pending result, if fail, pending result is lost
	char* fgets(const Ads_String& filename, char *buffer, int size);
	bool open(const Ads_String& filename);
	bool batch_open(const Forecast_Service::AF_Job_Context* job_ctx, const Ads_String_List& paths, const Ads_String& file_last);

	//copy to redis/NFS. 
	static int copy(const Forecast_Service::AF_Job_Context* job_ctx, const Ads_String& src_filename, const Ads_String& dst_filename);
	static int touch_progress(const Forecast_Service::AF_Job_Context* job_ctx, const time_t fc_date);
	//exist in Redis|NFS
	static int reload_pending_files(Forecast_Service::AF_Job_Context* job_ctx, time_t start_time, time_t end_time);
	static bool exist(const Forecast_Service::AF_Job_Context* job_ctx, const Ads_String& filename);
};


#endif // FORECAST_SERVICE_H
