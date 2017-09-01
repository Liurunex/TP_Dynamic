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

#include <zlib.h>
#include "Forecast_Task.h"
#include "Forecast_Service.h"
#include "Forecast_Collector.h"
#include "Forecast_Simulator.h"
#include "Forecast_Planner.h"
#include "Forecast_Store.h"
#include "Forecast_Pusher.h"

#include "True_Table_Conflict_Detector.h"
#include "Forecast_Aggregator.h"

#include "server/Ads_Server.h"
#include "server/Ads_Request.h"
#include "server/Ads_Response.h"
#include "server/Ads_Log_Store.h"
#include "server/Ads_Cacher.h"
#include <google/protobuf/text_format.h>
#include "cajun/json/elements.h"
#include "cajun/json/reader.h"
#include "cajun/json/writer.h"

#include "server/Ads_Bloom_Filter.h"
#include "stdlib.h"
#define FORECAST_SERVICE_INFO_LAST_COMMAND "last_command"
#define FORECAST_SERVICE_INFO_COMMAND_STATUS "command_status"
#define FORECAST_MAX_SIMULATE_DAY 1000

Forecast_Service::Forecast_Service()
: collector_guard_counter_(0)
, collecting_mutex_()
, collector_cond_(collecting_mutex_)
, nightly_cond_(collecting_mutex_)
, conflict_detection_job_service_(new Ads_Job_Queue_Service()) 
, pusher_(new Forecast_Pusher())
, planner_(new Forecast_Planner())
, simulator_(Forecast_Simulator::instance())
{
	info_[FORECAST_SERVICE_INFO_LAST_COMMAND] = json::String("idle");
	info_[FORECAST_SERVICE_INFO_COMMAND_STATUS] = json::String("");
	load_request_manager_ = new Forecast_Task_Manager("load_requests", -1, false);
	simulate_manager_ = new Forecast_Task_Manager("simulate");
	calculate_metric_manager_ = new Forecast_Task_Manager("calc_metric", Ads_Server::config()->forecast_num_calculation_buffer_);
	output_pending_manager_ = new Forecast_Task_Manager("output_pending", Ads_Server::config()->forecast_num_output_buffer_);
	pre_aggregate_manager_ = new Forecast_Task_Manager("pre_aggregate");
	aggregate_manager_ = new Forecast_Task_Manager("aggregate");
	/* zxliu added */
	managers_supervisor_ = new Forecast_Manager_Supervisor("supervisor", 50, 6);
}

Forecast_Service::~Forecast_Service()
{
	if (load_request_manager_) delete load_request_manager_;
	if (simulate_manager_) delete simulate_manager_;
	if (calculate_metric_manager_) delete calculate_metric_manager_;
	if (conflict_detection_job_service_) delete conflict_detection_job_service_;
	if (output_pending_manager_) delete output_pending_manager_;
	if (pre_aggregate_manager_) delete pre_aggregate_manager_;
	if (aggregate_manager_) delete aggregate_manager_;
	if (pusher_) delete pusher_;
	if (planner_) delete planner_;
	/* zxliu added */
	if (managers_supervisor_) delete managers_supervisor_;
}

Forecast_Service *
Forecast_Service::instance()
{
	return ads::Singleton<Forecast_Service>::instance();
}


int
Forecast_Placement_Update_Job::run()
{
	if (!forecast_service_) return -1;
	Ads_GUID_Set ids;
	{
		ads::Guard __g(forecast_service_->placements_updated_mutex_);
		ids.insert(forecast_service_->placements_updated_.begin(), forecast_service_->placements_updated_.end());
		forecast_service_->placements_updated_.clear();
	}
	if (ids.empty()) return 0;
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	Ads_String_List dimensions;
	Ads_Delta_Repository *delta_repo = new (std::nothrow) Ads_Delta_Repository;
	if (delta_repo == nullptr)
	{
		ADS_LOG((LP_ERROR, "operator new Ads_Delta_Repository failed.\n"));
		return -1;
	}
	return Ads_Pusher::instance()->load_delta_repository(repo, Ads_Pusher::FLAG_DELTA_LOAD_PLACEMENT_UPDATE, Ads_Pusher::AUTO_GARBAGE_COLLECTION, ids, dimensions, *delta_repo);
}

int
Forecast_Rule_Update_Job::run()
{
	if (!forecast_service_) return -1;
	Ads_GUID_Set ids;
	{
		ads::Guard __g(forecast_service_->rules_updated_mutex_);
		ids.insert(forecast_service_->rules_updated_.begin(), forecast_service_->rules_updated_.end());
		forecast_service_->rules_updated_.clear();
	}
	if (ids.empty()) return 0;
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	Ads_String_List dimensions;
	Ads_Delta_Repository *delta_repo = new (std::nothrow) Ads_Delta_Repository;
	if (delta_repo == nullptr)
	{
		ADS_LOG((LP_ERROR, "operator new Ads_Delta_Repository failed.\n"));
		return -1;
	}
	return Ads_Pusher::instance()->load_delta_repository(repo, Ads_Pusher::FLAG_DELTA_LOAD_MRM_RULE, Ads_Pusher::AUTO_GARBAGE_COLLECTION, ids, dimensions, *delta_repo);
}

void Forecast_Service::AF_Daily_Simulate_Jobs::when_complet()
{
	Waitable_Group_Guard __g(context_->wg_);
	
}

int Forecast_Service::Forecast_Conflict_Detection_Job::run()
{
	Ads_Conflict_Detector::detection_helper(timestamp_, infos_);
	return 0;
}

int
Forecast_Service::open()
{
	const Ads_Server_Config *conf =  Ads_Server::config();

	Ads_String_List dirs;
	if (! conf->forecast_working_dir_.empty())
		dirs.push_back( conf->forecast_working_dir_);
	if (! conf->forecast_collect_ekv_dir_.empty())
		dirs.push_back( conf->forecast_collect_ekv_dir_);
	if (! conf->forecast_collect_dir_.empty())
		dirs.push_back( conf->forecast_collect_dir_);
	if (! conf->forecast_split_collect_input_dir_.empty())
		dirs.push_back( conf->forecast_split_collect_input_dir_);
	if (! conf->forecast_split_collect_output_dir_.empty())
		dirs.push_back( conf->forecast_split_collect_output_dir_);
	if (! conf->forecast_plan_dir_.empty())
		dirs.push_back( conf->forecast_plan_dir_);
	if (! conf->forecast_pending_dir_.empty())
		dirs.push_back( conf->forecast_pending_dir_);
	if (! conf->forecast_pre_aggregate_dir_.empty())
		dirs.push_back( conf->forecast_pre_aggregate_dir_);
	if (! conf->forecast_result_dir_.empty())
		dirs.push_back( conf->forecast_result_dir_);
	if (! conf->forecast_exchange_dir_.empty())
		dirs.push_back(conf->forecast_exchange_dir_);
	if (! conf->forecast_store_dir_.empty())
		dirs.push_back(conf->forecast_store_dir_);

	for (Ads_String_List::const_iterator it = dirs.begin(); it != dirs.end(); ++it)
	{
		const Ads_String& dir = *it;
		if (ads::ensure_directory(dir.c_str()) < 0)
			ADS_LOG_RETURN((LP_ERROR, "can not open output dir: %s\n", dir.c_str()), -1);
	}
	{
		ads::Guard _g(this->feedback_mutex_);
		if (this->reload_feedback(this->feedback_) < 0) 
			ADS_LOG_RETURN((LP_ERROR, "failed to reload feedback\n"), -1);
	}
	{
		ads::Guard _g(this->external_feedback_mutex_);
		if (this->reload_external_feedback(this->external_feedback_) < 0) 
			ADS_LOG_RETURN((LP_ERROR, "failed to reload external feedback\n"), -1);
	}
	this->register_forecast_dispatchers();
	if (load_request_manager_->open(conf->forecast_num_load_request_threads_) < 0) return -1;
	if (simulate_manager_->open(conf->forecast_num_simulator_threads_) < 0) return -1;
	if (calculate_metric_manager_->open(conf->forecast_num_calculation_threads_) < 0) return -1;
	this->conflict_detection_job_service_->num_threads(conf->forecast_num_conflict_detection_threads_);
	if (conflict_detection_job_service_->open() < 0) return -1;
	if (output_pending_manager_->open(conf->forecast_num_output_threads_) < 0) return -1;
	if (pre_aggregate_manager_->open(conf->forecast_num_pre_aggregate_jobs_) < 0) return -1;
	if (aggregate_manager_->open(conf->forecast_num_aggregate_jobs_) < 0) return -1;

	/* zxliu added */
	managers_supervisor_->set_thread_limit(50);
	managers_supervisor_->num_tp(6);
	if (managers_supervisor_->openself() < 0) return -1;

	/* added worker who need tp_size_adjustment to supervisor, notice this process was expected 
		to be done in 50 seconds, otherwise the supervisor will wake up without storing all
		worker pointers
	*/
	
	managers_supervisor_->addworker(load_request_manager_);
	managers_supervisor_->addworker(simulate_manager_);
	managers_supervisor_->addworker(calculate_metric_manager_);
	managers_supervisor_->addworker(output_pending_manager_);
	managers_supervisor_->addworker(pre_aggregate_manager_);
	managers_supervisor_->addworker(aggregate_manager_);

	this->read_repa_info();
	return Ads_Service_Base::open();
}

int
Forecast_Service::stop()
{
	for (std::map<Ads_GUID, Forecast_Store *>::iterator it = stores_.begin(); it != stores_.end(); ++it)
		if (it->second){delete it->second; it->second = NULL;}
	ADS_LOG((LP_INFO, "(fcst: %llu) %llu stores closed.\n", ads::thr_id(), stores_.size()));
	stores_.clear();

	stop_and_wait_collect();

	destroy_job_contexts();
	write_repa_info();

	/* zxliu added */
	managers_supervisor_->stop();

	load_request_manager_->stop();
	simulate_manager_->stop();
	calculate_metric_manager_->stop();
	conflict_detection_job_service_->stop();
	output_pending_manager_->stop();
	pre_aggregate_manager_->stop();
	aggregate_manager_->stop();
	return Ads_Service_Base::stop();
}

int
Forecast_Service::destroy_job_contexts()
{
	{
	ads::Guard __g(nightly_simulate_jobs_mutex_);
	for (std::map<Ads_String, AF_Job_Context*>::const_iterator it = nightly_simulate_jobs_.begin(); it != nightly_simulate_jobs_.end(); ++it)
		delete it->second;
	nightly_simulate_jobs_.clear();
	}

	{
	ads::Guard __g(od_simulate_jobs_mutex_);
	for (std::map<Ads_String, AF_Job_Context*>::const_iterator it = od_simulate_jobs_.begin(); it != od_simulate_jobs_.end(); ++it)
		delete it->second;
	od_simulate_jobs_.clear();
	}
	return 0;
}

int
Forecast_Service::svc()
{
	ADS_LOG((LP_INFO, "(fcst: %llu) service started. sizeof(Forecast_Request_Meta) = %u, sizeof(Site_Section_Feedback) = %u\n", 
				ads::thr_id(),
				sizeof(Forecast_Request_Meta),
				sizeof(Site_Section_Feedback)));

	Ads_Service_Base::svc();

	ADS_LOG((LP_INFO, "(fcst: %llu) service stopped.\n", ads::thr_id()));

	return 0;
}

int
Forecast_Service::dispatch_message(Ads_Message_Base *msg)
{
	char buf[0xff];
	::snprintf(buf, sizeof buf, "Forecast_Service: %08lx", ads::thr_id());
	Ads_Server::instance()->mark_busy(buf, ::time(NULL), msg->type());

	switch (msg->type())
	{
	default:
		Ads_Server::instance()->clear_busy(buf);
		return Ads_Service_Base::dispatch_message(msg);
	}

	Ads_Server::instance()->clear_busy(buf);
	return 0;
}

int
Forecast_Service::update_stats(json::ObjectP& o)
{
	return 0;
}

#define FORECAST_SERVICE_STATUS_FAILED "FAILED"
#define FORECAST_SERVICE_STATUS_OK	   "OK"
#define FORECAST_SERVICE_STATUS_NOT_IMPLEMENTED "ERROR not implemented"

#define FORECAST_SERVICE_FAILED_RETURN(str) \
{ \
	result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED); \
	result["error"] = json::String(str); \
	return -1; \
} \

int
Forecast_Service::generate_response(int ret, const json::UnknownElement* data, json::ObjectP& result)
{
	if (ret < 0)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		if (data) result["error"] = *data;
	}
	else
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		if (data) result["data"] = *data;
	}
	return 0;
}

namespace
{
	static std::map<Ads_String, Forecast_Service::Forecast_Dispatcher *> __dispatchers;
	static std::map<Ads_String, ads::Mutex*> __job_mutex_map;
};


#define BEGIN_FORECAST_DISPATCHER(name) \
	struct Forecast_Dispatcher_##name##_: public Forecast_Service::Forecast_Dispatcher { \
		Forecast_Dispatcher_##name##_() { __dispatchers.insert(std::make_pair(#name, this)); }  \
		int handle_request(Ads_Request *req, Ads_Response *res, json::ObjectP& result) { \
				++ n_used_; \
				result["start_time"] = json::Number(::time(NULL)); \
					

#define __END_FORECAST_DISPATCHER(R) ++n_succeeded_; result["duration"] = json::Number(::time(NULL) - ((json::Number)result["start_time"]).Value()); return 0; } } __dispatcher_##R##__;
#define _END_FORECAST_DISPATCHER(R) __END_FORECAST_DISPATCHER(R)
#define END_FORECAST_DISPATCHER _END_FORECAST_DISPATCHER(__LINE__)


int Forecast_Service::register_forecast_dispatchers()
{
	ads::Guard _g(this->request_mutex_);
	for (std::map<Ads_String, Forecast_Service::Forecast_Dispatcher *>::const_iterator it = __dispatchers.begin();
		it != __dispatchers.end(); ++it)
	{
		const Ads_String& s = it->first;
		Forecast_Dispatcher *dispatcher = it->second;
		Ads_String command = ads::replace("__", "::", s);
		ADS_LOG((LP_INFO, "registering forecast_dispatcher for %s: 0x%08lx\n", command.c_str(), dispatcher));
		this->dispatchers_.insert(std::make_pair(command, dispatcher));
	}
	ADS_LOG((LP_INFO, "%ld forecast dispatchers registered\n", __dispatchers.size()));
	return 0;
}

#include "Forecast_Service.Dispatch.inl"

int Forecast_Service::dispatch(Ads_Request *req, Ads_Response *res)
{
	ADS_LOG((LP_INFO, "dispatch start: %s\n", req->query_string().c_str()));
	Ads_String content_type = "text/plain";
	json::ObjectP result;
	result["status"] = json::String(FORECAST_SERVICE_STATUS_NOT_IMPLEMENTED);
	result["error"] = json::String("");

	Ads_String command = req->p("command");
	if (command.empty())
	{
		result["error"] = json::String("please provide the 'command' parameter");

		std::stringstream sb;
		json::Writer::Write(result, sb);
		res->content(content_type, sb.str());
		res->ready(true);
		return -1;
	}
	if (command == "get_service_info")
	{
		ads::Guard __g(this->info_mutex_);
		result[FORECAST_SERVICE_INFO_LAST_COMMAND] = info_[FORECAST_SERVICE_INFO_LAST_COMMAND];
		result[FORECAST_SERVICE_INFO_COMMAND_STATUS] = info_[FORECAST_SERVICE_INFO_COMMAND_STATUS];
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);

		std::stringstream sb;
		json::Writer::Write(result, sb);
		res->content(content_type, sb.str());
		res->ready(true);
		return 0;
	}

	{
		ads::Guard __g(this->info_mutex_);
		info_[FORECAST_SERVICE_INFO_LAST_COMMAND] = json::String(command);
	}

	std::map<Ads_String, Forecast_Dispatcher *>::const_iterator it = dispatchers_.find(command);
	if (it == dispatchers_.end())
	{
		result["error"] = json::String("ERROR not implemented");
		
		std::stringstream sb;
		json::Writer::Write(result, sb);
		res->content(content_type, sb.str());

		res->ready(true);
		return 0;
	}

	Forecast_Dispatcher *dispatcher = it->second;
	Ads_String job_name = req->p("job_tag").empty()? req->p("plan_id") : req->p("job_tag");
	Forecast_Service::AF_Job_Context* job_ctx = Forecast_Service::instance()->get_job_context(job_name);
	if (job_ctx && job_ctx->flags_ & Forecast_Service::AF_Job_Context::FLAG_DELETABLE) 
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
	 	result["error"] = json::String("job is being destroyed");
 		std::stringstream sb;
 		json::Writer::Write(result, sb);
 		res->content(content_type, sb.str());
 		res->ready(true);
 		return -1;
	}
	
	if(job_ctx) job_ctx->wg_.inc();
	dispatcher->handle_request(req, res, result);
	if (!job_ctx) job_ctx = Forecast_Service::instance()->get_job_context(job_name); //necessary for prepare_nightly and prepare_ondemand dispatch
	if (job_ctx) job_ctx->wg_.dec();
	if (job_ctx && command == "remove_job_context") //don't replace the check with FLAG_DELETABLE flag
	{
		job_ctx->wg_.wait();
		Forecast_Service::instance()->remove_job_context(job_ctx);
	}
	if (!res->ready())
	{
		std::stringstream sb;
		json::Writer::Write(result, sb);
		res->content(content_type, sb.str());
		res->ready(true);
	}
	ADS_LOG((LP_INFO, "dispatch end: %s\n", req->query_string().c_str()));
	return 0;
}

BEGIN_FORECAST_DISPATCHER(reload_feedback)
	{
		time_t start = ::time(NULL) / 86400 * 86400 - 8 * 86400;
		if (!req->p("start_date").empty())
			start = ads::str_to_time(req->p("start_date").c_str(), "%Y%m%d");
		time_t end = start + 7 * 86400;
		if (!req->p("end_date").empty())
			end = ads::str_to_time(req->p("end_date").c_str(), "%Y%m%d");
		if (start >= end)
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			result["error"] = json::String(Ads_String("start_date >= end_date : ") + ads::string_date(start) + " >= " + ads::string_date(end));
			return -1;
		}
		bool network_only = req->p("network_only") == "1" || req->p("network_only") == "true";
		Forecast_Service *fs = Forecast_Service::instance();

		{
			ads::Guard __g(fs->feedback_mutex_);
			if (fs->reload_feedback(fs->feedback_, start, end, network_only) < 0)
			{
				result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
				result["error"] = json::String("reload feedback error");
				return -1;
			}
		}
		{
			ads::Guard __g(fs->external_feedback_mutex_);
			if (fs->reload_external_feedback(fs->external_feedback_, start, end) < 0)
			{
				result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
				result["error"] = json::String("reload external feedback error");
				return -1;
			}
		}

		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	}
END_FORECAST_DISPATCHER

int Forecast_Service::get_updated_ids(const Ads_String& query, Ads_GUID_Set& id_set, UPDATED_ID_TYPE type)
{
	ADS_LOG((LP_INFO, "delta load input sql: %s\n", query.c_str()));
	// get list of all updated approved_upc/placement/rule ids from last pusher load
	Ads_Pusher *pusher = Ads_Pusher::instance();
	ads::Guard _g(pusher->mutex_);
	Ads_Pusher::DB_Connection *conn = 0;
	if (pusher->get_connection(conn, false) < 0)
	{
		if (pusher->new_connection(conn) < 0 || pusher->get_connection(conn, false) < 0)
		{
			ADS_LOG((LP_ERROR, "failed to get updated %s list when delta load\n", type == Forecast_Service::PLACEMENT ? "placement" : (type == Forecast_Service::RULE ? "rule" : "upc")));
			return -2;
		}
	}

	DB_View_generic_id v;
	Ads_GUID_HSet results;
	v.ids_ = &results;
	v.query_ = query; 
	int ret = 0;
	if (conn->load(v, NULL, NULL, true) < 0)
	{
		ret = -1;
		ADS_LOG((LP_ERROR, "try delta load sql failed: %s\n",  v.query_.c_str()));
	}
	else
		ADS_LOG((LP_INFO, "delta load sql: %s\n", v.query_.c_str()));
	pusher->put_connection(conn);
	for (Ads_GUID_HSet::const_iterator it = results.begin(); it != results.end(); ++it) 
	{
		if (type == Forecast_Service::PLACEMENT || type == Forecast_Service::UPFRONT_PLAN_COMPONENT)
			id_set.insert(ads::entity::make_id(ADS_ENTITY_TYPE_ADVERTISEMENT, *it));
		else if (type == Forecast_Service::RULE)
			id_set.insert(ads::entity::make_id(ADS_ENTITY_TYPE_MRM_RULE, *it));
	}
	ADS_LOG((LP_INFO, "delta load result: %d\n", results.size()));
	return ret;
}

int
Forecast_Service::copy_mrm_delivered_repo_to_rpm(const Ads_Repository* repo, Fast_Simulate_Config& config, Ads_GUID rpm_id)
{
	const Ads_Advertisement* excluded_ad = NULL;
	if (config.delta_repo_->advertisements_.find(rpm_id) == config.delta_repo_->advertisements_.end() || config.delta_repo_->advertisements_[rpm_id]->mrm_id_ == rpm_id
			|| Ads_Repository::find_advertisement_in_repos(repo, config.delta_repo_, config.delta_repo_->advertisements_[rpm_id]->mrm_id_, excluded_ad) < 0) {
//		config.self_rpm_placements_.erase(rpm_id);
		return 0;
	}
	Ads_Advertisement* self_ad = config.delta_repo_->advertisements_[rpm_id];
	ADS_DEBUG((LP_INFO, "update mrm placement %d to rpm placement %d\n", config.delta_repo_->advertisements_[rpm_id]->mrm_id_, rpm_id));
	self_ad->targeted_delivered_currency_ = excluded_ad->targeted_delivered_currency_;
	self_ad->delivered_impression_ = excluded_ad->delivered_impression_;
	self_ad->expense_ = excluded_ad->expense_;
	self_ad->delivered_budget_before_restart_ = excluded_ad->delivered_budget_before_restart_;
	self_ad->historical_avail_ = excluded_ad->historical_avail_;
	for (Ads_Advertisement::Targeted_Demographic_Map::const_iterator dit = excluded_ad->targeted_demographics_.begin(); dit != excluded_ad->targeted_demographics_.end(); ++dit)
	{
		if (config.delta_repo_->advertisements_[rpm_id]->targeted_demographics_.find(dit->first) != config.delta_repo_->advertisements_[rpm_id]->targeted_demographics_.end())
		{
			self_ad->targeted_demographics_[dit->first] = dit->second;
		}
	}
	return 1;
}

int Forecast_Service::get_excluded_placements(AF_Job_Context *job_ctx, const Ads_Repository* repo, Fast_Simulate_Config& config)
{
	for (auto it = config.lost_placements_.begin(); it != config.lost_placements_.end(); ++it)
	{
		job_ctx->excluded_placements_.insert(*it);
		ADS_DEBUG((LP_DEBUG, "add excluded rpm lost placements %s\n", ads::entity::str(*it).c_str()));
	}

	for (Ads_GUID_Set::iterator it = config.self_rpm_placements_.begin(); it != config.self_rpm_placements_.end(); ++it)
	{
		job_ctx->excluded_placements_.insert(config.delta_repo_->advertisements_[*it]->mrm_id_);
		ADS_DEBUG((LP_DEBUG, "add excluded mrm placements %s for self rpm placements\n", ads::entity::str(config.delta_repo_->advertisements_[*it]->mrm_id_).c_str()));
	}

	for (Ads_GUID_Set::iterator it = config.placements_.begin(); it != config.placements_.end(); ++it)
	{
		Ads_GUID id = *it;
		const Ads_Advertisement* excluded_ad = NULL;
		const Ads_Advertisement* mrm_ad = NULL;
		if (Ads_Repository::find_advertisement_in_repos(repo, config.delta_repo_, id, excluded_ad) >= 0)  
		{
			if (id == excluded_ad->mrm_id_) // mrm plcs: exclude rpm ids except those in self_rpm_placements
			{
				Ads_GUID_RMap::const_iterator it = config.delta_repo_->mrm_rpm_map_.find(id);
				if (it != config.delta_repo_->mrm_rpm_map_.end() 
					 || (it = repo->mrm_rpm_map_.find(id)) != repo->mrm_rpm_map_.end())
				{
					Ads_GUID rpm_id = it->second;
					if (config.self_rpm_placements_.find(rpm_id) == config.self_rpm_placements_.end())
					{
						job_ctx->excluded_placements_.insert(rpm_id);
						ADS_DEBUG((LP_DEBUG, "add excluded rpm placements %s for delta mrm placements\n", ads::entity::str(rpm_id).c_str()));
					}
				}
			}
			else if (config.self_rpm_placements_.find(id) == config.self_rpm_placements_.end()) // non self rpm plcs: exclude rpm id if corresponding mrm id existed in repa/delta repa
			{
				if (Ads_Repository::find_advertisement_in_repos(repo, config.delta_repo_, excluded_ad->mrm_id_, mrm_ad) >= 0)
				{
					job_ctx->excluded_placements_.insert(id);
					ADS_DEBUG((LP_DEBUG, "add excluded rpm placements %s for delta non self rpm placements\n", ads::entity::str(id).c_str()));
				}
			}
		}
	}

   return 0;
}

int Forecast_Service::prepare_fast_conf(const Ads_Network* network, const Ads_Repository* repo, const Ads_Request* req, Fast_Simulate_Config& config, size_t& flags, time_t& start_date, time_t& end_date, json::ObjectP& result)
{
	config.network_id_ = network->id_;
	if (req->p("type") == "full")
		config.is_full_ = true;
	//YPP-893
	if (req->p("enabled_req_filter") == "1")
		config.enabled_req_filter_ = true;
	//config.max_daily_request_ = 0;
	if (!req->p("max_daily_request").empty())
	{
		int64_t num = ads::str_to_i64(req->p("max_daily_request").c_str());
		if (num < 0) config.max_daily_request_ = (size_t) -1;
		else config.max_daily_request_ = (size_t) num;
	}
	//YPP-285 for netapp
	if (!req->p("model_id").empty())
	{
		config.model_id_ = req->p("model_id");
	}

	time_t predict_start_date;
	if (req->p("predict_start_date").empty())
		predict_start_date = ::time(NULL) / 86400 * 86400; //today
	else
		predict_start_date = ads::str_to_time(req->p("predict_start_date").c_str(), "%Y%m%d") / 86400 * 86400;

	time_t predict_end_date = -1;
	if (req->p("predict_end_date").empty())
		predict_end_date = predict_start_date + this->forecast_days(network) * 86400;
	else
		predict_end_date = ads::str_to_time(req->p("predict_end_date").c_str(), "%Y%m%d");

	time_t wimas_start_date = -1, wimas_end_date = -1;
	if (!req->p("wimas_start_date").empty())
		wimas_start_date = ads::str_to_time(req->p("wimas_start_date").c_str(), "%Y%m%d") / 86400 * 86400;
	if (!req->p("wimas_end_date").empty())
		wimas_end_date = ads::str_to_time(req->p("wimas_end_date").c_str(), "%Y%m%d") / 86400 * 86400;
	
	//jobs=ABCD~5012,XYZ~5013, EFG~5014~3;4;5
	Ads_String_List jobs_str;
	ads::split(req->p("jobs"), jobs_str, ',');
	Ads_String ids = "(";
	Ads_String dimension_items = "";
	bool found_placement_id = false;
	bool found_dimension_item = false;
	for(Ads_String_List::const_iterator jit = jobs_str.begin(); jit != jobs_str.end(); ++jit)
	{
		Ads_String_List params;
		Ads_String placement_id;
		ads::split(*jit, params, '~');
		// tag~placement_id
		if (params.size() != 2 && params.size() != 3) continue;
		if (params.size() == 2) 
			placement_id = params.back();
		// tag~placement_id~dimension_items
		else
		{
			placement_id = params.at(1);
			if (found_dimension_item) dimension_items += ";"; else found_dimension_item = true;
			dimension_items += params.at(2);
		}
		if (found_placement_id) ids += ","; else found_placement_id = true;
		ids += placement_id;
		config.placements_.insert(ads::str_to_entity_id(ADS_ENTITY_TYPE_ADVERTISEMENT, placement_id));
		config.self_rpm_placements_.insert(ads::str_to_entity_id(ADS_ENTITY_TYPE_ADVERTISEMENT, placement_id));
	}
	ids += ")";
	ads::split(dimension_items, config.dimension_items_, ';');

	
	//placements=5012,5013,5612
	Ads_GUID_Set delta_placements; //nightly pusher to od pusher
	if (!req->p("placements").empty())
	{
		ads::str_to_entities(req->p("placements"), ADS_ENTITY_TYPE_ADVERTISEMENT, 0, std::inserter(config.placements_, config.placements_.begin()));
	}
	else
	{
		// load all updated plc from last pusher load
		Ads_Server::Repository_Guard __g(Ads_Server::instance());
		Ads_String time_loaded = ads::string_date_time_(__g.asset_repository()->time_loaded());
		Ads_String upc_query = "SELECT upc.ad_tree_node_id FROM upfront_plan_component upc JOIN upfront_plan up ON up.id = upc.upfront_plan_id WHERE upc.upfront_id not in " + ids + " AND upc.network_id =" + ADS_ENTITY_ID_CSTR(network->id_) + " AND upc.updated_at >= '" + time_loaded + "' AND up.is_current_version = 1;";
		if (Forecast_Service::instance()->get_updated_ids(upc_query, config.placements_, Forecast_Service::UPFRONT_PLAN_COMPONENT) < 0 )
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			result["error"] = json::String("delta load failed");
			return -2;
		}
		ADS_LOG((LP_INFO, "delta load plc count: %d\n", config.placements_.size()));

		Ads_String query =  "SELECT id FROM ad_tree_node WHERE node_type = 'PLACEMENT' AND updated_at >= '" + time_loaded +"' AND network_id = " + ADS_ENTITY_ID_CSTR(network->id_) + " AND placement_stage != 'FORECAST_ONLY' AND staging = 0 AND placement_category NOT IN ('TEMPLATE', 'UPFRONT_PLAN') AND original_reference_id IS NULL";
		if (Forecast_Service::instance()->get_updated_ids(query, config.placements_, Forecast_Service::PLACEMENT) < 0 )
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			result["error"] = json::String("delta load failed");
			return -2;
		}
		ADS_LOG((LP_INFO, "delta load plc count: %d\n", config.placements_.size()));

		time_t nightly_time_loaded = get_nightly_pusher_timestamp(__g.asset_repository(), config.model_id_, network->id_);
		if (nightly_time_loaded >= 0)
		{
			Ads_String upc_query = "SELECT upc.ad_tree_node_id FROM upfront_plan_component upc JOIN upfront_plan up ON up.id = upc.upfront_plan_id WHERE upc.upfront_id not in " + ids + " AND upc.network_id =" + ADS_ENTITY_ID_CSTR(network->id_) + " AND upc.updated_at >= '" + ads::string_date_time_(nightly_time_loaded) + "' AND upc.updated_at < '" + time_loaded + "' AND up.is_current_version = 1;";
			if (Forecast_Service::instance()->get_updated_ids(upc_query, delta_placements, Forecast_Service::UPFRONT_PLAN_COMPONENT) < 0 )
			{
				result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
				result["error"] = json::String("delta load failed");
				return -2;
			}
			ADS_LOG((LP_INFO, "pusher to od timestamp, delta load plc count: %d\n", config.placements_.size()));

			Ads_String query =  "SELECT id FROM ad_tree_node WHERE node_type = 'PLACEMENT' AND updated_at < '" + time_loaded + "' AND updated_at >= '" + ads::string_date_time_(nightly_time_loaded) + "' AND network_id = " + ADS_ENTITY_ID_CSTR(network->id_) + " AND placement_stage != 'FORECAST_ONLY' AND staging = 0 AND placement_category NOT IN ('TEMPLATE', 'UPFRONT_PLAN') AND original_reference_id IS NULL";
			if (Forecast_Service::instance()->get_updated_ids(query, config.placements_, Forecast_Service::PLACEMENT) < 0 )
			{
				result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
				result["error"] = json::String("delta load failed");
				return -2;
			}
			ADS_LOG((LP_INFO, "delta load plc count: %d\n", config.placements_.size()));
		}

		if (Ads_Server::config()->pusher_load_flags_ & Ads_Server_Config::LOAD_RPM_ADS)
		{
			// load newly lost plc from last pusher load
			Ads_String io_lost_query =  "SELECT ad.id FROM ad_tree_node ad JOIN io_node_trafficking_trait iot ON iot.`ad_tree_node_id` = ad.`parent_ad_tree_node_id` WHERE iot.`updated_at` >= '" + time_loaded + "' AND ad.network_id = " + ADS_ENTITY_ID_CSTR(network->id_) +" AND `workflow_step_type` = 'LOST_DEAL'";
			if (Forecast_Service::instance()->get_updated_ids(io_lost_query, config.lost_placements_, Forecast_Service::PLACEMENT) < 0 )
			{
				result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
				result["error"] = json::String("delta load failed");
				return -2;
			}
			ADS_LOG((LP_INFO, "delta load plc count: %d\n", config.placements_.size()));

			Ads_String variant_lost_query ="SELECT plc.`ad_tree_node_id` FROM placement plc JOIN `proposal_io_variant` piv ON plc.`variant_id` = piv.id WHERE piv.`updated_at` >= '" + time_loaded + "' AND piv.network_id = " + ADS_ENTITY_ID_CSTR(network->id_) + " AND piv.`status` = 'LOST'";
			if (Forecast_Service::instance()->get_updated_ids(variant_lost_query, config.lost_placements_, Forecast_Service::PLACEMENT) < 0 )
			{
				result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
				result["error"] = json::String("delta load failed");
				return -2;
			}
			ADS_LOG((LP_INFO, "delta load plc count: %d\n", config.placements_.size()));
		}
	}
	ADS_LOG((LP_INFO, "delta load plc list: %s\n", ads::entities_to_str(config.placements_).c_str()));

	flags = 0;
	if (!req->p("enable_tracer").empty())
		flags |= Forecast_Simulator::Context::FLAG_ENABLE_TRACER;
	if (!req->p("collect_reject_reason").empty())
		flags |= Forecast_Simulator::Context::FLAG_COLLECT_REJECT_REASON;
	//delta load
	Ads_Pusher_Base::instance()->override_now_ = predict_start_date;
	ADS_LOG((LP_INFO, "delta load override now = %s\n", ads::string_date(predict_start_date).c_str()));
	config.delta_repo_ = new (std::nothrow) Ads_Delta_Repository;
	if (config.delta_repo_ == nullptr)
	{
		ADS_LOG((LP_ERROR, "operator new Ads_Delta_Repository failed.\n"));
		return -1;
	}
	if (Ads_Pusher::instance()->load_delta_repository(repo, Ads_Pusher::FLAG_DELTA_LOAD_PLACEMENT, Ads_Pusher::CAN_CACHE, config.placements_, config.dimension_items_, *config.delta_repo_, network->id_, -1, -1, &config.self_rpm_placements_) < 0)
	{
		ADS_LOG((LP_ERROR, "delta load failed\n"));

		config.delta_repo_->destroy();
		config.delta_repo_ = nullptr;
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String("delta load failed");
		return -2;
	}
	//insert placements updated during Nightly pusher to OD pusher
	config.placements_.insert(delta_placements.begin(), delta_placements.end());
	
	//clear invalid rpm id
	for (Ads_GUID_Set::iterator it = config.self_rpm_placements_.begin(); it != config.self_rpm_placements_.end();) 
	{
		Ads_GUID rpm_id = *it;
		it++;
		const Ads_Advertisement* excluded_ad = NULL;
		if (config.delta_repo_->advertisements_.find(rpm_id) == config.delta_repo_->advertisements_.end() || config.delta_repo_->advertisements_[rpm_id]->mrm_id_ == rpm_id
			|| Ads_Repository::find_advertisement_in_repos(repo, config.delta_repo_, config.delta_repo_->advertisements_[rpm_id]->mrm_id_, excluded_ad) < 0) 
		{
			config.self_rpm_placements_.erase(rpm_id); //rpm plcs whose mrm record existed in repo/delta_repo
		}
	}
	//update delivered impression and targeted delivered impression for io, placement, ad level
	for (Ads_GUID_Set::iterator it = config.self_rpm_placements_.begin(); it != config.self_rpm_placements_.end(); ++it)
	{
		int r = copy_mrm_delivered_repo_to_rpm(repo, config, *it);
		if (r == 0) 
		{
			continue;
		}
		copy_mrm_delivered_repo_to_rpm(repo, config, config.delta_repo_->advertisements_[*it]->parent_id_);
		for (Ads_GUID_Set::iterator tmp_it1 = config.delta_repo_->advertisements_[*it]->children_.begin(); tmp_it1 != config.delta_repo_->advertisements_[*it]->children_.end(); ++tmp_it1)
		{
			copy_mrm_delivered_repo_to_rpm(repo, config, *tmp_it1);
		}
	}
	
	//insert shared_io (only available for MRM) siblings into config.placements_
	Ads_GUID_Set siblings;
	for(Ads_GUID_Set::iterator it = config.placements_.begin(); it != config.placements_.end(); ++ it)
	{
		if (Ads_Repository::get_shared_budget_siblings(repo, config.delta_repo_, *it, siblings) < 0)
			ADS_LOG((LP_ERROR, "failed to find shared budget silbings for placement %s\n", ADS_ADVERTISEMENT_ID_CSTR(*it)));
	}
	config.placements_.insert(siblings.begin(), siblings.end());
	Ads_GUID_Set invalid_placements;
	for(Ads_String_List::const_iterator jit = jobs_str.begin(); jit != jobs_str.end(); ++jit)
	{
		Ads_String_List params;
		Ads_String pid;
		ads::split(*jit, params, '~');
		// tag~pid
		if (params.size() != 2 && params.size() != 3) continue;
		if (params.size() == 2)
			pid = params.back();
		// tag~pid~dimension_items
		else
			pid = params.at(1);
		Ads_String tag = params.at(0);
		Fast_Simulate_Job job;
		job.job_id_ = tag;
		Ads_GUID placement_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_ADVERTISEMENT, pid);
		config.delta_repo_->find_advertisement(placement_id, job.placement_);
		if (!job.placement_)
		{
			Ads_GUID_Set_RMap::const_iterator upc_it = config.delta_repo_->loaded_upcs_.find(placement_id);
			if (upc_it == config.delta_repo_->loaded_upcs_.end())
			{
				ADS_LOG((LP_ERROR, "placement %s not found in delta_repo.\n", pid.c_str()));
				invalid_placements.insert(placement_id);
				continue;
			}
			else
			{
				bool is_valid_upc = false;
				for (Ads_GUID_RSet::const_iterator upcp_it = upc_it->second.begin(); upcp_it != upc_it->second.end(); ++upcp_it)
				{
					Fast_Simulate_Job upcp_job;
					upcp_job.job_id_ = tag; 
					config.delta_repo_->find_advertisement(*upcp_it, upcp_job.placement_);
					if (upcp_job.placement_ && upcp_job.placement_->network_id_ == network->id_ && upcp_job.placement_->end_date_ >= predict_start_date)
					{
						is_valid_upc = true;
						config.jobs_.push_back(upcp_job);
						config.original_set_[*upcp_it] = upcp_job.job_id_;
						for (Ads_GUID_Set::const_iterator it = upcp_job.placement_->children_.begin(); it != upcp_job.placement_->children_.end(); ++it)
							config.original_set_[*it] = upcp_job.job_id_;
					}
				}
				if (!is_valid_upc)
				{
					ADS_LOG((LP_ERROR, "UPC %s not loaded in delta_repo.\n", pid.c_str()));
					invalid_placements.insert(placement_id);
				}
				continue;
			}			
		}
		if (!job.placement_->is_portfolio() && //transactional filter
			((!job.placement_->active_) || job.placement_->is_evergreen()))
		{
			ADS_LOG((LP_ERROR, "placement %s inactive or evergreen\n", pid.c_str()));
			invalid_placements.insert(placement_id);
			continue;
		}
		if (job.placement_->network_id_ != network->id_)
		{
			ADS_LOG((LP_ERROR, "placement %s (nw: %s) should be in network %s\n", ADS_ENTITY_ID_CSTR(job.placement_->id_), ADS_ENTITY_ID_CSTR(job.placement_->network_id_), ADS_ENTITY_ID_CSTR(network->id_) ));  
			invalid_placements.insert(placement_id);
			continue;
		}

		if (!job.placement_->is_portfolio() && job.placement_->end_date_ < predict_start_date)
		{
			ADS_LOG((LP_ERROR, "placement %s is not in forecast range\n", ADS_ENTITY_ID_CSTR(job.placement_->id_) ));  
			invalid_placements.insert(placement_id);
			continue;
		}
		config.jobs_.push_back(job);//small, so deep copy
		config.original_set_[placement_id] = job.job_id_;
		for (Ads_GUID_Set::const_iterator it = job.placement_->children_.begin(); it != job.placement_->children_.end(); ++it)
			config.original_set_[*it] = job.job_id_;
	}

	if (config.jobs_.empty())
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		result["error"] = json::String("placement_not_ready");
		result["deleted_placements"] = json::String(ads::entities_to_str(invalid_placements));
		return -1;
	}
	if (!invalid_placements.empty())
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		result["deleted_placements"] = json::String(ads::entities_to_str(invalid_placements));
	}
	//assert candidates load
	std::map<Ads_GUID, const Ads_Advertisement*> candidate_placements_map;

	for (Ads_GUID_Set::const_iterator it = config.placements_.begin(); it != config.placements_.end(); ++it)
	{
		const Ads_Advertisement* placement = NULL;
		if (Ads_Repository::find_advertisement_in_repos(repo, config.delta_repo_, *it, placement) < 0 || !placement)
		{
			Ads_GUID_Set_RMap::const_iterator upc_it = config.delta_repo_->loaded_upcs_.find(*it);
			if (upc_it == config.delta_repo_->loaded_upcs_.end())
				ADS_LOG((LP_ERROR, "placement %s lost!\n", ADS_ENTITY_ID_CSTR(*it)));
			else
			{
				for (Ads_GUID_RSet::const_iterator upcp_it = upc_it->second.begin(); upcp_it != upc_it->second.end(); ++upcp_it)
				{
					const Ads_Advertisement* upcp_plc = NULL;
					if (Ads_Repository::find_advertisement_in_repos(repo, config.delta_repo_, *upcp_it, upcp_plc) >= 0 && upcp_plc)
					{
						config.placements_.insert(upcp_plc->id_);
						config.ads_.insert(upcp_plc->children_.begin(), upcp_plc->children_.end());
						candidate_placements_map[*upcp_it] = upcp_plc;
					}
				}
				config.placements_.erase(it);
			}
		}
		else
			candidate_placements_map[*it] = placement;
	}
	if (req->p("treat_self_booked") == "1") config.treat_self_booked();

	if (config.placements_.size() == 0 || candidate_placements_map.size() != config.placements_.size())
	{
		ADS_LOG((LP_ERROR, "candidate_placements diff %lu %lu\n", candidate_placements_map.size(), config.placements_.size()));
	}

	//pruning date range (Union_i Ri) n R0, Ri is the range of placement i, R0 is the range of nightly prediction
	start_date = predict_start_date;
	end_date = predict_end_date;
	{
		std::list<time_t> start_dates, end_dates;
		bool has_portfolio = false;
		//if (!config.is_full_){
			//config.placements_.clear();
		//}
		for (Fast_Simulate_Jobs::const_iterator it = config.jobs_.begin(); it != config.jobs_.end(); ++it)
		{
			const Ads_Advertisement* placement = it->placement_;
			config.placements_.insert(placement->id_);
			config.ads_.insert(placement->children_.begin(), placement->children_.end());
			if (placement->start_date_ > 0) start_dates.push_back(placement->start_date_);
			if (placement->end_date_ > 0) end_dates.push_back(placement->end_date_);
			if (placement->is_portfolio()) has_portfolio = true;
		}
		//no pruning date range if has portfolio CLIENTHELP-3274 MRM-19585
		if (!has_portfolio)
		{
			if (!start_dates.empty())
				start_date = std::max(start_date, *std::min_element(start_dates.begin(), start_dates.end()));
			if (!end_dates.empty())
				end_date = std::min(end_date, *std::max_element(end_dates.begin(), end_dates.end()) + 86400);
		}
		if (flags & Forecast_Simulator::Context::FLAG_COLLECT_REJECT_REASON) 
		{
			if ( (wimas_start_date != -1 && wimas_start_date < start_date / 86400 * 86400)
				|| (wimas_end_date != -1 && wimas_end_date > end_date / 86400 * 86400) )
			{
				ADS_LOG((LP_ERROR, "wimas_start_date(include), wimas_end_date(exclude) [%s, %s) is not valid.\n", ads::string_date(wimas_start_date).c_str(), ads::string_date(wimas_end_date).c_str()));
				result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
				result["error"] = json::String(Ads_String("wimas_start_date and wimas_end_date should be within placement's remaining life period (") 
												+ ads::string_date(start_date) + Ads_String(",") + ads::string_date(end_date) 
												+ Ads_String("). If wimas_end_date is not specified, only ONE day will be run."));
				return -1;
			}
			if (wimas_start_date >= start_date / 86400 * 86400) 
				start_date = wimas_start_date;
			if (wimas_end_date != -1 && wimas_end_date <= end_date / 86400 * 86400)
				end_date = wimas_end_date;
			else if(wimas_end_date == -1)
				end_date = std::min(end_date, start_date + 86400);
		}
	}
	start_date = start_date / 86400 * 86400;
	end_date = end_date / 86400 * 86400;
	return 0;
}

BEGIN_FORECAST_DISPATCHER(prepare_ondemand)
	Forecast_Service::instance()->prepare_ondemand(req, result);
END_FORECAST_DISPATCHER

void Forecast_Service::prepare_ondemand(Ads_Request* req, json::ObjectP& result)
{
	//prepare fast_conf
	//load counter_service
	//feedback=global_feedback
	result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
	if (req->p("network_id").empty())
	{
		result["error"] = json::String("Please specify network_id for od placement");
		return;
	}
	Ads_GUID network_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_NETWORK, req->p("network_id"));
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	const Ads_Network* network = NULL;
	if (!ads::entity::is_valid_id(network_id) || repo->find_network(network_id, network) < 0
		|| !network || !network->config())
	{
		result["error"] = json::String(Ads_String ("invalid network id: ") + req->p("network_id"));
		return;
	}
	AF_Job_Context* job_ctx = this->initialize_job_context(req, result);
	if (!job_ctx) return;

	size_t flags;
	time_t start_date, end_date;
	Fast_Simulate_Config* config = new Fast_Simulate_Config;
	int ret = this->prepare_fast_conf(network, repo, req, *config, flags, start_date, end_date, result);
	if (ret < 0)
	{
		delete config;
		return;
	}

	get_excluded_placements(job_ctx, repo, *config);
	job_ctx->fast_conf_ = config;
	job_ctx->flags_ |= flags;

	job_ctx->predict_start_date_ = start_date;
	job_ctx->predict_end_date_ = end_date;
	job_ctx->resume_start_date_ = start_date;
	job_ctx->flags_ |= AF_Job_Context::FAST_SIMULATE_OD;
	if (config->is_full_) job_ctx->flags_ |= AF_Job_Context::FAST_SIMULATE_OD_FULL;
	if (flags & Forecast_Simulator::Context::FLAG_COLLECT_REJECT_REASON)
		job_ctx->flags_ |= AF_Job_Context::FAST_SIMULATE_WIMAS;
	else
	{
		job_ctx->current_network_id_ = network_id;
	}

	{
		ads::Guard __g(feedback_mutex_);
		job_ctx->feedback_ = this->feedback_; //copy to global feedback to job context for concurrent od
	}
	{
		ads::Guard __g(external_feedback_mutex_);
		job_ctx->external_feedback_ = this->external_feedback_;
	}

	if (job_ctx->flags_ & AF_Job_Context::FLAG_PRE_AGGREGATE)
	{
		Forecast_Aggregate_Task* task = new Forecast_Aggregate_Task(job_ctx);
		if (aggregate_manager_->add_task(task) < 0)
		{
			delete task;
			ADS_LOG((LP_ERROR, "[REQUESTS] job=%s, cannot add aggregate task to manager\n", job_ctx->name().c_str()));
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			result["error"] = json::String("failed to add aggregate task");
			return;
		}
	}

	ADS_LOG((LP_INFO, "prepare_ondemand succeed for nw=%s, job_ctx=%s\n", req->p("network_id").c_str(), job_ctx->to_string().c_str()));
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
}

BEGIN_FORECAST_DISPATCHER(placement_update)
	{
		Ads_GUID_Set placements, valid_placements, deleted_placements;
		ads::str_to_entities(req->p("placements"), ADS_ENTITY_TYPE_ADVERTISEMENT, 0, std::inserter(placements, placements.begin()));
		Ads_String deleted_query = "SELECT id FROM ad_tree_node WHERE id in (" + req->p("placements") + ");";
		Forecast_Service * service = Forecast_Service::instance();
		if (service->get_updated_ids(deleted_query, valid_placements, Forecast_Service::PLACEMENT) >= 0)
		{
			std::set_difference(placements.begin(), placements.end(), valid_placements.begin(), valid_placements.end(), std::insert_iterator<Ads_GUID_Set>(deleted_placements, deleted_placements.begin()));
		}
		{
			ads::Guard __g(service->placements_updated_mutex_);
			service->placements_updated_.insert(placements.begin(), placements.end());
		}
		if (req->p("action") == "book")
		{
			Ads_GUID_Set updated_placement_deleted;
			ads::Guard __g(service->placements_deleted_mutex_);
			std::set_difference(service->placements_deleted_.begin(), service->placements_deleted_.end(), placements.begin(), placements.end(), std::insert_iterator<Ads_GUID_Set>(updated_placement_deleted, updated_placement_deleted.begin()));
			service->placements_deleted_.swap(updated_placement_deleted);
		}

		if (!deleted_placements.empty())
		{
			ads::Guard __g(service->placements_deleted_mutex_);
			service->placements_deleted_.insert(deleted_placements.begin(), deleted_placements.end());
			ADS_LOG((LP_INFO, "deleted placements %s in placement_update.\n", ads::entities_to_str(deleted_placements).c_str()));
			ADS_LOG((LP_INFO, "deleted placements %s in service.\n", ads::entities_to_str(service->placements_deleted_).c_str()));
		}
		Forecast_Placement_Update_Job* job = new Forecast_Placement_Update_Job(service);
		if (Ads_Job_Queue_Service_TP_Adaptive::instance()->enqueue_job(job) < 0)
		{
			job->destroy();
			Ads_String msg = Ads_String("create delta repository for placement ") + req->p("placements") + " failed";
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			ADS_LOG((LP_ERROR, "%s\n", msg.c_str()));
			result["error"] = json::String(msg);
		}
		else
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		}
	}
END_FORECAST_DISPATCHER


BEGIN_FORECAST_DISPATCHER(get_updated_placements)
	{
		Forecast_Service *service = Forecast_Service::instance();
		ads::Guard _g1(service->placements_deleted_mutex_);
		result["deleted_placements"] = json::String(ads::entities_to_str(service->placements_deleted_));
		ads::Guard _g2(service->placements_updated_mutex_);
		result["updated_placements"] = json::String(ads::entities_to_str(service->placements_updated_));
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	}

END_FORECAST_DISPATCHER

BEGIN_FORECAST_DISPATCHER(rule_update)
	{
		Ads_GUID_Set rules, valid_rules, deleted_rules;
		ads::str_to_entities(req->p("rules"), ADS_ENTITY_TYPE_MRM_RULE, 0, std::inserter(rules, rules.begin()));
		Forecast_Service * service = Forecast_Service::instance();
		if (req->p("action") == "cancel") 
			deleted_rules.insert(rules.begin(), rules.end());
		else //action == "change" or "book"
		{
			Ads_String deleted_query = "SELECT id FROM mrm_access_rule WHERE id in (" + req->p("rules") + ");";
			if (service->get_updated_ids(deleted_query, valid_rules, Forecast_Service::RULE) >= 0)
			{
				std::set_difference(rules.begin(), rules.end(), valid_rules.begin(), valid_rules.end(), std::insert_iterator<Ads_GUID_Set>(deleted_rules, deleted_rules.begin()));
			}
			{
				ads::Guard __g(service->rules_updated_mutex_);
				service->rules_updated_.insert(rules.begin(), rules.end());
			}
			if (req->p("action") == "book")
			{
				Ads_GUID_Set updated_rules_deleted;
				ads::Guard __g(service->rules_deleted_mutex_);
				std::set_difference(service->rules_deleted_.begin(), service->rules_deleted_.end(), rules.begin(), rules.end(), std::insert_iterator<Ads_GUID_Set>(updated_rules_deleted, updated_rules_deleted.begin()));
				service->rules_deleted_.swap(updated_rules_deleted);
			}
		}
		if (!deleted_rules.empty())
		{
			ads::Guard __g(service->rules_deleted_mutex_);
			service->rules_deleted_.insert(deleted_rules.begin(), deleted_rules.end());
			ADS_LOG((LP_INFO, "deleted rules %s in rule_update.\n", ads::entities_to_str(deleted_rules).c_str()));
			ADS_LOG((LP_INFO, "deleted rules %s in service.\n", ads::entities_to_str(service->rules_deleted_).c_str()));
		}
		Forecast_Rule_Update_Job* job = new Forecast_Rule_Update_Job(service);
		if (Ads_Job_Queue_Service_TP_Adaptive::instance()->enqueue_job(job) < 0)
		{
			job->destroy();
			Ads_String msg = Ads_String("create delta repository for mrm rules ") + req->p("rules") + " failed";
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			ADS_LOG((LP_ERROR, "%s\n", msg.c_str()));
			result["error"] = json::String(msg);
		}
		else
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		}
	}
END_FORECAST_DISPATCHER

BEGIN_FORECAST_DISPATCHER(get_updated_rules)
	{
		Forecast_Service * service = Forecast_Service::instance();
		ads::Guard _g1(service->rules_deleted_mutex_);
		result["deleted_rules"] = json::String(ads::entities_to_str(service->rules_deleted_));
		ads::Guard _g2(service->rules_updated_mutex_);
		result["updated_rules"] = json::String(ads::entities_to_str(service->rules_updated_));
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	}
END_FORECAST_DISPATCHER

BEGIN_FORECAST_DISPATCHER(split_collect)
	Forecast_Service::instance()->split_collect(req,result);
END_FORECAST_DISPATCHER

void Forecast_Service::split_collect(Ads_Request* req, json::ObjectP& result)
{
	if (collector_guard_counter_ != 0)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		return;
	}
	Collect_Guard __cg(this, false);
	Ads_Log_Processor_Manager log_mgr;

	const size_t processor_mask = (1 << Ads_Log_Processor_Manager::POS_REQUEST) | (1 << Ads_Log_Processor_Manager::POS_ACK);

	const Ads_Server_Config *split_conf = Ads_Server::config();
	const Ads_String matcher_working_dir(ads::path_join(split_conf->forecast_working_dir_, "split_match"));
	ads::ensure_directory(matcher_working_dir.c_str());
	Ads_Server_Config::Matcher_Config split_matcher_conf;
	split_matcher_conf.matcher_working_dir_ = matcher_working_dir;
	split_matcher_conf.matcher_incoming_binary_log_dir_ = split_conf->forecast_split_collect_input_dir_;
	split_matcher_conf.matcher_num_threads_ = split_conf->forecast_num_collector_threads_;
	split_matcher_conf.check_repository_timestamp_ = false;
	split_matcher_conf.matcher_retry_hours_ = 0;
	split_matcher_conf.matcher_window_seconds_ = 3600;
	ads::ensure_directory(matcher_working_dir.c_str());
	const Ads_String& now = ads::string_date_time(::time(NULL)).c_str();

	Forecast_Split_Collector *p = new Forecast_Split_Collector(now);
	log_mgr.register_processor(p, processor_mask);
	size_t flags = Ads_Log_Match_Parser::PASS_THROUGH;
	if (Ads_Log_Matcher::instance()->process_logfiles(&split_matcher_conf, log_mgr, flags) < 0)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
	}
	else
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	}
	log_mgr.close();
	for(std::vector<Ads_String>::const_iterator f_it = Ads_Server::config()->all_forecast_servers.begin(); f_it != Ads_Server::config()->all_forecast_servers.end(); ++f_it)
	{
		const Ads_String& worker_id = *f_it;
		rename(ads::path_join(Ads_Server::config()->forecast_split_collect_output_dir_, worker_id, "." + now).c_str(), ads::path_join(Ads_Server::config()->forecast_split_collect_output_dir_, worker_id, now).c_str());
	}
}

BEGIN_FORECAST_DISPATCHER(collect)
{
	Forecast_Service::instance()->collect(req, result);
}
END_FORECAST_DISPATCHER

void Forecast_Service::stop_and_wait_collect()
{
	Collect_Guard(this, true);
}

void Forecast_Service::collect(Ads_Request* req, json::ObjectP& result)
{
	if (collector_guard_counter_ != 0)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		return;
	}
	Collect_Guard __cg(this, false);
	ADS_LOG((LP_INFO, "wait to start collect job\n"));
	bool is_scenario = false;
	if (!req->p("is_scenario").empty()) is_scenario = true;
//	ADS_LOG((LP_INFO, "XXX collect waiting\n"));
	result["start_time"] = json::Number(::time(NULL));
	ADS_LOG((LP_INFO, "start collect job\n"));
	//FDB-6092 Embed forecast log processor into forecast log collector
	Ads_Log_Processor_Manager log_mgr; 

	const size_t processor_mask = (1 << Ads_Log_Processor_Manager::POS_REQUEST)
		| (1 << Ads_Log_Processor_Manager::POS_MATCH)
		| (1 << Ads_Log_Processor_Manager::POS_AD_MATCH);
	const Ads_Server_Config *conf = Ads_Server::config();
	const Ads_String matcher_working_dir(ads::path_join(conf->forecast_working_dir_, "match"));
	ads::ensure_directory(matcher_working_dir.c_str());

	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	if (!repo || !repo->extra_)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String("no repo, no collect");
	}
	else
	{
		Forecast_Collector *p = new Forecast_Collector(repo, is_scenario);

		Ads_Server_Config::Matcher_Config matcher_conf;
		matcher_conf.matcher_working_dir_ = matcher_working_dir;
		matcher_conf.matcher_incoming_binary_log_dir_ = conf->forecast_collect_dir_;
		matcher_conf.matcher_num_threads_ = conf->forecast_num_collector_threads_;
		matcher_conf.check_repository_timestamp_ = false;
		matcher_conf.matcher_retry_hours_ = 0;
		matcher_conf.matcher_window_seconds_ = 3600;

		Ads_Log_Processor_Manager log_mgr;
		log_mgr.register_processor(p, processor_mask);
		if	(p->collect_key_values() < 0
			|| Ads_Log_Matcher::instance()->process_logfiles(&matcher_conf, log_mgr, 
				(size_t)Ads_Log_Parser::DISCARD_ORPHAN_ACK) < 0)
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		}
		else
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		}
		log_mgr.close();
	}

	for (std::map<Ads_GUID, Forecast_Store*>::const_iterator it = stores_.begin(); it != stores_.end(); ++it)
	{
		Forecast_Store* store = it->second;
		store->close_inventory();
		store->close_feedback();
		store->close_external_feedback();
		store->close_database(Forecast_Store::DB_EKV);
		store->close_database(Forecast_Store::DB_REQUEST);
		store->close_database(Forecast_Store::DB_REQUEST_META);
		store->close_database(Forecast_Store::DB_REQUEST_TARGETING);
	}
}

BEGIN_FORECAST_DISPATCHER(stop_collect)
{
	Forecast_Service::instance()->stop_and_wait_collect();
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
}
END_FORECAST_DISPATCHER


BEGIN_FORECAST_DISPATCHER(plan)
{
	Forecast_Service::AF_Job_Context* job_ctx = Forecast_Service::instance()->get_job_context(req, result);
	size_t event_count = 0;
	if (!req->p("event_count").empty()) event_count = ads::str_to_u64(req->p("event_count"));
	Ads_String sample_dimensions = "";
	if (!req->p("sample_dimensions").empty()) sample_dimensions = req->p("sample_dimensions");
	Ads_String output_networks = "";
	if (!req->p("output_networks").empty()) output_networks = req->p("output_networks");
	int64_t vnode_count = 1024;
	if (!req->p("vnode_count").empty()) vnode_count = ads::str_to_i64(req->p("vnode_count"));
	bool only_fake_requests = (req->p("update_faked_requests") == "1" || req->p("update_faked_requests") == "true");
	Forecast_Service::generate_response(Forecast_Service::instance()->plan(job_ctx, event_count, sample_dimensions, only_fake_requests, output_networks, vnode_count, result), NULL, result);
}
END_FORECAST_DISPATCHER

int 
Forecast_Service::plan(AF_Job_Context* job_ctx, size_t event_count, Ads_String& sample_dimensions, bool only_fake_requests, Ads_String& output_networks, int64_t vnode_count, json::ObjectP& result)
{
	if (!job_ctx) return -1;
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	if (!repo || !repo->extra_)
	{
		result["error"] = json::String("no repo, no plan");
		return -1;
	}

	ADS_ASSERT((job_ctx->predict_start_date_ > 0));
	ADS_ASSERT((job_ctx->event_count_ > 0));
	ADS_ASSERT((job_ctx->plan_id_));
	Ads_String msg = Ads_String("build Plan : ") + "WARNING: empty closure networks to plan for job_ctx=" + job_ctx->to_string();
	if (job_ctx->scenario_ids_.find(0) != job_ctx->scenario_ids_.end()) // PRD mode
	{
		Forecast_Planner::Plan_Context context(repo, &(job_ctx->feedback_), job_ctx->plan_id_, 0, only_fake_requests, job_ctx->predict_start_date_, event_count, vnode_count);
		context.forecast_days_ = std::min(size_t((job_ctx->predict_end_date_ - job_ctx->predict_start_date_) / 86400), job_ctx->max_forecast_days_);
		context.set_sample_dimensions(sample_dimensions);
		context.set_output_networks(output_networks);
		context.perf_ = new Perf_Counter();
		for (Ads_GUID_Set::const_iterator nit = job_ctx->closure_networks_.begin(); nit != job_ctx->closure_networks_.end(); ++nit)
		{
			Ads_GUID network_id = *nit;
			if (this->enabled_networks_.find(network_id) != this->enabled_networks_.end())
			{	
				context.networks_.insert(std::make_pair(network_id, Forecast_Planner::Network_Holder_Ptr()));
				context.nw_perf_map_.insert(std::make_pair(network_id, new Perf_Counter()));
			}
		}
		if (context.networks_.empty())
		{
			result["data"] = json::String(msg);
			ADS_LOG_RETURN((LP_INFO, "%s\n", msg.c_str()), 0);
		}
#if defined(ADS_ENABLE_AB_OFFLINE)
		context.forecast_days_ = std::max(Ads_Server::config()->forecast_days_, 0);
		context.sample_ratio_ = 1.0 / std::max(Ads_Server::config()->forecast_node_sample_ratio_, 1);
		if (planner_->build_simple_plan(context) < 0)
#else
		if (planner_->build_plan(context) < 0)
#endif
		{
			Ads_String error = Ads_String("failed to build plan for production job_ctx=") + job_ctx->to_string();
			result["error"] = json::String(error);
			ADS_LOG_RETURN((LP_ERROR, "%s\n", error.c_str()), -1);
		}
		json::ObjectP data;
		context.to_json(data);
		result["data"] = data;
		context.log_plan_performance();
		context.reset_plan_performance();
	}
	else
	{
		//INK-2668: parallel scenrio id when plan
		size_t n_threads = Ads_Server::config()->forecast_num_scenario_planner_threads_;
		Scenario_Plan_Context scenario_context(planner_, job_ctx, repo, event_count, &enabled_networks_, &sample_dimensions, &output_networks, only_fake_requests, vnode_count);
		pthread_t *thread_ids = new pthread_t[n_threads];
		for (size_t i = 0; i < n_threads; ++i)
			::pthread_create(&thread_ids[i], 0, &Forecast_Service::scenario_plan_helper, &scenario_context);
		for (Ads_Network_Map::const_iterator it = repo->networks_.begin(); it != repo->networks_.end(); ++it)
		{
			scenario_context.network_queue_.enqueue(it->second);
		}
		scenario_context.network_queue_.enqueue((const Ads_Network*) -1); 
		for (size_t i = 0; i < n_threads; ++i)
			::pthread_join(thread_ids[i], 0);
		delete []thread_ids;
	}

	for (std::map<Ads_GUID, Forecast_Store*>::const_iterator it = stores_.begin(); it != stores_.end(); ++it)
	{
		Forecast_Store* store = it->second;
		store->close_inventory();
	}
	
	return 0;
}
 
//INK-2668
void*
Forecast_Service::scenario_plan_helper(void* ctx)
{
	Scenario_Plan_Context& scenario_context = *(reinterpret_cast<Scenario_Plan_Context*>(ctx));
	while (true)
	{
		const Ads_Network* network = NULL;
		if (scenario_context.network_queue_.dequeue(network) < 0 || !network) break;
		if (network == (const Ads_Network*) -1)
		{
			scenario_context.network_queue_.enqueue(network);
			break;
		}
		if (network->type_ != Ads_Network::NORMAL) continue;
		if (!Ads_Server::config()->forecast_network_enabled(network->id_)) continue;
		if (!network->config()->forecast_enable_scenario_forecasting_with_testing_) continue;
		if (network->config()->forecast_scenarios_.empty()) continue;
		ADS_DEBUG((LP_INFO, "build plan %ld for network_id %s\n", scenario_context.job_ctx_->plan_id_, ADS_ENTITY_ID_CSTR(network->id_)));
		Forecast_Planner::Plan_Context context(scenario_context.repo_, &(scenario_context.job_ctx_->feedback_), scenario_context.job_ctx_->plan_id_, 0, scenario_context.only_fake_requests_, scenario_context.job_ctx_->predict_start_date_, scenario_context.event_count_, scenario_context.vnode_count_);
		context.forecast_days_ = forecast_days(network);
		context.set_sample_dimensions(*scenario_context.sample_dimensions_);
		context.set_output_networks(*scenario_context.output_networks_);
		context.perf_ = new Perf_Counter();
		// only build plan for CRO networks
		for (Ads_GUID_RSet::const_iterator nit = network->config()->network_ids_in_upstream_.begin(); nit != network->config()->network_ids_in_upstream_.end(); ++nit)
			// if (job_ctx->closure_networks_.find(*nit) != job_ctx->closure_networks_.end())
			if (scenario_context.enabled_networks_->find(*nit) != scenario_context.enabled_networks_->end())
			{
				context.networks_.insert(std::make_pair(*nit, Forecast_Planner::Network_Holder_Ptr()));
				context.nw_perf_map_.insert(std::make_pair(*nit, new Perf_Counter()));
			}
		if (context.networks_.empty())
		{
			ADS_LOG((LP_INFO, "WARNING: empty closure networks to plan for nw=%s\n", ads::entity_id_to_str(network->id_).c_str()));
			continue;
		}
		for (Ads_Forecast_Scenario_List::const_iterator s_it = network->config()->forecast_scenarios_.begin(); s_it != network->config()->forecast_scenarios_.end(); ++s_it)
		{
			Ads_GUID scenario_id = (*s_it)->id_;
			context.scenario_id_ = scenario_id;
			ADS_LOG((LP_INFO, "network %d, scenario %d started\n", ads::entity::id(network->id_), scenario_id));
			if (scenario_context.planner_->build_plan(context) < 0)
			{
				ADS_LOG((LP_ERROR, "failed to build plan %ld for scenario %ld\n", context.plan_id_, context.scenario_id_));
			}
			context.log_plan_performance();
			context.reset_plan_performance();
		}
	}
	return 0;
}

bool
Forecast_Service::AF_Job_Context::update_network_cache(std::vector<Ads_String>& vnode_vec)
{
	//add vnodes
	for(unsigned int i = 0; i < vnode_vec.size(); i++)
	{
		if(!vnode_vec[i].empty())
			vnode_set_.insert(Ads_String("vnode") + vnode_vec[i]);
	}

	struct timeval start, end;
	gettimeofday(&start, NULL);
	if(flags_ & NIGHTLY)
	{
		//update network cache for normal nightly and scenario
		int64_t cur_version = 0;
		for(unsigned int i = 0; i < vnode_vec.size(); i++)
		{
			if(!vnode_vec[i].empty())
			{
				Ads_String vnode = Ads_String("vnode") + vnode_vec[i];
				for (Ads_GUID_Set::const_iterator it = scenario_ids_.begin(); it != scenario_ids_.end(); ++it)
				{
					Ads_GUID scenario_id = *it;
					Network_Cache* network_cache = NULL;
					if(network_caches_.end() == network_caches_.find(scenario_id))
					{
						network_cache = new Network_Cache();
						network_caches_[scenario_id] = network_cache;
					}
					network_cache = network_caches_[scenario_id];

					Scenario_Plan_Info_Map::const_iterator pit = plan_infos_.find(scenario_id);
					if (pit == plan_infos_.end()) 
					{
						ADS_LOG((LP_ERROR, "[update_network_cache] scenario_id=%ld, empty plan\n", scenario_id));
						continue;
					}
					Plan_Info &plan_info = plan_infos_[scenario_id];
					time_t plan_id = plan_info.plan_id_;
					Ads_String file = ads::path_join(Ads_Server::config()->forecast_plan_dir_, ads::i64_to_str(plan_id) + '_' + ads::i64_to_str(timezone_), ads::i64_to_str(scenario_id));
					for ( auto nit = plan_info.network_plan_infos_.begin(); nit != plan_info.network_plan_infos_.end(); ++nit)
					{
						//update index_map_ in Network_Cache
						Ads_String path = ads::path_join(file, ads::entity_id_to_str(nit->first), vnode + ".index");
						network_cache->update_index(path, nit->first, vnode, ads::i64_to_str(cur_version));
					}
				}
			}
		} 
	}
	else 
	{
		//update network cache for ondemand
		int64_t max_version = model_version_;
		Fast_Simulate_Config* fast_conf = reinterpret_cast<Fast_Simulate_Config*>(fast_conf_);
		Ads_String model_id = fast_conf->model_id_;
		Ads_GUID scenario_id = 0;
		Network_Cache* network_cache = NULL;
		if(network_caches_.end() == network_caches_.find(scenario_id))
		{
			network_cache = new Network_Cache();
			network_caches_[scenario_id] = network_cache;
		}
		network_cache = network_caches_[scenario_id];
		for(unsigned int i = 0; i < vnode_vec.size(); i++)
		{
			if(!vnode_vec[i].empty())
			{
				Ads_String vnode = Ads_String("vnode") + vnode_vec[i];
				for (int64_t cur_version = 0; cur_version <= max_version; ++cur_version)
				{
					for (Ads_GUID_Set::const_iterator nit = closure_networks_.begin(); nit != closure_networks_.end(); ++nit)
					{
						//update network_set_ in Network_Cache
						Forecast_Store* store = Forecast_Service::instance()->get_store(*nit);
						Ads_GUID resellable_network = store->network_id();
						Ads_String path = store->get_od_model_filename(model_id, vnode, cur_version);
						path = ads::replace("req.gz", "network", path);
						network_cache->update_network(path, resellable_network, vnode, ads::i64_to_str(cur_version));

						//update index_map_ in Network_Cache
						path = ads::replace("network", "index", path);
						network_cache->update_index(path, resellable_network, vnode, ads::i64_to_str(cur_version));
					}
				}
			}
		} 
	}
	gettimeofday(&end, NULL);
	ADS_LOG((LP_INFO, "[update_network_cache] total cost: %ld\n", (end.tv_sec - start.tv_sec)*1000000 + end.tv_usec - start.tv_usec));
	return true;
}

int
Forecast_Service::AF_Job_Context::merge_wimas_result(void* simulate_ctx)
{
	if (flags_ & (FAST_SIMULATE_WIMAS | FLAG_COLLECT_STATS))
	{
		ads::Guard __g(global_ctx_mutex_);
		Forecast_Simulator::Context* sim_ctx = reinterpret_cast<Forecast_Simulator::Context*>(simulate_ctx);
		reinterpret_cast<Forecast_Simulator::Context*>(global_ctx_)->merge(*sim_ctx);
	}
	return 0;
}

Forecast_Service::AF_Job_Context::~AF_Job_Context()
{
	destroy_running(); //called in end_simulate
	stop_aggregate();
	log_performance();
	if (fast_conf_) delete reinterpret_cast<Fast_Simulate_Config*>(fast_conf_);
	if (collect_guard_) delete collect_guard_;
	if (repo_guard_) delete repo_guard_;
	for(auto it = network_caches_.begin(); it != network_caches_.end(); ++it)
		delete it->second;
}

BEGIN_FORECAST_DISPATCHER(prepare_nightly)
	Forecast_Service::instance()->prepare_nightly(req, result);
END_FORECAST_DISPATCHER

void Forecast_Service::prepare_nightly(Ads_Request* req, json::ObjectP& result)
{
	const Ads_String& plan_name = req->p("plan_id");
	if (plan_name.empty())
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String("Please specify plan_id for nightly job");
		return;
	}
	// TODO this part will be discussed in 6.12, and we could let collect to parallel run with nightly jobs
	Collect_Guard* collect_guard = new Collect_Guard(this, true);
	//INK-10078
	//switch repa
	time_t plan_id = ads::str_to_i64(plan_name);
	if (req->p("load_new_repa") == "1")
		plan_id_to_repa_path_.erase(plan_id);
	Ads_Pusher_Service::instance()->load_repository(plan_id);
	{
		Ads_Server::Repository_Guard __g(Ads_Server::instance());
		const Ads_Repository* repo = __g.asset_repository();
		if (!repo || repo->networks().size() == 0) //invalid repo
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			result["error"] = json::String("repo is error");
			delete collect_guard;
			return;
		}
		result["repo_file"] = json::String(ads::basename(Ads_Pusher_Service::instance()->current_mapped_repa_file().c_str()));
	}
	Forecast_Service::AF_Job_Context* job_ctx = Forecast_Service::instance()->initialize_job_context(req, result);
	if (!job_ctx) { delete collect_guard; return; }
	job_ctx->collect_guard_ = collect_guard;
	job_ctx->flags_ |= AF_Job_Context::NIGHTLY;

	if (!req->p("enabled_networks").empty())
	{
		job_ctx->flags_ |= AF_Job_Context::NIGHTLY_TIMEZONE_SPECIFIC;
		ads::str_to_entities(req->p("enabled_networks"), ADS_ENTITY_TYPE_NETWORK, 0, std::inserter(job_ctx->original_enabled_networks_, job_ctx->original_enabled_networks_.begin()));
	}
	job_ctx->max_forecast_days_ = Forecast_Service::max_forecast_days(job_ctx, req->p("enabled_networks"));
	job_ctx->predict_end_date_ = std::min(job_ctx->predict_end_date_, time_t(job_ctx->predict_start_date_ + job_ctx->max_forecast_days_ * 86400));
	json::ObjectP data;
	job_ctx->to_json(data);
	result["data"] = data;

	ADS_LOG((LP_INFO, "job context initialized %s\n", job_ctx->to_string().c_str()));
	if (job_ctx->flags_ & AF_Job_Context::FLAG_PRE_AGGREGATE)
	{
		Forecast_Aggregate_Task* task = new Forecast_Aggregate_Task(job_ctx);
		if (aggregate_manager_->add_task(task) < 0)
		{
			delete task;
			ADS_LOG((LP_ERROR, "[REQUESTS] job=%s, cannot add aggregate task to manager\n", job_ctx->name().c_str()));
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			result["error"] = json::String("failed to add aggregate task");
			return;
		}
	}
	{
		bool skip_feedback = (req->p("skip_feedback") == "1" || req->p("skip_feedback") == "true");
		time_t start = (std::min(::time(NULL), job_ctx->predict_start_date_) / 86400 - 8) * 86400;
		{
			ads::Guard __g(this->feedback_mutex_);
			if (!skip_feedback)
			{
				if (this->reload_feedback(this->feedback_, start, start + 7 * 86400) < 0)
				{
					result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
					result["error"] = json::String("reload feedback error");
					return;
				}
			}
			job_ctx->feedback_ = this->feedback_;
		}
		{
			ads::Guard __g(this->external_feedback_mutex_);
			if (!skip_feedback)
			{
				if (this->reload_external_feedback(this->external_feedback_, start, start + 7* 86400) < 0)
				{
					result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
					result["error"] = json::String("reload external feedback error");
					return;
				}
			}
			job_ctx->external_feedback_ = this->external_feedback_;
		}
	}
	{
		Ads_Server::Repository_Guard __g(Ads_Server::instance());
		job_ctx->nightly_time_loaded_ = __g.asset_repository()->time_loaded();
	}
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);

}

Forecast_Service::AF_Job_Context* 
Forecast_Service::initialize_job_context(Ads_Request* req, json::ObjectP& result)
{
	Ads_String plan_name = req->p("job_tag").empty() ? req->p("plan_id") : req->p("job_tag");
	if (plan_name.empty())
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String("Please specify plan_id or job_tag for od to initialize job context.");
		return NULL;
	}
	Ads_String err = Ads_String("job context for ") + plan_name + " already exists. Please use force_restart=1 to remake job context.";
	bool force_restart = (req->p("force_restart") == "1");
	AF_Job_Context* job_ctx = new AF_Job_Context();
	if (!req->p("job_tag").empty())
	{
		ads::Guard __g(od_simulate_jobs_mutex_);
		std::map<Ads_String, AF_Job_Context*>::iterator it = this->od_simulate_jobs_.find(plan_name);
		if (it != this->od_simulate_jobs_.end())
		{
			if (!force_restart)
			{
				ADS_LOG((LP_ERROR, "%s\n", err.c_str()));
				delete job_ctx;
				result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
				result["error"] = json::String(err);
				return NULL;
			}
			else
			{
				ADS_LOG((LP_INFO, "destroy pre-existing job_ctx %s\n", plan_name.c_str()));
				delete it->second;
			}
		}
		this->od_simulate_jobs_[plan_name] = job_ctx;
	}
	else
	{
		ads::Guard __g(nightly_simulate_jobs_mutex_);
		std::map<Ads_String, AF_Job_Context*>::iterator it = this->nightly_simulate_jobs_.find(plan_name);
		if (it != this->nightly_simulate_jobs_.end())
		{
			if (!force_restart)
			{
				ADS_LOG((LP_ERROR, "%s\n", err.c_str()));
				delete job_ctx;
				result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
				result["error"] = json::String(err);
				return NULL;
			}
			else
			{
				ADS_LOG((LP_INFO, "destroy pre-existing job_ctx %s\n", plan_name.c_str()));
				delete it->second;
			}
		}
		this->nightly_simulate_jobs_[plan_name] = job_ctx;
		this->stats_.clean_slow_pairs();
	}
	if (!req->p("plan_id").empty()) job_ctx->plan_id_ = ads::str_to_i64(req->p("plan_id"));
	if (!req->p("job_tag").empty()) job_ctx->job_tag_ = req->p("job_tag");
	if (!req->p("scenario_id").empty()) job_ctx->current_scenario_id_ = ads::str_to_i64(req->p("scenario_id")); //it is only used when simulate disturb
		
	parse_enabled_networks(req, job_ctx->closure_networks_, job_ctx->enabled_networks_, job_ctx->excluded_networks_);
	get_timezone_info(job_ctx->gmt_offsets_);

	// For ab_offline
	if (!req->p("ab_test_offline_ids").empty())
	{
		ads::str_to_entities(req->p("ab_test_offline_ids"), 0, 0, std::inserter(job_ctx->scenario_ids_, job_ctx->scenario_ids_.begin()));
	}
	else if (req->p("is_scenario") == "1" || req->p("is_ab_test_offline") == "1")
	{
		Ads_Server::Repository_Guard __g(Ads_Server::instance());
		const Ads_Repository* repo = __g.asset_repository();
#if defined(ADS_ENABLE_AB_OFFLINE)
		for (const auto& collections : {repo->ab_test_collections_, Forecast_Service::instance()->offline_ab_test_collections_})
		{
			for (const auto& collection : collections)
			{
				if (collection.second->get_metadata(COLLECTION_META_KEY_MODEL) != "do") continue;

				for (const auto& bucket : collection.second->buckets_)
				{
					job_ctx->scenario_ids_.insert(bucket.first);
					ADS_LOG((LP_INFO, "[AB OFFLINE] load bucket id %s, insert into job_ctx->scenario_ids_\n", ADS_ENTITY_ID_CSTR(bucket.first)));
				}
			}
		}
		job_ctx->scenario_ids_.insert(0); // RRD scenario
#else
		for (Ads_Network_Map::const_iterator it = repo->networks_.begin(); it != repo->networks_.end(); ++it)
		{
			const Ads_Network* network = it->second;
			if (network->type_ != Ads_Network::NORMAL) continue;
			if (!Ads_Server::config()->forecast_network_enabled(it->first)) continue;
			if (!network->config()->forecast_enable_scenario_forecasting_with_testing_) continue;
			for (Ads_Forecast_Scenario_List::const_iterator s_it = network->config()->forecast_scenarios_.begin(); s_it != network->config()->forecast_scenarios_.end(); ++s_it)
				job_ctx->scenario_ids_.insert((*s_it)->id_);
		}
#endif
	}
	else job_ctx->scenario_ids_.insert(0);

	if (req->p("predict_start_date").empty()) job_ctx->predict_start_date_ = ::time(NULL) / 86400 * 86400;
	else job_ctx->predict_start_date_ = ads::str_to_time(req->p("predict_start_date").c_str(), "%Y%m%d") / 86400 * 86400;
	job_ctx->predict_end_date_ = job_ctx->predict_start_date_ + FORECAST_MAX_SIMULATE_DAY * 86400;
	if (!req->p("predict_end_date").empty()) job_ctx->predict_end_date_ = ads::str_to_time(req->p("predict_end_date").c_str(), "%Y%m%d") / 86400 * 86400;

	if (!req->p("resume_start_date").empty())
	{
		job_ctx->flags_ |= AF_Job_Context::NIGHTLY_RESTARTED; 
		job_ctx->resume_start_date_ = ads::str_to_time(req->p("resume_start_date").c_str(), "%Y%m%d") / 86400 * 86400;
	}
	else job_ctx->resume_start_date_ = job_ctx->predict_start_date_;
	if (req->p("fast_nightly") == "true") job_ctx->fast_nightly_ = true;
	Ads_String_List workers;
	if (!req->p("active_workers").empty())
	{
		ads::split(req->p("active_workers"), workers, ',');
		for (const auto& worker : workers)
		{
			if (Ads_Server::config()->forecast_active_workers_.find(worker) != Ads_Server::config()->forecast_active_workers_.end())
				job_ctx->active_workers_.insert(worker);
		}
	}
	else
	{
		job_ctx->active_workers_.insert(Ads_Server::config()->forecast_active_workers_.begin(), Ads_Server::config()->forecast_active_workers_.end());
	}
	//INK-9578 for switched to redis, all simulate output filename
	{
		job_ctx->simulate_output_filename_["transactional_competing"] = false;
		job_ctx->simulate_output_filename_["transactional_scheduled_competing"] = false;
		job_ctx->simulate_output_filename_["transactional_exclusivity_competing"] = false;
		job_ctx->simulate_output_filename_["portfolio_competing"] = false;
		job_ctx->simulate_output_filename_["transactional"] = true;
		job_ctx->simulate_output_filename_["transactional_rule"] = true;
		job_ctx->simulate_output_filename_["hourly_transactional"] = true;
		job_ctx->simulate_output_filename_["transactional_rbb"] = false;
		job_ctx->simulate_output_filename_["portfolio"] = false;
		job_ctx->simulate_output_filename_["custom_portfolio"] = false;
		job_ctx->simulate_output_filename_["custom_portfolio_rule"] = false;
		job_ctx->simulate_output_filename_["custom_portfolio_competing"] = false;
	}
	job_ctx->repo_guard_ = new Ads_Server::Repository_Guard(Ads_Server::instance());
	if (req->p("pre_aggregate") == "1" || req->p("pre_aggregate") == "true")
		job_ctx->flags_ |= AF_Job_Context::FLAG_PRE_AGGREGATE;
	return job_ctx;
}

int Forecast_Service::exchange_feedback(time_t start, time_t end)
{
	ADS_LOG((LP_INFO, "Exchange Feedback begin, start date:%s, end date:%s\n", ads::string_date(start).c_str(), ads::string_date(end).c_str()));
	Ads_String name = "feedback";
	std::list<Forecast_Store*> stores;
	this->list_store(stores);
	Ads_String root_dir(ads::path_join(Ads_Server::config()->forecast_exchange_dir_, name)); 
	for (time_t date = start; date < end; date += 86400)
	{
		for(std::list<Forecast_Store*>::const_iterator it = stores.begin(); it != stores.end(); ++it)
		{
			Scenario_Feedback_Map feedback;
			(**it).load_feedback(date, date + 86400, feedback);
			(**it).write_feedback(root_dir, date, feedback);			
		}
	}
	ADS_LOG((LP_INFO, "Exchange Feedback end, start date:%s, end date:%s\n", ads::string_date(start).c_str(), ads::string_date(end).c_str()));
	return 0;
}

BEGIN_FORECAST_DISPATCHER(exchange_feedback)

	time_t end = ::time(NULL) / 86400 * 86400;
	if (!req->p("end_date").empty())
		end = ads::str_to_time(req->p("end_date").c_str(), "%Y%m%d");
	time_t start = end - 86400;
	if (!req->p("start_date").empty())
		start = ads::str_to_time(req->p("start_date").c_str(), "%Y%m%d");
	if (start >= end)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String(Ads_String("start_date >= end_date : ") + ads::string_date(start) + " >= " + ads::string_date(end));
		return -1;
	}
	Forecast_Service::instance()->exchange_feedback(start, end);
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
END_FORECAST_DISPATCHER

int Forecast_Service::exchange_external_feedback(time_t start, time_t end)
{
	ADS_LOG((LP_INFO, "Exchange External Feedback begin, start date:%s, end date:%s\n", ads::string_date(start).c_str(), ads::string_date(end).c_str()));

	const Ads_String name = "external_feedback";

	std::list<Forecast_Store*> stores;
	this->list_store(stores);

	const Ads_String root_dir( ads::path_join(Ads_Server::config()->forecast_exchange_dir_, name) );

	for (time_t date = start; date < end; date += 86400)
	{
		for(std::list<Forecast_Store*>::const_iterator it = stores.begin(); it != stores.end(); ++it)
		{
			Scenario_External_Feedback_Map scenario_external_feedback;
			(**it).load_external_feedback(date, date + 86400, scenario_external_feedback);
			(**it).write_external_feedback(root_dir, date, scenario_external_feedback);
		}
	}

	ADS_LOG((LP_INFO, "Exchange External Feedback end, start date:%s, end date:%s\n", ads::string_date(start).c_str(), ads::string_date(end).c_str()));
	return 0;
}

BEGIN_FORECAST_DISPATCHER(exchange_external_feedback)

	time_t end = ::time(NULL) / 86400 * 86400;
	if (!req->p("end_date").empty())
		end = ads::str_to_time(req->p("end_date").c_str(), "%Y%m%d");
	time_t start = end - 86400;
	if (!req->p("start_date").empty())
		start = ads::str_to_time(req->p("start_date").c_str(), "%Y%m%d");
	if (start >= end)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String(Ads_String("start_date >= end_date : ") + ads::string_date(start) + " >= " + ads::string_date(end));
		return -1;
	}
	Forecast_Service::instance()->exchange_external_feedback(start, end);
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
END_FORECAST_DISPATCHER

BEGIN_FORECAST_DISPATCHER(start_simulate)
	Forecast_Service::AF_Job_Context* job_ctx = Forecast_Service::instance()->get_job_context(req, result);
	if (!job_ctx) return -1;
	{
	ads::Guard __g(job_ctx->perf_.mu_);
	job_ctx->perf_["daily_finish"] = ads::gettimeofday(); //init stat
	}
	if (! req->p("scenario_id").empty()) job_ctx->current_scenario_id_ = ads::str_to_i64(req->p("scenario_id"));
	else job_ctx->current_scenario_id_ = 0;
	// For ab_offline
	if (! req->p("ab_test_bucket_id").empty()) job_ctx->current_scenario_id_ = ads::str_to_i64(req->p("ab_test_bucket_id"));

	if (job_ctx->current_scenario_id_ != 0) //is_scenario
	{
		Ads_Server::Repository_Guard __g(Ads_Server::instance());
		const Ads_Repository* repo = __g.asset_repository();
#if defined(ADS_ENABLE_AB_OFFLINE)
		for (const auto& ab_c : repo->ab_test_collections_)
		{
			for (const auto& ab_b : ab_c.second->buckets_)
				if (ab_b.first == job_ctx->current_scenario_id_) job_ctx->current_network_id_ = ab_c.second->network_id_;
		}
#else
		const Ads_Forecast_Scenario* forecast_scenario = NULL;
		if (repo->find_forecast_scenario(job_ctx->current_scenario_id_, forecast_scenario) >= 0 && forecast_scenario
			&& ads::entity::is_valid_id(forecast_scenario->network_id_))
			job_ctx->current_network_id_ = forecast_scenario->network_id_;
#endif
	}
	int ret = Forecast_Service::instance()->start_simulate(job_ctx, req, result);
	Forecast_Service::generate_response(ret, NULL, result);
END_FORECAST_DISPATCHER

Ads_String 
Forecast_Service::AF_Job_Context::to_string() const
{
	Ads_String fast_conf = (fast_conf_ ? reinterpret_cast<Fast_Simulate_Config*>(fast_conf_)->to_string() : "");
	Ads_String scenarios;
	for (Ads_GUID_Set::const_iterator it = scenario_ids_.begin(); it != scenario_ids_.end(); ++it)
		scenarios += ads::i64_to_str(*it) + ",";
	char buf[8192*2];
	snprintf(buf, sizeof(buf),
			" [name=%s, flag=%ld, scenario_ids=%s, prediction_days=%ld, predict_start_date=%s, predict_end_date=%s, resume_start_date=%s, enabled_networks=%s excluded_networks=%s closure_networks=%s fast_conf=%s",
			name().c_str(), flags_, scenarios.c_str(), (max_forecast_days_ == (size_t)-1) ? -1 : max_forecast_days_,
			ads::string_date(predict_start_date_).c_str(), ads::string_date(predict_end_date_).c_str(), ads::string_date(resume_start_date_).c_str(),
			ads::entities_to_str(enabled_networks_).c_str(), ads::entities_to_str(excluded_networks_).c_str(), ads::entities_to_str(closure_networks_).c_str(),
			fast_conf.c_str());
	return Ads_String(buf);
}

void
Forecast_Service::AF_Job_Context::to_json(json::ObjectP& o) const
{
	o["name"] = json::String(name());
	o["predict_start_date"] = json::String(ads::string_date(predict_start_date_));
	o["predict_end_date"] = json::String(ads::string_date(predict_end_date_));
	o["resume_start_date"] = json::String(resume_start_date_ > 0 ? ads::string_date(resume_start_date_) : "");
	o["flags"] = json::Number(flags_);
	o["n_daily_simulate_threads"] = json::Number(n_daily_simulate_threads_);
	o["enabled_networks"] = json::String(ads::entities_to_str(enabled_networks_));
	o["excluded_networks"] = json::String(ads::entities_to_str(excluded_networks_));
	o["closure_networks"] = json::String(ads::entities_to_str(closure_networks_));
	json::ArrayP scenarios;
	for (Ads_GUID_Set::const_iterator it = scenario_ids_.begin(); it != scenario_ids_.end(); ++it) scenarios.Insert(json::Number(*it));
	o["scenario_ids"] = scenarios;
	o["fast_conf"] = json::String(fast_conf_ ? reinterpret_cast<Fast_Simulate_Config*>(fast_conf_)->to_string() : "");
	o["pre_aggregate_start_date"] =  json::Number(predict_start_date_);
	json::ArrayP workers;
	for (const auto& worker : active_workers_) workers.Insert(json::String(worker));
	o["active_workers"] = workers; 
}

int
Forecast_Service::AF_Job_Context::prepare_running(Ads_Request* req)
{
	ADS_ASSERT((predict_start_date_ > 0));
	ADS_ASSERT((predict_end_date_ > 0));
	Ads_GUID_Set dummy;
    timezone_ = req->p("timezone").empty() ? 0 : ads::str_to_i64(req->p("timezone"));

	if (plan_id_ > 0 && (flags_ & NIGHTLY))
	{
#if defined(ADS_ENABLE_AB_OFFLINE)
		// Only run scenario_id = 0 for ab_offline
		Ads_GUID_Set scenario_ids;
		scenario_ids.insert(0);
#else
		const Ads_GUID_Set& scenario_ids = scenario_ids_;
#endif
        for (Ads_GUID_Set::const_iterator it = scenario_ids.begin(); it != scenario_ids.end(); ++it)
		{
			Plan_Info plan_info;
			time_t latest_plan_id = 0;
			if (Forecast_Service::plan_info(closure_networks_, plan_info, latest_plan_id, timezone_, *it) < 0)
			{
				if (flags_ & NIGHTLY_TIMEZONE_SPECIFIC) continue;
				ADS_LOG((LP_ERROR, "Failed to prepare context running: No plan info found for plan_id %d, scenario %d\n", plan_id_, *it));
				continue;
			}
			ADS_DEBUG((LP_TRACE, "Prepare context running: plan info %d, found for plan_id %d, scenario %d\n", latest_plan_id, plan_id_, *it));

			plan_infos_[*it] = plan_info;
		}
	}

	//update model version
	if(update_model_version() < 0)
		ADS_LOG_RETURN((LP_ERROR, "failed to update model version for job_context\n"), -1);

	//get vnode count
//	if(req->p("vnode_count").empty())
//		ADS_LOG_RETURN((LP_ERROR, "failed to get vnode count\n"), -1);
//	vnode_count_ = ads::str_to_i64(req->p("vnode_count"));

	// add vnode info to job context
	if(!req->p("vnodes").empty())
	{
		std::vector<Ads_String> vnode_vec;
		ads::split(req->p("vnodes"), vnode_vec, ',');
		//update network_cache
		if(!update_network_cache(vnode_vec))
			ADS_LOG_RETURN((LP_ERROR, "failed to update network cache\n"), -1);
		ADS_LOG((LP_INFO, "update network cache, vnode : %s\n", req->p("vnodes").c_str()));
	}

	n_daily_simulate_threads_ = Ads_Server::config()->forecast_num_simulator_threads_per_job_;
	if (n_daily_simulate_threads_ <= 0 || n_daily_simulate_threads_ > (size_t)Ads_Server::config()->forecast_num_simulator_threads_)
		n_daily_simulate_threads_ = Ads_Server::config()->forecast_num_simulator_threads_;
	n_metrics_ = Ads_Server::config()->forecast_num_simulator_metrics_;
	if (n_metrics_ <= 0) // default case, one metrics per thread
		n_metrics_ = Ads_Server::config()->forecast_num_calculation_threads_;
	// load counter for nightly
	Ads_String_Pair_Vector* global_counter_addresses = nullptr;
	const Ads_String& counter_connect = req->p("counter_connect");
	if (!counter_connect.empty())
	{
		global_counter_addresses = new Ads_String_Pair_Vector();
		Forecast_Service::instance()->connect_parse(counter_connect, global_counter_addresses);
	 }	
	Ads_String counter_name = name() + "_" + ads::i64_to_str(current_scenario_id_);
	counter_service_ = Forecast_Service::instance()->get_counter(counter_name, global_counter_addresses);
	global_ctx_ = new Forecast_Simulator::Context();
	output_pending_dates_.clear();
	pre_aggregate_dates_.clear();
	start_pre_aggregate_date_ = predict_start_date_;
	if (current_scenario_id_ != 0) 
	{
		user_cookies_.clear(); //clear user cookie for scenario
		cookies_clear_time_.clear(); // YPP-191
	}
	return 0;
}

int 
Forecast_Service::destroy_aggregate(AF_Job_Context* job_ctx)
{
	ads::Guard _g(job_ctx->aggregate_dates_mu_);
	if (!job_ctx->aggregate_date_ranges_[job_ctx->current_scenario_id_]) return 0;
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	Forecast_Aggregator aggregator(repo, *job_ctx);
	time_t last_output_date = get_last_output_pending_date(job_ctx);
	if (!last_output_date) last_output_date = job_ctx->predict_start_date_; //empty plan for the scenario
	Ads_GUID_Set tmp_networks; //as flag in init_networks
	time_t aggregate_end = aggregator.aggregate_end(job_ctx, job_ctx->current_scenario_id_, tmp_networks);
	if (last_output_date < aggregate_end - 86400) //empty plan for these dates
	{
		time_t start_date = std::max(job_ctx->predict_start_date_, aggregator.portfolio_interval_month(last_output_date));
		for (time_t date = start_date; date < aggregate_end; date += 86400)
		{
			if (aggregator.is_portfolio_interval_end(date, aggregate_end))
			{
				job_ctx->aggregate_date_ranges_[job_ctx->current_scenario_id_]->enqueue(new std::pair<time_t, time_t>(start_date, date + 86400));
				ADS_LOG((LP_INFO, "enqueue empty aggregate date: <%s, %s> sceanrio=%d job=%s \n", ads::string_date(start_date).c_str(), ads::string_date(date + 86400).c_str(), job_ctx->current_scenario_id_, job_ctx->name().c_str()));
				start_date = date + 86400;
			}
		}
	}	
	job_ctx->aggregate_date_ranges_[job_ctx->current_scenario_id_]->enqueue((Date_Pair*)-1);
	return 0;
}

int Forecast_Service::clear_load_request_cache(AF_Job_Context* job_ctx)
{
	ADS_LOG((LP_INFO, "[REQUESTS] job=%s\n", job_ctx->name().c_str()));
	//remove waiting task
	load_request_manager_->remove_waiting_tasks(job_ctx->name());
	// clean data in queue
	load_request_manager_->wait(job_ctx->name());
	{
		ads::Guard _g(job_ctx->load_request_mutex_);
		for (const auto& it: job_ctx->load_request_cache_)
		{
			if (it.second && !it.second->is_empty())
			{
				ADS_LOG((LP_INFO, "[REQUESTS] job=%s, date=%s: clean up data in cache\n", job_ctx->name().c_str(), ads::string_date(it.first).c_str()));
				Forecast_Batch* batch = NULL;
				while (it.second->dequeue(batch, true, false) >= 0 && batch)
				{
					batch->destroy_elements();
					batch = NULL;
				}
				delete it.second;
			}
		}
		job_ctx->load_request_cache_.clear();
	}
	return 0;
}

int Forecast_Service::AF_Job_Context::destroy_running()
{
	ADS_LOG((LP_INFO, "destory running for %s\n", name().c_str()));

	// clean data in queue
	Forecast_Service::instance()->clear_load_request_cache(this);

	Forecast_Service::instance()->simulate_manager_->wait(name());
	Forecast_Service::instance()->calculate_metric_manager_->wait(name());
	Forecast_Service::instance()->output_pending_manager_->wait(name());
	//OPP-2175
	ADS_LOG((LP_INFO, "job=%s: output_job finished\n", name().c_str()));
	Forecast_Service::instance()->pre_aggregate_manager_->wait(name());
	ADS_LOG((LP_INFO, "job=%s: pre_aggregate_job finished\n", name().c_str()));
	if (global_ctx_)
	{
		ads::Guard __g(global_ctx_mutex_);
		Forecast_Simulator::Context* global_ctx = reinterpret_cast<Forecast_Simulator::Context*>(global_ctx_);
		if (flags_ & AF_Job_Context::FAST_SIMULATE_WIMAS)
		{
			json::ObjectP p;
			global_ctx->wimas_to_json(p);
			json_result_["reject_reason"] = p;
		}

		if (flags_ & AF_Job_Context::FLAG_COLLECT_STATS)
		{
			json::ObjectP p;
			global_ctx->stats_to_json(p);
			json_result_["total"] = p;
		}
		delete global_ctx;
		global_ctx_ = 0;
	}
	if (counter_service_)
	{
		Forecast_Service::instance()->destroy_counter(name() + "_" + ads::i64_to_str(current_scenario_id_));
		counter_service_ = NULL;
	}
	return 0;
}

const Ads_String &
Forecast_Service::AF_Job_Context::get_pending_prefix(Network_Pending_Path_Map* pending_paths, Ads_GUID network_id)
{
	Network_Pending_Path_Map::const_iterator it = pending_paths->find(network_id);
	if (it != pending_paths->end()) return it->second;
	Ads_String prefix = ads::path_join(Ads_Server::config()->forecast_pending_dir_,this->name(), Ads_Server::config()->forecast_server_);
	prefix = ads::path_join(prefix, ads::u64_to_str(this->current_scenario_id_), ads::entity_id_to_str(network_id));
	return pending_paths->insert(std::make_pair(network_id, prefix)).first->second;
}


/*
 * update model version
 * 	1. write model while run nightly
 * 	2. read model while run ondemand
 */
int64_t
Forecast_Service::AF_Job_Context::update_model_version()
{
	if (this->current_scenario_id_ > 0) return 0;
	/* read model version from file and update it */
	Ads_String model_version_file;
	if(fast_conf_ == NULL){ //nightly
		model_version_file = ads::path_join(Ads_Server::config()->forecast_working_dir_, "model", ads::i64_to_str(plan_id_), ads::i64_to_str(plan_id_)+".version");
	}else{  //od
		model_version_file = ads::path_join(Ads_Server::config()->forecast_working_dir_, "store", ((struct Fast_Simulate_Config*)fast_conf_)->model_id_, ((struct Fast_Simulate_Config*)fast_conf_)->model_id_+".version");
	}
	FILE *fp = fopen(model_version_file.c_str(), "r");
	if(!fp)
	{
		ADS_LOG((LP_ERROR, "cannot open : %s for read\n", model_version_file.c_str()));
		ADS_DEBUG((LP_INFO, "model_version_file: %s, model_version_ : %ld\n", model_version_file.c_str(), model_version_));
		return -1;
	}
	fscanf(fp, "%ld", &model_version_);
	fclose(fp);
	ADS_DEBUG((LP_INFO, "model_version_file: %s, model_version_ : %ld\n", model_version_file.c_str(), model_version_));
	return model_version_;
}

//delete items which key >= date in index file
int
Forecast_Service::AF_Job_Context::reset_index_file(Forecast_Store* store)
{
	int64_t reset_date = resume_start_date_;
	Ads_String path = ads::path_join(store->prefix(), "model_w");
	std::map<Ads_String, std::pair<Ads_String, int64_t>> path_vnode_version_map;
	if(store->list_model_file(path, path_vnode_version_map) < 0)
		return -1;

	for(std::map<Ads_String, std::pair<Ads_String, int64_t>>::iterator it = path_vnode_version_map.begin(); it != path_vnode_version_map.end(); ++it)
	{
		Ads_String index_path = ads::replace("req.gz", "index", it->first); 
		Ads_String index_path_new = ads::replace("index", "index.new", index_path);
		FILE *fin = fopen(index_path.c_str(), "r");
		if(!fin)
			return -1;
		FILE *fin_new = fopen(index_path_new.c_str(), "w");
		if (!fin_new)
		{
			ADS_LOG((LP_ERROR, "open %s for write error\n", index_path_new.c_str()));
			fclose(fin);
			return -1;
		}
		char date[16] = {0};
		size_t start = 0;
		size_t end = 0;
		size_t size = 0;
		while (fscanf(fin, "%s\t%ld\t%ld\t%ld\n", date, &start, &end, &size) != EOF)
		{
			if(ads::str_to_time(date, "%Y%m%d") >= reset_date)
				continue;
			fprintf(fin_new, "%s\t%ld\t%ld\t%ld\n", date, start, end, size);
		}
		fclose(fin);
		fclose(fin_new);
		::rename(index_path_new.c_str(), index_path.c_str());
		ADS_LOG((LP_INFO, "reset index file %s ok\n", index_path.c_str()));
	}
	return 0;
}

bool
Forecast_Service::AF_Job_Context::filtered(const Ads_Advertisement* ad, time_t fc_date, bool is_competing_or_portfolio)
{
	if (! is_competing_or_portfolio) return false;
	// don't output for excluded networks
	if (!this->excluded_networks_.empty() && this->excluded_networks_.find(ad->network_id_) != this->excluded_networks_.end()) return true;
	// don't output for scenario/OD's non owner network
	if (ads::entity::is_valid_id(current_network_id_) && current_network_id_ != ad->network_id_) return true;
	// don't output for OD's non self ads
	Fast_Simulate_Config* fast_conf = NULL;
	if (fast_conf_) fast_conf = reinterpret_cast<Fast_Simulate_Config*>(fast_conf_);
	if (fast_conf && fast_conf->original_set_.find(ad->id_) == fast_conf->original_set_.end()) return true;
	if (predict_start_date_ + Forecast_Service::instance()->forecast_days(ad->network_id_) * 86400 <= fc_date) return true;
	return false;
}

bool
Forecast_Service::AF_Job_Context::filtered(const Forecast_Metrics::P_Key& p_key, time_t fc_date)
{
	if (!this->excluded_networks_.empty() && this->excluded_networks_.find(p_key.network_id()) != this->excluded_networks_.end()) return true;
	if (ads::entity::is_valid_id(current_network_id_) && current_network_id_ != p_key.network_id()) return true;
	Fast_Simulate_Config* fast_conf = NULL;
	if (fast_conf_) fast_conf = reinterpret_cast<Fast_Simulate_Config*>(fast_conf_);
	if (fast_conf && fast_conf->original_set_.find(p_key.pretty_id_) == fast_conf->original_set_.end()) return true;
	if (predict_start_date_ + Forecast_Service::instance()->forecast_days(p_key.network_id()) * 86400 <= fc_date) return true;
	return false;
}

bool
Forecast_Service::AF_Job_Context::filtered(const Forecast_Metrics::Custom_P_Key& custom_p_key, time_t fc_date)
{
	if (!this->excluded_networks_.empty() && this->excluded_networks_.find(custom_p_key.network_id()) != this->excluded_networks_.end()) return true;
	if (ads::entity::is_valid_id(current_network_id_) && current_network_id_ != custom_p_key.network_id()) return true;
	Fast_Simulate_Config* fast_conf = NULL;
	if (fast_conf_) fast_conf = reinterpret_cast<Fast_Simulate_Config*>(fast_conf_);
	if (fast_conf && fast_conf->original_set_.find(custom_p_key.ad_->placement_->id_) == fast_conf->original_set_.end()) return true; // TODO in OD: need to add dimension judgetment here
	if (predict_start_date_ + Forecast_Service::instance()->forecast_days(custom_p_key.network_id()) * 86400 <= fc_date) return true;
	return false;
}


/*
		|			|  						|		|      
		|			|						|		|
 	 GMT+0  		GMT+8  		          GMT-8    GMT+0  
 				gmtoffset +8*60*60)   	-8*60*60

gmt_offsets_:-12*60*60, -11*a60*60, ... , 11*60*60, 12*60*60, 13*60*60(ignored) 	
*///TODO lxu UT
void
Forecast_Service::AF_Job_Context::get_interval(int virtual_time, Interval& interval)
{
	int seconds = virtual_time % 86400;
	int gmt_index = seconds > 43200 ? seconds - 86400 : seconds;
	for (const auto& label : gmt_offsets_)
	{
		if (gmt_index <= label) { //gmt_index never eauql to -12*60*60
			interval.second = label;
			break;
		}
		interval.first = label;
	}
}
bool
Forecast_Service::AF_Job_Context::output_simulate_metrics_files(time_t fc_date, bool is_reload)
{
	bool ret = true;
	//full all output files to pendings
	////1. transactional -> closure
	////2. nightly -> closre - exclude
	////3. od -> enabled
	const Ads_String& forecast_pending_dir = Ads_Server::config()->forecast_pending_dir_;
	const Ads_String& forecast_exchange_dir = Ads_Server::config()->forecast_exchange_dir_;
	Ads_String file_format = Ads_Server::config()->forecast_output_file_format_;
	Forecast_Service::AF_Job_Context::Network_Pending_Path_Map network_pending_path_map;
	for (const auto& network_id : this->closure_networks_)
	{
		for (const auto& files: this->simulate_output_filename_)
		{
			bool is_outputted = false;
			if (files.second) is_outputted = true; //transactional.csv
			if (!this->fast_conf_ && this->excluded_networks_.find(network_id) == this->excluded_networks_.end()) is_outputted = true;
			if (this->enabled_networks_.find(network_id) != this->enabled_networks_.end()) is_outputted = true;
			if (!is_outputted) continue;
			const Ads_String& prefix = this->get_pending_prefix(&network_pending_path_map, network_id);
			if (prefix.empty())
			{
				ADS_LOG((LP_ERROR, "get pending prefix for network_id: %d error\n", network_id));
				continue;
			}
			Ads_String src_file;
			if (!Ads_Server::config()->forecast_enable_timezone_ && files.first.find("hourly") < files.first.length()) continue;
			src_file = prefix + "/" + files.first + "_" + ads::string_date(fc_date) + ".csv" + file_format;

			Ads_String final_filename = ads::replace(forecast_pending_dir, forecast_exchange_dir, src_file);
			if (is_reload & Forecast_Pending_Processer::exist(this, final_filename)) continue;
			if (Forecast_Pending_Processer::copy(this, src_file, final_filename) < 0)
			{
				this->pending_lost_ = true;
				ADS_LOG((LP_ERROR, "write pending file %s to %s error\n", src_file.c_str(), final_filename.c_str()));
				ret = false;
			}
		}
	}
	if ( ret ) Forecast_Pending_Processer::touch_progress(this, fc_date);
	return ret;
}

int
Forecast_Service::copy_mrm_counter_to_rpm(Fast_Simulate_Config* config, AF_Job_Context* job_ctx, Ads_GUID rpm_id)
{
	if (config->delta_repo_->advertisements_.find(rpm_id) == config->delta_repo_->advertisements_.end())
	{
		return 0;
	}
	// add rpm placement to counter and copy mrm placements global counter to it
	for (int ext_id = 0; ext_id <= Ads_Shared_Data_Service::EXT_ID_TARGETED; ++ext_id)
	{
		for(int type = 0; type <= Ads_Shared_Data_Service::COUNTER_ADVERTISEMENT_LAST; ++type)
		{
			int64_t value_local = 0, value_global = 0, new_value;
			time_t timestamp_local, timestamp_global;
			job_ctx->counter_service_->read_counter(config->delta_repo_->advertisements_[rpm_id]->mrm_id_, ext_id, type, value_global, timestamp_global, false/*global*/);
			job_ctx->counter_service_->read_counter(config->delta_repo_->advertisements_[rpm_id]->mrm_id_, ext_id, type, value_local, timestamp_local, true/*local*/);
			ADS_DEBUG((LP_INFO, "assign mrm global/local counter %d:%d/%d, to rpm counter %d\n", config->delta_repo_->advertisements_[rpm_id]->mrm_id_, value_global, value_local, rpm_id));
			// assign the global/local counter for rpm
			if (value_local != 0) {
				job_ctx->counter_service_->update_counter(false/*passive*/, false/*reset*/, rpm_id, ext_id, type, value_local, timestamp_local, new_value);
			}
			if (value_global != 0) {
				job_ctx->counter_service_->update_counter(true/*active*/, false/*reset*/, rpm_id, ext_id, type, value_global, timestamp_global, new_value);
			}
		}
	}
	return 0;
}
 
int
Forecast_Service::start_simulate(AF_Job_Context* job_ctx, Ads_Request* req, json::ObjectP& result)
{
	if (!job_ctx) return -1;

	if (job_ctx->prepare_running(req) < 0)
	{
		result["error"] = json::String("failed to prepare running for job_context");
		ADS_LOG_RETURN((LP_ERROR, "failed to prepare running job_context\n"), -1);
	}
	json::ObjectP data;
	job_ctx->to_json(data);

	if (!(job_ctx->flags_ & AF_Job_Context::NIGHTLY_RESTARTED) && !(job_ctx->flags_&AF_Job_Context::FAST_SIMULATE_OD) && job_ctx->current_scenario_id_ == 0)
	{
		//truncate local model_w for write
		ADS_LOG((LP_INFO, "truncate local model_w\n"));
		std::list<Forecast_Store*> stores;
		Forecast_Service::instance()->list_all_store(job_ctx, stores);
		for (std::list<Forecast_Store*>::const_iterator it = stores.begin(); it != stores.end(); ++it)
		{
			(**it).truncate_model_for_write();
		}
	}

	//When restarted: clean up possible leftover model_w on local store
	if ((job_ctx->flags_ & AF_Job_Context::NIGHTLY_RESTARTED)
			&& !(job_ctx->flags_& AF_Job_Context::FAST_SIMULATE_WIMAS))//wimas has no output
	{
		ADS_LOG((LP_INFO, "clean up leftover model which is after %s on local store\n", ads::string_date(job_ctx->resume_start_date_).c_str()));
		std::list<Forecast_Store*> stores;
		Forecast_Service::instance()->list_all_store(job_ctx, stores);
		for (std::list<Forecast_Store*>::const_iterator it = stores.begin(); it != stores.end(); ++it)
		{
			if(job_ctx->reset_index_file(*it) < 0)
			{
				result["error"] = json::String("reset index file error");
				ADS_LOG((LP_ERROR, "reset index file error!\n"));
				return -1;
			}
		}
	}

	job_ctx->current_start_date_ = job_ctx->resume_start_date_;
	job_ctx->current_end_date_ = job_ctx->predict_end_date_;
	if (job_ctx->flags_ &  AF_Job_Context::NIGHTLY)
	{
#if defined(ADS_ENABLE_AB_OFFLINE)
		// For ab_offline, only run scenario_id = 0
		Ads_GUID scenario_id = 0;
#else
		Ads_GUID scenario_id = job_ctx->current_scenario_id_;
#endif
		AF_Job_Context::Scenario_Plan_Info_Map::const_iterator it = job_ctx->plan_infos_.find(scenario_id);
		if (it != job_ctx->plan_infos_.end())
		{
			const Plan_Info& plan_info = it->second;
			if (plan_info.forecast_networks_.empty()) job_ctx->current_end_date_ = job_ctx->current_start_date_;
			else job_ctx->current_end_date_ = std::min(job_ctx->predict_end_date_, plan_info.forecast_networks_.rbegin()->first + 86400);
		}
		else
		{
			job_ctx->current_end_date_ = job_ctx->current_start_date_;
		}
	}
	data["predict_start_date"] = json::Number(job_ctx->current_start_date_);
	data["predict_end_date"] = json::Number(job_ctx->current_end_date_);
	result["data"] = data;

	//load counters,
	//MRM-28808: 
	//(1)Nightly, if the request has resume start date(not scenario), must load forecast counter, else must load ad server counter
	//(2)Ondemand, must forecast counter
	const Ads_String& counter_path = req->p("counter_path");
	Ads_String error = "";
	do
	{
		if (job_ctx->flags_ & AF_Job_Context::NIGHTLY) //nighlty
		{
			bool is_forecast_counter = (job_ctx->predict_start_date_ < job_ctx->resume_start_date_) ? true : false;
			// if (job_ctx->current_scenario_id_ == 0 && !Ads_Server::config()->shared_data_enable_server_) break; //do nothing, if normal nodes not set as GC
			if (!counter_path.empty())
			{
				ADS_LOG((LP_INFO, "load counter from file: %s\n", counter_path.c_str()));
				if (job_ctx->counter_service_->load_counters(Ads_Server::config()->shared_data_enable_server_, counter_path, is_forecast_counter) < 0)
					error = "Failed to load counter " + counter_path;
				if (error == "" && Ads_Server::config()->shared_data_enable_server_)
					if (job_ctx->counter_service_->load_counters(false, counter_path, is_forecast_counter) < 0)
						error = "Failed to load local counter " + counter_path;
			}
			else if (req->content() && req->content()->length())
			{
				ADS_LOG((LP_INFO, "load counter from data: size=%d\n", req->content()->length()));
				if (job_ctx->counter_service_->load_counters_from_data(Ads_Server::config()->shared_data_enable_server_, req->content()->c_str(), is_forecast_counter) < 0)
					error = "Failed to load counter from req";
				if (error == "" && Ads_Server::config()->shared_data_enable_server_)
					if (job_ctx->counter_service_->load_counters_from_data(false, req->content()->c_str(), is_forecast_counter) < 0)
						error = "Failed to load local counter from req";
			}
			else
				error = "Failed to load counter from req";
		}
		else if (job_ctx->flags_ & AF_Job_Context::FAST_SIMULATE_OD)
		{
			if (! job_ctx->fast_conf_) 
			{
				error = "fast_conf does not exists";
				break;
			}
			Fast_Simulate_Config* fast_conf = reinterpret_cast<Fast_Simulate_Config*>(job_ctx->fast_conf_);
			// fall back priority: counter/0/network/file -> counter/0/timezone/file -> counter/0/0/file
			Ads_String base_path = ads::path_join(Ads_Server::config()->forecast_working_dir_, "counter", "0");
			Ads_String_List fallback_list = {ads::entity_id_to_str(fast_conf->network_id_), ads::i64_to_str(job_ctx->timezone_), "0"};
			Ads_String error_msg = "failed to load OD counter " + base_path + "/{" + ads::entity_id_to_str(fast_conf->network_id_) + "/" + ads::i64_to_str(job_ctx->timezone_) + "/0}/" + ads::string_date(job_ctx->current_start_date_) + ".dat.serving";
			Ads_String_List counter_file_list;
			for (const auto& f: fallback_list)
				counter_file_list.push_back(ads::path_join(base_path, f, ads::string_date(job_ctx->current_start_date_) + ".dat.serving"));
			// local counter
			bool success = false;
			for (const auto& f: counter_file_list)
			{
				if (job_ctx->counter_service_->load_counters(Ads_Server::config()->shared_data_enable_server_, f, true, fast_conf->delta_repo_) == 0)
				{
					ADS_LOG((LP_INFO, "[COUNTER] init local counter: job=%s, file=%s\n", job_ctx->name().c_str(), f.c_str()));
					success = true;
					break;
				}
			}
			// global counter
			if (success && Ads_Server::config()->shared_data_enable_server_)
			{
				success = false;
				for (const auto& f: counter_file_list)
				{
					if (job_ctx->counter_service_->load_counters(false, f, true, fast_conf->delta_repo_) == 0)
					{
						ADS_LOG((LP_INFO, "[COUNTER] init global counter: job=%s, file=%s\n", job_ctx->name().c_str(), f.c_str()));
						success = true;
						break;
					}
				}
			}
			if (!success)
				error = error_msg;

			// assign mrm couter to rpm
			for (Ads_GUID_Set::iterator it = fast_conf->self_rpm_placements_.begin(); it != fast_conf->self_rpm_placements_.end(); ++it)
			{
				copy_mrm_counter_to_rpm(fast_conf, job_ctx, *it);
				copy_mrm_counter_to_rpm(fast_conf, job_ctx, fast_conf->delta_repo_->advertisements_[*it]->parent_id_);
				for (Ads_GUID_Set::iterator tmp_it1 = fast_conf->delta_repo_->advertisements_[*it]->children_.begin(); 
						tmp_it1 != fast_conf->delta_repo_->advertisements_[*it]->children_.end(); ++tmp_it1)
				{
					copy_mrm_counter_to_rpm(fast_conf, job_ctx, *tmp_it1);
				}
			}
		}
	} while(0);
	
	if (job_ctx->pending_lost_)
		error = "pending result lost, please resume nightly/rerun od"; 
	if (! error.empty())
	{
		ADS_LOG((LP_ERROR, "%s\n", error.c_str()));
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String(error);
		return -1;
	}
	else
	{
		ADS_LOG((LP_INFO, "load counter succefully\n"));
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	}
	
	//OPP-2175
	if (job_ctx->resume_start_date_ > job_ctx->predict_start_date_)
	{
		reload_pre_aggregate_status(job_ctx, job_ctx->current_scenario_id_, job_ctx->pre_aggregate_dates_);
		if (add_pre_aggregate_tasks(job_ctx, job_ctx->resume_start_date_) < 0)
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			result["error"] = json::String("reload pending files to redis or NFS error");
			return -1;
		}
	}
	// start N jobs to load requests
	for (int i = 0; i < Forecast_Load_Requests_Task::N_PRE_LOAD_DAYS; ++i)
		create_load_request_job(job_ctx, job_ctx->current_start_date_ + i * 86400);

	return 0;
}

BEGIN_FORECAST_DISPATCHER(simulate_daily)
    Forecast_Service::instance()->simulate_daily(req, result);
END_FORECAST_DISPATCHER

void
Forecast_Service::simulate_daily(Ads_Request* req, json::ObjectP& result)
{
    result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
    AF_Job_Context* job_ctx = get_job_context(req, result);
	if (!job_ctx) return;

    if(job_ctx->timezone_ == -1)
    {
        job_ctx->timezone_ = req->p("timezone").empty() ? 0 : ads::str_to_i64(req->p("timezone"));
    }
    const Ads_String& virtual_date = req->p("virtual_date");
    if (virtual_date.empty())
    {
        result["error"] = json::String("Please specify virtual_date to begin simulating.");
        return;
    }
    time_t fc_date = ads::str_to_time(virtual_date.c_str(), "%Y%m%d") / 86400 * 86400;

	//need to update model version when simulate_daily
	if(job_ctx->update_model_version() < 0)
	{
		result["error"] = json::String("failed to update model version for job_context");
		ADS_LOG((LP_ERROR, "failed to update model version for job_context\n"));
		return;
	}
	// add vnode info to job context
	if(!req->p("vnodes").empty())
	{
		std::vector<Ads_String> vnode_vec;
		ads::split(req->p("vnodes"), vnode_vec, ',');
		//update network_cache
		if(!job_ctx->update_network_cache(vnode_vec))
		{
			result["error"] = json::String("failed to update network cache");
			ADS_LOG((LP_ERROR, "failed to update network cache\n"));
			return;
		}
		ADS_LOG((LP_INFO, "update network cache, vnode : %s\n", req->p("vnodes").c_str()));

		//clear load request cache
		clear_load_request_cache(job_ctx);
		// reload fc_date to fc_date+N-1 requests
		for(time_t next_date = fc_date; next_date < job_ctx->current_end_date_ && next_date < fc_date + Forecast_Load_Requests_Task::N_PRE_LOAD_DAYS * 86400; next_date += 86400)
			create_load_request_job(job_ctx, next_date);
	}

	FORECAST_PERFORMANCE_MONITOR;
	Forecast_Service::Perf_Counter* perf = new Forecast_Service::Perf_Counter();
	std::auto_ptr<Forecast_Service::Perf_Counter> __p(perf);
	{
	ads::Guard __g(job_ctx->perf_.mu_);
	job_ctx->perf_["daily_begin"] = ads::gettimeofday();
	}
	ads::Time_Value t0 = ads::gettimeofday();
	time_t last_output_pending_date = ads::str_to_i64(req->p("last_output_pending_date")); 
   //OPP-2175
	if (job_ctx->flags_ & AF_Job_Context::FLAG_PRE_AGGREGATE)
	{
		if (!req->p("only_aggregate").empty() && !(job_ctx->flags_ & AF_Job_Context::FLAG_DRY_RUN))
		{
			reload_pre_aggregate_status(job_ctx, job_ctx->current_scenario_id_, job_ctx->pre_aggregate_dates_);
			job_ctx->flags_ |=  AF_Job_Context::FLAG_DRY_RUN;
		}
		add_pre_aggregate_tasks(job_ctx, last_output_pending_date + 86400);
		if (job_ctx->flags_ & AF_Job_Context::FLAG_DRY_RUN)
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
			return;
		}
	}

    job_ctx->stats_ = &this->stats_;
    job_ctx->stats_->slow_threshold_ = get_simulate_slow_threshold();

    Ads_String msg = Ads_String("simulate_daily failed plan_id ") + job_ctx->name() + " on date " + virtual_date + " Err:";
    ADS_LOG((LP_INFO, "simulate_daily: begin daily %s\n", virtual_date.c_str()));
    //rotate_virtual_date
    Ads_String error_message;
    if (rotate_virtual_date(job_ctx->counter_service_, fc_date, error_message) < 0)
    {
        msg += error_message;
        result["error"] = json::String(msg);
        ADS_LOG_RETURN((LP_ERROR, "%s\n", msg.c_str()), );
    }
    save_counters(job_ctx, virtual_date + ".dat");

	if (!job_ctx->fast_conf_)
	{
		Ads_String base_path = ads::path_join(Ads_Server::config()->forecast_working_dir_, "simulate_scenes", ads::i64_to_str(job_ctx->plan_id_), ads::i64_to_str(job_ctx->current_scenario_id_));
		Ads_String_List file_list;
		get_scenes_file(base_path, ads::string_date(fc_date), file_list);
		for (const auto& it: file_list)
			::remove(it.c_str());
	}

	PERFORMANCE_CALC(daily_prepare);
	if(job_ctx->vnode_set_.size() == 0)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		return;
	}
	Request_Queue* request_queue = NULL; //for create simulate tasks, delete in output_pending task
	if (merge_from_cached_requests(job_ctx, fc_date, request_queue) < 0)
	{
		result["error"] = json::String("merge_from_cached_requests failed");
		return;
	}
	// create a load_request job for next day T+N
	time_t next_date = fc_date + Forecast_Load_Requests_Task::N_PRE_LOAD_DAYS * 86400;
	if (next_date < job_ctx->current_end_date_)
		create_load_request_job(job_ctx, fc_date + Forecast_Load_Requests_Task::N_PRE_LOAD_DAYS * 86400);
	PERFORMANCE_CALC(daily_merge_request);

	ADS_LOG((LP_INFO, "simulate_daily: begin_daily %s\n", virtual_date.c_str()));
	std::pair<Forecast_Metrics_Map*, Interval_Forecast_Metrics*>* metrics = NULL; //delete in output_pending task
	if (create_simulate_tasks(job_ctx, fc_date, request_queue, metrics) < 0) return;
	PERFORMANCE_CALC(daily_job_create);
    // wait for simulating thread to complete
	simulate_manager_->wait(job_ctx->name(), fc_date, (size_t)std::max(Ads_Server::config()->forecast_num_skip_simulator_threads_, 0));
	ADS_LOG((LP_INFO, "simulate_daily: simulate_i %s\n", virtual_date.c_str()));
	PERFORMANCE_CALC(daily_simulate_complete);

	Forecast_Output_Pending_Task* task = new Forecast_Output_Pending_Task(job_ctx, fc_date, request_queue, metrics);
	if (Forecast_Service::instance()->output_pending_manager_->add_task(task) < 0)
	{
		delete task;
		ADS_LOG((LP_ERROR, "[OUTPUT] job=%s, date=%s: cannot add output_pending task to manager\n", job_ctx->name().c_str(), virtual_date.c_str()));
		return;
	}

	if (Ads_Server::config()->forecast_flush_counter_daily_) // MRM-28946
	{
		job_ctx->counter_service_->update_counter_with_message(Ads_Shared_Data_Message::MESSAGE_FLUSH_COUNTER_INCREMENTS);
	}
	else
	{
		job_ctx->counter_service_->request_sync_counters(true);
	}
	PERFORMANCE_CALC(daily_update_counter);

   	//OPP-2175
	if (!(job_ctx->flags_ & Forecast_Service::AF_Job_Context::FAST_SIMULATE_WIMAS)
			&& fc_date + 86400 == job_ctx->current_end_date_ && (job_ctx->flags_ & AF_Job_Context::FLAG_PRE_AGGREGATE))
	{
		while (get_last_output_pending_date(job_ctx) < fc_date)
			sleep(1);
	}
	result["last_output_pending_date"] = json::Number(get_last_output_pending_date(job_ctx));
	ads::Time_Value df = ads::gettimeofday();
	// daily performance
	std::map<Ads_String, int> time_stat; // ms
	{
	ads::Guard __g(job_ctx->perf_.mu_);
	int daily_gap = ads::Time_Interval_msec(job_ctx->perf_["daily_finish"], job_ctx->perf_["daily_begin"]);
	time_stat["daily_wait"] = daily_gap > 0 ? daily_gap : 0;  /*start_simulate or simulate_daily dispatch wait, yesterday's data*/
	job_ctx->perf_["daily_finish"] = df;
	}
	if (job_ctx->n_daily_simulate_threads_ > 0)
	{
		ads::Guard __g(job_ctx->simulating_perf_.mu_);
		for (auto& it: perf_keys["simulate_daily"])
		{
			if (it == "daily_simulate_total" || it == "daily_total" || it == "daily_wait") continue;
			else 
			{
				time_stat[it] = (*perf)[it] / 1000;
				time_stat["daily_simulate_total"] += (*perf)[it] / 1000;
			}
		}
		time_stat["daily_total"] += time_stat["daily_simulate_total"] + time_stat["daily_wait"];
		//simulate thread stats
		time_stat["request_wait"]   = job_ctx->simulating_perf_["queue_sum"] / job_ctx->n_daily_simulate_threads_ / 1000;
		time_stat["thread_wait"]  = (job_ctx->simulating_perf_["finish_max"] - job_ctx->simulating_perf_["finish_min"]) / 1000;
		time_stat["thread_min"]  = job_ctx->simulating_perf_["finish_min"] / 1000;
		time_stat["thread_max"]  = job_ctx->simulating_perf_["finish_max"] / 1000;
		time_stat["thread_ave"]  = job_ctx->simulating_perf_["finish_sum"] / job_ctx->n_daily_simulate_threads_ / 1000;
		time_stat["thread_run"]  = time_stat["thread_min"] - time_stat["request_wait"];
		//simulate breakdown
		for (auto& it: perf_keys["simulate"])
			time_stat[it] = job_ctx->simulating_perf_[it] / job_ctx->n_daily_simulate_threads_ / 1000;
		//market ad statistics
		for (auto& it: perf_keys["market_ad"])
			time_stat[it] = job_ctx->simulating_perf_[it];
		time_stat["market_req_ratio"] = time_stat["n_req"] ? time_stat["n_req_market"] * 1000 / time_stat["n_req"] : 0;
		time_stat["avg_ext_ads"] = time_stat["n_req_market"] ? time_stat["n_ext_ads"] * 1000 / time_stat["n_req_market"] : 0;
		time_stat["avg_bids"] = time_stat["n_req_market"] ? time_stat["n_bids"] * 1000 / time_stat["n_req_market"] : 0;
		time_stat["avg_bids_no_error"] = time_stat["n_req_market"] ? time_stat["n_bids_no_error"] * 1000 / time_stat["n_req_market"] : 0;
		time_stat["avg_bids_returned"] = time_stat["n_ext_ads_openrtb"] ? (time_stat["n_bids"] - time_stat["n_ext_ads"]) * 1000 / time_stat["n_ext_ads_openrtb"] : 0;

		job_ctx->simulating_perf_.clear();
	}

	json::Object perf_j;
	Ads_String buf;
	for (auto& it: perf_keys)
		for (auto& iit: it.second)
		{
			if (time_stat.find(iit) != time_stat.end())
			{
				perf_j[iit] = json::Number(time_stat[iit]);
				buf += ads::i64_to_str(time_stat[iit]) + ","; 
			}
		}
	//
	result["performance"] = perf_j;
	Ads_String job_str = req->p("job_tag").empty() ? req->p("plan_id") : req->p("job_tag");
	ADS_LOG((LP_INFO, "[PERFORMANCE] job=%s, scenario=%d, date=%s, perf=%s\n", job_str.c_str(), job_ctx->current_scenario_id_, virtual_date.c_str(), buf.c_str()));

	json::Object job;
	job["job"] = json::String(job_str);
	job["scenario_id"] = json::Number(job_ctx->current_scenario_id_);
	job["date"] = json::String(virtual_date);
	result["job"] = job;

	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	if (job_ctx->pending_lost_)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String( "pending result lost, please resume nightly/rerun od");
	}

	if (job_ctx->resume_error_)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String( "resume job error, please check data parameter");
	}
}

int Forecast_Service::create_load_request_job(AF_Job_Context* job_ctx, time_t date)
{
	Forecast_Load_Requests_Task* task = new Forecast_Load_Requests_Task(job_ctx, date);
	if (load_request_manager_->add_task(task) < 0)
	{
		delete task;
		ADS_LOG((LP_ERROR, "[REQUESTS] job=%s, date=%s: cannot add load_request task to manager\n", job_ctx->name().c_str(), ads::string_date(date).c_str()));
		return -1;
	}
	return 0;
}

int Forecast_Service::merge_from_cached_requests(AF_Job_Context* job_ctx, time_t fc_date, Request_Queue*& request_queue)
{
	if (fc_date >= job_ctx->current_end_date_) //for global counter
	{
		request_queue = new Request_Queue();
		request_queue->enqueue((Forecast_Batch*)-1);
		return 0;
	}

	// wait task if there is
	int n_waiting, n_running, n_done;
	load_request_manager_->get_status(job_ctx->name(), fc_date, n_waiting, n_running, n_done);
	ADS_LOG((LP_INFO, "[zxliu] n_waiting = %d, n_running = %d, n_done = %d\n", n_waiting, n_running, n_done));
	if (n_waiting > 0 || n_running > 0)
	{
		ADS_LOG((LP_INFO, "[REQUESTS] job=%s, date=%s: load job is slow, waiting\n", job_ctx->name().c_str(), ads::string_date(fc_date).c_str()));
		load_request_manager_->wait(job_ctx->name(), fc_date);
	}

	// check request cache, manually run if needed
	bool is_ready = false;
	{
		ads::Guard _g(job_ctx->load_request_mutex_);
		is_ready = job_ctx->load_request_cache_.find(fc_date) != job_ctx->load_request_cache_.end();
	}
	if (!is_ready)
	{
		ADS_LOG((LP_INFO, "[REQUESTS] job=%s, date=%s: missing load job/cache, triggering again\n", job_ctx->name().c_str(), ads::string_date(fc_date).c_str()));
		create_load_request_job(job_ctx, fc_date);
		load_request_manager_->wait(job_ctx->name(), fc_date);
		ads::Guard _g(job_ctx->load_request_mutex_);
		is_ready = job_ctx->load_request_cache_.find(fc_date) != job_ctx->load_request_cache_.end();
	}

	{
		ads::Guard _g(job_ctx->load_request_mutex_);
		if (!job_ctx->load_request_cache_[fc_date])
			ADS_LOG_RETURN((LP_ERROR, "[REQUESTS] job=%s, date=%s: merge_from_cached_requests FAILED, NULL queue\n", job_ctx->name().c_str(), ads::string_date(fc_date).c_str()), -1);
		request_queue = job_ctx->load_request_cache_[fc_date];
		job_ctx->load_request_cache_.erase(fc_date);
	}
	request_queue->enqueue((Forecast_Batch*)-1); //make sure simulate job will be finished
	ADS_LOG_RETURN((LP_INFO, "[REQUESTS] job=%s, date=%s: merge_from_cached_requests DONE\n", job_ctx->name().c_str(), ads::string_date(fc_date).c_str()), 0);
}

int Forecast_Service::create_simulate_tasks(AF_Job_Context* job_ctx, time_t fc_date, Request_Queue* request_queue, std::pair<Forecast_Metrics_Map*, Interval_Forecast_Metrics*>*& metrics) 
{
	metrics = new std::pair<Forecast_Metrics_Map*, Interval_Forecast_Metrics*>[job_ctx->n_metrics_]; //which will be transfered to calculate_metreics and output_pending task. 
	for (size_t i = 0; i < job_ctx->n_metrics_; ++i)
	{
		metrics[i].first = new Forecast_Metrics_Map();
		(*metrics[i].first)[fc_date].count_ = 0;
		if (Ads_Server::config()->forecast_enable_timezone_)
		{
			(*metrics[i].first)[fc_date - 86400].count_ = 0;
			(*metrics[i].first)[fc_date + 86400].count_ = 0;
		}
		metrics[i].second = new Interval_Forecast_Metrics();
	}

	size_t n_tasks = job_ctx->n_daily_simulate_threads_;	
	for (size_t i = 0; i < n_tasks; ++i)
	{
		Forecast_Simulator::Context* ctx = new Forecast_Simulator::Context();
		ctx->queue_ = request_queue;
		ctx->conf_ = Ads_Server::config();
		ctx->metrics_ = metrics;
		ctx->copy_job_context(job_ctx);
		
		Forecast_Simulate_Task* simulate_task = new Forecast_Simulate_Task(job_ctx, fc_date, ctx);
		if (simulate_manager_->add_task(simulate_task) < 0)
		{
			delete simulate_task;
			ADS_LOG((LP_ERROR, "[REQUESTS] job=%s, date=%s: cannot add simulate task to manager\n", job_ctx->name().c_str(), ads::string_date(fc_date).c_str()));
			return -1;
		} 
	}
	return 0;
}


Forecast_Service::AF_Job_Context* 
Forecast_Service::get_job_context(Ads_String job_name)
{
	{
	    ads::Guard __g(od_simulate_jobs_mutex_);
		std::map<Ads_String, AF_Job_Context*>::iterator cit = this->od_simulate_jobs_.find(job_name);
		if (cit != this->od_simulate_jobs_.end())
			return cit->second;
	}
	{
		ads::Guard __g(nightly_simulate_jobs_mutex_);
		std::map<Ads_String, AF_Job_Context*>::iterator cit = this->nightly_simulate_jobs_.find(job_name);
		if (cit != this->nightly_simulate_jobs_.end())
			return cit->second;
	}
	return nullptr;
}

Forecast_Service::AF_Job_Context* 
Forecast_Service::get_job_context(Ads_Request* req, json::ObjectP& result)
{
	if (req->p("plan_id").empty() && req->p("job_tag").empty())
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String("Please specify plan_id or job_tag(for od).");
		return NULL;
	}
	Ads_String name = req->p("job_tag").empty()? req->p("plan_id") : req->p("job_tag");
	if (!req->p("job_tag").empty())
	{
		ads::Guard __g(od_simulate_jobs_mutex_);
		std::map<Ads_String, AF_Job_Context*>::iterator cit = this->od_simulate_jobs_.find(name);
		if (cit == this->od_simulate_jobs_.end())
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			result["error"] = json::String("No job context for " + name + ", please prepare first");
			return NULL;
		}
		return cit->second;
	}
	else
	{
		ads::Guard __g(nightly_simulate_jobs_mutex_);
		std::map<Ads_String, AF_Job_Context*>::iterator cit = this->nightly_simulate_jobs_.find(name);
		if (cit == this->nightly_simulate_jobs_.end())
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			result["error"] = json::String("No job context for " + name + ", please prepare first");
			return NULL;
		}
		return cit->second;
	}
}

BEGIN_FORECAST_DISPATCHER(end_simulate)
	Forecast_Service::instance()->end_simulate(req, result);
END_FORECAST_DISPATCHER

void Forecast_Service::end_simulate(Ads_Request* req, json::ObjectP& result)
{
	AF_Job_Context* job_ctx = get_job_context(req, result);
	if (!job_ctx) return;
	if (job_ctx->flags_ & AF_Job_Context::FLAG_PRE_AGGREGATE)
		add_pre_aggregate_tasks(job_ctx, job_ctx->current_end_date_);
	if (Ads_Server::config()->forecast_user_cache_capacity_ > 0)
		Ads_Cacher::instance()->reset_user_segments();
	
	save_counters(job_ctx, job_ctx->name() + ".0.nt");
	{
		ADS_ASSERT(!job_ctx->simulate_threads_);
		job_ctx->destroy_running();//pending 
		destroy_aggregate(job_ctx); //for current scenario aggregate
		ADS_LOG((LP_INFO, "job=%s: aggregate_job finished\n", job_ctx->name().c_str()));
	}
	if (job_ctx->pending_lost_)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String("pending result lost, please resume nightly/rerun od");
		return;
	}
	if (job_ctx->flags_ & AF_Job_Context::NIGHTLY)
		rename_counter_files(job_ctx);

	ADS_LOG((LP_INFO, "start switching model\n"));
	if (job_ctx->flags_ & AF_Job_Context::NIGHTLY && job_ctx->current_scenario_id_ == 0)
	{
		std::list<Forecast_Store*> stores;
		list_all_store(job_ctx, stores);
		for(std::list<Forecast_Store*>::const_iterator it = stores.begin(); it != stores.end(); ++it)
		{
			(**it).switch_model();
			(**it).output_pusher_timestamp(job_ctx->nightly_time_loaded_);
		}
	}
	else if(job_ctx->flags_ & AF_Job_Context::FAST_SIMULATE_WIMAS)
		result["reject_reason"] = job_ctx->json_result_["reject_reason"];

	if (job_ctx->flags_ & AF_Job_Context::FLAG_COLLECT_STATS)
		result["total"] = job_ctx->json_result_["total"];

	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
}

BEGIN_FORECAST_DISPATCHER(remove_job_context)
	if (req->p("remove_all") == "all")
	{
		Forecast_Service::instance()->destroy_job_contexts();
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		return 0;
	}
	// if (!req->p("plan_id").empty()) Ads_Pusher_Service::instance()->load_repository(); //nt switch repa
	Forecast_Service::AF_Job_Context* job_ctx = Forecast_Service::instance()->get_job_context(req, result);
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	if (!job_ctx) return -1;
	job_ctx->stop_output_pending(); //decrease stop waiting time
	Forecast_Service::instance()->pre_aggregate_manager_->remove_waiting_tasks(job_ctx->name());
	job_ctx->exit_pre_aggregate_ = true;
	job_ctx->flags_ |= Forecast_Service::AF_Job_Context::FLAG_DELETABLE;
	result["data"] = json::String(job_ctx->name() + "is clean up");
END_FORECAST_DISPATCHER

int Forecast_Service::remove_job_context(AF_Job_Context* job_ctx)
{
	if (!job_ctx->job_tag_.empty())
	{
		ads::Guard __g(this->od_simulate_jobs_mutex_);
		std::map<Ads_String, AF_Job_Context*>::iterator it = this->od_simulate_jobs_.find(job_ctx->name());
		if (it != this->od_simulate_jobs_.end())
			this->od_simulate_jobs_.erase(it);
	}
	else
	{
		ads::Guard __g(this->nightly_simulate_jobs_mutex_);
		std::map<Ads_String, AF_Job_Context*>::iterator it = this->nightly_simulate_jobs_.find(job_ctx->name());
		if (it != this->nightly_simulate_jobs_.end())
			this->nightly_simulate_jobs_.erase(it);
	}
	Ads_Shared_Data_Service::destroy_expired_instances(86400);
	delete job_ctx;
	return 0;
}

BEGIN_FORECAST_DISPATCHER(list_job_contexts)
	Forecast_Service::instance()->list_job_contexts(result);
END_FORECAST_DISPATCHER

void Forecast_Service::list_job_contexts(json::ObjectP& result)
{
	json::Object jobs;
	ads::Guard __g_nt(nightly_simulate_jobs_mutex_);
	for (std::map<Ads_String, AF_Job_Context*>::const_iterator cit = this->nightly_simulate_jobs_.begin(); cit != this->nightly_simulate_jobs_.end(); ++cit)
	{
		jobs[cit->first] = cit->second != NULL ? json::String(cit->second->to_string()) : json::String("");
	}
	ads::Guard __g_od(od_simulate_jobs_mutex_);
	for (std::map<Ads_String, AF_Job_Context*>::const_iterator cit = this->od_simulate_jobs_.begin(); cit != this->od_simulate_jobs_.end(); ++cit)	{					  
		jobs[cit->first] = cit->second != NULL ? json::String(cit->second->to_string()) : json::String("");
	}
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	result["data"] = jobs;
}

BEGIN_FORECAST_DISPATCHER(clear_global_cookies)
	Forecast_Service::instance()->clear_global_cookies(result);
END_FORECAST_DISPATCHER

void
Forecast_Service::clear_global_cookies(json::ObjectP& result)
{
	ads::Guard _g(this->cookies_mutex_);
	this->cookies_map_.clear();
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
}

BEGIN_FORECAST_DISPATCHER(run_request)
	Forecast_Service::instance()->run_request(req, result);
END_FORECAST_DISPATCHER

void Forecast_Service::run_request(Ads_Request* req, json::ObjectP& result) 
{
	Forecast_Request_Meta meta;
	Forecast_Store *store = NULL;
	meta.from_csv(req->p("meta").c_str(), req->p("meta").size());
	bool enable_timezone = req->p("tz") == "true" || req->p("tz") == "1";
	Ads_GUID scenario_id = 0;
	if (!req->p("scenario_id").empty())
		scenario_id = ads::str_to_entity_id(0, req->p("scenario_id"));
	if (!ads::entity::is_valid_id(meta.network_id_))
		result["error"] = json::String(Ads_String ("invalid meta: ") + req->p("meta"));
	else if ((store = Forecast_Service::instance()->get_store(meta.network_id_)) == NULL)
		result["error"] = json::String(Ads_String ("store lost"));
	else
	{
		bool is_od = (req->p("type") == "model") || (!req->p("tid").empty() || !req->p("jobs").empty());//backward compatibility 
		report::Request_Log_Record request;
		int ret = 0;
		if (is_od)
			ret = store->get_model(meta.virtual_time_ / 86400 * 86400, meta.key_, request);
		else
			ret = store->get_request(meta.request_time_ / 86400 * 86400, meta.key_, request);

		Ads_GUID_Set* excluded_placements = new Ads_GUID_Set();
		std::auto_ptr<Ads_GUID_Set> __p(excluded_placements);
		if (!req->p("excluded_placements").empty())
		{
			ads::str_to_entities(req->p("excluded_placements"), ADS_ENTITY_TYPE_ADVERTISEMENT, 0, std::inserter(*excluded_placements, (*excluded_placements).begin()));
		}

		if (ret < 0)
			result["error"] = json::String(Ads_String ("request lost"));
		else
		{
			//print Request_Log_Record text
			Ads_String s;
			::google::protobuf::TextFormat::PrintToString(request, &s);
			Ads_String_List lines;
			ads::split(s, lines, '\n');
			for(Ads_String_List::const_iterator line = lines.begin(); line != lines.end(); ++line)
				ADS_DEBUG((LP_TRACE, "%s\n", line->c_str()));
			//print simulation 10 phases and 4 scenarios
			Ads_Server::Repository_Guard __g(Ads_Server::instance());
			Forecast_Simulator::Context ctx;
			const Ads_Repository* repo = __g.asset_repository();
			ctx.repo_ = repo;
			ctx.conf_ = Ads_Server::config();
			ctx.counter_service_ = Forecast_Service::get_counter(req->p("plan_id"));
			ctx.scenario_id_ = scenario_id;
			ctx.feedback_ = &feedback_;
			ctx.external_feedback_ = &external_feedback_;
			ctx.flags_ |= Forecast_Simulator::Context::FLAG_ENABLE_TRACER;
			ctx.flags_ |= Forecast_Simulator::Context::FLAG_SINGLE_REQUEST;
			ctx.excluded_placements_ = excluded_placements;
			Cookies* cookies = NULL;
			if (req->p("update_counter") == "false" || req->p("update_counter") == "0")
				ctx.flags_ |= Forecast_Simulator::Context::FLAG_DISABLE_UPDATE_COUNTER;
			if (req->p("update_cookies") == "false" || req->p("update_cookies") == "0")//opp-2913
			{
				Cookies new_cookie;
				cookies = &new_cookie;
			}
			else
			{
				long uid = meta.key_.uid_;
				cookies = &this->cookies_map_[uid];
			}
			Ads_GUID network_id = ads::entity::make_id(ADS_ENTITY_TYPE_NETWORK, meta.network_id_);
			const Ads_Network* network = NULL;
			if (repo->find_network(network_id, network) < 0 || !network)
				result["error"] = json::String(Ads_String ("invalid network id in meta"));
			else
			{
				ret = 0;
				Fast_Simulate_Config config;
				if (!req->p("jobs").empty())
				{
					size_t flags;
					time_t start_date, end_date;
					ret = prepare_fast_conf(network, repo, req, config, flags, start_date, end_date, result);
					if (ret >=0)
					{
						ctx.fast_conf_ = &config;
						ctx.flags_ |= flags;
					}
				}
				if (ret >= 0)
				{
					{
						ads::Guard _g(this->cookies_mutex_);
						Forecast_Transaction transaction;
						transaction.meta_ = &meta; 	
						transaction.req_ =  &request;	
						Forecast_Metrics delta_metrics;
						json::ObjectP tracer_content;
						simulator_->run_request(&ctx, *cookies, &transaction);
						transaction.calculate_metrics(&delta_metrics);
						Forecast_Simulator::generate_tracer_content(&delta_metrics, &transaction, tracer_content, enable_timezone);
						result["tracer"] = tracer_content;
						transaction.destroy_scenarios();
					}
					result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
					json::ObjectP p, pp;
					ctx.uga_wimas_stat_.to_json(p);
					ctx.real_wimas_stat_.to_json(pp);
					result["uga_wimas"] = p;
					result["real_wimas"] = pp;
				}
			}
		}
	}
}

BEGIN_FORECAST_DISPATCHER(simulator__execute_request)
	Forecast_Service::instance()->simulator__execute_request(req, result);
END_FORECAST_DISPATCHER

void Forecast_Service::simulator__execute_request(Ads_Request* req, json::ObjectP& result)
{
	if (req->h().request_method_ == "POST" && req->content())
	{
		req->type(Ads_Request::AD_CALL);
		req->handle_content(req->content());
		Ads_Server::Repository_Guard __g(Ads_Server::instance());

		ads::Error_List errors;

		Cookies* cookies = NULL;
		//opp-2913  handle cookie for req 
		bool enable_timezone = req->p("tz") == "true" || req->p("tz") == "1";
		if (!(req->p("update_cookies") == "false" || req->p("update_cookies") == "0"))		
		{
			//uid string to long (use hash )
			Ads_String uid = req->user_id();
			size_t pos = uid.find("_");
			long hash = -1;
			if (pos != std::string::npos)
			{
				hash = ads::str_to_i64(uid.substr(pos+1).c_str());
			}
			if (hash <= 0)
			{
				hash = Ads_Bloom_Filter_Manager::BloomHash(leveldb::Slice(uid));
			}
			//pass recorded cookie to request
			cookies = &this->cookies_map_[hash];
			Ads_String_Map set_cookies;
			set_cookies[FW_COOKIE_USER_ID] = uid;
			cookies->update(set_cookies);
			Ads_String cookie_str;
			cookies->to_string(cookie_str);
			if(!cookie_str.empty())
				req->handle_cookie(cookie_str);
		}
		else
		{
			Cookies new_cookie;
			cookies = &new_cookie;
		}
		if (req->parse(__g.asset_repository(), __g.asset_repository(), &errors) < 0)
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		else
		{
			Forecast_Simulator::Context ctx;
			ctx.conf_ = Ads_Server::config();
			ctx.counter_service_ = Ads_Shared_Data_Service::instance();
			{
				ads::Guard _g(this->feedback_mutex_);
				ctx.feedback_ = &this->feedback_;
			}
			{
				ads::Guard _g(this->external_feedback_mutex_);
				ctx.external_feedback_ = &this->external_feedback_;
			}
			ctx.flags_ |= Forecast_Simulator::Context::FLAG_ENABLE_TRACER;
//			ctx.flags_ |= Forecast_Simulator::Context::FLAG_DISABLE_UPDATE_COUNTER;
			ctx.flags_ |= Forecast_Simulator::Context::FLAG_SINGLE_REQUEST;
			if (req->p("update_counter") == "false" || req->p("update_counter") == "0")//opp-2913
				ctx.flags_ |= Forecast_Simulator::Context::FLAG_DISABLE_UPDATE_COUNTER;
			ctx.request_ = req;
			if (!req->p("scenario_id").empty())
				ctx.scenario_id_ = ads::str_to_entity_id(0, req->p("scenario_id"));
			Ads_GUID_Set* excluded_placements = new Ads_GUID_Set();
			std::auto_ptr<Ads_GUID_Set> __p(excluded_placements);
			if (!req->p("excluded_placements").empty())
			{
				ads::str_to_entities(req->p("excluded_placements"), ADS_ENTITY_TYPE_ADVERTISEMENT, 0, std::inserter(*excluded_placements, (*excluded_placements).begin()));
			}
			ctx.excluded_placements_ = excluded_placements;
			ctx.repo_ = __g.asset_repository();
			//opp-2913
			{
				ads::Guard _g(this->cookies_mutex_);
				Forecast_Transaction transaction;
				transaction.meta_ = new Forecast_Request_Meta(); //dummy
				transaction.req_ =  new report::Request_Log_Record(); //dummy
				Forecast_Metrics delta_metrics;
				json::ObjectP tracer_content;
				simulator_->run_request(&ctx, *cookies, &transaction);
				transaction.calculate_metrics(&delta_metrics);
				Forecast_Simulator::generate_tracer_content(&delta_metrics, &transaction, tracer_content, enable_timezone);
				result["tracer"] = tracer_content;
				transaction.destroy_pointers();
			}

			result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
			json::ObjectP p, pp;
			ctx.uga_wimas_stat_.to_json(p);
			ctx.real_wimas_stat_.to_json(pp);
			result["uga_wimas"] = p;
			result["real_wimas"] = pp;
		}
	}
}

BEGIN_FORECAST_DISPATCHER(run_request_in_file)
	Forecast_Service::instance()->run_request_in_file(req, result);
END_FORECAST_DISPATCHER

void Forecast_Service::run_request_in_file(Ads_Request* req, json::ObjectP& result) 
{
	bool enable_timezone = req->p("tz") == "true" || req->p("tz") == "1";
	if (req->p("uid").empty() || req->p("tid").empty())
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String(Ads_String("missing parameter: uid or tid"));
		return;
	}
	long uid = ads::str_to_i64(req->p("uid"));
	Ads_String tid = req->p("tid");
	if (req->p("file").empty())
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String(Ads_String("missing parameter: file"));
		return;
	}
	Forecast_User_Batch user_batches;
	if (Forecast_Store::read_requests_from_file(req->p("file"), user_batches) < 0)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String(Ads_String("cannot read requests from specified file"));
		return;
	}
	bool enable_request = req->p("show_request") == "1" || req->p("show_request") == "true";
	bool enable_performance = req->p("performance") == "1" || req->p("performance") == "true";
	bool enable_tracer = req->p("tracer") == "1" || req->p("tracer") == "true";

	for (const auto& uit: user_batches)
	{
		if (uit.first != uid)
		{
			uit.second->destroy_elements();
			continue;
		}
		for (const auto& bit: (*uit.second))
		{
			Forecast_Request_Meta* meta = bit.first;
			report::Request_Log_Record *request = bit.second;
			if (!meta || meta->key_.uid_ != uid || Ads_String(meta->key_.transaction_id_) != tid || !request)
			{
				if (meta) delete meta;
				if (request) delete request;
				continue;
			}

			Forecast_Simulator::Context ctx;
			ctx.conf_ = Ads_Server::config();
			ctx.counter_service_ = Forecast_Service::get_counter(req->p("plan_id"));
			ctx.scenario_id_ = 0;
			ctx.feedback_ = &feedback_;
			ctx.external_feedback_ = &external_feedback_;
			ctx.flags_ |= Forecast_Simulator::Context::FLAG_SINGLE_REQUEST;
			ctx.flags_ |= Forecast_Simulator::Context::FLAG_DISABLE_UPDATE_COUNTER;
			if (enable_performance)
				ctx.flags_ |= Forecast_Simulator::Context::FLAG_SELECTION_PERFORMANCE;
			Forecast_Transaction transaction;
			transaction.meta_ = meta; 	
			transaction.req_ =  request;	
			Cookies cookies;
			Perf_Counter perf;
			{
				Ads_Server::Repository_Guard __g(Ads_Server::instance());
				ctx.repo_ = __g.asset_repository();
				simulator_->run_request(&ctx, cookies, &transaction, false, &perf);
			}

			if (enable_request)
			{
				Ads_String s;
				::google::protobuf::TextFormat::PrintToString((*request), &s);
				result["request"] = json::String(s);
			}
			if (enable_performance)
			{
				result["perf_uga"] = json::String(ctx.selection_perf_["uga"]);
				result["perf_ga"] = json::String(ctx.selection_perf_["ga"]);
				result["perf_na"] = json::String(ctx.selection_perf_["na"]);
				result["perf_real"] = json::String(ctx.selection_perf_["real"]);
			}
			if (enable_tracer)
			{
				Forecast_Metrics delta_metrics;
				transaction.calculate_metrics(&delta_metrics);
				json::ObjectP tracer_content;
				Forecast_Simulator::generate_tracer_content(&delta_metrics, &transaction, tracer_content, enable_timezone);
				result["tracer"] = tracer_content;
			}
			transaction.destroy_pointers();

			char buf[1024] = {0};
			for (auto& pit: perf_keys["simulate"])
				sprintf(buf, "%s%ld,", buf, perf[pit] / 1000);
			result["perf_af"] = json::String(buf);
			result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		}
	}
}

time_t Forecast_Service::get_last_output_pending_date(AF_Job_Context* job_ctx)
{
	ads::Guard __g(job_ctx->output_mutex_);
	auto min_date = job_ctx->output_pending_dates_.begin();
	if (*min_date > job_ctx->resume_start_date_)
		return 0;
	for (auto it = job_ctx->output_pending_dates_.begin(); it != job_ctx->output_pending_dates_.end(); ++it)
	{
		if ((*it == (*min_date) + 86400) || *it == *min_date) //next day are outputted
			min_date = it;
		else
			break;
	}
	return *min_date;
}


//Including loading local pending files to redis/files
int Forecast_Service::add_pre_aggregate_tasks(AF_Job_Context* job_ctx, time_t end_date)
{
	if (!job_ctx) return -1;
	if (end_date > job_ctx->current_end_date_ || !(job_ctx->flags_ & AF_Job_Context::FLAG_PRE_AGGREGATE)) return 0;
	for (time_t date = job_ctx->start_pre_aggregate_date_; date < end_date; date += 86400)
	{
		Ads_GUID_Set networks;
		for (const auto& network_id : job_ctx->enabled_networks_)
		{
			if (job_ctx->pre_aggregate_dates_.find(network_id) == job_ctx->pre_aggregate_dates_.end()
				|| job_ctx->pre_aggregate_dates_[network_id].find(date) == job_ctx->pre_aggregate_dates_[network_id].end())
				networks.insert(network_id);
		}
		if (networks.size() != 0 && !Ads_Server::config()->forecast_only_as_counter_)
		{
			Forecast_Pre_Aggregate_Task* task = new Forecast_Pre_Aggregate_Task(job_ctx, date, networks);
			if (Forecast_Service::instance()->pre_aggregate_manager_->add_task(task) < 0)
			{
				delete task;
				ADS_LOG((LP_ERROR, "[PRE AGGREGATE] job=%s, date=%s: cannot add task to manager\n", job_ctx->name().c_str(), ads::string_date(date).c_str()));
			}
		}
		job_ctx->start_pre_aggregate_date_ = date + 86400;
	}
	return 0;
}

void Forecast_Service::AF_Job_Context::stop_output_pending()
{
	Forecast_Service::instance()->output_pending_manager_->remove_waiting_tasks(name());
	time_t last_date = Forecast_Service::instance()->get_last_output_pending_date(this);
	ads::Guard __g(output_mutex_);
	for (time_t date = last_date; date < current_end_date_; date += 86400)
		output_pending_dates_.insert(date);
}

void Forecast_Service::AF_Job_Context::stop_aggregate()
{
	{
	ads::Guard __g(aggregate_dates_mu_);
	for (auto &scenario_aggregate: aggregate_date_ranges_)
	{
		if (!scenario_aggregate.second) return;
		Date_Pair* date_range = NULL;
		while (scenario_aggregate.second->dequeue(date_range, true, false) >= 0 && date_range && date_range != (Date_Pair*)-1)
		{
			ADS_LOG((LP_INFO, "delete aggregate job: scenario %d job %s date: for %s\n", scenario_aggregate.first, name().c_str(), ads::string_date(date_range->first).c_str()));
			delete date_range;
		}
		scenario_aggregate.second->enqueue((Date_Pair*)-1);
	}
	}
	Forecast_Service::instance()->aggregate_manager_->wait(name());
	{
		ads::Guard __g(aggregate_dates_mu_);
		for (auto &scenario: aggregate_date_ranges_)
		{
			if (scenario.second) 
			{
				delete scenario.second;
				scenario.second = nullptr;
			}
		}
	}
}


BEGIN_FORECAST_DISPATCHER(aggregate)
    //-1: waiting finish, 0: restart from beginning, 1: resume && continue
	if (!req->p("redo").empty() && ads::str_to_i64(req->p("redo")) >= 0)
	{
		Forecast_Service::instance()->aggregate(req, result);
	}
	else
	{
		ADS_LOG((LP_INFO, "waiting aggregate job finish\n"));
		Forecast_Service::AF_Job_Context* job_ctx = Forecast_Service::instance()->get_job_context(req, result);
		if (job_ctx) Forecast_Service::instance()->aggregate_manager_->wait(job_ctx->name());
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	}
END_FORECAST_DISPATCHER

void Forecast_Service::reload_pre_aggregate_status(AF_Job_Context* job_ctx, Ads_GUID scenario_id,  std::map<Ads_GUID, std::set<time_t> > & pre_aggregate_dates)
{
	if (!job_ctx || (!(job_ctx->flags_ & AF_Job_Context::FLAG_PRE_AGGREGATE))) return;
	pre_aggregate_dates.clear();
	for (const auto& network_id : job_ctx->enabled_networks_)
	{
		Ads_String status_filename = ads::path_join(Ads_Server::config()->forecast_pre_aggregate_dir_, job_ctx->name(), ads::i64_to_str(scenario_id), ads::entity::str(network_id), "status.csv");
		FILE* read_status_file = ::fopen(status_filename.c_str(), "rb");
//		Ads_Text_File read_status_file;
//		if (read_status_file.fopen(status_filename.c_str()))
		if (read_status_file)
		{
			::fseek(read_status_file, 0, SEEK_SET);	
			char buf[1024 * 8] = {0};
			while (::fgets(buf, sizeof(buf), read_status_file))
//			while (read_status_file.fgets(buf, sizeof(buf)))
			{
				time_t fc_date;
				sscanf(buf, "%ld", &fc_date);
				pre_aggregate_dates[network_id].insert(fc_date);
			}
			::fclose(read_status_file);
		}
	}
	return ;
}

void Forecast_Service::reload_aggregate_status(AF_Job_Context* job_ctx, std::map<Ads_GUID, std::set<time_t> >& finished_dates)
{
	ads::Guard __g(job_ctx->aggregate_dates_mu_);
	Ads_String status_filename = ads::path_join(Ads_Server::config()->forecast_result_dir_, job_ctx->name(), "status.csv");
	Ads_Text_File read_status_file;
	finished_dates.clear();
	if (read_status_file.fopen(status_filename.c_str()))
	{
			char buf[1024 * 8] = {0};
			while (read_status_file.fgets(buf, sizeof(buf)))
			{
				time_t fc_date;
				Ads_GUID scenario_id;
				sscanf(buf, "%ld, %ld", &scenario_id, &fc_date);
				finished_dates[scenario_id].insert(fc_date);
			}
	}
	return;
}

void Forecast_Service::aggregate(Ads_Request* req, json::ObjectP& result)
{
	size_t redo = ads::str_to_i64(req->p("redo")); //-1: waiting finish, 0: restart from beginning, 1: resume && continue
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	AF_Job_Context* job_ctx = get_job_context(req, result);
	if (!job_ctx || job_ctx->scenario_ids_.size() == 0 ) return;
	if (job_ctx->enabled_networks_.empty() || Ads_Server::config()->forecast_only_as_counter_)
	{
		if (!(job_ctx->flags_ & AF_Job_Context::FAST_SIMULATE_OD))//touch empty result dir for NT/Timezone
			ads::ensure_directory(ads::path_join(Ads_Server::config()->forecast_result_dir_, job_ctx->name()).c_str());

		Ads_String msg = "aggregate done with empty closure_networks or enabled_networks for ";
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		result["data"] = json::String(msg + job_ctx->name());
		ADS_LOG_RETURN((LP_INFO, "%s%s\n", msg.c_str(), job_ctx->name().c_str()), );
	}
	if (job_ctx->flags_ & AF_Job_Context::FAST_SIMULATE_WIMAS)
		return;

	Ads_GUID last_scenario_id = *job_ctx->scenario_ids_.rbegin();
	job_ctx->current_scenario_id_ = last_scenario_id; //only last scenario need to pre_aggregate data again
//	if (job_ctx->current_scenario_id_ == 0) 
	{
		reload_pre_aggregate_status(job_ctx, job_ctx->current_scenario_id_, job_ctx->pre_aggregate_dates_);
		job_ctx->flags_ |= AF_Job_Context::FLAG_PRE_AGGREGATE;
		job_ctx->output_pending_dates_.clear();
		job_ctx->start_pre_aggregate_date_ = job_ctx->predict_start_date_;
		job_ctx->current_end_date_ = job_ctx->predict_end_date_;
		add_pre_aggregate_tasks(job_ctx, job_ctx->predict_end_date_);
		Forecast_Service::instance()->pre_aggregate_manager_->wait(job_ctx->name());
	}

	ADS_LOG((LP_INFO, "aggregate: %s\n", job_ctx->to_string().c_str()));
	Forecast_Aggregator aggregator(repo, *job_ctx);
	int ret = 0;
	if (job_ctx->flags_ & AF_Job_Context::FAST_SIMULATE_OD)
	{
		if (job_ctx->aggregate_date_ranges_[0]) job_ctx->aggregate_date_ranges_[0]->enqueue((Date_Pair*)-1); //make sure aggregate job will be finished
		ret = aggregator.aggregate_on_demand(job_ctx);
	}
	else
	{
		if (redo == 0)  //restart from beginning
		{
			job_ctx->stop_aggregate();
			remove_directory(ads::path_join(Ads_Server::config()->forecast_result_dir_, job_ctx->name()));
			ret = aggregator.aggregate_nightly(job_ctx);
		} 
		else 
		{
			while (!job_ctx->enqueue_aggregate_finish_) sleep(1); //wait signal, prepare_nightly task has all been inserted
			{
			ads::Guard __g(job_ctx->aggregate_dates_mu_);
			if (job_ctx->aggregate_date_ranges_[last_scenario_id]) job_ctx->aggregate_date_ranges_[last_scenario_id]->enqueue((Date_Pair*)-1); //make sure aggregate job will be finished
			}
			Forecast_Service::instance()->aggregate_manager_->wait(job_ctx->name());
		}
	}
	ADS_LOG((LP_INFO, "aggregate done: %s\n", job_ctx->to_string().c_str()));
	if (ret < 0 || job_ctx->pending_lost_)
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String("pending result lost, please resume nightly/rerun od"); //TODO: report exact fail reason
	}
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
}

BEGIN_FORECAST_DISPATCHER(plan_info)
	Forecast_Service::instance()->plan_info(req, result);
END_FORECAST_DISPATCHER

void Forecast_Service::plan_info(Ads_Request* req, json::ObjectP& result)
{
	Ads_GUID_Set networks;
	ads::str_to_entities(req->p("networks"), ADS_ENTITY_TYPE_NETWORK, 0, std::inserter(networks, networks.begin()));
    int64_t timezone = req->p("timezone").empty() ? -1 : ads::str_to_i64(req->p("timezone"));

	Ads_String plan_dir = Ads_Server::instance()->config()->forecast_plan_dir_;
	time_t plan_id = 0;
	if (!req->p("plan_id").empty())
		plan_id = ads::str_to_i64(req->p("plan_id").c_str());
	if (!plan_id)
    {
        time_t sample_updated_plan_id = 0;
        if (Forecast_Planner::latest_job_id(plan_dir, plan_id, sample_updated_plan_id, timezone) < 0) return;
	}
	Ads_String_List scenario_ids;
	ads::Dirent::children(ads::path_join(plan_dir, ads::i64_to_str(plan_id) + '_' + ads::i64_to_str(timezone)), DT_DIR, scenario_ids);
	json::Array plans;
	json::Array errors;
	for (Ads_String_List::const_iterator s_it = scenario_ids.begin(); s_it != scenario_ids.end(); ++s_it)
	{
		Ads_GUID scenario_id = ads::str_to_i64(*s_it);
		Plan_Info plan_info;
		if (Forecast_Service::plan_info(networks, plan_info, plan_id, timezone, scenario_id) < 0)
		{
			errors.Insert(json::String("no plan for plan_id = " + ads::i64_to_str(plan_id) + " scenario_id = " + ads::i64_to_str(scenario_id))); 
		}
		else
		{
			json::Object plan;
			json::Array data;
			for (Plan_Info::Network_Plan_Infos::const_iterator nit = plan_info.network_plan_infos_.begin(); nit != plan_info.network_plan_infos_.end(); ++ nit)
			{
				json::Object net_plan;
				net_plan["network_id"] = json::Number(ads::entity::id(nit ->first));
				net_plan["start_date"] = json::String(ads::string_date(nit ->second.first));
				net_plan["end_date"] = json::String(ads::string_date(nit ->second.second));
				data.Insert(net_plan);
			}
			plan["plan_id"] = json::Number(plan_info.plan_id_);
			plan["scenario_id"] = json::Number(plan_info.scenario_id_);
			plan["capacity"] = json::Number(plan_info.capacity_);
			plan["sample_ratio"] = json::Number(plan_info.sample_ratio_);
			plan["total_sampled_request"] = json::Number(plan_info.total_sampled_request_);
			plan["data"] = data;
			plans.Insert(plan);
		}
	}
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	result["data"] = plans;
	if (errors.Size() > 0)
		result["error"] = errors;
}

int
Forecast_Service::dispatch(const json::Object& o)
{
	return 0;
}

int
Forecast_Service::reload_feedback(Scenario_Feedback_Map& feedback, time_t start, time_t end, bool network_only)
{
	ADS_LOG((LP_INFO, "reload feedback date range [%s, %s) network_only=%d\n", ads::string_date(start).c_str(), ads::string_date(end).c_str(), int(network_only) ));
	feedback.clear();
	Ads_String root_dir(ads::path_join(Ads_Server::config()->forecast_exchange_dir_, "feedback"));
	Ads_String_List network_ids;
	if (ads::Dirent::children(root_dir, DT_DIR, network_ids) < 0) 
		ADS_LOG_RETURN((LP_ERROR, "Cannot opendir to scan %s.\n", root_dir.c_str()), -1);

	for (Ads_String_List::const_iterator it = network_ids.begin(); it != network_ids.end(); ++it)
	{
		Ads_String base_dir = ads::path_join(root_dir, *it);
		Ads_GUID network_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_NETWORK, *it);
		if (ads::entity::is_valid_id(network_id))
		{
			Forecast_Store *store = new Forecast_Store(network_id, root_dir);
			store->read_feedback(base_dir, start, end, feedback);
			delete store;
		}
	}

	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	Scenario_Feedback_Map networks;
	int64_t num_ss = 0;
	for (Scenario_Feedback_Map::const_iterator sit = feedback.begin(); sit != feedback.end(); ++sit)
	{
		Ads_GUID scenario_id = sit->first;
		for (Feedback_Map::const_iterator it = sit->second.begin(); it !=sit->second.end(); ++it)
		{
			num_ss ++;
			const Ads_Asset* section = NULL;
			if (Ads_Repository::find_section(repo, it->first, section) >= 0 && section)
				networks[scenario_id][section->network_id_] += it->second;
		}
	}
	ADS_LOG((LP_INFO, "%d networks %d scenario %ld site sections loaded\n", networks.size(), feedback.size(), num_ss));
	if (network_only)
		feedback.swap(networks);
	else
	{
		for (Scenario_Feedback_Map::const_iterator sit = networks.begin(); sit != networks.end(); ++ sit)
			feedback[sit->first].insert(sit->second.begin(), sit->second.end());
	}
	return 0;
}

int
Forecast_Service::reload_external_feedback(Scenario_External_Feedback_Map& scenario_external_feedback, time_t start, time_t end)
{
	ADS_LOG(( LP_INFO, "start reload external feedback date range [%s, %s)\n", ads::string_date(start).c_str(), ads::string_date(end).c_str() ));
	scenario_external_feedback.clear();

	const Ads_String name = "external_feedback";
	const Ads_String root_dir( ads::path_join(Ads_Server::config()->forecast_exchange_dir_, name) );
	Ads_String_List network_ids;

	if (ads::Dirent::children(root_dir, DT_DIR, network_ids) < 0)
    {
		ADS_LOG_RETURN((LP_ERROR, "Cannot opendir to scan %s.\n", root_dir.c_str()), -1);
    }

	for (Ads_String_List::const_iterator it = network_ids.begin(); it != network_ids.end(); ++it)
	{
		Ads_String base_dir = ads::path_join(root_dir, *it);
		Ads_GUID network_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_NETWORK, *it);
		if ( ads::entity::is_valid_id(network_id) )
		{
			Forecast_Store *store = new Forecast_Store(network_id, root_dir);
			store->read_external_feedback(base_dir, start, end, scenario_external_feedback);
			delete store;
		}
	}

	scenario_external_feedback.init_stats_variables();

	ADS_LOG(( LP_INFO, "finish reload external feedback date range [%s, %s)\n", ads::string_date(start).c_str(), ads::string_date(end).c_str() ));
	return 0;
}

void
Forecast_Service::dump_feedback_by_day(Ads_GUID network_id, Ads_GUID scenario_id, Ads_String& type, Ads_GUID id, time_t start, time_t end, bool is_local, json::ObjectP& result) 
{
	AF_Job_Context job_ctx;
	Ads_String name = "feedback";
	Forecast_Store* store = NULL;
	Ads_GUID_Set scenarios;
	if (is_local) 
		store = get_store(network_id);
	else
	{
		Ads_String root_dir(ads::path_join(Ads_Server::config()->forecast_exchange_dir_, name));
		store = new Forecast_Store(network_id, root_dir);
	}
	if (!store) return;
	if (scenario_id >= 0) 
		scenarios.insert(scenario_id);
	else 
	{
		if (is_local)
			store->get_scenarios(scenarios);
		else
		{
			Ads_String base_dir(ads::path_join(Ads_Server::config()->forecast_exchange_dir_, name, ADS_ENTITY_ID_CSTR(network_id)));
			store->get_scenarios(base_dir, scenarios);
		}
	}

	for (time_t date = start; date < end; date += 86400)
	{
		json::ObjectP daily;
		for (Ads_GUID_Set::const_iterator sit = scenarios.begin(); sit != scenarios.end(); ++sit)
		{
			json::ObjectP scenario;
			Feedback_Map feedback_map;
			if (is_local)
			{
				if (type == "site_section" && ads::entity::is_valid_id(id))
					store->get_ack_ratio(*sit, date, id, feedback_map[id]);
				else
					store->load_feedback(*sit, date, date + 86400, feedback_map);
			}
			else
			{
				Ads_String base_dir(ads::path_join(Ads_Server::config()->forecast_exchange_dir_, name, ADS_ENTITY_ID_CSTR(network_id)));
				store->read_feedback(*sit, base_dir, date, date + 86400, feedback_map);
			}	
			Pod_Section_Ack_Ratio feedback;
			for (Feedback_Map::const_iterator it = feedback_map.begin(); it != feedback_map.end(); ++it)
			{
				if (type == "site_section")
				{
					if (it->first != id)
						continue;
				}
				else //aggregate network level
				{
					feedback += it->second;
				}
				json::ObjectP o;
				it->second.to_json(o);
				scenario[ads::entity::tag_str(it->first)] = o;
			}
			if (type != "site_section")
			{
				json::ObjectP o;
				feedback.to_json(o);
				scenario[ads::entity::tag_str(network_id)] = o;
			}
			daily[ads::i64_to_str(*sit)] = scenario;
		}
		result[ads::string_date(date)] = daily;
	}
	if (!is_local)
	{
		delete store;
	}
}

void
Forecast_Service::dump_external_feedback_by_day(const Ads_GUID network_id, const Ads_GUID scenario_id, const Ads_GUID ad_id, const time_t start, const time_t end, const bool is_local, json::ObjectP& result)
{
	const Ads_String name = "external_feedback";
	Forecast_Store* store = NULL;

	if (is_local)
	{
		store = get_store(network_id);
	}
	else
	{
		Ads_String root_dir(ads::path_join(Ads_Server::config()->forecast_exchange_dir_, name));
		store = new Forecast_Store(network_id, root_dir);
	}
	if (!store) return;

	Ads_GUID_Set scenarios;
	if (scenario_id >= 0)
	{
		scenarios.insert(scenario_id);
	}
	else
	{
		if (is_local)
		{
			store->get_scenarios(scenarios);
		}
		else
		{
			Ads_String base_dir(ads::path_join(Ads_Server::config()->forecast_exchange_dir_, name, ADS_ENTITY_ID_CSTR(network_id)));
			store->get_scenarios(base_dir, scenarios);
		}
	}

	for (time_t date = start; date < end; date += 86400)
	{
		json::ObjectP daily;
		for (const auto& scenario_id : scenarios)
		{
			json::ObjectP scenario;
			External_Feedback_Map external_feedback;
			if (is_local)
			{
				store->load_external_feedback(scenario_id, date, date + 86400, external_feedback);
			}
			else
			{
				Ads_String base_dir(ads::path_join(Ads_Server::config()->forecast_exchange_dir_, name, ADS_ENTITY_ID_CSTR(network_id)));
				store->read_external_feedback(scenario_id, base_dir, date, date + 86400, external_feedback);
			}

			for (const auto& ad_it : external_feedback)
			{
				const Ads_GUID cur_ad_id = ad_it.first;
				const External_Candidate_Stats& cand_stats = ad_it.second;

				if (ad_id != -1 && cur_ad_id != ad_id)
					continue;

				json::ObjectP o;
				cand_stats.to_json(o);
				scenario[ads::entity_id_to_str(cur_ad_id)] = o;
			}
			daily[ads::i64_to_str(scenario_id)] = scenario;
		}
		result[ads::string_date(date)] = daily;
	}

	if (!is_local)
	{
		delete store;
	}
}

Forecast_Store *
Forecast_Service::get_store(Ads_GUID network_id)
{
	ads::Guard __g(this->store_mutex_);

	Forecast_Store *&store = stores_[network_id];
	if (! store)
	{
		store = new Forecast_Store(network_id, Ads_Server::config()->forecast_store_dir_);
	}

	return store;
}

int
Forecast_Service::list_store(std::list<Forecast_Store*>& stores)
{
	const Ads_String& dir = Ads_Server::config()->forecast_store_dir_;
	ads::Dirent dirent;
	if (dirent.open(dir.c_str()) < 0)
		ADS_LOG_RETURN((LP_ERROR, "Cannot opendir to scan %p.\n", dir.c_str()), -1);
	struct dirent *dp;
	while (( dp = dirent.read() ))
	{
		Ads_GUID network_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_NETWORK, dp->d_name);
		if (this->enabled_networks_.find(network_id) != this->enabled_networks_.end())
		{
			Forecast_Store * store = get_store(network_id);
			if (store) stores.push_back(store);
		}
	} 
	return 0;
}

int
Forecast_Service::list_all_store(const AF_Job_Context* job_ctx, std::list<Forecast_Store*>& stores)
{
	const Ads_String& dir = Ads_Server::config()->forecast_store_dir_;
	ads::Dirent dirent;
	if (dirent.open(dir.c_str()) < 0)
		ADS_LOG_RETURN((LP_ERROR, "Cannot opendir to scan %p.\n", dir.c_str()), -1);

	for (Ads_GUID_Set::const_iterator it = job_ctx->closure_networks_.begin(); it != job_ctx->closure_networks_.end(); ++it)
	{
		Ads_GUID network_id = *it;
		if (job_ctx->excluded_networks_.find(network_id) != job_ctx->excluded_networks_.end()) continue;
		ads::ensure_directory(ads::path_join(dir, ads::entity_id_to_str(network_id)).c_str());
		Forecast_Store * store = get_store(network_id);
		if (store) stores.push_back(store);
	} 
	return 0;
}


Ads_Shared_Data_Service*
Forecast_Service::get_counter(const Ads_String& counter_name, const Ads_String_Pair_Vector* global_counter_addresses)
{
	if (counter_name.empty()) return Ads_Shared_Data_Service::instance();
	Ads_Shared_Data_Service* counter = Ads_Shared_Data_Service::instance_by_name(counter_name);
	counter->enable_client("");
	if (Ads_Server::config()->shared_data_enable_server_) counter->enable_server("", false);
	counter->connect_addresses(global_counter_addresses);
	counter->open();
	return counter;
}

int
Forecast_Service::destroy_counter(const Ads_String& counter_name)
{
	if (Ads_Server::config()->shared_data_enable_server_)
	{
		ADS_LOG_RETURN((LP_INFO, "TODO don't cleanup counter for %s due to global counter", counter_name.c_str()), -1);
	}
	Ads_Shared_Data_Service* counter = Ads_Shared_Data_Service::instance_by_name(counter_name, false);
	if (counter && !counter->name().empty())
	{
		counter->stop();
		Ads_Shared_Data_Service::destroy_instance(counter_name);
	}
	return 0;
}

//AFCounter01.fwmrm.net:1081~AFScheduler03.fwmrm.net:1080
void
Forecast_Service::connect_parse(const Ads_String& connects, Ads_String_Pair_Vector* connect_addresses) const
{
	Ads_String_List conns;
	Ads_String_List addr_port;
	ads::split(connects,conns,"~");
	for (auto& conn: conns )
	{
		ads::split(conn,addr_port,":");
		if (addr_port.size() == 2)
			connect_addresses->push_back(std::make_pair(addr_port[0],addr_port[1]));
	}
}

void
Forecast_Service::get_timezone_info(std::set<int>& gmt_offsets) const
{
	//lxu TODO OD optimization
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	for (auto& it: repo->tzoffset2_) //second
		gmt_offsets.insert(it.second);
	return;
}

void
Forecast_Service::parse_enabled_networks(Ads_Request* req, Ads_GUID_Set& closure, Ads_GUID_Set& enabled_networks, Ads_GUID_Set& excluded_networks) const
{
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	Network_Map networks;
	if (!req->p("excluded_networks").empty()) 
	{
		ads::str_to_entities(req->p("excluded_networks"), ADS_ENTITY_TYPE_NETWORK, 0, std::inserter(excluded_networks, excluded_networks.begin()));
		for (Ads_Network_Map::const_iterator nit = repo->networks().begin(); nit != repo->networks().end(); ++ nit) 
		{
			const Ads_Network* network = nit->second;
			if (excluded_networks.find(network->id_) != excluded_networks.end()) continue;
			if (!Ads_Server::config()->forecast_network_enabled(network->id_)) continue;
			if (network->type_ != Ads_Network::NORMAL) continue;
			networks.insert(std::make_pair(nit->first, network));
		}
	}
	else if (!req->p("network_id").empty())
	{
		Ads_GUID network_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_NETWORK, req->p("network_id"));
		const Ads_Network* network = NULL;
		if (repo->find_network(network_id, network) < 0 || !network) return;
		networks.insert(std::make_pair(network_id, network));
	}
	else if (!req->p("enabled_networks").empty()) 
	{
		Ads_GUID_Set network_ids;
		ads::str_to_entities(req->p("enabled_networks"), ADS_ENTITY_TYPE_NETWORK, 0, std::inserter(network_ids, network_ids.begin()));
		for (Ads_GUID_Set::const_iterator it = network_ids.begin(); it != network_ids.end(); ++it)
		{
			const Ads_Network* network = NULL;
			if (repo->find_network(*it, network) < 0 || !network) continue;
			if (network->type_ != Ads_Network::NORMAL) continue;
			networks.insert(std::make_pair(*it, network));
		}
		for (Ads_Network_Map::const_iterator nit = repo->networks().begin(); nit != repo->networks().end(); ++ nit) 
		{
			const Ads_Network* network = nit->second;
			if (network_ids.find(network->id_) != network_ids.end()) continue;
			if (network->type_ != Ads_Network::NORMAL) continue;
			excluded_networks.insert(network->id_);
		}
	}

	if (req->p("enabled_networks").empty() && req->p("excluded_networks").empty() && req->p("network_id").empty()) 
	{
		for (Network_Map::const_iterator it = enabled_networks_.begin(); it != enabled_networks_.end(); ++it)
		{
			const Ads_Network* network = it->second;
			enabled_networks.insert(network->id_);
		}
		for (Ads_Network_Map::const_iterator nit = repo->networks().begin(); nit != repo->networks().end(); ++ nit) 
		{
			const Ads_Network* network = nit->second;
			if (network->type_ != Ads_Network::NORMAL) continue;
			closure.insert(network->id_);
		}

	}
	else
	{
		for (Network_Map::const_iterator it = networks.begin(); it != networks.end(); ++it)
		{
			const Ads_Network* network = it->second;
			closure.insert(network->config()->network_ids_in_closure_.begin(), network->config()->network_ids_in_closure_.end());

			if (enabled_networks_.find(network->id_) != enabled_networks_.end()) 
				enabled_networks.insert(network->id_);
		}
	}
}

int Forecast_Service::rotate_virtual_date(Ads_Shared_Data_Service* counter_service, time_t fc_date, Ads_String& error_message)
{
	if (! counter_service) return -1;
	//reset
	const size_t mask = Ads_Repository::periodic_mask(fc_date);
	if (counter_service->reset_access_rule_counters(mask) < 0)
	{
		error_message = "reset_access_rule_counters failed";
		return -1;
	}
	if (counter_service->rotate_ad_delivery_history(false) < 0)
	{
		error_message += "rotate_ad_delivery_history failed";
		return -1;
	}
	if (Ads_Server::config()->forecast_flush_counter_daily_) // MRM-28946
	{
		counter_service->update_counter_with_message(Ads_Shared_Data_Message::MESSAGE_SYNC_COUNTERS);
	}
	else
		counter_service->request_sync_counters(true);
	return 0;
}

int
Forecast_Service::save_counters(AF_Job_Context* context, const Ads_String& filename)
{
	if (context->flags_ & AF_Job_Context::FAST_SIMULATE_OD) return 0;
	Ads_String path = ads::path_join(Ads_Server::config()->forecast_working_dir_, "counter", ads::i64_to_str(context->current_scenario_id_), ads::i64_to_str(context->timezone_));
	Ads_String filepath = ads::path_join(path, filename);
	ads::ensure_directory_for_file(filepath.c_str());
	ADS_LOG((LP_INFO, "before %s save, pending_increments size: %d, pending_packets size: %d\n", filepath.c_str(), context->counter_service_->num_pending_increments(), context->counter_service_->num_pending_packets()));
	context->counter_service_->save_counters(Ads_Server::config()->shared_data_enable_server_, filepath.c_str());
	return 0;
}

int
Forecast_Service::rename_counter_files(AF_Job_Context* context)
{
	Ads_String path = ads::path_join(Ads_Server::config()->forecast_working_dir_, "counter", ads::i64_to_str(context->current_scenario_id_), ads::i64_to_str(context->timezone_));
	ADS_LOG((LP_INFO, "renaming counter\n"));
	{
		Ads_String_List counter_files;
		ads::Dirent::children(path, DT_REG, counter_files);
		for (Ads_String_List::const_iterator counter_file = counter_files.begin(); counter_file != counter_files.end(); ++counter_file)
		{
			if (counter_file ->length() > 4 && counter_file ->rfind(".dat") == counter_file ->length() - 4)
				rename(ads::path_join(path, *counter_file).c_str(), ads::path_join(path, (*counter_file) + ".serving").c_str());
		}
	}
	return 0;
}

int Forecast_Service::plan_info(const Ads_GUID_Set & networks, Plan_Info & plan_info, time_t& plan_id, int64_t& timezone, const Ads_GUID scenario_id)
{
	Ads_String plan_dir = Ads_Server::instance()->config()->forecast_plan_dir_;
	bool is_networks_empty = networks.empty();
	time_t sample_updated_plan_id = 0;
	if(!plan_id)
	{
		if (Forecast_Planner::latest_job_id(plan_dir, plan_id, sample_updated_plan_id, timezone) < 0) return -1;
	}
	if (!plan_id || timezone < 0) return -1;
	plan_info.plan_id_ = plan_id;
	plan_info.scenario_id_ = scenario_id;
	plan_dir = ads::path_join(plan_dir, ads::entity::str(plan_id) + '_' + ads::i64_to_str(timezone), ads::i64_to_str(scenario_id));
    ADS_DEBUG((LP_DEBUG, "plan_id: %d, timezone: %d, plan_dir: %s\n", plan_id, timezone, plan_dir.c_str()));
    Ads_String_List network_ids;
	{
		Ads_String stats_file =  ads::path_join(plan_dir, "stats");
		FILE * fin = fopen(stats_file.c_str(), "r");
		if (fin) 
		{
			char buf[1024] = {0};
			fread(buf, 1, sizeof(buf), fin);
			sscanf(buf, "sample_ratio=%lf, capacity=%ld, total_sampled_request=%ld",
				&plan_info.sample_ratio_,
				&plan_info.capacity_,
				&plan_info.total_sampled_request_);
			fclose(fin);
		}		
	}

	if (ads::Dirent::children(plan_dir, DT_DIR, network_ids) < 0) return -1;
	for (Ads_String_List::const_iterator n_it = network_ids.begin(); n_it != network_ids.end(); ++n_it)
	{
		Ads_GUID network_id = ads::str_to_i64(n_it->c_str());
		ADS_LOG((LP_INFO, "load plan info for %d\n", ads::entity::id(network_id)));
		if (ads::entity::str(network_id) != *n_it) continue;
		network_id = ads::entity::make_id(ADS_ENTITY_TYPE_NETWORK, network_id);
		if (!is_networks_empty && networks.find(network_id) == networks.end()) continue;

		size_t start_date = (size_t)-1, end_date = 0;
		Ads_String dt_file = ads::path_join(plan_dir, *n_it, "dateinfo");
		FILE *fp = fopen(dt_file.c_str(), "r");
		if(fp)
		{
			char buf[9] = {0};
			size_t plan_date;
			while(fgets(buf, sizeof(buf), fp))
			{
				plan_date = ads::str_to_time(buf, "%Y%m%d");
				plan_info.forecast_networks_[plan_date].insert(network_id);
				if (start_date > plan_date) start_date = plan_date;
				if (end_date < plan_date) end_date = plan_date;
			}
		}
		if (start_date <= end_date) plan_info.network_plan_infos_[network_id] = std::make_pair(start_date, end_date);
	}
	return 0;
}

int
Forecast_Service::remove_directory(const Ads_String & path)
{
	Ads_String_List children;
	ads::Dirent::children(path, DT_DIR, children);
	for (Ads_String_List::const_iterator child = children.begin(); child != children.end(); ++child)
	{
		Ads_String child_name = ads::path_join(path, *child);
		Forecast_Service::remove_directory(child_name);
	}
	children.clear();
	ads::Dirent::children(path, size_t(-1), children);
	for (Ads_String_List::const_iterator child = children.begin(); child != children.end(); ++child)
	{
		::remove(ads::path_join(path, *child).c_str());
	}
	::remove(path.c_str());
	return 0;
}

int
Forecast_Service::init_closures()
{
	ads::Guard _g(network_map_mutex_);
	this->closure_networks_.clear();
	this->enabled_networks_.clear();
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	const Ads_Server_Config* server_config = Ads_Server::config();
	for (Ads_Network_Map::const_iterator it = repo->networks_.begin(); it != repo->networks_.end(); ++it)
	{
		const Ads_Network* network = it->second;
		if (network->type_ == Ads_Network::NORMAL) this->forecast_days_[it->first] = forecast_days(it->second);
		if (network->type_ == Ads_Network::NORMAL && server_config->forecast_collector_network_enabled(it->first))
		{
			this->enabled_networks_.insert(std::make_pair(it->first, network));
			this->closure_networks_.insert(std::make_pair(it->first, network));
			if (!network->config()) continue;
			for (Ads_GUID_RSet::const_iterator n_it = network->config()->network_ids_in_closure_.begin(); n_it != network->config()->network_ids_in_closure_.end(); ++n_it)
			{
				if (this->closure_networks_.find(*n_it) != this->closure_networks_.end()) continue;
				const Ads_Network* network = NULL;
				if (repo->find_network(*n_it, network) < 0 || !network) continue;
				this->closure_networks_.insert(std::make_pair(*n_it, network));
			}
		}
	}
	ADS_LOG((LP_INFO, "# enabled networks = %d, closure networks = %d\n", this->enabled_networks_.size(), this->closure_networks_.size()));
	return 0;
}

int 
Forecast_Service::forecast_days(const Ads_Network* network)
{
	if (network && network->config() && network->config()->forecast_days_ > 0)
		return network->config()->forecast_days_;
	return Ads_Server::config()->forecast_days_;
}

int
Forecast_Service::forecast_days(Ads_GUID network_id)
{
	std::map<Ads_GUID, int>::const_iterator it = forecast_days_.find(network_id);
	if (it != forecast_days_.end()) return it->second;
	return Ads_Server::config()->forecast_days_;
}

int
Forecast_Service::max_forecast_days_on_r_chains(const Ads_Network* network)
{
	if (network && network->config() && network->config()->max_forecast_days_on_r_chains_ > 0)
		return network->config()->max_forecast_days_on_r_chains_;
	return Ads_Server::config()->forecast_days_;
}

size_t
Forecast_Service::max_forecast_days(const AF_Job_Context* job_ctx, const Ads_String& enabled_networks)
{
	int max_days = -1;
	Ads_Server::Repository_Guard __g(Ads_Server::instance());
	const Ads_Repository* repo = __g.asset_repository();
	if (job_ctx->flags_ & AF_Job_Context::NIGHTLY_TIMEZONE_SPECIFIC)
	{
		Ads_GUID_Set network_ids;
		ads::str_to_entities(enabled_networks, ADS_ENTITY_TYPE_NETWORK, 0, std::inserter(network_ids, network_ids.begin()));
		for (Ads_GUID_Set::const_iterator it = network_ids.begin(); it != network_ids.end(); ++it)
		{
			const Ads_Network* network = NULL;
			if (repo->find_network(*it, network) < 0 ||!network) continue;
			max_days = std::max(max_days, Forecast_Service::forecast_days(network));
		}
	}
	else if (job_ctx->flags_ & AF_Job_Context::NIGHTLY)
	{
		for (Ads_Network_Map::const_iterator it = repo->networks_.begin(); it != repo->networks_.end(); ++it)
		{
		   if (job_ctx->excluded_networks_.find(it->first) != job_ctx->excluded_networks_.end()) continue;
		   max_days = std::max(max_days, Forecast_Service::forecast_days(it->second));
		}
	}
	return (size_t) max_days;
}


int Forecast_Service::ingest_feedback(Ads_String& path, json::ObjectP& result)
{
	Ads_String_List data_files;
	ads::Dirent::children(path, DT_REG, data_files);
	ads::Guard _g(this->feedback_mutex_);
	this->feedback_.clear();
	for (Ads_String_List::const_iterator data_file = data_files.begin(); data_file != data_files.end(); ++data_file)
	{
		if (data_file->length() > 4 && data_file->rfind(".sig") == data_file->length() - 4) continue;
		FILE * fin = fopen(ads::path_join(path, *data_file).c_str(), "r");
		if (!fin) 
		{
			result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
			result["error"] = json::String(Ads_String("Cannot open data_file ") + *data_file);
			return -1;
		}
		else
		{
			char buf[2048] = {0};
			if (data_file->find("inferred_item_view_ratio") != Ads_String::npos)
			{
				while (fgets(buf, sizeof(buf), fin))
				{
					Ads_GUID ss_id = 0, sid = -1;
					size_t num_req = 0, video_view = 0, page_view = 0;
					sscanf(buf, "%ld,%ld,%lu,%lu,%lu", &sid, &ss_id, &num_req, &video_view, &page_view);
					Ads_GUID section_id = ads::entity::make_id(ADS_ENTITY_TYPE_SECTION, ss_id);
					if (!ads::entity::is_valid_id(section_id)) continue;//-1 is eliminated
					if (sid < 0) continue;
					Pod_Section_Ack_Ratio& pf = this->feedback_[sid][section_id];
					pf.requests_ = num_req;
					pf.inferred_video_views_ = video_view;
					pf.inferred_page_views_ = page_view;
				}
			}
			else if (data_file->find("video_view_ratio") != Ads_String::npos)
			{
				while (fgets(buf, sizeof(buf), fin))
				{
					Ads_GUID ss_id = 0, sid = -1;
					size_t video_view = 0, video_req = 0, page_req = 0;
					sscanf(buf, "%ld,%ld,%lu,%lu,%lu", &sid, &ss_id, &video_view, &video_req, &page_req);
					Ads_GUID section_id = ads::entity::make_id(ADS_ENTITY_TYPE_SECTION, ss_id);
					if (!ads::entity::is_valid_id(section_id)) continue;//-1 is eliminated
					if (sid < 0) continue;
					Pod_Section_Ack_Ratio& pf = this->feedback_[sid][section_id];
					pf.video_views_ = video_view;
					pf.requests_ = video_req + page_req;
				}
			}
			else if (data_file->find("site_section_slot_ack_ratio") != Ads_String::npos)
			{
				while (fgets(buf, sizeof(buf), fin))
				{
					Ads_GUID ss_id = 0, sid = -1;
					size_t avail = 0, total = 0;
					char slot_type[0xff];
					memset(slot_type, 0, sizeof(slot_type));
					sscanf(buf, "%ld,%ld,%[^,],%lu,%lu", &sid, &ss_id, slot_type, &avail, &total);
					Ads_GUID section_id = ads::entity::make_id(ADS_ENTITY_TYPE_SECTION, ss_id);
					if (!ads::entity::is_valid_id(section_id)) continue;//-1 is eliminated
					if (sid < 0) continue;
					Pod_Section_Ack_Ratio& pf = this->feedback_[sid][section_id];
					Ads_String _type = slot_type;
					ads::tolower(_type);
					Ads_Log_Processor::TIME_POSITION_CLASS tpcl = Ads_Log_Processor::time_position_class(_type, false);
					pf.ack_[tpcl] = avail;
					pf.total_[tpcl] = total;
				}
			}
			else if (data_file->find("network_slot_ack_ratio") != Ads_String::npos)
			{
				while (fgets(buf, sizeof(buf), fin))
				{
					Ads_GUID nw_id = 0, sid = -1;
					size_t avail = 0, total = 0;
					char slot_type[0xff];
					memset(slot_type, 0, sizeof(slot_type));
					sscanf(buf, "%ld,%ld,%[^,],%lu,%lu", &sid, &nw_id, slot_type, &avail, &total);
					Ads_GUID network_id = ads::entity::make_id(ADS_ENTITY_TYPE_NETWORK, nw_id);
					if (!ads::entity::is_valid_id(network_id)) continue;//-1 is eliminated
					if (sid < 0) continue;
					Pod_Section_Ack_Ratio& pf = this->feedback_[sid][network_id];
					Ads_String _type = slot_type;
					ads::tolower(_type);
					Ads_Log_Processor::TIME_POSITION_CLASS tpcl = Ads_Log_Processor::time_position_class(_type, false);
					pf.ack_[tpcl] = avail;
					pf.total_[tpcl] = total;
				}
			}
			else if (data_file->find("pod_slot_ack_ratio") != Ads_String::npos)
			{
				while (fgets(buf, sizeof(buf), fin))
				{
					Ads_GUID ss_id = 0, sid = -1;
					size_t avail[Pod_Section_Ack_Ratio::MAX_POD_RECORD_NUM] = {0};
					size_t total[Pod_Section_Ack_Ratio::MAX_POD_RECORD_NUM] = {0};
					char slot_type[0xff];
					memset(slot_type, 0, sizeof(slot_type));
					sscanf(buf, "%ld,%ld,%[^,],%lu/%lu,%lu/%lu,%lu/%lu,%lu/%lu,%lu/%lu,%lu/%lu", &sid, &ss_id, slot_type,
									&avail[0], &total[0],
									&avail[1], &total[1],
									&avail[2], &total[2],
									&avail[3], &total[3],
									&avail[4], &total[4],
									&avail[5], &total[5]);
					Ads_GUID section_id = ads::entity::make_id(ADS_ENTITY_TYPE_SECTION, ss_id);
					if (!ads::entity::is_valid_id(section_id)) continue;
					if (sid < 0) continue;
					Pod_Section_Ack_Ratio& pf = this->feedback_[sid][section_id];
					Ads_String _type = slot_type;
					ads::tolower(_type);
					Ads_Log_Processor::TIME_POSITION_CLASS tpcl = Ads_Log_Processor::time_position_class(_type, false);
					if (tpcl == Ads_Log_Processor::MIDROLL)
					{
						for (int i = 0; i < Pod_Section_Ack_Ratio::MAX_POD_RECORD_NUM; ++i)
						{
							pf.mid_records_[i].ack_ = avail[i];
							pf.mid_records_[i].total_ = total[i];
						}
					}
					else if (tpcl == Ads_Log_Processor::OVERLAY)
					{
						for (int i = 0; i < Pod_Section_Ack_Ratio::MAX_POD_RECORD_NUM; ++i)
						{
							pf.overlay_records_[i].ack_ = avail[i];
							pf.overlay_records_[i].total_ = total[i];
						}
					}
				}
			}

		}
		fclose(fin);
	}
	result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	return 0;
}

BEGIN_FORECAST_DISPATCHER(update_conf)
{
	Forecast_Service::instance()->update_conf(req, result);
}
END_FORECAST_DISPATCHER

int Forecast_Service::update_conf(Ads_Request* req, json::ObjectP& result)
{
	Ads_String err = "";

	Ads_String k = req->p("k");
	Ads_String v = req->p("v");
	if (k.empty() || v.empty())
	{
		err = "missing parameter: k=... or v=...";
	}
	else
	{
		if (k == "simulate_slow_threshold")
		{
			int tmp = ads::str_to_i64(v);
			if (tmp >= 0)
			{
				int old_value = set_simulate_slow_threshold(tmp);
				result["old_value"] = json::Number(old_value);
				result["new_value"] = json::Number(tmp);
				ADS_LOG((LP_INFO, "update conf: k=%s, old=%d, new=%d\n", k.c_str(), old_value, tmp));
			}
			else
				err = "invalid value [" + v + "]";
		}
		else
			err = "conf [" + k + "] is not supported";
	}

	if (err.empty())
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
	else
	{
		result["status"] = json::String(FORECAST_SERVICE_STATUS_FAILED);
		result["error"] = json::String(err);
	}
	return 0;
}

int Forecast_Service::set_simulate_slow_threshold(int v)
{
	ads::Guard _g(simulate_slow_threshold_mutex_);
	int old = Ads_Server::config()->forecast_simulate_slow_threshold_;
	Ads_Server::mutable_config()->forecast_simulate_slow_threshold_ = v;
	return old;
}

int Forecast_Service::get_simulate_slow_threshold()
{
	ads::Guard _g(simulate_slow_threshold_mutex_);
	return Ads_Server::config()->forecast_simulate_slow_threshold_;
}

BEGIN_FORECAST_DISPATCHER(conflict_detection) 
	{
		Ads_GUID network_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_NETWORK, req->p("network_id"));
		Ads_GUID placement_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_ADVERTISEMENT, req->p("placement_id"));
		Ads_GUID rule_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_MRM_RULE, req->p("rule_id"));
		const Ads_String &timestamp = req->p("timestamp");

		Ads_String type = ads::entity::is_valid_id(placement_id) ? "placement" : "rule";

		json::Object resp, ui_res;
		if ( !ads::entity::is_valid_id(network_id) || !req->content() || req->content()->empty()
			|| (!ads::entity::is_valid_id(placement_id) && !ads::entity::is_valid_id(rule_id))
			)
		{
			resp["message"] = json::String("bad request");
			ui_res["error"] = resp;
		}
		else
		{
			Ads_Conflict_Reason reason;
			ADS_LOG((LP_INFO, "conflict detection for %s %s: content = %s\n", type.c_str(), type == "placement" ? ADS_ENTITY_ID_CSTR(placement_id) : ADS_RULE_ID_CSTR(rule_id), req->content()->c_str()));

			if (type == "placement")
				Ads_Conflict_Detector::detect_conflict_placement(placement_id, network_id, timestamp, *req->content(), reason);
			else
				Ads_Conflict_Detector::detect_conflict_rule(rule_id, network_id, timestamp, *req->content(), reason);
			if (reason.get_result() == "error")
			{
				resp["message"] = json::String(reason.message_);
				ui_res["error"] = resp;
			}
			else
			{
				resp["result"] = json::String(reason.get_result());
				resp["reason"] = json::String(reason.to_old_ui_string());
				json::ObjectP new_result;
				reason.delete_ignored_object();
				reason.to_json(new_result);
				resp["new_result"] = new_result;
				ui_res["result"] = resp;
			}
		}
		result["status"] = json::String(FORECAST_SERVICE_STATUS_OK);
		result["ui_res"] = ui_res;
	}
END_FORECAST_DISPATCHER

BEGIN_FORECAST_DISPATCHER(upgrade_model) 
	Ads_GUID network_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_NETWORK, req->p("network_id"));
	Ads_String status = FORECAST_SERVICE_STATUS_OK;
	if (!ads::entity::is_valid_id(network_id)) status = FORECAST_SERVICE_STATUS_FAILED;
	else
	{
		Forecast_Service * service = Forecast_Service::instance();
		int ret = service->get_store(network_id)->upgrade_model();
		if (ret < 0)
			status = FORECAST_SERVICE_STATUS_FAILED;
	}
	result["status"] = json::String(status);
END_FORECAST_DISPATCHER

BEGIN_FORECAST_DISPATCHER(load_counter)
	//load counters
	const Ads_String& counter_path = req->p("counter_path");
	const Ads_String& plan_id = req->p("plan_id");
	bool is_forecast_counter = req->p("is_forecast_counter") == "true";
	Forecast_Service* service = Forecast_Service::instance();
	Ads_Shared_Data_Service* counter_service = service->get_counter(plan_id);
	Ads_String error = "";
	if (!counter_path.empty())
	{
		if (counter_service->load_counters(Ads_Server::config()->shared_data_enable_server_, counter_path, is_forecast_counter) < 0)
			error = "Failed to load counter " + counter_path;
	}
	else if (req->content() && req->content()->length() &&
				counter_service->load_counters_from_data(Ads_Server::config()->shared_data_enable_server_, req->content()->c_str()) >= 0);
	else
		error = "Failed to load counter from req";
	result["status"] = json::String(error.empty() ? FORECAST_SERVICE_STATUS_OK : FORECAST_SERVICE_STATUS_FAILED);
	result["error"] = json::String(error);
END_FORECAST_DISPATCHER

//INK-12576
int Forecast_Service::read_repa_info()
{
	const Ads_Server_Config *conf =  Ads_Server::config();
	Ads_String repa_filename = ads::path_join(conf->forecast_working_dir_, "repa.log");
	Ads_Text_File read_file;
	if (!read_file.fopen(repa_filename.c_str(), "rb"))
		return -1;
	char buf[1024 * 8] = {0};
	while(read_file.fgets(buf, sizeof(buf)))
	{
		time_t plan_id;
		char repa_filename[128] = {0};
		sscanf(buf, "%ld,%s", &plan_id, repa_filename);
		time_t now = ::time(NULL);
		if (now > plan_id + 86400 * 3) continue; //ignore old nightly_job > 3 days
		plan_id_to_repa_path_[plan_id] = repa_filename;
	}
	read_file.fclose();
	return 0;
}

int Forecast_Service::write_repa_info()
{
	const Ads_Server_Config *conf =  Ads_Server::config();
	Ads_String repa_filename = ads::path_join(conf->forecast_working_dir_, "repa.log");
	Ads_Output_File file;
	if (file.fopen(repa_filename.c_str(), "wb") < 0)
		return -1;
	for (const auto& kv : plan_id_to_repa_path_)
	{
		time_t now = ::time(NULL);
		if (now > kv.first + 86400 * 3) continue; //ignore old nightly_job > 3 days
		if (kv.second.empty()) continue;
		//plan_id,repa_filename
		Ads_String content = ads::i64_to_str(kv.first) + "," + kv.second + "\n";
		file.fwrite(content);
	}
	file.fclose();
	return 0;
}

int Forecast_Service::append_repa_info(const time_t plan_id, const Ads_String& repa_path)
{
	const Ads_Server_Config *conf =  Ads_Server::config();
	Ads_String repa_filename = ads::basename(repa_path.c_str());
	if (plan_id == 0 || repa_filename.empty()) return 0;
	plan_id_to_repa_path_[plan_id] = repa_filename;
	Ads_String repa_log = ads::path_join(conf->forecast_working_dir_, "repa.log");
	FILE* file = ::fopen(repa_log.c_str(), "ab+");
	if (!file) return -1;
	Ads_String content = ads::i64_to_str(plan_id) + "," + repa_filename + "\n";
	::fputs(content.c_str(), file);
	::fclose(file);
	return 0;
}

int Forecast_Service::load_old_repa(const time_t plan_id, const Ads_String& pattern, Ads_String& file, int& index_out, time_t &time, int& version_out) 
{
	if (plan_id == 0 || pattern != "repa-x-"
			|| plan_id_to_repa_path_.find(plan_id) == plan_id_to_repa_path_.end()
			|| plan_id_to_repa_path_[plan_id].empty())
		return -1;
	const Ads_Server_Config *conf =  Ads_Server::config();
	Ads_String filename = plan_id_to_repa_path_[plan_id];
	Ads_String fn = ads::path_join(conf->pusher_working_dir_, filename);
	Ads_String prefix = filename.substr(0, pattern.length());
	int index = index_out;
	if(index == 0 && prefix != "repa-0-") return -1;
	if(index == 1 && prefix != "repa-1-") return -1;
	Ads_String name_format = "repa-%d-%04d%02d%02d%02d%02d%02d-v%d.dat";
	Ads_String pending_file = filename.substr(0, filename.size() - 4) + ".pen";
	if (::access(ads::path_join(conf->pusher_working_dir_, pending_file).c_str(), R_OK) >= 0)
	{
		ADS_LOG((LP_ERROR, "pending file %s found\n", pending_file.c_str()));
		return -1;
	}
	struct stat st;
	if(stat(fn.c_str(), &st) < 0) 
		return -1;
	struct tm tms;
	int version = 0;
	int idx = -1;
	if (::sscanf(filename.c_str(), name_format.c_str(),
			&idx, &tms.tm_year, &tms.tm_mon, &tms.tm_mday,
			&tms.tm_hour, &tms.tm_min, &tms.tm_sec,
			&version) < 7) 
		return -1;
	if ((size_t)version != Ads_Repository::version()) return -1;
	file = ads::path_join(conf->pusher_working_dir_, filename);
	index_out = idx;
	version_out = version;
	tms.tm_year -= 1900;
	tms.tm_mon -= 1;
	time = mktime(&tms);
	ADS_LOG((LP_INFO, "load old repa %s for plan_id %ld.\n", file.c_str(), plan_id));
	return 0;
}

int Forecast_Pending_Processer::touch_progress(const Forecast_Service::AF_Job_Context* job_ctx, const time_t fc_date)
{
//	bool is_redis = Ads_Redis_Cluster_Client::instance()->enabled(Ads_Redis_Cluster_Client::DB_FORECAST_SIMULATE) && job_ctx;
//	int ret = -1;
	Ads_String key = ads::path_join(job_ctx->name(), ads::i64_to_str(job_ctx->current_scenario_id_), ads::string_date(fc_date));
/*	if (is_redis)
	{
		ret = Ads_Redis_Cluster_Client::instance()->hset_redis_cluster_value(Ads_Redis_Cluster_Client::DB_FORECAST_SIMULATE, key, Ads_Server::config()->forecast_server_, "", 3 * 86400); //3 days
		if (ret == 0)
			ADS_DEBUG((LP_DEBUG, "set pending date %s, worker %s into redis success\n", key.c_str(), Ads_Server::config()->forecast_server_.c_str()));
	}
	if (ret != 0) //fallback to NFS
	{ */
		Ads_String full_file = ads::path_join(Ads_Server::config()->forecast_exchange_dir_, key, Ads_Server::config()->forecast_server_);
		ads::ensure_directory_for_file(full_file.c_str());
		Ads_Output_File* file = new Ads_Output_File();
		if (file->fopen(full_file.c_str(), "wb") < 0) 
		{
			delete file;
			ADS_LOG_RETURN((LP_ERROR, "error opening %s\n", full_file.c_str()), -1);
		}
		file->fclose();
		delete file;
		ADS_DEBUG((LP_DEBUG, "touch pending date file to NFS: %s\n", full_file.c_str()));
//	}
	return 0;
}


//OPP-5409
//write to redis|NFS
int Forecast_Pending_Processer::copy(const Forecast_Service::AF_Job_Context* job_ctx, const Ads_String& src_filename, const Ads_String& dst_filename)
{
	bool is_redis = Ads_Redis_Cluster_Client::instance()->enabled(Ads_Redis_Cluster_Client::DB_FORECAST_SIMULATE) && job_ctx;
	int ret = -1;
	bool is_empty_file = false;
	{
		struct stat st;
		if (::stat(src_filename.c_str(), &st) < 0)
			is_empty_file = true;
	}
	if (is_redis)
	{
		Ads_String_List path;
		ads::split(dst_filename, path, '/');
		//job/scenario/network/XXX_fc_date.csv.gz
		Ads_String key = ads::path_join(job_ctx->name(), ads::i64_to_str(job_ctx->current_scenario_id_), path[path.size()-2], path[path.size()-1]);
		Ads_String value="";
		if (!is_empty_file)
		{
			//read entire file
			FILE * src_file = ::fopen(src_filename.c_str(), "rb");
			if (!src_file)
				ADS_LOG_RETURN((LP_ERROR, "open file %s error\n", src_filename.c_str()), -1);
			int64_t lSize=0;
			size_t result;
			fseek(src_file, 0, SEEK_END);
			lSize = ftell(src_file);
			if (lSize > 0)
			{
				rewind(src_file);
				value.resize(lSize);
				result = fread(const_cast<char*>(value.data()), 1, lSize, src_file);
				if ((int64_t)result != lSize) 
					ADS_LOG_RETURN((LP_ERROR, "read entire file %s error\n", src_filename.c_str()), -1);
			}
			fclose(src_file);
		}
		ret = Ads_Redis_Cluster_Client::instance()->hset_redis_cluster_value(Ads_Redis_Cluster_Client::DB_FORECAST_SIMULATE, key, dst_filename, value, 3 * 86400); //3 days
		if (ret == 0)
			ADS_DEBUG((LP_DEBUG, "set file %s(size: %d)into redis success\n", dst_filename.c_str(), value.size()));
	}
	if (ret != 0) //fallbacl to NFS
	{
		ads::ensure_directory_for_file(dst_filename.c_str());
		if (!is_empty_file)
			ads::copy_file(src_filename.c_str(), dst_filename.c_str());
		else
		{
			Ads_Output_File* file = new Ads_Output_File();
			if (file->fopen(dst_filename.c_str(), "wb") < 0) {
				delete file;
				ADS_LOG_RETURN((LP_ERROR, "error opening %s\n", dst_filename.c_str()), -1);
			}
			file->fclose();
			delete file;
		}
		ADS_DEBUG((LP_DEBUG, "copy pending file to NFS: %s\n", dst_filename.c_str()));
	}
	return 0;
}
//read from redis|NFS
bool
Forecast_Pending_Processer::batch_open(const Forecast_Service::AF_Job_Context* job_ctx, const Ads_String_List& paths, const Ads_String& file_last)
{
	bool is_redis = Ads_Redis_Cluster_Client::instance()->enabled(Ads_Redis_Cluster_Client::DB_FORECAST_SIMULATE) && job_ctx;
	Ads_String key = "";
	int ret = 0;
	if (is_redis)
	{
		Ads_String_List path;
		ads::split(paths[0], path, '/');
		key = ads::path_join(job_ctx->name(), ads::i64_to_str(job_ctx->current_scenario_id_), path[path.size()-1], file_last);
		int size = paths.size();
		if (job_ctx->fast_conf_) size = 0;
		ret = Ads_Redis_Cluster_Client::instance()->hget_redis_cluster_value(Ads_Redis_Cluster_Client::DB_FORECAST_SIMULATE, key, redis_contents_, size);
	}

	Ads_String_List items;
	Ads_String_List date;
	ads::split(file_last, items, ".");
	ads::split(items[0], date, "_");
	Ads_String date_dir = ads::path_join(Ads_Server::config()->forecast_exchange_dir_, job_ctx->name(), ads::i64_to_str(job_ctx->current_scenario_id_), date[date.size()-1]);
	Ads_String_List workers;
	ads::Dirent::children(date_dir, DT_REG, workers);

	for (const auto& path : paths)
	{
		Ads_String_List items;
		ads::split(path, items, '/');
		Ads_String worker = items[items.size()-3];
		Ads_String filename = ads::path_join(path, file_last);
		if (redis_contents_.find(filename) == redis_contents_.end())
		{
			Ads_Text_File* file = new Ads_Text_File;
			if (!file)
				ADS_LOG_RETURN((LP_ERROR, "malloc file %s fail\n", filename.c_str()), false);
			if (! (file->fopen(filename.c_str(), "rb")))
			{
				ADS_LOG((LP_ERROR, "load pending file %s(key: %s) is missing: %d vs %d (%d)\n", filename.c_str(), key.c_str(), redis_contents_.size(), paths.size(), ret));
				delete file;
				if (std::find(workers.begin(), workers.end(), worker) != workers.end())
					return false;
			}
			else
			{
				files_[filename] = file;
			}
		}
	}
	return true;
}

bool
Forecast_Pending_Processer::open(const Ads_String& filename)
{		
	if (redis_contents_.find(filename) != redis_contents_.end())
	{
		if (redis_contents_[filename].size() != 0)
		{
			if (is_stream_ready_)
			{
				inflateEnd(&redis_stream_);
				is_stream_ready_ = false;
			}
			rd_pos_ = 0;
			wr_pos_ = 0;
			eof_ = false;
			capacity_ = Ads_Text_File::DEFAULT_BUFFER_SIZE;
			last_total_out_ = 0;
			redis_stream_.zalloc = Z_NULL;
			redis_stream_.zfree = Z_NULL;
			redis_stream_.opaque = Z_NULL;
			redis_stream_.avail_in = 0;
			redis_stream_.next_in = Z_NULL;
			if (inflateInit2(&redis_stream_, 16+MAX_WBITS) != Z_OK)
				ADS_LOG_RETURN((LP_ERROR, "initialize inflate stream %s failed!\n", filename.c_str()), false);
			redis_stream_.avail_in = redis_contents_[filename].size();
			redis_stream_.next_in = (Bytef*)(redis_contents_[filename].c_str());
			is_stream_ready_ = true;
			if (buf_) delete []buf_;
			buf_ = new char[ADS_FILE_WRITE_BUFFER_SIZE];
			if (buf_ == NULL) 
				ADS_LOG_RETURN((LP_ERROR, "malloc for input stream %s write buffer failed!\n", filename.c_str()), false);
		}
		ADS_DEBUG((LP_DEBUG, "read pending files %s from redis\n", filename.c_str()));
	}
	return true;
}

char*
Forecast_Pending_Processer::fgets(const Ads_String& filename, char *buffer, int size)
{
	if (files_.find(filename) != files_.end())
		return files_[filename]->fgets(buffer, size);
	if (redis_contents_[filename].size() == 0) return 0;
	//read from redis
	void *p = 0;
	while ((p = ::memchr(buf_ + rd_pos_, '\n', wr_pos_ - rd_pos_)) == 0)
	{
		if (eof_)
		{
			if ((wr_pos_ - rd_pos_) <= 0 ) return NULL;
			if (size > (wr_pos_ - rd_pos_)) size = (wr_pos_ - rd_pos_);
			else size = size - 1;
			::memcpy(buffer, buf_ + rd_pos_, size);
			buffer[size] = '\0';
			rd_pos_ += size;
			return buffer;
		}
		::memmove(buf_, buf_ + rd_pos_, wr_pos_ - rd_pos_);
		wr_pos_ -= rd_pos_; rd_pos_ = 0;
		if ((capacity_ - wr_pos_) <= 0)
		{
			ADS_LOG((LP_ERROR, "invalid line: too long, len=%d\n", (wr_pos_ - rd_pos_)));
			buffer[0] = 0;
			return 0;
		}
		redis_stream_.next_out = (unsigned char*)(&buf_[wr_pos_]);
		redis_stream_.avail_out = capacity_ - wr_pos_; 
		int ret = inflate(&redis_stream_, Z_SYNC_FLUSH);
		if (ret != Z_OK && ret != Z_STREAM_END)
		{
			ADS_LOG((LP_ERROR, "failed to read from gzip\n"));
			eof_ = true;
			return 0;
		}
		else if (ret == Z_STREAM_END)
			eof_ = true;
		wr_pos_ += redis_stream_.total_out - last_total_out_;
		last_total_out_ = redis_stream_.total_out;
	}
	int len = (char *)p - (char *)(buf_ + rd_pos_) + 1;
	::memcpy(buffer, buf_ + rd_pos_, len);
	buffer[len] = '\0';
	rd_pos_ += len;
	return buffer;
}

int
Forecast_Pending_Processer::reload_pending_files(Forecast_Service::AF_Job_Context* job_ctx, time_t start_time, time_t end_time)
{
	for (time_t curr = start_time; curr < end_time; curr += 86400)
		if (!job_ctx->output_simulate_metrics_files(curr, true))
			return -1;
	return 0;
}

bool
Forecast_Pending_Processer::exist(const Forecast_Service::AF_Job_Context* job_ctx, const Ads_String& filename)
{
	bool is_redis = Ads_Redis_Cluster_Client::instance()->enabled(Ads_Redis_Cluster_Client::DB_FORECAST_SIMULATE) && job_ctx;
	int ret = 0;
	if (is_redis)
	{
		Ads_String_List path;
		ads::split(filename, path, '/');
		Ads_String key = ads::path_join(job_ctx->name(), ads::i64_to_str(job_ctx->current_scenario_id_), path[path.size()-2], path[path.size()-1]);
		ret = Ads_Redis_Cluster_Client::instance()->hexist_in_redis_cluster(Ads_Redis_Cluster_Client::DB_FORECAST_SIMULATE, key, filename);
	}
	//exist in NFS
	if (ret != 0)
	{
		struct stat st;
		if (::stat(filename.c_str(), &st) < 0)
			return false;
	}
	ADS_DEBUG((LP_DEBUG, "pending file %s exists in redis or NFS\n", filename.c_str()));
	return true;
}

time_t
Forecast_Service::get_nightly_pusher_timestamp(const Ads_Repository* repo, Ads_String& model_id, Ads_GUID network_id)
{
	const Ads_String& basepath = ads::path_join(Ads_Server::config()->forecast_store_dir_, model_id, ads::entity_id_to_str(network_id), "model_r");
	ads::Dirent dirent;
	if (dirent.open(basepath.c_str()) < 0)
		ADS_LOG_RETURN((LP_ERROR, "Cannot opendir to scan %p.\n", basepath.c_str()), -1);

	struct dirent *dp;
	time_t time_loaded = -1;
	while (( dp = dirent.read() ))
	{
		Ads_String alias = dp->d_name;
		FILE *fp = fopen(ads::path_join(basepath, alias, "pusher_timestamp").c_str(),"rb");
		if (! fp)
		{
			//Change to ADS_DEBUG V6.13
			ADS_LOG((LP_ERROR, "Fail to open file %s\n", ads::path_join(basepath, "pusher_timestamp").c_str()));
			delete fp; 
			continue;
		}
		char buf[16];
		fgets(buf, sizeof(buf), fp);
		if (time_t(buf) > time_loaded) time_loaded = ads::str_to_i64(buf);
		fclose(fp);
	}
	ADS_LOG((LP_INFO, "load pusher timestamp: %s\n", ads::i64_to_str(time_loaded).c_str()));
	return time_loaded;
}
