#if !defined(FORECAST_TASK_H)
#define FORECAST_TASK_H

#include "server/Ads_Job_Queue_Service.h"
#include "Forecast_Service.h"
#include "Forecast_Simulator.h"

class Forecast_Task_Manager;

//==============================================================================
// abstract Manageable_Task
//
// 1. how to identify a task
//   by <job, date>, where job is job_id or job_tag, date is virtual_date
//
// 2. data driven design
//   when the input is ready, application creates certain tass
//   the task itself only read data from input, and write data to output
//
// 3. what's in task
//   does NOT contain any data, since it will be auto destroyed once finish
//   can have some pointers pointing to the input/output data
//==============================================================================

// TODO: add scenario_id to job ID
class Forecast_Manageable_Task: public Ads_Job
{
	public:
		friend class Forecast_Task_Manager;

		// <job, date> the identifier of the task
		Forecast_Manageable_Task(const Ads_String& job, time_t date);

		// run() is used to communcate with the manager
		// run_i() is used to actually execute the actions
		virtual int run();
		virtual int run_i() = 0;
		virtual void discard() {};
		int task_id() {return id_;}

	protected:
		Ads_String job_;
		time_t date_;
		Ads_String performance_str_;

	private:
		Forecast_Task_Manager* manager_;
		bool discarded_;
		ads::Time_Value created_at_;
		int id_;
};

//==============================================================================
// Task_Manager
//
// 1. how many tasks for a given <job, date>
//   if allow_duplicate, there could be N tasks, otherwise there is atmost 1 task for a <job, date>
//
// 2. what stat information is managed
//   for each <job, date>, store # of waiting/running/done tasks
//
// 3. when to update the stat
//   add a task;     waiting++
//   before running: waiting--, running++
//   after running:             running--, done++
//   after running:  if waiting=running=0 (all tasks are finished), will remove the <job, date> from stat
//==============================================================================

class Forecast_Task_Manager
{
	public:
		Forecast_Task_Manager(const Ads_String& type = "default_task", size_t max_days = -1, bool allow_duplicate = true);
		~Forecast_Task_Manager();

		int open(int n_threads);
		int stop();
		Ads_String type() { return type_; }
		size_t num_pending_jobs() { return job_service_->num_pending_jobs(); }

		int add_task(Forecast_Manageable_Task* task);
		void get_status(const Ads_String& job, time_t date, int& n_waiting, int& n_running, int& n_done);
		void wait(const Ads_String& job, time_t date, size_t skip_number = 0);
		void wait(const Ads_String& job);
		void remove_waiting_tasks(const Ads_String& job);

		// called by the managed task
		void before_run(Forecast_Manageable_Task* task);
		void notify_done(Forecast_Manageable_Task* task);

	private:
		Ads_String type_;
		size_t max_days_;
		bool allow_duplicate_;
		Ads_Job_Queue_Service* job_service_; // build-in job_queue_service, used to handle all the tasks

		// used for the stat data
		struct Stat_Value
		{
			int running_, done_;
			std::set<Forecast_Manageable_Task*> waiting_tasks_;
		};
		typedef std::map<time_t, Stat_Value> Stat_Daily_Value;
		typedef std::map<Ads_String, Stat_Daily_Value> Stat_Job_Daily_Value;
		ads::Mutex stat_mutex_;
		ads::Condition_Var stat_wait_cond_, stat_full_cond_;
		Stat_Job_Daily_Value stat_map_; // job -> date -> <waiting, running>
};

//==============================================================================
// for load requests
//==============================================================================

class Forecast_Load_Requests_Task: public Forecast_Manageable_Task
{
	public:
		static const int N_PRE_LOAD_DAYS = 3;
		Forecast_Load_Requests_Task(Forecast_Service::AF_Job_Context* af_job_ctx, time_t date) :
			Forecast_Manageable_Task(af_job_ctx->name(), date)
			, af_job_ctx_(af_job_ctx), vnodes_(af_job_ctx->vnode_set_) {}
		virtual int run_i();

	private:
		int load_from_plan();
		int load_from_model();

		Forecast_Service::AF_Job_Context* af_job_ctx_;
		std::set<Ads_String> vnodes_;
};

//==============================================================================
// for simulate 
//==============================================================================

class Forecast_Simulate_Task: public Forecast_Manageable_Task
{
	public:
		Forecast_Simulate_Task(Forecast_Service::AF_Job_Context* af_job_ctx, time_t date, Forecast_Simulator::Context* context) :
		Forecast_Manageable_Task(context->job_name_, date)
		, af_job_ctx_(af_job_ctx), sim_date_(date), context_(context){}
		virtual int run_i();
	private:
		Forecast_Service::AF_Job_Context* af_job_ctx_;
		time_t sim_date_;
		Forecast_Simulator::Context* context_;
};
//==============================================================================
// for calculate metrics
//==============================================================================

class Forecast_Calculate_Metric_Task: public Forecast_Manageable_Task
{
	public:
		Forecast_Calculate_Metric_Task(Forecast_Service::AF_Job_Context* af_job_ctx, time_t date, Forecast_Simulator::Forecast_Transaction_Vec* transaction_vec, std::pair<Forecast_Metrics_Map*, Interval_Forecast_Metrics*>* metrics, size_t index) :
			Forecast_Manageable_Task(af_job_ctx->name(), date)
			, af_job_ctx_(af_job_ctx), transaction_vec_(transaction_vec), metrics_(metrics), index_(index)
			, cur_version_(af_job_ctx->model_version_)
		{
		//	for(std::set<Ads_String>::iterator it = af_job_ctx->vnode_set_.begin(); it != af_job_ctx->vnode_set_.end(); it++)
		//		vnodes_.push_back(*it);
		}
		virtual int run_i();
		virtual void discard();

	private:
		void add_model_to_buffer(Forecast_Store *store, const Forecast_Request_Meta* meta, report::Request_Log_Record* model);
		void output_model();

		Forecast_Service::AF_Job_Context* af_job_ctx_;
		Forecast_Simulator::Forecast_Transaction_Vec* transaction_vec_;
		std::pair<Forecast_Metrics_Map*, Interval_Forecast_Metrics*>* metrics_;
		size_t index_;
		int64_t cur_version_;
		//std::vector<Ads_String> vnodes_;
		//store -> vnodeid -> date -> batch
		typedef std::map<Forecast_Store*, std::map<Ads_String, Forecast_User_Batch> > Network_Model_Buffer_List;
		Network_Model_Buffer_List model_buffer_;
};

//==============================================================================
// for output pending
//==============================================================================

class Forecast_Output_Pending_Task: public Forecast_Manageable_Task
{
	public:
		Forecast_Output_Pending_Task(Forecast_Service::AF_Job_Context* af_job_ctx, time_t date, Request_Queue* request_queue, std::pair<Forecast_Metrics_Map*, Interval_Forecast_Metrics*>* metrics) :
			Forecast_Manageable_Task(af_job_ctx->name(), date)
			, af_job_ctx_(af_job_ctx), request_queue_(request_queue), metrics_(metrics), n_metrics_(af_job_ctx->n_metrics_) {}
		virtual int run_i();
		virtual void discard();

	private:
		Forecast_Service::AF_Job_Context* af_job_ctx_;
		Request_Queue* request_queue_;
		std::pair<Forecast_Metrics_Map*, Interval_Forecast_Metrics*>* metrics_;
		size_t n_metrics_;
};

//==============================================================================
// for pre aggregate
//==============================================================================

class Forecast_Pre_Aggregate_Task: public Forecast_Manageable_Task
{
	public:
		Forecast_Pre_Aggregate_Task(Forecast_Service::AF_Job_Context* af_job_ctx, time_t date, const Ads_GUID_Set& networks) :
			Forecast_Manageable_Task(af_job_ctx->name(), date)
			, af_job_ctx_(af_job_ctx), networks_(networks) {}
		virtual int run_i();

	private:
		Forecast_Service::AF_Job_Context* af_job_ctx_;
		Ads_GUID_Set networks_;
};

//==============================================================================
// for aggregate
//==============================================================================

class Forecast_Aggregate_Task: public Forecast_Manageable_Task
{
	public:
		Forecast_Aggregate_Task(Forecast_Service::AF_Job_Context* af_job_ctx) : Forecast_Manageable_Task(af_job_ctx->name(), -1), af_job_ctx_(af_job_ctx){}
		virtual int run_i();
	private:
		Forecast_Service::AF_Job_Context* af_job_ctx_;
};

#endif // FORECAST_TASK_H

