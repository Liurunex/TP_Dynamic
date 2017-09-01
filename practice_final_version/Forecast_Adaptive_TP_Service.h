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

#ifndef FORECAST_ADAPTIVE_TP_SERVICE_H
#define FORECAST_ADAPTIVE_TP_SERVICE_H

#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "sys/types.h"
#include "sys/sysinfo.h"
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>

#include "server/Ads_Job_Queue_Service.h"
//#include "Forecast_Task.h"
//#include "server/Ads_Service_Base.h"

//==============================================================================
// zxliu modification: TP_Adaptive
//==============================================================================

#include <unordered_map>

class Forecast_Task_Manager;

#define TYPE_JOB				1
#define TYPE_EXIT				0
#define TYPE_CURTAIL			2
#define UNDEFINE_TYPE			3
#define MQ_THRESHOLD 			1
#define TP_MIN_THRESHOLD 		1
#define TP_IDLE_THRESHOLD 		5
#define TP_GR_THRESHOLD			4.99

class Ads_Job_Queue_Service_TP_Adaptive: public Ads_Job_Queue_Service {
public:
	typedef void * Job_Arg;
	typedef int (*Job_Func)(Job_Arg, Job_Arg);
	struct Job : public Ads_Job {
		Job_Func		func_;
		Job_Arg			arg1_;
		Job_Arg			arg2_;

		Job(Job_Func f, Job_Arg a1, Job_Arg a2)
			:func_(f), arg1_(a1), arg2_(a2)
		{}

		virtual int run()	{ return this->func_(arg1_, arg2_); }
		/* zxliu added */
		virtual int type()	{ return TYPE_JOB; }
	};

	struct No_Op : public Ads_Job {
		virtual int run()	{ return 0; }
		/* zxliu added */
		virtual int type()	{ return TYPE_EXIT; }
	};

/* zxliu modification */
private:
	Ads_String type_;
public:
	struct TP_Throughput {
	    size_t cur_thread_size;
		size_t pre_job_count;
		size_t cur_job_count;
		double cur_diff_percent;
		double pre_thrput;
	};

	Ads_Job_Queue_Service_TP_Adaptive(const Ads_String& type = "default_task");
	virtual ~Ads_Job_Queue_Service_TP_Adaptive();

	struct Curtail_TP : public Ads_Job {
		virtual int run()  { return 0; }
		virtual int type() { return TYPE_CURTAIL; }
	};
	
	/* override base function */
	int open();
	int wait();
	int stop();
	int svc();

	/* thread_pool size_modification function */
	int extend_threadpool	(size_t extend_size);
	int curtail_threadpool	(size_t curtail_size);
	int curtail_action();

	void get_tp_thrput_diff ();
	size_t count_idle_threads();
	size_t tp_size()			{ return this->n_threads_; }
	void mutex_size_lock()		{ this->mutex_size_.acquire(); } 
	void mutex_size_unlock()	{ this->mutex_size_.release(); }
	TP_Throughput* tp_thrput() 	{ return this->tp_throughput_; }
	
protected:
	ads::Mutex mutex_map_, mutex_job_, mutex_size_;
	std::unordered_map<pthread_t, int> thread_ids_map_;

	//volatile int signal_worker_start_;

	int deleteNode			(pthread_t target);
	int thread_status_set	(pthread_t pid, int set_sta);

	/* calculate thread_pool throughput in a while */
	size_t job_done_counter_;
	TP_Throughput *tp_throughput_;
	void init_tp_thrput ();
	void job_counter_increasement();
};


//==============================================================================
// zxliu modification: Supervisor
//==============================================================================
#define THREAD_LMIT 				100
#define THREAD_LMIT_GR				10
#define TP_MODIFY_CURTAIL_SCALE 	4
#define TP_MODIFY_EXTEND_SCALE 		4
#define CPU_LIMIT					99.9999
#define MEM_LIMIT					99.9999
#define IO_LIMIT					99.9999

class Forecast_Manager_Supervisor {
public:
	struct CpuInfo {
	    unsigned long long cpu_totalUser;
	    unsigned long long cpu_totalUserLow;
	    unsigned long long cpu_totalSys;
	    unsigned long long cpu_totalIdle;
	};

	struct ResultInfo {
	    unsigned int num_cpu;
	    unsigned int r_process;
	    unsigned int b_process;
	    double cpu_percent;
	    double mem_percent;
	    double util_percent;
	    double io_load; // await = qutim + svctim, here target = qutim/svctim
	};

	Forecast_Manager_Supervisor(const Ads_String& type = "supervisor", 
		const size_t thread_limit = 50, const size_t n_tp = 6);
	virtual ~Forecast_Manager_Supervisor();

	int stop();
	int openself();
	int supervisor_func();
	int addworker(Forecast_Task_Manager *worker);

	Ads_String type() 						{ return type_; }
	void 	   num_tp(size_t i)				{ this->n_tp_ = i; }
	size_t 	   return_ntp() 				{ return this->n_tp_; }
	void 	   set_thread_limit(size_t i)	{ this->thread_limit_ = i > THREAD_LMIT ? THREAD_LMIT:i; }

	static void *supervisor_func_run (void *arg);
	void add_thread_limit() 				{ thread_limit_ = thread_limit_+THREAD_LMIT_GR > THREAD_LMIT ? THREAD_LMIT: thread_limit_+THREAD_LMIT_GR; }
	std::vector<Forecast_Task_Manager *> &return_tp_group() { return this->tp_group_; }

protected:
	Ads_String 		type_;
	pthread_t 		supervisor_id_;
	volatile bool 	exitting_;
	//volatile int 	signal_supervisor_start_;
	
	size_t n_tp_;
	size_t thread_limit_;
	size_t added_counter_; /* record added worker number in addworker() */
	
	std::vector<int> tp_modification_;
	std::vector<double> tp_weights_;
	std::vector<Forecast_Task_Manager *> tp_group_;

	/* grabing info from /proc */
	char buffer_[512]; 
	ResultInfo *res_integration_;
	CpuInfo *cur_cpu_, *pre_cpu_;
	const std::string vmstat_ = "vmstat";
	const std::string iostat_ = "iostat -x";
	
	static void init_proc_parameter (CpuInfo *pre_cpu);
	void get_cpu_number(ResultInfo *res_integration);
	std::string popen_usage(std::string command);
	void get_current_value (CpuInfo *cur_cpu, CpuInfo *pre_cpu, ResultInfo *res_integration);

};

#endif /* FORECAST_ADAPTIVE_TP_SERVICE_H */
