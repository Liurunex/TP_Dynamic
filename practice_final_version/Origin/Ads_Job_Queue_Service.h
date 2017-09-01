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
 *              2007/09/24      jack
 *                      Created.
 *
 */
//=============================================================================

#ifndef ADS_JOB_QUEUE_SERVICE_H
#define ADS_JOB_QUEUE_SERVICE_H

#include "Ads_Service_Base.h"

struct Ads_Job
{
	bool abandoned_;
	bool waitable_;

	Ads_Job()
		: abandoned_(true), waitable_(false)
	{}

	virtual ~Ads_Job()
	{}

	virtual int run() = 0;

	bool waitable() const	{ return this->waitable_; }
	bool abandoned() const	{ return this->abandoned_; }
	void destroy() 			{ delete this; }
};

class Ads_Waitable_Job: public Ads_Job
{
private:
	bool done_;

	ads::Mutex mutex_;
	ads::Condition_Var cond_;

public:	
	Ads_Waitable_Job()
		: done_(false), mutex_(), cond_(mutex_)
	{
		this->abandoned_ = false;
		this->waitable_ = true;
	}

	virtual ~Ads_Waitable_Job()
	{}

	void signal()
	{
		mutex_.acquire();
		done_ = true;
		cond_.signal();
		mutex_.release();
	}

	int wait(/* const ads::Time_Value *abstime = 0 */)
	{
		int ret = 0;
		mutex_.acquire();
		if(! done_) ret = cond_.wait(/* abstime */);
		mutex_.release();
		return ret;
	}

	virtual int run() = 0;
};

class Ads_Job_Queue_Service: public Ads_Service_Base
{
public:
	typedef void * Job_Arg;
	typedef int (*Job_Func)(Job_Arg, Job_Arg);
	
	struct Job : public Ads_Job
	{
		Job_Func		func_;
		Job_Arg			arg1_;
		Job_Arg			arg2_;

		Job(Job_Func f, Job_Arg a1, Job_Arg a2)
			:func_(f), arg1_(a1), arg2_(a2)
		{}

		virtual int run()
		{ return this->func_(arg1_, arg2_); }
	};

	struct No_Op: public Ads_Job
	{
		virtual int run()	{ return 0; }
	};

private:
//	bool exitting_;
//	int num_threads_;
	ads::Message_Queue<Ads_Job> msg_queue_;

public:
	Ads_Job_Queue_Service();
	virtual ~Ads_Job_Queue_Service();
	
	static Ads_Job_Queue_Service *instance();

public:
//	int open();
	int svc();

	int stop();
	void capacity (size_t cap) { this->msg_queue_.capacity(cap); }
	size_t capacity () { return msg_queue_.capacity(); }

public:
	int async_do(Job_Func f, Job_Arg arg1, Job_Arg arg2);
	int enqueue_job(Ads_Job *job);
	int enqueue_job(Ads_Job *job, bool front, bool wait);

	size_t num_pending_jobs();
};

#endif /* ADS_JOB_QUEUE_SERVICE_H_ */

