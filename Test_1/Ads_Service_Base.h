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

#ifndef ADS_SERVICE_BASE_H
#define ADS_SERVICE_BASE_H
#include <string>
#include <sstream>
#include <list>
#include <set>
#include <map>
#include <vector>
#include <tuple>
#include <deque>
#include <algorithm>
#include <cstdlib>
#include <cmath>
#include <random>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <math.h>
#include <errno.h>

#include "Ads_Types.h"
class Ads_Message_Base {
public:
	enum PRIORITY {
		PRIORITY_HIGH = 20,
		PRIORITY_NORMAL = 10,
		PRIORITY_IDLE = 0
	};

	enum {
		MESSAGE_UNKNOWN = -1,
		MESSAGE_EXIT = 0,
		MESSAGE_IDLE = 1,
		MESSAGE_SERVICE = 100,
		MESSAGE_CURTAIL_THREADPOOL_SIZE = 200
	};

	typedef int TYPE;

	void type(TYPE type)	{ this->type_ = type; }
	TYPE	type() const	{ return this->type_; }

	void arg(void *d)	{ this->arg_ = d; }
	void *arg() const	{ return this->arg_; }

protected:
	Ads_Message_Base() {}
	Ads_Message_Base(TYPE type, void *data) : type_(type), arg_(data) {}
	virtual ~Ads_Message_Base() {}

private:
	TYPE type_;
	void *arg_;

public:
	void destroy() { delete this; }
	static Ads_Message_Base *create(TYPE type, void *data = 0);
};

class Ads_Waitable_Message_Base : public Ads_Message_Base {
public:
	int signal();
	int wait(ads::Time_Value *timeout);
};

class Ads_Service_Base {
public:
	Ads_Service_Base();
	virtual ~Ads_Service_Base();

	virtual int open();
	virtual int svc();
	static void *svc_run(void *arg);

	// stop the service
	virtual int stop();

	/* zxliu temporary modification */
	virtual int extend_threadpool_size();
	virtual int curtail_threadpool_size();
	virtual void *supervisor_func();

	int wait();

private:
	///XXX: to prevent mistaken usage
	//int close(u_long u = 0)	{ return ads::Task::close(u); }

public:
	virtual int post_message(Ads_Message_Base *msg, Ads_Message_Base::PRIORITY p = Ads_Message_Base::PRIORITY_NORMAL);

	size_t message_count() { return msg_queue_.message_count(); }

	size_t last_active_time() const { return (size_t)this->time_last_activity_.sec(); }
	void num_threads(int i)		{ this->n_threads_ = i; }

	const std::vector<pthread_t>& thread_ids() const { return thread_ids_; }

protected:
	virtual int dispatch_message(Ads_Message_Base *msg);
	virtual int release_message(Ads_Message_Base *msg);

	virtual int on_idle();
	virtual int on_info(Ads_String& info);

protected:
	//ACE_Message_Queue_Ex<Ads_Message_Base, ACE_MT_SYNCH> msg_queue_;
	ads::Mutex mutex_;
	ads::Message_Queue<Ads_Message_Base> msg_queue_;

	volatile bool exitting_;
	volatile int signal_supervisor_exit;
	ads::Time_Value time_last_activity_;

	size_t n_threads_;
	std::vector<pthread_t> thread_ids_;
	pthread_t supervisor_id;
};

#endif /* ADS_SERVICE_BASE_H */
