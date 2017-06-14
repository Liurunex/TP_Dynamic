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
#include <pthread.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <functional>
#include <sys/time.h>

#include "Ads_Service_Base.h"
Ads_Message_Base *
Ads_Message_Base::create(Ads_Message_Base::TYPE type, void *data) {
	Ads_Message_Base *msg = new Ads_Message_Base(type, data);
	return msg;
}

Ads_Service_Base::Ads_Service_Base()
: exitting_(false)
, time_last_activity_()
, n_threads_(1)
{}

Ads_Service_Base::~Ads_Service_Base() {}

int
Ads_Service_Base::open() {
	thread_ids_.resize(n_threads_);
	#if defined(ADS_ENABLE_SEARCH) || defined(ADS_ENABLE_MACOSX)
	pthread_attr_t _attr;
	pthread_attr_init(&_attr);
	pthread_attr_setstacksize(&_attr, 0x4000000); //64M
	#endif

	for (size_t i = 0; i < n_threads_; ++i)
	{
		pthread_attr_t *attr = 0;
		#if defined(ADS_ENABLE_SEARCH) || defined(ADS_ENABLE_MACOSX)
		attr = &_attr;
		#endif
		int ret = ::pthread_create(&this->thread_ids_[i], attr, &Ads_Service_Base::svc_run, this);
		if (ret != 0)
		{
			//ADS_LOG((LP_ERROR, "failed to create thread %d\n", i));
		}
	}

	return 0;
}

int
Ads_Service_Base::stop() {
	this->exitting_ = true;
	{
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_EXIT);
		if(this->post_message(msg) < 0) {
			std::cout << "cannot post exit message 1" << std::endl;
			msg->destroy();

			/// due to possible signal_dequeue_waiters failure, Message_Queue might continuously wait_not_empty_cond
			/// so here we enqueue MESSAGE_EXIT message twice.
			Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_EXIT);
			if (this->post_message(msg) < 0) {
				msg->destroy();
				std::cout << "cannot post exit message 2" << std::endl;
			}
		}
	}
	this->wait();

	Ads_Message_Base *msg = 0;
	while (this->msg_queue_.dequeue(msg, true, false) >= 0)
		this->release_message(msg);

	return 0;
}

void *
Ads_Service_Base::svc_run(void *arg) {
	Ads_Service_Base *s = reinterpret_cast<Ads_Service_Base *>(arg);
	s->svc();

	return 0;
}

int
Ads_Service_Base::svc() {
	//	ACE_DEBUG((LM_INFO, "[base: %t] Base Service started.\n"));
	Ads_Message_Base *msg = 0;
	while (msg_queue_.dequeue(msg) >= 0) {
		if(this->exitting_) {
			msg->destroy();
			break;
		}

		if (this->dispatch_message(msg) < 0)
			std::cout << "failed to dispatch msg" << std::endl;
			//ADS_DEBUG((LP_DEBUG, "failed to dispatch msg.\n"));

		msg->destroy();

		this->time_last_activity_ = ads::gettimeofday();
	}
	//	ACE_DEBUG((LM_INFO, "[base: %t] Base Service stopped.\n"));
	return 0;
}

int
Ads_Service_Base::wait() {
	for (size_t i = 0; i < thread_ids_.size(); ++i)
		::pthread_join(this->thread_ids_[i], 0);
	return 0;
}

int
Ads_Service_Base::post_message(Ads_Message_Base *msg, Ads_Message_Base::PRIORITY p /* = Ads_Message_Base::PRIORITY_IDLE */) {
	if(p == Ads_Message_Base::PRIORITY_HIGH) {
		if (this->msg_queue_.enqueue(msg) < 0)
			std::cout << "failed to enqueue msg (HIGH)" << std::endl;
			//ADS_LOG_RETURN((LP_ERROR, "failed to enqueue msg (HIGH)\n"),-1);
	}
	else if(p == Ads_Message_Base::PRIORITY_NORMAL) {
		if (this->msg_queue_.enqueue(msg) < 0)
			std::cout << "failed to enqueue msg (NORMAL)" << std::endl;
			//ADS_LOG_RETURN((LP_ERROR, "failed to enqueue msg (NORMAL)\n"),-1);
	}
	else if(p == Ads_Message_Base::PRIORITY_IDLE) {
	//if (this->msg_queue_.enqueue(msg, (ACE_Time_Value *)&ACE_Time_Value::zero) < 0)
		if (this->msg_queue_.enqueue(msg, false, false) < 0)
			std::cout << "failed to enqueue msg (IDLE)" << std::endl;
			//ADS_LOG_RETURN((LP_ERROR, "failed to enqueue (IDLE)\n"),-1);
	}
	else
		std::cout << "invalid priority" << std::endl;
		//ADS_LOG_RETURN((LP_ERROR, "invalid priority %d\n", p),-1);

	return 0;
}

int
Ads_Service_Base::dispatch_message(Ads_Message_Base *msg) {
	ADS_ASSERT(msg != 0);
	switch (msg->type()) {
	// exit log manager
	case Ads_Message_Base::MESSAGE_EXIT: {
			this->exitting_ = true;
			return 0;
	}
	case Ads_Message_Base::MESSAGE_IDLE:
			return this->on_idle();
	default:
		ADS_ASSERT(0);
		break;
	}

	return 0;
}

int
Ads_Service_Base::on_idle() {
	this->time_last_activity_ = ads::gettimeofday();
	return 0;
}

int
Ads_Service_Base::on_info(Ads_String&) {
	return 0;
}

int
Ads_Service_Base::release_message(Ads_Message_Base *msg) {
	ADS_ASSERT(msg != 0);

	switch (msg->type()) {
		case Ads_Message_Base::MESSAGE_EXIT:
		case Ads_Message_Base::MESSAGE_IDLE:
		case Ads_Message_Base::MESSAGE_SERVICE:
			break;
		default:
			std::cout << "invalid message" << msg->type() << std::endl;
			//ADS_DEBUG((LP_ERROR, "invalid message %d\n", msg->type()));
			//ADS_ASSERT(0);
			break;
	}

	msg->destroy();

	return 0;
}



/* zxliu modification */

/* START_thread_safe */
size_t
Ads_Service_Base_TP_Adaptive::count_idle_threads() {
	mutex_map.acquire();

	size_t all_idle = 0;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map.begin();
	while(it != thread_ids_map.end()) {
		if (!it->second) all_idle ++;
		it ++;
	}

	mutex_map.release();
	return all_idle;
}

int
Ads_Service_Base_TP_Adaptive::thread_status_set(pthread_t pid, int set_sta) {
	mutex_map.acquire();

	//std::cout << "thread_status_set() on " << pthread_self() << " target: " << pid << std::endl;
	std::unordered_map<pthread_t, int>::iterator got = thread_ids_map.find(pid);
	if (got != thread_ids_map.end()) got->second = set_sta;
	else std::cout << "thread_status_set() not found target: " << pid << std::endl;

	mutex_map.release();
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::deleteNode(pthread_t target) {
	mutex_map.acquire();

	std::unordered_map<pthread_t, int>::iterator got = thread_ids_map.find(target);
	if (got != thread_ids_map.end())
		thread_ids_map.erase(got);
	int res = thread_ids_map.find(target) == thread_ids_map.end() ? 1:0;
	std::cout << "erase the pid: " << target << " , check success: " << res << std::endl;

	this->n_threads_ --;
	std::cout << "\ncurtail() thread_pool size: " << (int)n_threads_ << std::endl;
	std::cout << "\nafter curtail, allthreads:" <<std::endl;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map.begin();
	while(it != thread_ids_map.end()) {
		std::cout << it->first << " ";
		it ++;
	}
	std::cout << "curtail_end()\n" <<std::endl;

	mutex_map.release();
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::extend_threadpool(int extend_scale) {
	mutex_map.acquire();

	extend_scale = TP_EXTEND_SCALE;
	this->signal_worker_start = 0;
	size_t start_index = n_threads_;
	n_threads_ += (n_threads_ / extend_scale);

	pthread_attr_t _attr;
	pthread_attr_init(&_attr);
	pthread_attr_setstacksize(&_attr, 0x4000000); //64M

	for (size_t i = start_index; i < n_threads_; ++ i) {
		pthread_t pth_id;
		pthread_attr_t *attr = 0;
		attr = &_attr;
		int ret = ::pthread_create(&pth_id, attr, &Ads_Service_Base_TP_Adaptive::svc_run, this);
		if (ret != 0) std::cout << "failed to create thread " << i << std::endl;
		thread_ids_map[pth_id] = 0;
	}

	std::cout << "extend thread_pool size: " <<  (int)n_threads_<< std::endl;
	std::cout << "\nafter extend, allthreads:" <<std::endl;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map.begin();
	while(it != thread_ids_map.end()) {
		std::cout << it->first << " ";
		it ++;
	}
	std::cout << "extend_end()\n" <<std::endl;
	this->signal_worker_start = 1;

	mutex_map.release();
	return 0;
}

/* END_thread_safe */

int
Ads_Service_Base_TP_Adaptive::curtail_threadpool(int curtail_size) {
	int curtail_counter = 0;
	while (1) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_CURTAIL_TP_SIZE);
		if(this->post_message(msg) < 0) {
			std::cout << "cannot post curtail message" << std::endl;
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
Ads_Service_Base_TP_Adaptive::open() {
	/* supervisor thread initial */
	pthread_attr_t _sattr;
	pthread_attr_init(&_sattr);
	pthread_attr_t *sattr;
	sattr = &_sattr;
	if (::pthread_create(&supervisor_id, sattr, &Ads_Service_Base_TP_Adaptive::supervisor_func_run, this))
		std::cout << "failed to create supervisor thread" << std::endl;

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
		int ret = ::pthread_create(&pth_id, attr, &Ads_Service_Base_TP_Adaptive::svc_run, this);
		if (ret != 0) std::cout << "failed to create thread " << i << std::endl;
		thread_ids_map[pth_id] = 0;
	}

	std::cout << "open() thread_pool size: " << (int)n_threads_ << std::endl;
	std::cout << "\nallthreads:" <<std::endl;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map.begin();
	while(it != thread_ids_map.end()) {
		std::cout << it->first << " ";
		it ++;
	}
	std::cout << "open_end()\n" <<std::endl;

	/* make supervisor and worker start to work */
	this->signal_worker_start = 1;
	this->signal_supervisor_start = 1;

	return 0;
}

void *
Ads_Service_Base_TP_Adaptive::supervisor_func_run(void *arg) {
	Ads_Service_Base_TP_Adaptive *s = reinterpret_cast<Ads_Service_Base_TP_Adaptive *>(arg);
	s->supervisor_func();

	return 0;
}

int
Ads_Service_Base_TP_Adaptive::supervisor_func() {
	int try_extend = 0;
	while (!this->signal_supervisor_start)
		;
	while (!this->signal_supervisor_exit) {
		sleep(1);
		try_extend ++;
		if ((int)this->message_count() == 0 && this->count_idle_threads() >= TP_IDLE_THRESHOLD) {
			std::cout << "do curtail" << std::endl;
			this->curtail_threadpool(TP_CURTAIL_SIZE);
		}
		else if (try_extend >= TIME_THRESHOLD && (int)this->message_count() >= MQ_THRESHOLD) {
			std::cout << "do extend" << std::endl;
			this->extend_threadpool(TP_EXTEND_SCALE);
			try_extend = 0;
		}
	}
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::svc() {
	Ads_Message_Base *msg = 0;
	while (!this->signal_worker_start)
		;
	while (msg_queue_.dequeue(msg) >= 0) {
		if(this->exitting_) {
			msg->destroy();
			break;
		}

		if (msg->type() == Ads_Message_Base::MESSAGE_SERVICE && this->thread_status_set(pthread_self(), 1))
			std::cout << "set thread status failed 1 " << std::endl;

		int dispatch_return = this->dispatch_message(msg);
		if (dispatch_return < 0)
			std::cout << "failed to dispatch msg" << std::endl;


		/* terminate current thread */
		if (dispatch_return == SIGNAL_EXIT_THREAD) {
			msg->destroy();
			return 0;
		}

		if (msg->type() == Ads_Message_Base::MESSAGE_SERVICE && this->thread_status_set(pthread_self(), 0))
			std::cout << "set thread status failed 0" << std::endl;

		msg->destroy();
		this->time_last_activity_ = ads::gettimeofday();

	}
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::wait() {
	//mutex_map.acquire();
	std::cout << "wait() in" << std::endl;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map.begin();

	while(it != thread_ids_map.end()) {
		if (it->first == supervisor_id) continue;
		::pthread_join(it->first, 0);
		std::cout << "join: " << it->first << std::endl;
		it ++;
	}
	//mutex_map.release();
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::stop() {
	std::cout << "stop() in " << std::endl;
	this->signal_supervisor_exit = 1;
	this->exitting_ = true;

	::pthread_join(supervisor_id, 0);
	std::cout << "join() supervisor done " << std::endl;

	for (int i = 0; i < n_threads_; ++ i) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_EXIT);
		if (this->post_message(msg) < 0) {
			msg->destroy();
			std::cout << "cannot post exit message " << std::endl;
			i --;
		}
	}

	this->wait();

	Ads_Message_Base *msg = 0;
	while (this->msg_queue_.dequeue(msg, true, false) >= 0)
		this->release_message(msg);

	thread_ids_map.clear();
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::release_message(Ads_Message_Base *msg) {
	ADS_ASSERT(msg != 0);

	switch (msg->type()) {
		case Ads_Message_Base::MESSAGE_EXIT:
		case Ads_Message_Base::MESSAGE_IDLE:
		case Ads_Message_Base::MESSAGE_CURTAIL_TP_SIZE:
		case Ads_Message_Base::MESSAGE_SERVICE:
			break;
		default:
			std::cout << "invalid message" << msg->type() << std::endl;
			//ADS_DEBUG((LP_ERROR, "invalid message %d\n", msg->type()));
			//ADS_ASSERT(0);
			break;
	}

	msg->destroy();

	return 0;
}

int
Ads_Service_Base_TP_Adaptive::dispatch_message(Ads_Message_Base *msg) {
	ADS_ASSERT(msg != 0);
	switch (msg->type()) {
	// exit log manager
	case Ads_Message_Base::MESSAGE_EXIT: {
			this->exitting_ = true;
			this->signal_supervisor_exit = 1;
			return 0;
	}
	case Ads_Message_Base::MESSAGE_IDLE:
			return this->on_idle();
	case Ads_Message_Base::MESSAGE_CURTAIL_TP_SIZE: {
		if ((int)n_threads_ > TP_MIN_THRESHOLD) {
			if (!this->deleteNode(pthread_self()))
				return SIGNAL_EXIT_THREAD;
				//pthread_exit(0);
		}
		else std::cout << "curtail action forbidden: thread_pool size is: " << (int)n_threads_ <<std::endl;
		return 0;
	}
	case Ads_Message_Base::MESSAGE_SERVICE: {
		sleep(5);
		return 0;
	}
	default:
		ADS_ASSERT(0);
		break;
	}

	return 0;
}

int main() {
	Ads_Service_Base_TP_Adaptive testASB;
	testASB.num_threads(5);
	for (int i = 0; i < 1; ++ i) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_SERVICE);
		testASB.post_message(msg);
	}
	std::cout << "MQ: Message count =  " << testASB.message_count() << std::endl;

	if(!testASB.open()) std::cout << "open() down" << std::endl;;

	sleep(5);

	for (int i = 0; i < 10; ++ i) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_SERVICE);
		testASB.post_message(msg);
	}
	std::cout << "MQ: Message count =  " << testASB.message_count() << std::endl;

	sleep(10);

	if(!testASB.stop()) std::cout << "stop() done" << std::endl;
	return 0;
}
