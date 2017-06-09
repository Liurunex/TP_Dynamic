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

#define MQ_THRESHOLD 10
#define TIME_THRESHOLD 5
#define TP_SIZE_THRESHOLD 1
#define TP_EXTEND_SCALE 2

Ads_Message_Base *
Ads_Message_Base::create(Ads_Message_Base::TYPE type, void *data){
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

	for (size_t i = 0; i < n_threads_; ++i) {
		pthread_attr_t *attr = 0;
		#if defined(ADS_ENABLE_SEARCH) || defined(ADS_ENABLE_MACOSX)
		attr = &_attr;
		#endif
		int ret = ::pthread_create(&this->thread_ids_[i], attr, &Ads_Service_Base::svc_run, this);
		if (ret != 0) std::cout << "failed to create thread " << i << std::endl;
	}

	pthread_attr_t _sattr;
	pthread_attr_init(&_sattr);
	pthread_attr_t *sattr;
	sattr = &_sattr;
	if (::pthread_create(&this->supervisor_id, sattr, &Ads_Service_Base::supervisor_func_run, this))
		std::cout << "failed to create supervisor thread" << std::endl;
	else signal_supervisor_exit = 0;

	std::cout << "thread_pool size: " << thread_ids_.size() << std::endl;

	return 0;
}

int
Ads_Service_Base::stop() {
	signal_supervisor_exit = 1;
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
	::pthread_join(this->supervisor_id, NULL);
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
	case Ads_Message_Base::MESSAGE_CURTAIL_TP_SIZE: {
		if ((int)n_threads_ > TP_SIZE_THRESHOLD) {
			pthread_t cur_id = pthread_self();
			std::vector<pthread_t>::iterator position = std::find(this->thread_ids_.begin(), this->thread_ids_.end(), cur_id);
			if (position != this->thread_ids_.end()) {
    				this->thread_ids_.erase(position);
				n_threads_ = this->thread_ids_.size();
				std::cout << "thread_pool size: " << thread_ids_.size() << std::endl;
				pthread_exit(NULL);
			}
		}
		else std::cout << "curtail action forbidden: thread_pool size is: " << n_threads_ <<std::endl;
			//ADS_LOG_RETURN((LP_ERROR, "curtail action forbidden: thread_pool size is %d\n"), TP_SIZE_THRESHOLD);
		return 0;
	}
	case Ads_Message_Base::MESSAGE_SERVICE: {
		std::cout << "do sleep" << pthread_self() << std::endl;
		sleep(1);
		std::cout << "down sleep" << pthread_self() << std::endl;
		return 0;
	}
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
		case Ads_Message_Base::MESSAGE_CURTAIL_TP_SIZE:
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

/* zxliu modify*/

/* double the size of thread_pool */
int
Ads_Service_Base::extend_threadpool_size() {
	size_t start_index = n_threads_;
	n_threads_ += n_threads_ / TP_EXTEND_SCALE;
	thread_ids_.resize(n_threads_);
	pthread_attr_t _attr;
	pthread_attr_init(&_attr);
	pthread_attr_setstacksize(&_attr, 0x4000000); //64M

	for (size_t i = start_index; i < n_threads_; ++ i) {
		pthread_attr_t *attr = 0;
		attr = &_attr;
		int ret = ::pthread_create(&this->thread_ids_[i], attr, &Ads_Service_Base::svc_run, this);
		if (ret != 0) std::cout << "failed to create thread " << i << std::endl;
		//ADS_LOG((LP_ERROR, "failed to create thread %d\n", i));
	}
	std::cout << "thread_pool size: " <<  thread_ids_.size()<< std::endl;
	return 0;
}

int
Ads_Service_Base::curtail_threadpool_size() {
	while (1) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_CURTAIL_TP_SIZE);
		if(this->post_message(msg) < 0) {
			std::cout << "cannot post curtail message" << std::endl;
			//ADS_LOG((LP_ERROR, "cannot post curtail message\n"));
			msg->destroy();
		}
		else return 0;
	}
	return 0;
}

void *
Ads_Service_Base::supervisor_func_run(void *arg) {
	Ads_Service_Base *s = reinterpret_cast<Ads_Service_Base *>(arg);
	s->supervisor_func();

	return 0;
}

int
Ads_Service_Base::supervisor_func() {
	int try_extend = 0;
	while (!this->signal_supervisor_exit) {
		sleep(1);
		try_extend ++;
		if ((int)this->message_count() == 0)
			this->curtail_threadpool_size();
		else if ((int)this->message_count() >= MQ_THRESHOLD && try_extend >= TIME_THRESHOLD) {
			this->extend_threadpool_size();
			try_extend = 0;
		}
	}
	return 0;
}

int main() {
	Ads_Service_Base testASB;
	testASB.num_threads(5);
	for (int i = 0; i < 20; ++ i) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_SERVICE);
		testASB.post_message(msg);
	}
	if (testASB.open()) std::cout << "open() error" << std::endl;
		//ADS_LOG((LP_ERROR, "open() error\n"));
	sleep(5);
	/* add more message to queue*/
	for (int i = 0; i < 20; ++ i) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_SERVICE);
		testASB.post_message(msg);
	}
	sleep(5);
	if(testASB.stop()) std::cout << "stop() error" << std::endl;
		//ADS_LOG((LP_ERROR, "stop() error\n"));
	return 0;
}
