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
size_t
Ads_Service_Base_TP_Adaptive::count_idle_threads() {
	ListNode* dummy = thread_ids_start->next;
	size_t all_idle = 0;
	while (dummy) {
		if (!dummy->status) all_idle ++;
		dummy = dummy->next;
	}
	//std::cout << "Idle counts: " << all_idle << std::endl;
	return all_idle;
}

int
Ads_Service_Base_TP_Adaptive::deleteList() {
	ListNode* toDelete = this->thread_ids_start;
	while (toDelete) {
		ListNode* tem = toDelete;
		toDelete = toDelete->next;
		delete tem;
		tem = NULL;
	}
	this->thread_ids_start = NULL;
	this->thread_ids_tail = NULL;
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::thread_status_set(pthread_t pid, int set_sta) {
	ListNode* dummy = this->thread_ids_start->next;
	while (dummy) {
		if (dummy->val == pid) {
			dummy->status = set_sta;
			return 0;
		}
		dummy = dummy->next;
	}
	return 1;
}

int
Ads_Service_Base_TP_Adaptive::deleteListNode(pthread_t target) {
	this->signal_worker_start
	ListNode* dummy = thread_ids_start->next;
	int found = 0;
	while (dummy) {
		if (dummy->val == target) {
			found = 1;
			break;
		}
		dummy = dummy->next;
	}
	if (found) {
		ListNode* tem = dummy->next;
		dummy->val = tem->val;
		dummy->status = tem->status;
		dummy->next = tem->next;
		delete tem;
		tem = NULL;
		return 1;
	}
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::open() {
	/* supervisor thread initial */
	pthread_t supervisor_id;
	ListNode *supervisor_thread = new ListNode(supervisor_id);
	supervisor_thread->status = 1;
	this->thread_ids_start = supervisor_thread;

	pthread_attr_t _sattr;
	pthread_attr_init(&_sattr);
	pthread_attr_t *sattr;
	sattr = &_sattr;
	if (::pthread_create(&this->thread_ids_start->val, sattr, &Ads_Service_Base_TP_Adaptive::supervisor_func_run, this))
		std::cout << "failed to create supervisor thread" << std::endl;

	#if defined(ADS_ENABLE_SEARCH) || defined(ADS_ENABLE_MACOSX)
	pthread_attr_t _attr;
	pthread_attr_init(&_attr);
	pthread_attr_setstacksize(&_attr, 0x4000000); //64M
	#endif

	thread_ids_tail = thread_ids_start;
	for (size_t i = 0; i < n_threads_; ++ i) {
		/* new a ListNode added to LinkedList*/
		pthread_t pth_id;
		ListNode* newThread = new ListNode(pth_id);
		thread_ids_tail->next = newThread;
		thread_ids_tail = thread_ids_tail->next;

		pthread_attr_t *attr = 0;
		#if defined(ADS_ENABLE_SEARCH) || defined(ADS_ENABLE_MACOSX)
		attr = &_attr;
		#endif
		int ret = ::pthread_create(&newThread->val, attr, &Ads_Service_Base_TP_Adaptive::svc_run, this);
		if (ret != 0) std::cout << "failed to create thread " << i << std::endl;
	}
	/* make supervisor and worker start to work */
	this->signal_worker_start = 1;
	this->signal_supervisor_start = 1;
	std::cout << "open() thread_pool size: " << n_threads_ << std::endl;

	return 0;
}

int
Ads_Service_Base_TP_Adaptive::extend_threadpool() {
	this->signal_worker_start = 0;

	size_t start_index = n_threads_;
	n_threads_ += (n_threads_ / TP_EXTEND_SCALE);

	pthread_attr_t _attr;
	pthread_attr_init(&_attr);
	pthread_attr_setstacksize(&_attr, 0x4000000); //64M

	for (size_t i = start_index; i < n_threads_; ++ i) {
		pthread_t pth_id;
		ListNode* newThread = new ListNode(pth_id);
		thread_ids_tail->next = newThread;
		thread_ids_tail = thread_ids_tail->next;

		pthread_attr_t *attr = 0;
		attr = &_attr;
		int ret = ::pthread_create(&newThread->val, attr, &Ads_Service_Base_TP_Adaptive::svc_run, this);
		if (ret != 0) std::cout << "failed to create thread " << i << std::endl;
		//ADS_LOG((LP_ERROR, "failed to create thread %d\n", i));
	}
	this->signal_worker_start = 1;
	std::cout << "extend thread_pool size: " <<  (int)n_threads_<< std::endl;
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::curtail_threadpool() {
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
		std::cout << "supervisor do run" << std::endl;
		sleep(1);
		try_extend ++;
		if ((int)this->message_count() == 0 && this->count_idle_threads() >= TP_IDLE_THRESHOLD) {
			std::cout << "do curtail" << std::endl;
			this->curtail_threadpool();
		}
		else if (try_extend >= TIME_THRESHOLD && (int)this->message_count() >= MQ_THRESHOLD) {
			std::cout << "do extend" << std::endl;
			this->extend_threadpool();
			try_extend = 0;
		}
	}
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::svc() {
	//	ACE_DEBUG((LM_INFO, "[base: %t] Base Service started.\n"));
	Ads_Message_Base *msg = 0;
	pthread_t ppid = pthread_self();
	while (!this->signal_worker_start)
		;
	while (msg_queue_.dequeue(msg) >= 0) {
		if(this->exitting_) {
			msg->destroy();
			break;
		}

		pthread_t pid = pthread_self();
		//std::cout << "current pid: " << pid << std::endl;
		if(this->thread_status_set(pid, 1))
			std::cout << "set thread status failed 1 " << std::endl;

		if (this->dispatch_message(msg) < 0)
			std::cout << "failed to dispatch msg" << std::endl;
			//ADS_DEBUG((LP_DEBUG, "failed to dispatch msg.\n"));

		msg->destroy();

		if(this->thread_status_set(pid, 0))
			std::cout << "set thread status failed 0" << std::endl;

		this->time_last_activity_ = ads::gettimeofday();

	}
	//	ACE_DEBUG((LM_INFO, "[base: %t] Base Service stopped.\n"));
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::wait() {
	std::cout << "wait()" << std::endl;
	ListNode* dummy = this->thread_ids_start->next;
	while (dummy) {
		::pthread_join(dummy->val, 0);
		std::cout << "join: " << dummy->val << std::endl;
		dummy = dummy->next;
		this->exit_threads_count ++;
	}
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::stop() {
	std::cout << "stop()" << std::endl;
	this->signal_supervisor_exit = 1;
	this->exitting_ = true;

	::pthread_join(this->thread_ids_start->val, 0);

	for (int i = 0; i < n_threads_; ++ i) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_EXIT);
		if (this->post_message(msg) < 0) {
			msg->destroy();
			std::cout << "cannot post exit message 2" << std::endl;
			i --;
		}
	}


	this->wait();

	Ads_Message_Base *msg = 0;
	while (this->msg_queue_.dequeue(msg, true, false) >= 0)
		this->release_message(msg);

	if (this->deleteList())
		std::cout << "failed to delete LinkedList " << std::endl;

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
			pthread_t cur_id = pthread_self();
			if (this->deleteListNode(cur_id)) {
				//std::cout << "curtail in " << std::endl;
				this->n_threads_ --;
				std::cout << "curtail() thread_pool size: " << (int)n_threads_ << std::endl;
				pthread_exit(NULL);
			}
		}
		else std::cout << "curtail action forbidden: thread_pool size is: " << (int)n_threads_ <<std::endl;
			//ADS_LOG_RETURN((LP_ERROR, "curtail action forbidden: thread_pool size is %d\n"), TP_MIN_THRESHOLD);
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
	testASB.num_threads(10);
	for (int i = 0; i < 1; ++ i) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_SERVICE);
		testASB.post_message(msg);
	}
	std::cout << "MQ: Message count =  " << testASB.message_count() << std::endl;

	if (testASB.open()) std::cout << "open() error" << std::endl;
	else std::cout << "open() run " << std::endl;
		//ADS_LOG((LP_ERROR, "open() error\n"));

	sleep(10);

	for (int i = 0; i < 40; ++ i) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_SERVICE);
		testASB.post_message(msg);
	}
	std::cout << "MQ: Message count =  " << testASB.message_count() << std::endl;

	sleep(10);

	if(testASB.stop()) std::cout << "stop() error" << std::endl;
	else std::cout << "stop() run" << std::endl;
		//ADS_LOG((LP_ERROR, "stop() error\n"));
	return 0;
}
