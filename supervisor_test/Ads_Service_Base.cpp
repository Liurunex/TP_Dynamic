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
#include <memory>

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
			////ADS_LOG((LP_ERROR, "failed to create thread %d\n", i));
		}
	}

	return 0;
}

int
Ads_Service_Base::stop() {
	this->exitting_ = true;
	{
		for (int i = 0; i < (int)n_threads_; ++ i) {
			Ads_Message_Base * msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_EXIT);
			if(this->post_message(msg) < 0) {
				//ADS_LOG((LP_ERROR, "cannot post exit message 1\n"));
				msg->destroy();

				/// due to possible signal_dequeue_waiters failure, Message_Queue might continuously wait_not_empty_cond
				/// so here we enqueue MESSAGE_EXIT message twice.
				Ads_Message_Base * msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_EXIT);
				if (this->post_message(msg) < 0) {
					msg->destroy();
					//ADS_LOG((LP_ERROR, "cannot post exit message 2\n"));
				}
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
			////ADS_LOG_RETURN((LP_ERROR, "failed to enqueue msg (HIGH)\n"),-1);
	}
	else if(p == Ads_Message_Base::PRIORITY_NORMAL) {
		if (this->msg_queue_.enqueue(msg) < 0)
			std::cout << "failed to enqueue msg (NORMAL)" << std::endl;
			////ADS_LOG_RETURN((LP_ERROR, "failed to enqueue msg (NORMAL)\n"),-1);
	}
	else if(p == Ads_Message_Base::PRIORITY_IDLE) {
	//if (this->msg_queue_.enqueue(msg, (ACE_Time_Value *)&ACE_Time_Value::zero) < 0)
		if (this->msg_queue_.enqueue(msg, false, false) < 0)
			std::cout << "failed to enqueue msg (IDLE)" << std::endl;
			////ADS_LOG_RETURN((LP_ERROR, "failed to enqueue (IDLE)\n"),-1);
	}
	else
		std::cout << "invalid priority" << std::endl;
		////ADS_LOG_RETURN((LP_ERROR, "invalid priority %d\n", p),-1);

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
/* mutex_added_function start */
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
	if (got != thread_ids_map.end()) {
		thread_ids_map.erase(got);
		this->n_threads_ --;
	}

	int res = thread_ids_map.find(target) == thread_ids_map.end() ? 1:0;
	std::cout << "erase the pid: " << target << " , check success: " << res << std::endl;
	/*
	std::cout << "\ncurtail() thread_pool size: " << (int)n_threads_ << std::endl;
	std::cout << "\nafter curtail, allthreads:" <<std::endl;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map.begin();
	while(it != thread_ids_map.end()) {
		std::cout << it->first << " ";
		it ++;
	}
	std::cout << "delete_node_end()\n" <<std::endl;
	*/
	mutex_map.release();
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::extend_threadpool(int extend_size) {
	mutex_map.acquire();

	this->signal_worker_start = 0; //might be removerd, since all theads will get stuck due to mutex
	size_t start_index = n_threads_;
	n_threads_ += extend_size;

	pthread_attr_t _attr;
	pthread_attr_init(&_attr);
	pthread_attr_setstacksize(&_attr, 0x4000000); //64M

	for (size_t i = start_index; i < n_threads_; ++ i) {
		pthread_t pth_id;
		pthread_attr_t *attr = 0;
		attr = &_attr;

		int ret = ::pthread_create(&pth_id, attr, &Ads_Service_Base_TP_Adaptive::svc_run, this);
		if (ret != 0) std::cout << "failed to create thread " << pth_id << std::endl;
		thread_ids_map[pth_id] = 0;
	}
	/*
	std::cout << "extend thread_pool size: " <<  (int)n_threads_<< std::endl;
	std::cout << "\nafter extend, allthreads:" <<std::endl;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map.begin();
	while(it != thread_ids_map.end()) {
		std::cout << it->first << " ";
		it ++;
	}
	std::cout << "extend_end()\n" <<std::endl;
	*/
	this->signal_worker_start = 1; //might be removerd

	mutex_map.release();
	return 0;
}

/* mutex_added_function end */

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

/* override fucntions */
int
Ads_Service_Base_TP_Adaptive::open() {
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
		if (ret != 0) std::cout << "failed to create thread " << pth_id << std::endl;
		thread_ids_map[pth_id] = 0;
	}
	/*
	std::cout << "open() thread_pool size: " << (int)n_threads_ << std::endl;
	std::cout << "\nallthreads:" <<std::endl;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map.begin();
	while(it != thread_ids_map.end()) {
		std::cout << it->first << " ";
		it ++;
	}
	std::cout << "open_end()\n" <<std::endl;
	*/

	/* make worker start to work */
	this->signal_worker_start = 1;

	return 0;
}

int
Ads_Service_Base_TP_Adaptive::wait() {
	std::cout << "wait() in" << std::endl;
	std::unordered_map<pthread_t, int>::iterator it = thread_ids_map.begin();

	while(it != thread_ids_map.end()) {
		::pthread_join(it->first, 0);
		std::cout << "join: " << it->first << std::endl;
		it ++;
	}

	return 0;
}

int
Ads_Service_Base_TP_Adaptive::stop() {
	std::cout << "stop() in " << std::endl;
	this->exitting_ = true;

	for (int i = 0; i < (int)n_threads_; ++ i) {
		Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_EXIT);
		while (this->post_message(msg) < 0) {
			msg->destroy();
			std::cout << "cannot post exit message " << std::endl;
			Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_EXIT);
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
			return 0;
	}
	case Ads_Message_Base::MESSAGE_IDLE:
			return this->on_idle();
	case Ads_Message_Base::MESSAGE_CURTAIL_TP_SIZE: {
		if ((int)n_threads_ > TP_MIN_THRESHOLD) {
			if (!this->deleteNode(pthread_self()))
				return SIGNAL_EXIT_THREAD;
		}
		else std::cout << "curtail action forbidden: thread_pool size is: " << (int)n_threads_ << std::endl;
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

		if (msg->type() == Ads_Message_Base::MESSAGE_SERVICE)
			if(this->thread_status_set(pthread_self(), 1))
				std::cout << "set thread status failed 1 " << std::endl;

		int dispatch_return = this->dispatch_message(msg);
		/* terminate current thread */
		if (dispatch_return == SIGNAL_EXIT_THREAD) {
			msg->destroy();
			::pthread_detach(pthread_self());
			return 0;
		}
		else if (dispatch_return < 0)
			std::cout << "failed to dispatch msg" << std::endl;

		if (msg->type() == Ads_Message_Base::MESSAGE_SERVICE )
			if(this->thread_status_set(pthread_self(), 0))
				std::cout << "set thread status failed 0" << std::endl;

		msg->destroy();
		this->time_last_activity_ = ads::gettimeofday();
	}

	return 0;
}


/* supervisor class implement */
int
Ads_Service_Base_Supervisor::openself() {
	/* supervisor thread initial */
	tp_modification.resize(n_tp_);

	pthread_attr_t _sattr;
	pthread_attr_init(&_sattr);
	pthread_attr_t *sattr;
	sattr = &_sattr;
	if (::pthread_create(&supervisor_id, sattr, &Ads_Service_Base_Supervisor::supervisor_func_run, this))
		std::cout << "failed to create supervisor thread" << std::endl;
	else {
		for (int i = 0; i < (int)n_tp_; ++ i)
			tp_group.push_back(std::unique_ptr<Ads_Service_Base_TP_Adaptive> (new Ads_Service_Base_TP_Adaptive()));
	}

	return 0;
}

int
Ads_Service_Base_Supervisor::openworker() {
	for (int i = 0; i < (int)this->n_tp_; ++ i)
		if (!tp_group[i]->open()) std::cout << "TP: " << i << "open() down" << std::endl;

	/* make supervisor to work after open all workers*/
	signal_supervisor_start = 1;
	return 0;
}

int
Ads_Service_Base_Supervisor::stop() {
	std::cout << "supervisor stop() in" << std::endl;
	this->exitting_ = true;

	::pthread_join(supervisor_id, 0);
	std::cout << "supervisor join() supervisor done " << std::endl;

	for (int i =0; i < (int)n_tp_; ++ i)
		if (!tp_group[i]->stop()) std::cout << "TP: " << i << "stop() done" << std::endl;

	return 0;
}

int
Ads_Service_Base_Supervisor::threads_sum() {
	int res = 0;
	for (int i = 0; i < (int)n_tp_; ++ i)
		res += (int)tp_group[i]->tp_size();
	return res;
}

void *
Ads_Service_Base_Supervisor::supervisor_func_run(void *arg) {
	Ads_Service_Base_Supervisor *s = reinterpret_cast<Ads_Service_Base_Supervisor *>(arg);
	s->supervisor_func();

	return 0;
}

int
Ads_Service_Base_Supervisor::supervisor_func() {
	while (!this->signal_supervisor_start)
		;
	while (1) {
		if (this->exitting_) return 0;

		sleep(5);
		waiting_mq_count = 0;
		idle_thread_count = 0;
		int all_threads_size = 0;
		std::fill(tp_modification.begin(), tp_modification.end(), 0);

		for (int i = 0; i < (int)n_tp_; ++ i) {
			int wait_message = tp_group[i]->message_count();
			int idle_threads = tp_group[i]->count_idle_threads();
			all_threads_size += tp_group[i]->tp_size();

			if (wait_message >= MQ_THRESHOLD) {
				tp_modification[i] = wait_message;
				waiting_mq_count += wait_message;
			}
			else if (idle_threads > TP_IDLE_THRESHOLD) {
				tp_modification[i] = -1 * idle_threads;
				idle_thread_count += idle_threads;
			}
		}

		int extend_sum = waiting_mq_count / TP_MODIFY_EXTEND_SCALE;
		int curtail_sum = idle_thread_count / TP_MODIFY_CURTAIL_SCALE;
		int modify_thread_size =  extend_sum - curtail_sum;

		if (all_threads_size == THREAD_LMIT) continue;
		else if (all_threads_size + modify_thread_size > THREAD_LMIT) {
			extend_sum =  (THREAD_LMIT - all_threads_size) / modify_thread_size * extend_sum;
			curtail_sum = extend_sum - (THREAD_LMIT - all_threads_size);
			all_threads_size = THREAD_LMIT;
		}
		else all_threads_size += modify_thread_size;

		/* individual thread modify */
		for (int i = 0; i < (int)n_tp_; ++ i) {
			if (tp_modification[i] > 0) {
				std::cout << "Thread_Pool: " << i << "do extend" << std::endl;
				int to_extend = tp_modification[i] / waiting_mq_count * extend_sum;
				tp_group[i]->extend_threadpool(to_extend);
			}
			else if (tp_modification[i] < 0) {
				std::cout << "Thread_Pool: " << i << "do curtail" << std::endl;
				int to_curtail = -1 * tp_modification[i] / idle_thread_count * curtail_sum;
				tp_group[i]->curtail_threadpool(to_curtail);
			}
		}

	}
	return 0;
}


/* main tester */

int main() {
	Ads_Service_Base_Supervisor supervisor_test;
	supervisor_test.set_thread_limit(100);
	supervisor_test.num_tp(2);
	if (!supervisor_test.openself()) std::cout << "supervisor open down" << std::endl;

	for (int j = 0; j < (int)supervisor_test.return_ntp(); ++ j) {
		supervisor_test.return_tp_group()->back()->num_threads(5);

		for (int i = 0; i < 10; ++ i) {
			Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_SERVICE);
			supervisor_test.return_tp_group()->back()->post_message(msg);
		}
		std::cout << "MQ:" << j << " Message count =  " << supervisor_test.return_tp_group()->back()->message_count() << std::endl;
	}

	if (!supervisor_test.openworker()) std::cout << "supervisor open all worker and start working" << std::endl;

	sleep(5);

	for (int j = 0; j < (int)supervisor_test.return_ntp(); ++ j) {
		for (int i = 0; i < 10; ++ i) {
			Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_SERVICE);
			supervisor_test.return_tp_group()->at(j)->post_message(msg);
		}
		std::cout << "MQ:" << j << " Message count =  " << supervisor_test.return_tp_group()->at(j)->message_count() << std::endl;
	}

	sleep(5);

	if(!supervisor_test.stop()) std::cout << "stop() done" << std::endl;

	return 0;
}
