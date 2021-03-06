#include <pthread.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <functional>
#include <sys/time.h>
#include <memory>

#include "Ads_Service_Base.h"

/* zxliu modification */
Ads_Service_Base_TP_Adaptive::Ads_Service_Base_TP_Adaptive()
	: Ads_Service_Base(), mutex_map()
	, signal_worker_start(0)
	{}

Ads_Service_Base_TP_Adaptive::~Ads_Service_Base_TP_Adaptive() {thread_ids_map.clear();}

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

	mutex_map.release();
	return 0;
}

int
Ads_Service_Base_TP_Adaptive::extend_threadpool(int extend_size) {
	mutex_map.acquire();

	this->signal_worker_start = 0;
	size_t start_index        = n_threads_;
	n_threads_                += extend_size;

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
	
	std::cout << "extend thread_pool size: " <<  (int)n_threads_<< std::endl;

	this->signal_worker_start = 1; 

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

Ads_Service_Base_Supervisor::Ads_Service_Base_Supervisor()
	: signal_supervisor_start(0), n_tp_(1)
	, exitting_(false), threads_size_limit(50)
	, waiting_mq_count(0), idle_thread_count(0)
	{}

Ads_Service_Base_Supervisor::~Ads_Service_Base_Supervisor() {}

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
			tp_group.push_back(new Ads_Service_Base_TP_Adaptive());
	}

	return 0;
}

int
Ads_Service_Base_Supervisor::openworker() {
	for (int i = 0; i < (int)n_tp_; ++ i)
		if (!tp_group[i]->open()) std::cout << "\n++++++++++++++++++++++ TP: " << i << " open() down" << std::endl;

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

	for (int i = 0; i < (int)n_tp_; ++ i) {
		if (!tp_group[i]->stop()) std::cout << "TP: " << i << " stop() done" << std::endl;
		
		/* delete the obejcts the ppointer pointed to */
		delete tp_group[i];
		tp_group[i] = NULL;
	}

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

/* changed the all_threads == LIMIT, not test yet */
int
Ads_Service_Base_Supervisor::supervisor_func() {
	while (!this->signal_supervisor_start)
		;
	while (1) {
		if (this->exitting_) return 0;

		sleep(5);
		waiting_mq_count     = 0;
		idle_thread_count    = 0;
		int all_threads_size = 0;

		std::fill(tp_modification.begin(), tp_modification.end(), 0);

		for (int i = 0; i < (int)n_tp_; ++ i) {
			int wait_message =  tp_group[i]->message_count();
			int idle_threads =  tp_group[i]->count_idle_threads();
			all_threads_size += tp_group[i]->tp_size();

			if (wait_message >= MQ_THRESHOLD) {
				tp_modification[i] = wait_message;
				waiting_mq_count   += wait_message;
			}
			else if (idle_threads  >=  TP_IDLE_THRESHOLD) {
				tp_modification[i] =  -1 * idle_threads;
				idle_thread_count  += idle_threads;
			}
		}
		std::cout << "\n----------------------Threads count: " << all_threads_size << std::endl;
		std::cout << "\n----------------------Waiting count: " << waiting_mq_count << std::endl;
		std::cout << "\n----------------------Idle threads count: " << idle_thread_count << std::endl;

		int extend_sum         = waiting_mq_count / TP_MODIFY_EXTEND_SCALE;
		int curtail_sum        = idle_thread_count / TP_MODIFY_CURTAIL_SCALE;
		int modify_thread_size = extend_sum - curtail_sum;

		if (all_threads_size == THREAD_LMIT) {
			for (int i = 0; i < (int)n_tp_; ++ i) {
				if (tp_modification[i] < 0) {
					int to_curtail = (-1 * tp_modification[i]) / TP_MODIFY_CURTAIL_SCALE;
					std::cout << "\n++++++++++++++++++++++ Thread_Pool:(LIMIT reached) " << i << " try curtail( " << to_curtail << " )" << std::endl;
					if (to_curtail > 0 && tp_group[i]->tp_size() >= (to_curtail + TP_MIN_THRESHOLD))
						tp_group[i]->curtail_threadpool(to_curtail);
				}
			}
			continue;
		};
		else if (all_threads_size + modify_thread_size > THREAD_LMIT) {
			extend_sum       = (THREAD_LMIT - all_threads_size) * extend_sum / modify_thread_size;
			curtail_sum      = extend_sum - (THREAD_LMIT - all_threads_size);
			all_threads_size = THREAD_LMIT;
		}
		else all_threads_size += modify_thread_size;

		std::cout << "\n----------------------Extend sum: " << extend_sum << std::endl;
		std::cout << "\n----------------------Curtail sum: " << curtail_sum << std::endl;
		std::cout << "\n----------------------Target threads size: " << all_threads_size << std::endl;

		/* individual thread modify */
		for (int i = 0; i < (int)n_tp_; ++ i) {
			if (tp_modification[i] > 0) {
				int to_extend = tp_modification[i] * extend_sum / waiting_mq_count;
				std::cout << "\n++++++++++++++++++++++ Thread_Pool: " << i << " try extend( " << to_extend << " )" << std::endl;
				if (to_extend > 0)
					tp_group[i]->extend_threadpool(to_extend);
			}
			else if (tp_modification[i] < 0) {
				int to_curtail = (-1 * tp_modification[i]) * curtail_sum / idle_thread_count;
				std::cout << "\n++++++++++++++++++++++ Thread_Pool: " << i << " try curtail( " << to_curtail << " )" << std::endl;
				if (to_curtail > 0 && tp_group[i]->tp_size() >= (to_curtail + TP_MIN_THRESHOLD))
					tp_group[i]->curtail_threadpool(to_curtail);
			}
		}

	}
	return 0;
}

/* main tester */
int main() {
	Ads_Service_Base_Supervisor supervisor_test;
	supervisor_test.set_thread_limit(50);
	supervisor_test.num_tp(2);
	if (!supervisor_test.openself()) std::cout << "\n----------------------Supervisor open down" << std::endl;

	for (int j = 0; j < (int)supervisor_test.return_ntp(); ++ j) {
		supervisor_test.return_tp_group().at(j)->num_threads(5);

		for (int i = 0; i < 10; ++ i) {
			Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_SERVICE);
			supervisor_test.return_tp_group().at(j)->post_message(msg);
		}
		std::cout << "MQ: " << j << " Message count =  " << supervisor_test.return_tp_group().at(j)->message_count() << std::endl;
	}

	if (!supervisor_test.openworker()) std::cout << "\n----------------------Supervisor open all worker and start working" << std::endl;

	sleep(5);

	for (int j = 1; j < (int)supervisor_test.return_ntp(); ++ j) {
		for (int i = 0; i < 10; ++ i) {
			Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Message_Base::MESSAGE_SERVICE);
			supervisor_test.return_tp_group().at(j)->post_message(msg);
		}
		std::cout << "MQ: " << j << " Message count =  " << supervisor_test.return_tp_group().at(j)->message_count() << std::endl;
	}

	sleep(15);

	if(!supervisor_test.stop()) std::cout << "\n----------------------All stop() done" << std::endl;

	return 0;
}
