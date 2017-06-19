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
//#include "Ads_Types.h"
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

#include <arpa/inet.h>	// inet_ntoa
#include <sys/syscall.h>

#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <assert.h>
#define ADS_ASSERT(a) assert(a)
typedef std::string Ads_String;

namespace ads {
	struct Time_Value {
		Time_Value() { this->tv_.tv_sec = 0; this->tv_.tv_usec = 0; }
		explicit Time_Value(const timeval& tv) { this->tv_.tv_sec = tv.tv_sec; this->tv_.tv_usec = tv.tv_usec; }
		explicit Time_Value(time_t sec, suseconds_t usec = 0) { this->tv_.tv_sec = sec; this->tv_.tv_usec = usec; }

		void set (time_t sec, suseconds_t usec = 0) { this->tv_.tv_sec = sec; this->tv_.tv_usec = usec; }

		time_t sec() const { return this->tv_.tv_sec; }
		suseconds_t usec() const { return this->tv_.tv_usec; }
		time_t msec() const { return this->tv_.tv_sec * 1000 + this->tv_.tv_usec / 1000; }

		timeval tv_;

		const Time_Value& operator -=(const Time_Value& that)
		{
			this->tv_.tv_sec -= that.tv_.tv_sec;
			this->tv_.tv_usec -= that.tv_.tv_usec;
			return *this;
		}

		const Time_Value& operator +=(const Time_Value& that)
		{
			this->tv_.tv_sec += that.tv_.tv_sec;
			this->tv_.tv_usec += that.tv_.tv_usec;
			return *this;
		}
	};
	inline ads::Time_Value
	gettimeofday() {
		timeval tv;
		return ::gettimeofday(&tv, 0) < 0? ads::Time_Value(time_t(0)): ads::Time_Value(tv);
	}
	class Mutex {
		public:
			Mutex()
			{
				::pthread_mutexattr_init(&ma_);
				::pthread_mutexattr_settype(&ma_, PTHREAD_MUTEX_RECURSIVE);
				::pthread_mutex_init(&mu_, &ma_);
			}
			~Mutex()
			{
				::pthread_mutex_destroy(&mu_);
				::pthread_mutexattr_destroy(&ma_);
			}

			int acquire()
			{
				//time_t t0 = ads::gettimeofday().msec();
				::pthread_mutex_lock(&mu_);
				//time_t t1 = ads::gettimeofday().msec();
				//if (t1 - t0 > 300)
				//ads::print_stacktrace();

				return 0;
			}
			int release() 	{ ::pthread_mutex_unlock(&mu_); return 0; }

		private:
			pthread_mutexattr_t ma_;
			pthread_mutex_t mu_;

			Mutex(const Mutex&);
			const Mutex& operator = (const Mutex&);

			friend class Condition_Var;
	};
	class Condition_Var {
		public:
			explicit Condition_Var(Mutex& m): mu_(m) { ::pthread_cond_init(&cv_, NULL); }
			~Condition_Var() { ::pthread_cond_destroy(&cv_); }

			int wait() 		{ ::pthread_cond_wait(&cv_, &mu_.mu_); return 0; }
			int signal()	{ ::pthread_cond_signal(&cv_);  return 0; }
			int broadcast() { ::pthread_cond_broadcast(&cv_); return 0; }

		private:
			pthread_cond_t cv_;
			Mutex& mu_;

			Condition_Var(const Condition_Var&);
			const Condition_Var& operator = (const Condition_Var&);
	};
	class Guard {
		public:
			explicit Guard(Mutex& m): mu_(m) { mu_.acquire(); }
			~Guard() 				{ mu_.release(); }

		private:
			Mutex& mu_;

			Guard(const Guard&);
			const Guard& operator = (const Guard&);
	};
	template <typename T> class TSS {
		public:
			TSS() { ::pthread_key_create(&key_, cleanup); }
			~TSS() {}

			T *get() const {
				void *data = ::pthread_getspecific(key_);
				if(data) return (T *)data;

				T *t = new T();
				::pthread_setspecific(key_, t);
				return t;
			}

			T* operator->() {  return get();  }
			operator T *(void) const { return get(); }

			static void cleanup(void *p) { delete(T *)p; }
		private:
			pthread_key_t key_;
	};

	template <typename T>
	class Message_Queue {
		public:
		Message_Queue(int capacity=-1)
			: mutex_(), not_full_cond_(mutex_), not_empty_cond_(mutex_), empty_cond_(mutex_),  count_(0), capacity_(size_t(capacity)), active_(true) {}

		int enqueue(T *t, bool front = false, bool wait = true)
		{
			mutex_.acquire();
			if (! active_)
			{
				mutex_.release();
				return -1;
			}

			while (count_ >= capacity_)
			{
				if (! wait || !active_)
				{
					mutex_.release();
					return -1;
				}

				not_full_cond_.wait();
			}
			if (front) items_.push_front(t);
			else items_.push_back(t);
			++count_;
			not_empty_cond_.signal();
			mutex_.release();
			return 0;
		}

		int dequeue(T *&t, bool front = true, bool wait = true, bool active_only = true)
		{
			mutex_.acquire();

			if (active_only)
			{
				if (! active_)
				{
					mutex_.release();
					return -1;
				}
			}

			while (items_.empty())
			{
				empty_cond_.signal();
				if (! wait || !active_)
				{
					mutex_.release();
					return -1;
				}

				not_empty_cond_.wait();
			}

			if (front) { t = items_.front(); items_.pop_front(); }
			else { t = items_.back(); items_.pop_back(); }
			--count_;
			not_full_cond_.signal();
			mutex_.release();
			return 0;
		}

		void wait_for_empty()
		{
			ads::Guard __g(mutex_);
			if(!items_.empty())
			{
				empty_cond_.wait();
			}
		}

		size_t message_count() const { return count_; }

		bool is_empty()
		{
			ads::Guard __g(mutex_);
			return items_.empty();
		}

		void deactivate()
		{
			mutex_.acquire();
			active_ = false;
			not_full_cond_.broadcast();
			not_empty_cond_.broadcast();
			mutex_.release();
		}

		void capacity(size_t cap) { this->capacity_ = cap; }

		private:
		Mutex mutex_;
		Condition_Var not_full_cond_, not_empty_cond_, empty_cond_;

		std::list<T *> items_;
		size_t count_;
		size_t capacity_;
		bool active_;
	};
}

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
		MESSAGE_CURTAIL_TP_SIZE = 200
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
	virtual int wait();

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
	ads::Time_Value time_last_activity_;

	size_t n_threads_;
	std::vector<pthread_t> thread_ids_;
};

/* zxliu modification */
#include <unordered_map>

#define MQ_THRESHOLD 5
#define EXTEND_TIME_THRESHOLD 3
#define TP_MIN_THRESHOLD 3
#define TP_EXTEND_SCALE 2
#define TP_CURTAIL_SIZE 1
#define TP_IDLE_THRESHOLD 2
#define SIGNAL_EXIT_THREAD 9 /* must be positive */

class Ads_Service_Base_TP_Adaptive: public Ads_Service_Base {
public:
	Ads_Service_Base_TP_Adaptive()
	: Ads_Service_Base(), mutex_map()
	, signal_worker_start(0)
	{}

	~Ads_Service_Base_TP_Adaptive() {thread_ids_map.clear();}

	/* override base function */
	int open();
	int wait();
	int stop();
	int svc();
	int dispatch_message(Ads_Message_Base *msg);
	int release_message(Ads_Message_Base *msg);

	/* thread_pool size_modification function */
	int extend_threadpool(int extend_scale);
	int curtail_threadpool(int curtail_size);

protected:
	ads::Mutex mutex_map;
	std::unordered_map<pthread_t, int> thread_ids_map;

	volatile int signal_worker_start;

	int deleteNode(pthread_t target);
	int thread_status_set(pthread_t pid, int set_sta);
	size_t count_idle_threads();
};

class Ads_Service_Base_Supervisor {
public:
	Ads_Service_Base_Supervisor()
	: signal_supervisor_start(0)
	, n_tp_(1), exitting_(false)
	{}

	~Ads_Service_Base_Supervisor();

	int openself();
	int openworker();
	int stop();
	int supervisor_func();
	static void *supervisor_func_run(void *arg);
	void num_tp(int i)		{ this->n_tp_ = i; }

protected:
	pthread_t supervisor_id;
	volatile int signal_supervisor_start;
	size_t n_tp_;
	volatile bool exitting_;
	std::vector<std::unique_ptr<Ads_Service_Base>> tp_group;
}
#endif /* ADS_SERVICE_BASE_H */
