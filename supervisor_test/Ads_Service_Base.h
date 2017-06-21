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

/* zxliu modification */
#include <unordered_map>

#define MQ_THRESHOLD 			5
#define TP_MIN_THRESHOLD 		2
#define TP_IDLE_THRESHOLD 		2
#define SIGNAL_EXIT_THREAD 		9 /* must be positive */

class Ads_Service_Base_TP_Adaptive: public Ads_Service_Base {
public:
	Ads_Service_Base_TP_Adaptive();
	~Ads_Service_Base_TP_Adaptive();

	/* override base function */
	int open();
	int wait();
	int stop();
	int svc();
	int dispatch_message	(Ads_Message_Base *msg);
	int release_message 	(Ads_Message_Base *msg);

	/* thread_pool size_modification function */
	int extend_threadpool	(int extend_scale);
	int curtail_threadpool	(int curtail_size);

	size_t tp_size() { return (int)this->n_threads_; }
	size_t count_idle_threads();

protected:
	ads::Mutex mutex_map;
	std::unordered_map<pthread_t, int> thread_ids_map;

	volatile int signal_worker_start;

	int deleteNode			(pthread_t target);
	int thread_status_set	(pthread_t pid, int set_sta);

};


/* indivadual supervisor */
#define THREAD_LMIT 				100
#define TP_MODIFY_CURTAIL_SCALE 	2
#define TP_MODIFY_EXTEND_SCALE 		2

class Ads_Service_Base_Supervisor {
public:
	Ads_Service_Base_Supervisor();
	virtual ~Ads_Service_Base_Supervisor();

	int stop();
	int openself();
	int openworker();
	int threads_sum();
	int supervisor_func();
	
	void 	num_tp(int i)			{ this->n_tp_ = i; }
	size_t 	return_ntp() 			{ return this->n_tp_; }
	void 	set_thread_limit(int i)	{ this->threads_size_limit = i > THREAD_LMIT ? THREAD_LMIT:i; }

	static void *supervisor_func_run(void *arg);
	std::vector<Ads_Service_Base_TP_Adaptive *> return_tp_group() { return this->tp_group; }

protected:
	pthread_t 		supervisor_id;
	volatile int 	signal_supervisor_start;
	volatile bool 	exitting_;

	size_t n_tp_;
	size_t threads_size_limit;
	int waiting_mq_count;
	int idle_thread_count;

	std::vector<int> tp_modification;
	std::vector<Ads_Service_Base_TP_Adaptive *> tp_group;
};
#endif /* ADS_SERVICE_BASE_H */
