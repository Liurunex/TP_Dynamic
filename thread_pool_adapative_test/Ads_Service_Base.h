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
	, signal_supervisor_start(0)
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

	size_t tp_size() { return (int)this->n_threads_; }

	/* the supervisor fucntion for test */
	int supervisor_func();
	static void *supervisor_func_run(void *arg);

protected:
	ads::Mutex mutex_map;
	std::unordered_map<pthread_t, int> thread_ids_map;
	pthread_t supervisor_id;

	volatile int signal_supervisor_start;
	volatile int signal_worker_start;

	int deleteNode(pthread_t target);
	int thread_status_set(pthread_t pid, int set_sta);
	size_t count_idle_threads();
};

#endif /* ADS_SERVICE_BASE_H */
