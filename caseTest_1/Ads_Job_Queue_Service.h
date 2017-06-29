//==============================================================================
// zxliu modification: TP_Adaptive
//==============================================================================
#include <unordered_map>

#define TYPE_JOB				1
#define TYPE_EXIT				0
#define TYPE_CURTAIL			2
#define MQ_THRESHOLD 			5
#define TP_MIN_THRESHOLD 		2
#define TP_IDLE_THRESHOLD 		2

class Ads_Job_Queue_Service: public Ads_Service_Base {

/* modification */
public:
	Ads_Job_Queue_Service();
	virtual ~Ads_Job_Queue_Service();

	struct Curtail_TP: public Ads_Job { virtual int type() { return TYPE_CURTAIL; } };
	/* override base function */
	int open();
	int wait();
	int stop();
	int svc();
	int curtail_action();
	int dispatch_message	(Ads_Message_Base *msg);
	int release_message 	(Ads_Message_Base *msg);

	/* thread_pool size_modification function */
	int extend_threadpool	(int extend_scale);
	int curtail_threadpool	(int curtail_size);

	size_t tp_size() { return (int)this->n_threads_; }
	size_t count_idle_threads();

protected:
	ads::Mutex mutex_map_;
	std::unordered_map<pthread_t, int> thread_ids_map_;

	volatile int signal_worker_start_;

	int deleteNode			(pthread_t target);
	int thread_status_set	(pthread_t pid, int set_sta);

};

#endif /* ADS_JOB_QUEUE_SERVICE_H_ */