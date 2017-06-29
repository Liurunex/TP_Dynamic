#if !defined(FORECAST_TASK_H)
#define FORECAST_TASK_H

#include "server/Ads_Job_Queue_Service.h"
#include "Forecast_Service.h"
#include "Forecast_Simulator.h"

//==============================================================================
// zxliu modification: Supervisor
//==============================================================================
#define THREAD_LMIT 				100
#define TP_MODIFY_CURTAIL_SCALE 	2
#define TP_MODIFY_EXTEND_SCALE 		2

class Forecast_Manager_Supervisor {
public:
	Forecast_Manager_Supervisor(const Ads_String& type = "superviosr", 
		const int threads_size_limit = 50, const int tp_size = 6, Forecast_Service *forecast_service = NULL);
	virtual ~Forecast_Manager_Supervisor();

	int stop();
	int openself();
	int supervisor_func();
	int addworker(Forecast_Task_Manager* worker);

	Ads_String type() 					{ return type_; }
	void 	   num_tp(int i)			{ this->n_tp_ = i; }
	size_t 	   return_ntp() 			{ return this->n_tp_; }
	void 	   set_thread_limit(int i)	{ this->threads_size_limit_ = i > THREAD_LMIT ? THREAD_LMIT:i; }

	static void *supervisor_func_run(void *arg);
	
	std::vector<Forecast_Task_Manager *> &return_tp_group() { return this->tp_group_; }

protected:
	pthread_t 		supervisor_id_;
	volatile int 	signal_supervisor_start_;
	volatile bool 	exitting_;
	Ads_String 		type_;
	
	size_t n_tp_;
	size_t added_counter;
	size_t threads_size_limit_;

	std::vector<int> tp_modification_;
	std::vector<Forecast_Task_Manager *> tp_group_;

	Forecast_Service *forecast_service_;

};

#endif // FORECAST_TASK_H

