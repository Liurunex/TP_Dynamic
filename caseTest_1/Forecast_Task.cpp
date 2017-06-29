#include "Forecast_Task.h"
#include "Forecast_Aggregator.h"

//==============================================================================
// zxliu modification: Supervisor
//==============================================================================

Forecast_Manager_Supervisor::Forecast_Manager_Supervisor(const Ads_String& type, 
	const int threads_size_limit, const int tp_size, Forecast_Service *forecast_service)
	: signal_supervisor_start_(0), exitting_(false), added_counter(0)
	, n_tp_(tp_size), type_(type), threads_size_limit_(threads_size_limit), forecast_service_(forecast_service)
	{}

Forecast_Manager_Supervisor::~Forecast_Manager_Supervisor() {}

int
Forecast_Manager_Supervisor::openself() {
	tp_modification_.resize(n_tp_);

	pthread_attr_t _sattr;
	pthread_attr_init(&_sattr);
	pthread_attr_t *sattr;
	sattr = &_sattr;
	
	if (::pthread_create(&supervisor_id_, sattr, &Forecast_Manager_Supervisor::supervisor_func_run, this))
		ADS_LOG((LP_ERROR, "failed to create supervisor thread\n"));
	
	return 0;
}

int
Forecast_Manager_Supervisor::addworker(Forecast_Task_Manager* worker) {
	tp_group_.push_back(worker);
	added_counter ++;
	
	if (added_counter == n_tp_)
		signal_supervisor_start_ = 1; /* signal supervisor to work after open all workers*/

	return 0;
}

int
Forecast_Manager_Supervisor::stop() {
	this->exitting_ = true;

	::pthread_join(supervisor_id_, 0);
	ADS_LOG((LP_INFO, "TP: supervisor join() done\n"));

	tp_modification_.clear();
	tp_group_.clear();

	return 0;
}

void *
Forecast_Manager_Supervisor::supervisor_func_run(void *arg) {
	Forecast_Manager_Supervisor *s = reinterpret_cast<Forecast_Manager_Supervisor *>(arg);
	s->supervisor_func();

	return 0;
}

/* changed the all_threads == LIMIT, not test yet */
int
Forecast_Manager_Supervisor::supervisor_func() {
	while (!this->signal_supervisor_start_)
		;
	while (!this->exitting_) {

		sleep(5);
		int waiting_mq_count  = 0;
		int idle_thread_count = 0;
		int all_threads_size  = 0;

		std::fill(tp_modification_.begin(), tp_modification_.end(), 0);

		for (int i = 0; i < (int)n_tp_; ++ i) {
			int wait_message =  tp_group_[i]->message_count();
			int idle_threads =  tp_group_[i]->count_idle_threads();
			all_threads_size += tp_group_[i]->tp_size();

			if (wait_message >= MQ_THRESHOLD) {
				tp_modification_[i] = wait_message;
				waiting_mq_count   += wait_message;
			}
			else if (idle_threads  >=  TP_IDLE_THRESHOLD) {
				tp_modification_[i] =  -1 * idle_threads;
				idle_thread_count  += idle_threads;
			}
		}

		int extend_sum         = waiting_mq_count / TP_MODIFY_EXTEND_SCALE;
		int curtail_sum        = idle_thread_count / TP_MODIFY_CURTAIL_SCALE;
		int modify_thread_size = extend_sum - curtail_sum;

		if (all_threads_size == THREAD_LMIT) {
			ADS_LOG((LP_INFO, " Total threads limit size reached before trying modifying\n"));
			for (int i = 0; i < (int)n_tp_; ++ i) {
				if (tp_modification_[i] < 0) {
					int to_curtail = (-1 * tp_modification_[i]) / TP_MODIFY_CURTAIL_SCALE;
					if (to_curtail > 0 && tp_group_[i]->tp_size() >= (to_curtail + TP_MIN_THRESHOLD)) {
						ADS_LOG((LP_INFO, "Thread_Pool:(LIMIT reached) %d do curtail(%d)\n", i, to_curtail));
						tp_group_[i]->curtail_threadpool(to_curtail);
					}
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

		ADS_LOG((LP_INFO, "\n----------------------Extend threads sum: %d\n", extend_sum));
		ADS_LOG((LP_INFO, "\n----------------------Curtail threads sum: %d\n", curtail_sum));
		ADS_LOG((LP_INFO, "\n----------------------Target threads size: %d\n", all_threads_size));
		
		/* individual thread pool modify */
		for (int i = 0; i < (int)n_tp_; ++ i) {
			if (tp_modification_[i] > 0) {
				int to_extend = tp_modification_[i] * extend_sum / waiting_mq_count_;
				ADS_LOG((LP_INFO, "\n++++++++++++++++++++++ Thread_Pool: %d try extend ( %d )\n", i, to_extend));		
				if (to_extend > 0)
					tp_group_[i]->extend_threadpool(to_extend);
			}
			else if (tp_modification_[i] < 0) {
				int to_curtail = (-1 * tp_modification_[i]) * curtail_sum / idle_thread_count_;
				ADS_LOG((LP_INFO, "\n++++++++++++++++++++++ Thread_Pool: %d try curtail ( %d )\n", i, to_curtail));	
				if (to_curtail > 0 && tp_group_[i]->tp_size() >= (to_curtail + TP_MIN_THRESHOLD))
					tp_group_[i]->curtail_threadpool(to_curtail);
			}
		}

	}
	return 0;
}
