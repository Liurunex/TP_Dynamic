input: util%, cpu%, mem%, io_load, r, b, cpu#;

revise: LIMIT, extend_scale, curtail_scale

unsigned int LIMIT
unsigned int extend_scale, curtail_scale;
int new_thread_forbidden = 0;
extend_scale  = TP_MODIFY_EXTEND_SCALE;
curtail_scale = TP_MODIFY_CURTAIL_SCALE


struct speedup {
	int eff_ratio = 1/(B + (1-B)/N); 
}
/* B:   serial part 
 * 1-B: parallel part 
 * N : 	# of exsiting threads 
 */


if (mem% > mem_threashold || cpu% > cpu_threshold) {
	/* not able to handle more threads */
	new_thread_forbidden = 1;
	LIMIT = non_increasable;
	curtail_scale -> decreasing;
}

/* lets say util% is meaningless as current server acquired multiple devices*/
if (util% -> 100% || io_load = qutim/svctim -> +infinite) {
	/* need more therads */
	curtail_scale -> incresaing;
	extend_scale  -> incresaing;
}

if (r > r_threshold) {
	/* cpu is overload */
	LIMIT = non_increasable;
	curtail_scale -> incresaing;
}

if (b > b_threashold) {
	/* io stay in heavy state; */
	LIMIT = increasable;
	curtail_scale -> incresaing;
	extend_scale  -> incresaing;
}

/* let n = total threads we have , r is threads # waiting for cpu, b is the threads # 
 * in uninterrrupted sleep; 
 *
 */

Qestion:
how to distribute the new threads to thread_pool based on performance, incresaing
size of IO-bound thread_pool while stablizing size of CPU_bound thread_pool

how to utilize Amdahl law: determine the serial part and parallel part, 
calcualte the speedup, judge whether the performance get inproved in significant degree



