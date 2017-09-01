#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <errno.h>
#include <sys/param.h>
#include <linux/major.h>

struct filepointer {
	FILE *iofp;
	FILE *cpufp;
};

struct cpu_info {
	unsigned long long user;
	unsigned long long system;
	unsigned long long idle;
	unsigned long long iowait;
} new_cpu, old_cpu;

void get_number_of_cpus() {
	FILE *ncpufp = fopen("/proc/cpuinfo", "r");

	handle_error("Can't open /proc/cpuinfo", !ncpufp);
	while (fgets(buffer, sizeof(buffer), ncpufp)) {
		if (!strncmp(buffer, "processor\t:", 11))
			ncpu++;
	}
	fclose(ncpufp);
	handle_error("Error parsing /proc/cpuinfo", !ncpu);
}