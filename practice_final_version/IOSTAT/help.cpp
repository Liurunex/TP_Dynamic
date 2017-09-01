/*
revise and cite from https://stackoverflow.com/questions/16011677/calculating-cpu-usage-using-proc-files
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <string>
#include <iostream>

struct pstat {
    long unsigned int utime_ticks;
    long int cutime_ticks;
    long unsigned int stime_ticks;
    long int cstime_ticks;
    long unsigned int vsize; // virtual memory size in bytes
    long unsigned int rss; //Resident  Set  Size in bytes
};

void calculate_precentage() {
	std::cout << "nothing done" << std::endl;
}

int accessProc(struct pstat* result) {
	/* generate the path */
	pid_t pro_id = getpid();
	std::string file_path = "/proc/";
	std::string target_file = "/stat";
	std::string the_id = std::to_string(pro_id);

	file_path += (the_id + target_file);
	
	/* try open file */
	std::cout << file_path.c_str() << std::endl;
	FILE *fileServer  = fopen("/proc/stat", "r");
	FILE *fileProcess = fopen(file_path.c_str(), "r");

	if (!fileProcess || !fileServer) {
		std::cout << "open operation failed" << std::endl;
		return -1;
	}

	/* clear data in struct before reading */
	bzero(result, sizeof(struct pstat));
    long int rss;
	/* access the file */
	if (fscanf(fileProcess, "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu"
                "%lu %ld %ld %*d %*d %*d %*d %*u %lu %ld",
           		&result->utime_ticks, &result->stime_ticks,
                &result->cutime_ticks, &result->cstime_ticks, &result->vsize,
                &rss) != EOF) {
		
		calculate_precentage();
	} else std::cout << "not inside" << std::endl;

	/* do close operation */
	if (fclose(fileProcess) == EOF || fclose(fileServer) == EOF) {
		std::cout << "close operation failed" << std::endl;
		return -1;
	}
	result->rss = rss * getpagesize();

	return 0;
}


int main() {
	pstat prev, curr;

	if(accessProc(&prev) < 0)
		std::cout << "error" << std::endl;
	return 0;
}
