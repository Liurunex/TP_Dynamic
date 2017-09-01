#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "sys/types.h"
#include "sys/sysinfo.h"
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm> 

const std::string iostat_ = "iostat -x";
const std::string vmstat_ = "vmstat";
char buffer_[512];   

struct CpuInfo {
    unsigned long long cpu_totalUser;
    unsigned long long cpu_totalUserLow;
    unsigned long long cpu_totalSys;
    unsigned long long cpu_totalIdle;
};

struct ResultInfo {
    unsigned int num_cpu;
    
    unsigned int r_process;
    unsigned int b_process;

    double cpu_percent;
    double mem_percent;
    
    double util_percent;
    double io_load; // await = qutim + svctim, here target = qutim/svctim
};


static void
init_proc_parameter (CpuInfo *pre_cpu) {
    FILE* cpufile = fopen("/proc/stat", "r");
    
    fscanf(cpufile, "cpu %llu %llu %llu %llu", &pre_cpu->cpu_totalUser, &pre_cpu->cpu_totalUserLow,
        &pre_cpu->cpu_totalSys, &pre_cpu->cpu_totalIdle);
    
    fclose(cpufile);
    return;
}

void 
get_cpu_number(ResultInfo *res_integration) {
    unsigned int num_cpu = 0;
    FILE *num_cpufile = fopen("/proc/cpuinfo", "r");
    while (fgets(buffer_, sizeof(buffer_), num_cpufile)) 
        if (!strncmp(buffer_, "processor\t:", 11))
            num_cpu ++;
    fclose(num_cpufile);
    res_integration->num_cpu = num_cpu;
    return;
}

std::string 
popen_usage(std::string command) {
    int skip_line = 0, cur_line = 0;
    if (command == vmstat_) skip_line = 2;
    if (command == iostat_) skip_line = 6;

    std::string res;
    FILE *shellin = popen(command.c_str(), "r");
    
    memset(buffer_, 0, sizeof(buffer_));

    if (!shellin) return res;
    while (fgets(buffer_, sizeof(buffer_), shellin)) {
        if (cur_line < skip_line) {
            cur_line ++;
            continue;
        }
        res += std::string(buffer_);
    }
    
    pclose(shellin);
    return res;
}

void
get_current_value (CpuInfo *cur_cpu, CpuInfo *pre_cpu, ResultInfo *res_integration) {
    
    /* IO util retrive from iostat */
    double util_percent = 0;
    double io_load      = 0;
    double await        = 0;
    double svctim       = 0;
	int io_counter      = 0;
    std::string io_string = popen_usage(iostat_);
    std::stringstream io_iss(io_string);
    std::string io_line;
	while (getline(io_iss, io_line)) {
		
		/*std::cout << "main: " << io_line << std::endl;
		std::cout << "value of the new line character is \n";
		for (char &i : io_line) {
			std::cout << (int)i << "-";
		}*/
	
		if(!io_line.length()) break;
		io_counter = 0;
		std::size_t prev = io_line.size()-1, pos = io_line.size()-1;
		while ((pos = io_line.find_last_of(" ", prev)) != 0) {
			if (io_counter >= 5)
				break;
			if (pos < prev) {
				io_counter ++;
				switch(io_counter) {
                    case 1:
                        util_percent = std::max(util_percent, std::stod(io_line.substr(pos, prev-pos+1)));
						break;
                    case 2:
                        svctim       = std::stod(io_line.substr(pos, prev-pos+1));
						break;
                    case 5:
                        await        = std::stod(io_line.substr(pos, prev-pos+1));
						break;
				}
			}
           prev = pos - 1;
        }
		//std::cout << "\nawait: " << await << " svctim: " << svctim << " util: " << util_percent << std::endl;
        io_load = std::max(io_load, (await - svctim)/svctim);
    }
    res_integration->util_percent = util_percent;
    res_integration->io_load      = io_load;

    /* r/w retrive from vmstats */
    std::vector<int> tem_res;
    int vm_counter = 0;
    std::string vm_string = popen_usage(vmstat_);
    std::stringstream vm_iss(vm_string);
    std::string vm_line;
    while (getline(vm_iss, vm_line)) {
        std::size_t prev = 0, pos = 0;
        while ((pos = vm_line.find_first_of(" ", prev)) != std::string::npos) {
            if (vm_counter == 2) break;
            if (pos > prev) {
                vm_counter ++;
                tem_res.push_back(std::stoi(vm_line.substr(prev, pos-prev)));
            }
            prev = pos + 1;
        }
        break;
    }
    res_integration->r_process = tem_res[0];
    res_integration->b_process = tem_res[1];

    /* RAM percent */
	double mem_percent;
	struct sysinfo memInfo;
    sysinfo(&memInfo);
    unsigned long long totalRAM  =  memInfo.totalram;
    totalRAM                    *=  memInfo.mem_unit;

    unsigned long long idleRAM   =  memInfo.freeram;
    idleRAM                     *=  memInfo.mem_unit;
    double ram_percent           =  100 *(totalRAM - idleRAM);
    ram_percent                 /=  totalRAM;
    res_integration->mem_percent =  ram_percent;

    /* CPU percent */
	unsigned long long cpu_total = 0;
	double cpu_percent;
    FILE* cpufile = fopen("/proc/stat", "r");
    
    fscanf(cpufile, "cpu %llu %llu %llu %llu", &cur_cpu->cpu_totalUser, &cur_cpu->cpu_totalUserLow,
        &cur_cpu->cpu_totalSys, &cur_cpu->cpu_totalIdle);
    
    fclose(cpufile);

    /* just in case overflow happened */
    if (cur_cpu->cpu_totalUser < pre_cpu->cpu_totalUser || cur_cpu->cpu_totalUserLow < pre_cpu->cpu_totalUserLow ||
        cur_cpu->cpu_totalSys  < pre_cpu->cpu_totalSys  || cur_cpu->cpu_totalIdle < pre_cpu->cpu_totalIdle)  
        cpu_percent = -1.0;
    else {
        cpu_total    =  (cur_cpu->cpu_totalUser    - pre_cpu->cpu_totalUser)    + 
                        (cur_cpu->cpu_totalUserLow - pre_cpu->cpu_totalUserLow) +
                        (cur_cpu->cpu_totalSys     - pre_cpu->cpu_totalSys);
        cpu_percent  =  cpu_total * 100;
        cpu_total   +=  (cur_cpu->cpu_totalIdle - pre_cpu->cpu_totalIdle);
        cpu_percent /=  (cpu_total);
    }

    pre_cpu->cpu_totalUser    = cur_cpu->cpu_totalUser;
    pre_cpu->cpu_totalUserLow = cur_cpu->cpu_totalUserLow;
    pre_cpu->cpu_totalSys     = cur_cpu->cpu_totalSys;
    pre_cpu->cpu_totalIdle    = cur_cpu->cpu_totalIdle;

    res_integration->cpu_percent  = cpu_percent;
    
    return;
}

int main() {
    ResultInfo res_integration;
    CpuInfo cur_cpu, pre_cpu;

    init_proc_parameter(&pre_cpu);
    /* get cpu number */
    get_cpu_number(&res_integration);

    std::cout << "cpu_#\t" << "r\t" << "b\t" << "cpu_%\t" << "mem_%\t" << "util_%\t" << "io_load" << std::endl; 
    
    while(1) {
        sleep(1);
        get_current_value(&cur_cpu, &pre_cpu, &res_integration);
        std::cout << res_integration.num_cpu << "\t" << res_integration.r_process << "\t" << res_integration.b_process << "\t" 
        << res_integration.cpu_percent << "\t" << res_integration.mem_percent << "\t" << res_integration.util_percent << "\t" << res_integration.io_load << std::endl;
    }
}
