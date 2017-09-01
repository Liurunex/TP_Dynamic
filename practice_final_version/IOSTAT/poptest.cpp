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

const std::string iostat_ = "iostat -x";
const std::string vmstat_ = "vmstat";
char buffer_[512]; 

std::string 
popen_usage(std::string command) {
    int skip_line = 0, cur_line = 0;
    if (command == vmstat_) skip_line = 2;
    if (command == iostat_) skip_line = 5;

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

int main() {
	std::vector<int> tem_res;
	tem_res.clear();

	std::string io_string = popen_usage(iostat_);
	std::cout << io_string << std::endl;
	std::stringstream io_iss(io_string);
	std::string io_line;
	while (getline(io_iss, io_line)) {
		std::size_t prev = io_line.size()-1, pos = 0;
		if ((pos = io_line.find_last_of(" ", prev)) != 0)
			if (pos < prev)
				std::cout << io_line.substr(pos, prev-pos+1) << std::endl;	
	}
	
	return 0;
}
