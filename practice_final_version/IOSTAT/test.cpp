#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <sstream>
#include <iostream>

/*
static void itoa(int number, char str[], char target[]) {
	int n, i, length;
	char reverseNum[25];
	if (number <= 0) {
		p1perror(1, "pid is not vaild\n");
		return;
	}
	i = 0;
	n = number;
	length = p1strlen(str);
	str[length] = '/';
	length += 1;
	while (n != 0) {
		reverseNum[i] = digits[n % BASE];
		n /= 10;
		i ++;
	}
	i --;
		
	while (i >= 0) {
		str[length] = reverseNum[i];
		length += 1;
		i--;
	}
	str[length] = '\0';
	
	i = 0;
	n = p1strlen(target);
	for (i = 0; i < n; i++)
		str[i + length] = target[i];
	length = i + length;
	str[length] = '\0';
}
*/

void adjustThreadLimits() {
	std::cout << "nothing done" << std::endl;
}

int accessProc() {
	/* generate the path */
	pid_t pro_id = getpid();
	std::string file_path = "/proc/";
	std::string target_file = "/io";
	std::string the_id = std::to_string(pro_id);

	file_path += (the_id + target_file);
	/* try open file */
	std::cout << file_path.c_str() << std::endl;
	int filedesc = open(file_path.c_str(), O_RDONLY);
	if (filedesc < 0) {
		std::cout << "open operation failed" << std::endl;
		return -1;
	}

	/* access the file */

	adjustThreadLimits();

	/* do close operation */
	if (close(filedesc) < 0) {
		std::cout << "close operation failed" << std::endl;
		return -1;
	}

	return 0;
}


int main() {
	accessProc();
	return 0;
}
