#include <iostream>
#include <cstdlib>
#include <pthread.h>

using namespace std;

#define NUM_THREADS 5

void *PrintHello(void *threadid) {
   long tid;
   tid = (long)threadid;
   cout << "Hello World! Thread ID, " << tid << endl;
   return 0;
}

int main () {
	pthread_t threads[NUM_THREADS];
	int rc;
	int i;

	for( i=0; i < NUM_THREADS; i++ ){
		cout << "main() : creating thread, " << i << endl;
		rc = pthread_create(&threads[i], NULL, PrintHello, (void *)i);

		if (rc){
			cout << "Error:unable to create thread," << rc << endl;
			exit(-1);
		}
	}
	sleep(5);
	return 0;
}
