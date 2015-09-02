#include <upc.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <sys/time.h>
#include "../../htable/htable.h"

double wctime() {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (tv.tv_sec + 1E-6 * tv.tv_usec);
}

void htable_throughput_benchmark(htable_ctx_t *ctx, uint64_t limit, uint64_t range) {
	uint64_t i = 0;
	uint64_t total_time_us = 0;
	uint64_t total_finds = 0, total_inserts = 0, total_errors = 0;
	
	for (; i < limit; i++) {
		// calculate some random, thread-dependent value in the range [0, ..., range - 1]
		uint64_t val = hash(i + hash(rand() + MYTHREAD + hash(total_time_us + 23641))) % range;

		// perform the operation
		bupc_tick_t start = bupc_ticks_now();
		char res = htable_find_or_put(ctx, val);
		bupc_tick_t stop = bupc_ticks_now();

		// count the number of finds, inserts, and errors
		if (res == HTABLE_FOUND) total_finds++;
		else if (res == HTABLE_INSERTED) total_inserts++;
		else total_errors++;

		// record the number of us passed..
		total_time_us += bupc_ticks_to_us(stop - start);
	}

	double total_time_s = (double)total_time_us / (double)(1000 * 1000);
	double ops_per_s = (double)limit / total_time_s;

	uint64_t total_ops = total_finds + total_inserts + total_errors;

	double finds = (double)total_finds / total_ops;
	double inserts = (double)total_inserts / total_ops;
	double errors = (double)total_errors / total_ops;

	printf("%i/%i - Performed a throughput benchmark:\n", MYTHREAD, THREADS);
	printf("%i/%i - Performed %lu find-or-put operations with random values from [0,...,%lu]\n", 
		MYTHREAD, THREADS, limit, range - 1);
	printf("%i/%i - Finds: %lu (%G), Inserts: %lu (%G), Errors: %lu (%G)\n", 
		MYTHREAD, THREADS, total_finds, finds, total_inserts, inserts, total_errors, errors);
	printf("%i/%i - Execution time: %G seconds\n", MYTHREAD, THREADS, total_time_s);
	printf("%i/%i - Throughput: %G ops/sec\n", MYTHREAD, THREADS, ops_per_s);
}

int main(int argc, char *argv[]) {
	int option = atoi(argv[optind]);

	htable_ctx_t ctx;
	htable_init(&ctx);

	upc_barrier;

	if (MYTHREAD == 0) {
		htable_print_info(&ctx);
	}

	switch (option) {
		case 1:
			// note: the following call should generate a read/write ratio of about 50%/50%
			htable_throughput_benchmark(&ctx, 2000000, 0.65 * THREADS * 2000000);
			break;
		case 2:
			// note: the following call should generate a read/write ratio of about 80%/20%
			htable_throughput_benchmark(&ctx, 2000000, 0.2 * THREADS * 2000000);
			break;
		case 3:
			// note: the following call should generate a read/write ratio of about 20%/80%
			htable_throughput_benchmark(&ctx, 2000000, 2.3 * THREADS * 2000000);
			break;
		default:
			printf("You are trying to run the benchmark in an unknown mode!\n");
	}

	upc_barrier;

	htable_free(&ctx);

	return 0;
}
