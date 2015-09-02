#include "htable.h"

void htable_init(htable_ctx_t *ctx) {
	int i;

	// allocate cache-line aligned memory for the chunks array
	int r = posix_memalign((void**)&ctx->chunks, HTABLE_CACHE_LINE_SIZE, 
		HTABLE_MAX_NR_OF_CHUNKS * HTABLE_CHUNK_SIZE * sizeof(bucket_t));

	// allocate handle pointers for chunk retrieval
	ctx->handle = (upc_handle_t*)malloc(2 * HTABLE_MAX_NR_OF_CHUNKS * sizeof(upc_handle_t*));

	// allocate the shared table 
	ctx->table = (shared [HTABLE_BLOCK_SIZE] bucket_t*)
		upc_all_alloc(THREADS, HTABLE_BLOCK_SIZE * sizeof(bucket_t));

	// now allocate the actual handles. We need 2 per chunk..
	for (i = 0; i < 2 * HTABLE_MAX_NR_OF_CHUNKS; i++) {
		ctx->handle[i] = NULL;
	}
}

void htable_free(htable_ctx_t *ctx) {
	uint64_t i;

	// make sure that all async operations on ctx->table have been completed..
	for (i = 0; i < 2 * HTABLE_MAX_NR_OF_CHUNKS; i++) {
		if (ctx->handle[i] != NULL) {
			upc_sync(ctx->handle[i]);
		}
	}

	upc_free(ctx->table);

	free(ctx->handle);
	free(ctx->chunks);
}

// This function blocks further execution of the thread until all networking
// operations on chunk 'n' have been completed..
static inline void sync_on_chunk(htable_ctx_t *ctx, uint64_t n) {
	int i, index;

	for (i = 0; i < 2; i++) {
		index = 2 * n + i;

		if (ctx->handle[index] != NULL) {
			upc_sync(ctx->handle[index]);
			ctx->handle[index] = NULL;
		}
	}
}

// Start an asynchronous query to chunk 'n', starting at bucket 'h'...
// Every call to query_chunk(n) needs a matching call to sync_on_chunk(n)
static inline void query_chunk(htable_ctx_t *ctx, uint64_t h, uint64_t n) {
	// make sure that all previous operations on chunk 'n' have been completed..
	// note: previous operations from previous calls to find-or-put
	sync_on_chunk(ctx, n);

	// calculate the indices of the begin- and ending elements of the chunk
	uint64_t index1 = h + (n * HTABLE_CHUNK_SIZE);
	uint64_t index2 = index1 + HTABLE_CHUNK_SIZE - 1;

	// determine the second 'block' for the first and last buckets of the chunk
	uint64_t owner1 = HTABLE_THREAD(index1);
	uint64_t owner2 = HTABLE_THREAD(index2);

	// if the two blocks are different, the query needs to be split-up into two parts...
	if (owner1 != owner2) {
		// calculate the sizes of each part of the chunk
		uint64_t size1 = HTABLE_BLOCK_SIZE - HTABLE_BLOCK(index1); 
		uint64_t size2 = HTABLE_CHUNK_SIZE - size1;

		// perform the actual queries..
		ctx->handle[2 * n] = upc_memget_nb(&ctx->chunks[n * HTABLE_CHUNK_SIZE],
			&ctx->table[HTABLE_ADDR(index1)], sizeof(bucket_t) * size1);

		ctx->handle[2 * n + 1] = upc_memget_nb(&ctx->chunks[n * HTABLE_CHUNK_SIZE + size1],
			&ctx->table[HTABLE_ADDR(index1 + size1)], sizeof(bucket_t) * size2);

		ADD_TO_ACTUAL_RTRIPS(2);
	}

	// otherwise, the query can simply be performed without needing to split
	else {
		ctx->handle[2 * n] = upc_memget_nb(&ctx->chunks[n * HTABLE_CHUNK_SIZE],
			&ctx->table[HTABLE_ADDR(index1)], sizeof(bucket_t) * HTABLE_CHUNK_SIZE);

		ctx->handle[2 * n + 1] = NULL;

		ADD_TO_ACTUAL_RTRIPS(1);
	}
}

char htable_find_or_put(htable_ctx_t *ctx, uint64_t data) {
	data &= HTABLE_MASK_DATA;

	uint64_t h = hash(data);
	uint64_t i, j;

	query_chunk(ctx, h, 0);

	for (i = 0; i < HTABLE_MAX_NR_OF_CHUNKS; i++) {
		if (i + 1 < HTABLE_MAX_NR_OF_CHUNKS) {
			query_chunk(ctx, h, i + 1);
		}

		ADD_TO_REQUIRED_RTRIPS(1);

		sync_on_chunk(ctx, i);

		for (j = 0; j < HTABLE_CHUNK_SIZE; j++) {
			uint64_t index = i * HTABLE_CHUNK_SIZE + j;

			if (!(ctx->chunks[index] & HTABLE_MASK_OCCUPIED)) {

				// try to claim the empty bucket with CAS
				bucket_t result = CAS(&ctx->table[HTABLE_ADDR(h + index)], 
					ctx->chunks[index], data | HTABLE_MASK_OCCUPIED); 

				// check if the CAS operation succeeded..
				if (ctx->chunks[index] == result) {
					return HTABLE_INSERTED;
				}

				// if not, check if some other thread has inserted 'data' in the bucket we wanted to claim..
				else if ((result & HTABLE_MASK_DATA) == data) {
					return HTABLE_FOUND;
				}
			}
			else if ((ctx->chunks[index] & HTABLE_MASK_DATA) == data) {
				return HTABLE_FOUND;
			}
		}
	}

	return HTABLE_FULL;
}

void htable_print_info(htable_ctx_t *ctx) {
	uint64_t nr_of_buckets = HTABLE_BLOCK_SIZE * THREADS;
	uint64_t size_b = sizeof(bucket_t) * nr_of_buckets;
	uint64_t size_mb = size_b / (1024 * 1024);

	printf("%i/%i - htable initialized\n", MYTHREAD, THREADS);
	printf("%i/%i - table size: %lu bytes (%lu MB)\n", MYTHREAD, THREADS, size_b, size_mb);
	printf("%i/%i - total number of buckets: %lu\n", MYTHREAD, THREADS, nr_of_buckets);
	printf("%i/%i - block size: %lu (%lu MB)\n", MYTHREAD, THREADS, HTABLE_BLOCK_SIZE * sizeof(bucket_t), HTABLE_BLOCK_SIZE * sizeof(bucket_t) / (1024 * 1024));
	printf("%i/%i - number of buckets in block: %lu\n", MYTHREAD, THREADS, HTABLE_BLOCK_SIZE);
	printf("%i/%i - number of blocks: %i\n", MYTHREAD, THREADS, THREADS);
}

size_t htable_owner(htable_ctx_t *ctx, uint64_t data) {
	uint64_t h = hash(data & HTABLE_MASK_DATA);
	return upc_threadof(&ctx->table[HTABLE_ADDR(h)]);
}

void htable_test_ownership(htable_ctx_t *ctx) {
	uint64_t limit = THREADS * HTABLE_BLOCK_SIZE;
	uint64_t i, prev, prev_i, curr;

	for (i = 0; i < limit; i++) {
		curr = upc_threadof(&ctx->table[i]);

		if (i == 0) {
			prev = curr;
			prev_i = 0;
		}

		if (prev != curr) {
			printf("table[%lu] .. table[%lu] is owned by thread %lu\n", prev_i, i - 1, prev);

			prev = curr;
			prev_i = i;
		}
	}

	printf("table[%lu] .. table[%lu] is owned by thread %lu\n", prev_i, limit, curr);
}

void htable_test_query_single(htable_ctx_t *ctx) {
	uint64_t i;

	for (i = 0; i < THREADS * HTABLE_BLOCK_SIZE; i++) {
		ctx->table[i] = i;
	}

	uint64_t index = 27;

	query_chunk(ctx, index, 0);
	sync_on_chunk(ctx, 0);

	printf("query for index %lu: ", index);

	for (i = 0; i < HTABLE_CHUNK_SIZE; i++) {
		printf("%lu (%lu) - ", ctx->chunks[i], 
			upc_threadof(&ctx->table[index + i]));
	}

	printf("\n");
}

void htable_test_query_splitting(htable_ctx_t *ctx) {
	uint64_t i;

	for (i = 0; i < THREADS * HTABLE_BLOCK_SIZE; i++) {
		ctx->table[i] = i;
	}

	uint64_t index = HTABLE_BLOCK_SIZE - 7;

	query_chunk(ctx, index, 0);
	sync_on_chunk(ctx, 0);

	printf("query for index %lu: ", index);

	for (i = 0; i < HTABLE_CHUNK_SIZE; i++) {
		printf("%lu (%lu) - ", ctx->chunks[i], 
			upc_threadof(&ctx->table[index + i]));
	}

	printf("\n");
}
