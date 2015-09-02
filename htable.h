#ifndef HTABLE_H
#define HTABLE_H

#include "hash.h"
#include <stdio.h>
#include <upc_relaxed.h>
#include <upc_nb.h>
#include <bupc_extensions.h>
#include "atomics.h"

// #define HTABLE_USE_STATS 1

// Denotes the size of a cache line, which is 64 bytes.
// So 8 buckets can be stored on a single cache line.
#define HTABLE_CACHE_LINE_SIZE 64

// Chunk-related information
#ifndef HTABLE_CHUNK_SIZE
#define HTABLE_CHUNK_SIZE 32
#endif
#ifndef HTABLE_MAX_NR_OF_CHUNKS
#define HTABLE_MAX_NR_OF_CHUNKS 64
#endif
#ifndef HTABLE_BLOCK_SIZE
#define HTABLE_BLOCK_SIZE (uint64_t)134217728 // which is 2^27
#endif

// adds counters for statistical purposes..
#ifdef HTABLE_USE_STATS
extern uint64_t _actual_rtrips;
extern uint64_t _required_rtrips;

#define ADD_TO_ACTUAL_RTRIPS(n) { _actual_rtrips += n; }
#define ADD_TO_REQUIRED_RTRIPS(n) { _required_rtrips += n; }
#else
#define ADD_TO_ACTUAL_RTRIPS(n)
#define ADD_TO_REQUIRED_RTRIPS(n)
#endif

// Bucket masks for buckets, with:
// - 1 bit to denote bucket occupation
// - 63 bits to denote the content of a bucket
#define HTABLE_MASK_DATA     ((uint64_t)0x7fffffffffffffff)
#define HTABLE_MASK_OCCUPIED ((uint64_t)0x8000000000000000)

// Results given by 'find-or-put(d)':
// - FULL means that d does not occur in the hash table and no empty bucket has
//   been found in the HTABLE_CHUNK_SIZE * HTABLE_MAX_NR_OF_CHUNKS consecutive
//   buckets starting from hash(d).
// - FOUND means that d was already in the hash table.
// - INSERTED means that d was not already in the table, but now is.
#define HTABLE_FULL 0
#define HTABLE_FOUND 1
#define HTABLE_INSERTED 2

// Macros to translate addresses to parts of indices in the hash table
#define HTABLE_BLOCK(addr)   ((addr) & (uint64_t)0x0000000007ffffff)
#define HTABLE_THREAD(addr) (((addr) & (uint64_t)0xfffffffff8000000) >> 27)
#define HTABLE_ADDR(addr)    ((addr) % (HTABLE_BLOCK_SIZE * THREADS))

typedef uint64_t bucket_t;

// The hash table context, containing:
// - an array 'chunks' in private memory to receive chunks
// - an array 'handle' for the handles used to receive chunks
typedef struct htable_ctx {
	bucket_t *chunks;
	upc_handle_t *handle;
	shared [HTABLE_BLOCK_SIZE] bucket_t *table;
} htable_ctx_t;

void htable_init(htable_ctx_t *ctx);
void htable_free(htable_ctx_t *ctx);
char htable_find_or_put(htable_ctx_t *ctx, uint64_t data);
void htable_print_info(htable_ctx_t *ctx);
size_t htable_owner(htable_ctx_t *ctx, uint64_t data);

void htable_test_ownership(htable_ctx_t *ctx);
void htable_test_query_single(htable_ctx_t *ctx);
void htable_test_query_splitting(htable_ctx_t *ctx);

#endif

