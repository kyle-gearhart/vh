/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_buffer_htpcache_H
#define vh_buffer_htpcache_H

/*
 * HeapTuple Cache
 *
 * HeapTuple caches are a way to manage multiple HeapTuplePtr references
 * to valid in memory pointers to HeapTuple.  The cache manages the pinning
 * and unpinning of the HeapTuple with the buffer.
 *
 * Caches are intended to be used when a sweep release of the in memory 
 * pointers is appropriate.
 */

typedef struct HtpCacheData *HtpCache;

/*
 * Create, Finalize, Free, Destroy
 */
HtpCache vh_htpc_create(void);
void vh_htpc_finalize(HtpCache htpc);
void vh_htpc_free(HtpCache htpc);
void vh_htpc_destroy(HtpCache htpc);

/*
 * Operations
 */
int32_t vh_htpc_get(HtpCache htpc, HeapTuplePtr htp,
					HeapTuple *ht);
int32_t vh_htpc_rel(HtpCache htpc, HeapTuplePtr htp);
int32_t vh_htpc_relht(HtpCache htpc, HeapTuple ht);
int32_t vh_htpc_relall(HtpCache htpc);
bool vh_htpc_ispinned(HtpCache htpc, HeapTuplePtr htp);

#endif


