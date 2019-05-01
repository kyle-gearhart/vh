/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_shard_shtm_H
#define vh_datacatalog_shard_shtm_H

/* Shard Heap Tuple Map (SHTM)
 *
 */

typedef struct ShardHeapTupleMapData *ShardHeapTupleMap;


ShardHeapTupleMap vh_shtm_create(void);
void vh_shtm_destroy(ShardHeapTupleMap shtm);

SList vh_shtm_get_shards(ShardHeapTupleMap shtm);
SList vh_shtm_get_htps(ShardHeapTupleMap shtm, Shard shard, bool *exists);
bool vh_shtm_add_htp(ShardHeapTupleMap shtm, Shard shard, HeapTuplePtr htp);
bool vh_shtm_rem_htp(ShardHeapTupleMap shtm, HeapTuplePtr htp);

#endif

