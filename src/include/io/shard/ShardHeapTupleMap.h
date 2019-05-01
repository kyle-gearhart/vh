/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_shard_ShardHeapTupleMap_H
#define vh_datacatalog_shard_ShardHeapTupleMap_H

/*
 * The ShardHeapTupleMap is used to group HeapTuples by their
 * beacon key (|bybeackey|) and then by shard (|byshard|).
 *
 * It's the responsibility of the generic beacon prepation 
 * functions to atleast complete the mapping for |bybeackey|.  
 * The beacon specific functions shall complete the shard 
 * assignment/determination.  
 *
 * The map leverages the HeapTuple structure itself, which 
 * contains the beacon key on the data page and room for a 
 * shard pointer.
 */

#include "vh.h"

struct pavl_table;
typedef struct ShardData *Shard;

typedef struct ShardHeapTupleMapData
{
	struct pavl_table *bybeackey;
	struct pavl_table *byshard;
	SList shards;
	SList beackey_changed;
	MemoryContext mctx;
} ShardHeapTupleMapData, *ShardHeapTupleMap;

struct ShardHeapTupleMapFuncs
{
	ShardHeapTupleMap (*CreateMap)(MemoryContext, int32_t);
	void (*DestroyMap)(ShardHeapTupleMap);
	void (*AddHeapTuplePtr)(ShardHeapTupleMap,
						 HeapTuplePtr);
	SList (*TuplesInShard)(ShardHeapTupleMap,
						   Shard);
};

extern struct ShardHeapTupleMapFuncs const vh_shtm;

#endif

