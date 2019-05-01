/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/shard/shtm.h"
#include "io/utils/kvlist.h"
#include "io/utils/kset.h"
#include "io/utils/SList.h"


struct ShardHeapTupleMapData
{
	KeyValueList shards;		/* key: Shard; value: SList of HeapTuplePtr */
	KeySet htps;				/* key: HeapTuplePtr */
	
	MemoryContext mctx;
	bool removed;
};

ShardHeapTupleMap
vh_shtm_create(void)
{
	MemoryContext mctx_shtm, mctx_old;
	ShardHeapTupleMap shtm;

	mctx_old = vh_mctx_current();
	mctx_shtm = vh_MemoryPoolCreate(mctx_old, 8192 * 4,
									"SHTM context");
	vh_mctx_switch(mctx_shtm);

	shtm = vhmalloc(sizeof(struct ShardHeapTupleMapData));
	shtm->shards = vh_kvlist_create();
	shtm->htps = vh_htp_kset_create();
	shtm->mctx = mctx_shtm;
	shtm->removed = false;

	vh_mctx_switch(mctx_old);

	return shtm;
}

void
vh_shtm_destroy(ShardHeapTupleMap shtm)
{
	if (shtm && shtm->mctx)
	{
		vh_mctx_destroy(shtm->mctx);
	}
}

SList
vh_shtm_get_shards(ShardHeapTupleMap shtm)
{
	return 0;
}

/*
 * If the removed flag is set, we should compare each value in the SList
 * for the given shard with |shtm->htps|.  If it exists in |htps| we can
 * push it into the returned SList.  If the removed flag isn't set then
 * just copy the SList for the Shard key.  We always want to return an
 * SList the user may change without impacting the SHTM.
 */
SList
vh_shtm_get_htps(ShardHeapTupleMap shtm, Shard shard,
				 bool *exists)
{
	return 0;
}

bool
vh_shtm_add_htp(ShardHeapTupleMap shtm, Shard shard,
				HeapTuplePtr htp)
{
	SList values;

	if (!vh_kset_key(shtm->htps, &htp))
	{
		vh_kvlist_value(shtm->shards, shard, values);
		vh_htp_SListPush(values, htp);

		return true;
	}

	return false;
}

bool
vh_shtm_rem_htp(ShardHeapTupleMap shtm, HeapTuplePtr htp)
{
	vh_kset_remove(shtm->htps, &htp);

	if (!shtm->removed)
		shtm->removed = true;

	return true;
}

