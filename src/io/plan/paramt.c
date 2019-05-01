/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/plan/paramt.h"
#include "io/shard/Shard.h"
#include "io/utils/kvmap.h"
#include "io/utils/SList.h"

#define VH_PLAN_PARAMT_SHARD_BIT	0x8000000000000000
#define VH_PLAN_PARAMT_SHARD_MASK	0x7fffffffffffffff

/*
 * We pack the pointer to the SList in ParamTransferMap.  
 * Certain parameters may be Shard independent, meaning they qualify for all 
 * shards.  It's a waste of cycles and memory to spin up ParameterTransferData 
 * structures for each shard accessed when the data is going to be identical.  
 * To indicate a given parameter is shard indenpendent, we set the high bit 
 * on the pointer to the SList.
 */

struct ParamTransferMapData
{
	KeyValueMap paramt_sharda; 	/* key: ShardAccess; value: SList ---
													 value: ParameterTransfer */

	bool it;
	ShardAccess it_sa;
	ParamTransfer *it_pt_head;
	uint32_t it_idx;
	uint32_t it_sz;	
};


ParamTransferMap
vh_plan_paramt_createmap(void)
{
	ParamTransferMap ptm;

	ptm = vhmalloc(sizeof(struct ParamTransferMapData));
	ptm->paramt_sharda = vh_kvmap_create();

	ptm->it = false;
	ptm->it_sa = 0;
	ptm->it_pt_head = 0;
	ptm->it_idx = 0;
	ptm->it_sz = 0;

	return ptm;
}

void
vh_plan_paramt_destroymap(ParamTransferMap ptm)
{
	vh_kvmap_destroy(ptm->paramt_sharda);

	vhfree(ptm);
}

void
vh_plan_paramt_it_init(ParamTransferMap ptm, ShardAccess sa)
{
	SList *values;

	if (ptm)
	{
		vh_kvmap_value(ptm->paramt_sharda, &sa, values);
		ptm->it_sz = vh_SListIterator(*values, ptm->it_pt_head);
		ptm->it_idx = 0;
		ptm->it = true;
	}
}

ParamTransfer
vh_plan_paramt_it_next(ParamTransferMap ptm, uint32_t *idx)
{
	ParamTransfer pt = 0;

	if (ptm && ptm->it)
	{
		if (++ptm->it_idx < ptm->it_sz)
		{
			pt = ptm->it_pt_head[ptm->it_idx];
		}

		if (idx)
			*idx = ptm->it_idx;
	}

	return pt;
}

ParamTransfer
vh_plan_paramt_it_first(ParamTransferMap ptm, uint32_t *idx)
{
	ParamTransfer pt = 0;

	if (ptm && ptm->it)
	{
		pt = ptm->it_pt_head[0];

		if (idx)
			*idx = 0;
	}

	return pt;
}

ParamTransfer
vh_plan_paramt_it_last(ParamTransferMap ptm, uint32_t *idx)
{
	ParamTransfer pt = 0;

	if (ptm && ptm->it)
	{
		pt = ptm->it_pt_head[ptm->it_sz - 1];

		if (idx)
			*idx = ptm->it_sz - 1;
	}

	return pt;
}

ParamTransfer
vh_plan_paramt_add(ParamTransferMap ptm, Node node,
				   ShardAccess sa)
{
	return 0;
}




