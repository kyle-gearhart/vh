/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/shard/Shard.h"
#include "io/utils/kvlist.h"

/*
 * Groups a list of Shard into a KeyValueList by each unique BackEnd at a
 * given ShardAccess layer.
 */
KeyValueList
vh_shard_group_be(Shard *shards, uint32_t shard_sz, uint16_t layer)
{
	KeyValueList kvl;
	Shard shard;
	uint32_t i;
	BackEnd be;
	ShardAccess accl;
	SList group;

	if (layer > 1)
		return 0;

	if (shards && shard_sz)
	{
		kvl = vh_kvlist_create();

		for (i = 0; i < shard_sz; i++)
		{
			shard = shards[i];
			accl = shard->access[layer];

			if (accl)
			{
				be = accl->be;
				vh_kvlist_value(kvl, &be, group);
				vh_SListPush(group, shard);
			}
		}

		return kvl;
	}

	return 0;
}

ShardAccess
vh_sharda_create(BackEndCredential becred, BackEnd be)
{
	ShardAccess sa = vhmalloc(sizeof(struct ShardAccessData));

	sa->be = be;
	sa->becred = becred;
	sa->schema = 0;
	sa->database = 0;

	return sa;
}

Shard
vh_shard_create(ShardId id, ShardAccess write, ShardAccess read)
{
	Shard shard = vhmalloc(sizeof(struct ShardData));

	shard->id = id;
	shard->access[VH_SHARD_LAYER_WRITE] = write;
	shard->access[VH_SHARD_LAYER_READ] = read;

	return shard;
}

