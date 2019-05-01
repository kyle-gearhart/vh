/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_shard_shard_H
#define vh_datacatalog_shard_shard_H

#include "io/catalog/BackEnd.h"

/*
 *
 * Logical shard: 	data set separated in the application's context but not
 * 					necessarily segregated by schema, database, and/or host.
 * Physical shard:	data set separated by schema, database and/or host.
 *
 * We have two levels of abstraction for shards.  The first is known as the Shard
 * itself.  This has an |id| which is a 16 byte hash of the identifier of the
 * Shard.  Internally we don't use it for much, but could in the future.
 *
 * The second is member is the |access| array.  This allows users to configure two 
 * distinct modes of access: write and read.  The first member of the |access|
 * array for Shard is always the write master.  If the a user wishes to define a
 * reader, these second member of hte |access| array should be set to the reader.
 *
 * ShardAccess holds a pointer to a BackEndCredential structure along with a
 * pointer to the BackEnd and the database containing the logical shard.
 *
 * Thus we may have multiple logical shards sharing the same physical shard by
 * using a single BackEndCredential to form multiple ShardAccess and Shards.
 *
 * NOTE: the VH.IO planner and executor are intelligent enough to recognize
 * when an underlying backend allows for a single connection to access all
 * databases in the cluster (i.e. SQL Server). *
 */  

typedef struct ShardId { unsigned char id[16]; } ShardId;

typedef struct ShardAccessData
{
	BackEndCredential becred;
	BackEnd be;
	String schema;
	String database;
} ShardAccessData, *ShardAccess;

ShardAccess vh_sharda_create(BackEndCredential becred, BackEnd be);
void vh_sharda_destroy(ShardAccess sa);

typedef struct ShardData
{
	ShardId id;
	ShardAccess access[2];
} ShardData, *Shard;

Shard vh_shard_create(ShardId id, ShardAccess write, ShardAccess read);

#define VH_SHARD_LAYER_WRITE		0
#define VH_SHARD_LAYER_READ 		1

#define vh_shard_write_be(s)		( s->access[VH_SHARD_LAYER_WRITE] ?			\
									  s->access[VH_SHARD_LAYER_WRITE]->be :		\
									  0 )						
#define vh_shard_read_be(s)			( s->access[VH_SHARD_LAYER_READ] ?			\
									  s->access[VH_SHARD_LAYER_READ]->be :		\
									  0 )

#define vh_shard_write_becred(s)	( s->access[VH_SHARD_LAYER_WRITE] ?			\
									  s->access[VH_SHARD_LAYER_WRITE]->becred :	\
									  0 )
#define vh_shard_read_becred(s)		( s->access[VH_SHARD_LAYER_READ] ?			\
									  s->access[VH_SHARD_LAYER_READ]->becred :	\
									  0 )
/*
 * vh_shard_group_be
 *
 * Groups a list of Shard into a KeyValueList by each unique BackEnd for
 * a given ShardAccess layer (i.e. write or read).
 */
KeyValueList vh_shard_group_be(Shard *shards, uint32_t shards_sz, uint16_t layer);

#endif

