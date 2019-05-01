/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_be_griddb_beacon_H
#define vh_io_be_griddb_beacon_H

#include "io/shard/BeaconImpl.h"

typedef struct GRDBbeaconData *GRDBbeacon;
typedef struct GRDBbeaconOptData *GRDBbeaconOpt;
typedef struct GRDBpartitionData *GRDBpartition;

struct GRDBbeaconData
{
	struct BeaconData beac;

	GRDBpartition prev_master;
	GRDBpartition master;
	GRDBpartition *partitions;

	int32_t partition_count;
	int32_t container_hash_mode;
};

#endif

