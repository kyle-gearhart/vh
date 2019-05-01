/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_shard_BeaconSimple_H
#define vh_datacatalog_shard_BeaconSimple_H

/*
 * Simple Beacon
 *
 */

#define VH_BEACM_SIMPLE		0x01

Beacon vh_beaci_simple_create(TableDef td, const char *name, 
							  ShardAccess read, ShardAccess write);

#endif

