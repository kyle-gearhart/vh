/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_shard_Beacon_H
#define vh_io_shard_Beacon_H

typedef struct BeaconData *Beacon;
typedef struct BeaconFuncTableData *BeaconFuncTable;

struct BeaconFuncTableData
{
	/*
	 * Lookup Shards for HeapTuple, assumes lock acquired
	 *
	 * Since the lock has been acquired, it's up to the caller if they
	 * want to assign the shard to the HeapTuple.
	 */
	Shard (*ht_shard)(void *beac, HeapTuple ht);
	HashTable (*ht_shard_list)(void *beac, SList ht, bool key_htp);


	/*
	 * Lookup Shards for HeapTuplePtr, must take lock
	 */
	Shard (*htp_shard)(void *beac, HeapTuplePtr, bool assign);
	HashTable (*htp_shard_list)(void *beac, SList htp, bool assign, bool key_htp);


	/*
	 * Lookup Shards for TableDef
	 */
	Shard (*td_shard)(void *beac, TableDef td);
	HashTable (*td_shard_list)(void *beac, SList htp, bool key_td);


	/*
	 * Configure from JSON
	 */

	bool (*connect)(void *beac);
	bool (*disconnect)(void *beac);
	void (*finalize)(void *beac);

	int32_t (*load_schema)(void *beac, TableCatalog tc);	
};

void *vh_beac_create(size_t sz, const struct BeaconFuncTableData*);
void vh_beac_disconnect(Beacon beacon);
void vh_beac_finalize(Beacon beacon);

Shard vh_beac_ft_htp_shard(Beacon beac, HeapTuplePtr htp, bool assign);
HashTable vh_beac_ft_htp_shard_list(Beacon beac, HeapTuplePtr htp,
									bool assign, bool key_htp);

Shard vh_beac_ft_td_shard(Beacon beac, TableDef td);
HashTable vh_beac_ft_td_shard_list(Beacon beac, TableDef td,
								   bool key_td);

#endif

