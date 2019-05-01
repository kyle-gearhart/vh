/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/be/griddb/griddb-beacon.h"
#include "io/be/griddb/griddb-int.h"
#include "io/shard/ConnectionCatalog.h"
#include "io/shard/Shard.h"


/*
 * ============================================================================
 * Beacon Function Table
 * ============================================================================
 */

static Shard grdb_beac_htp_shard(void *beac, HeapTuplePtr htp, bool assign);
static HashTable grdb_beac_htp_shard_list(void *beac, SList htp,
										  bool assign, bool key_htp);
static Shard grdb_beac_td_shard(void *beac, TableDef td);
static HashTable grdb_beac_td_shard_list(void *beac, SList td, bool key_td);

static bool grdb_beac_connect(void *beac);
static bool grdb_beac_disconnect(void *beac);
static void grdb_beac_finalize(void *beac);

static int32_t grdb_beac_load_schema(void *beac, TableCatalog tc);


static const struct BeaconFuncTableData grdb_beac_ft = {
	/*
	 * Shard Lookup Functions
	 */
	.htp_shard = grdb_beac_htp_shard,
	.htp_shard_list = grdb_beac_htp_shard_list,
	.td_shard = grdb_beac_td_shard,
	.td_shard_list = grdb_beac_td_shard_list,

	/*
	 * Startup and Shutdown Functions
	 */
	.connect = grdb_beac_connect,
	.disconnect = grdb_beac_disconnect,
	.finalize = grdb_beac_finalize,

	/*
	 * Schema Functions
	 */
	.load_schema = grdb_beac_load_schema
};


/*
 * ============================================================================
 * Partition Meta Data Structure
 * ============================================================================
 */

struct GRDBpartitionData
{
	int32_t partition_id;

	Shard primary;

	int32_t backup_count;
	Shard *backups;
};

/*
 * ============================================================================
 * Partition Resolving Routines
 * ============================================================================
 */

static int32_t get_partition_address(GRDBbeacon beac,
									 bool *address_found);

/*
 * ============================================================================
 * Partition Calculation Routines
 * ============================================================================
 */

#define SYSTEM_CONTAINER_PARTITION_ID 0

static const char SYSTEM_USER_CONTAINER_NAME[] = "";

static int32_t calc_partition_id(GRDBbeacon b, const char *container_name,
								 int32_t *partition_id);
static int32_t hash_container_name(int32_t partition_count, 
								   const char *container_name);
static char* container_name_lower(const char *container_name,
								  const char *up_to);

/*
 * vh_be_grdb_beac_create
 *
 * Declared in griddb-ext.h
 *
 * Creates a new GridDB beacon for resolving partitions on the fly.  Also
 * subscribes the channel to ensure we keep the number of paritions up to
 * date.
 */

Beacon
vh_be_grdb_beac_create(GRDBbeaconOpt opts)
{
}


/*
 * grdb_beac_htp_shard
 *
 * Finds the appropriate Shard for a given HeapTuplePtr.
 */
static Shard 
grdb_beac_htp_shard(void *beac, HeapTuplePtr htp, bool assign)
{
	GRDBbeacon grb = beac;
	GRDBpartition part_meta;
	Shard sh = 0;
	HeapTuple ht;
	TableDefVer tdv;
	TableDef td;
	int32_t partition_id, ret;

	ht = vh_htp(htp);

	if (ht)
	{
		tdv = (TableDefVer)ht->htd;

		if (tdv)
		{
			td = tdv->td;

			ret = calc_partition_id(grb, 
									vh_str_buffer(td->tname),
									&partition_id);

			if (ret)
			{
				/*
				 * We were unable to calculate the partition_id, since
				 * beacons are within the planner, we'll log quietly and
				 * return no shard.
				 */
			}
			
			part_meta = grb->partitions[partition_id];
			assert(part_meta);
			assert(part_meta->partition_id == partition_id);

			sh = part_meta->primary;
		}
	}

	return sh;
}

/*
 * calc_partition_id
 *
 * We have very tight return messages here.  Anyting other than zero
 * means failure.
 *
 * 	-1	Partitions not loaded
 * 	-2	Illegal container name, begins with /
 * 	-3	Illegal subpartition name
 * 	-4	Subpartition name could not be parsed to a number
 *	-5	Illegal partitioning count
 *	-6	Illegal affinity, cannot be first
 *	-7	Illegal affinity and subpartition combination
 */

static int32_t
calc_partition_id(GRDBbeacon b, const char *container_name,
				  int32_t *partition_id)
{
	char *pos_affinity, *pos_subpartition, *container_name_normalized;
   	const char *pos_partitioning;
	bool partitioning_rule = false;
	int32_t container_name_len = strlen(container_name);
	int32_t sub_container_id = -1, partitioning_count, lpartition_id;
	int32_t pbase, pmod, crcval;
	char *base, *affinity = 0, *i, *j;


	if (b->partition_count < 0)
	{
		return -1;
	}

	if (strcmp(container_name, SYSTEM_USER_CONTAINER_NAME) == 0)
	{
		*partition_id = SYSTEM_CONTAINER_PARTITION_ID;
		return 0;
	}

	pos_affinity = strchr(container_name, '@');
	pos_subpartition = strchr(container_name, '/');

	if (!pos_subpartition)
	{
		if (!pos_affinity)
		{
			//container_name_normalized = container_name_lower(container_name);
			*partition_id = hash_container_name(b->partition_count,
												container_name_normalized);
			vhfree(container_name_normalized);

			return 0;
		}

		pos_subpartition = pos_affinity + container_name_len;
	}
	else if (pos_subpartition == container_name)
	{
		return -2;
	}
	else
	{
		pos_partitioning = strchr(pos_subpartition + 1, '_');

		if (pos_partitioning)
		{
			partitioning_rule = true;
		}
		else
		{
			pos_partitioning = container_name + container_name_len;
		}

		if (pos_subpartition + 1 == pos_partitioning)
		{
			return -3;
		}

		sub_container_id = atoi(pos_subpartition + 1);
		
		if (!sub_container_id)
		{
			return -4;
		}

		if (partitioning_rule)
		{
			partitioning_count = atoi(pos_partitioning + 1);

			if (!partitioning_count)
			{
				return -5;
			}
		}
	}

	if (pos_affinity == container_name)
	{
		return -6;
	}
	else if (!pos_affinity)
	{
		base = container_name_lower(container_name, pos_subpartition);
	}
	else
	{
		if (pos_affinity > pos_subpartition ||
			pos_affinity + 1 == pos_subpartition)
		{
			return -7;
		}

		//affinity = vhmalloc(container_name_len - (pos_subpartition - pos_affinity));
		
		for (i = affinity, j = pos_affinity + 1; j < pos_subpartition; i++, j++)
			*i = *j;

		lpartition_id = atoi(affinity);

		if (lpartition_id)
		{
			if (sub_container_id != -1)
				lpartition_id += sub_container_id;
		}
		else
		{
			base = affinity;
		}
	}

	if (lpartition_id == -1)
	{
		lpartition_id %= b->partition_count;
	}
	else if (affinity == 0 && !partitioning_rule)
	{
		/*
		lpartition_id = calc_partition_id(b->partition_count,
										  affinity);
		*/
	}
	else
	{
		if (!partitioning_rule)
		{
			//lpartition_id = calc_partition_id(b->partition_count, base);

			if (sub_container_id != -1)
			{
				lpartition_id = ((lpartition_id + sub_container_id) % b->partition_count);
			}
		}
		else
		{
			if (b->partition_count <= partitioning_count)
			{
				*partition_id = sub_container_id % b->partition_count;

				if (base)
					vhfree(base);

				if (affinity)
					vhfree(affinity);

				return 0;
			}
			
			pbase = b->partition_count / partitioning_count;
			//pmod = b->partition_count % partition_count;

			lpartition_id = (pbase * sub_container_id + 
							(pmod < sub_container_id ? pmod : sub_container_id) +
							crcval % pbase);

		}
	}

	if (base)
		vhfree(base);

	if (affinity)
		vhfree(affinity);

	*partition_id = lpartition_id;

	return 0;
}

static int32_t
hash_container_name(int32_t partition_count, 
					const char *container_name)
{
	int32_t len = strlen(container_name);
	int32_t crcval;

	if (len)
	{
		return (crcval) % partition_count;
	}

	return 0;
}

static char* 
container_name_lower(const char *container_name,
					 const char *up_to)
{
	int32_t len;
	char *base, *i;
   	const char *j;

	if (up_to)
		len = up_to - container_name;
	else
	{
		len = strlen(container_name);
		up_to = container_name + len;
	}

	base = vhmalloc(len);

	for (i = base, j = container_name; j < up_to; i++, j++)
		*i = tolower(*j);

	return base;	
}

/*
 * get_partition_address
 *
 * 	-1	Master resolving wire protocol incomplete
 *
 * 	We need to break this up so that it takes a GRDBconn as a parameter and then
 * 	has a callback to process the result.
 */
static int32_t 
get_partition_address(GRDBbeacon beac,
	   				  bool *address_found)
{
	ConnectionCatalog cc;
	Shard shard;
	BackEndConnection bec;
	GRDBconn grdbconn;
	GRDBbuffer query;
	GRDBoptRequest optreq;
	TcpBuffer query_out;
	int32_t partition_count;
	int8_t owner_count, backup_count;
	bool master_resolving = true, master_matched;

	if (beac->master)
	{
		//cc = vh_CatalogContext()->catalogConnection;

		//shard = master->primary;

		if (shard)
		{
			bec = vh_ConnectionGet(cc, shard->access[0]);
			grdbconn = grdb_conn_bec(bec);

			query = grdb_buffer_create();

			fillRequestHeader(query, grdbconn,
							  GET_PARTITION_ADDRESS,
							  beac->master->partition_id,
							  0, false);

			optreq = grdb_optrequest_start(&query->buf);
			grdb_optrequest_finish(optreq);

			/*
			 * Flag as master resolving
			 */
			if (master_resolving)
			{
				vh_tcps_buf_pbool(&query->buf, true);
			}

			executeStatement(grdbconn, 0, query, &query_out);

			partition_count = vh_tcps_buf_gi32(query_out);

			if (master_resolving)
			{
				owner_count = vh_tcps_buf_gbool(query_out);
			  	backup_count = vh_tcps_buf_gbool(query_out);

				if (owner_count != 0 ||
					backup_count != 0 ||
					vh_tcps_buf_remain(query_out) == 0)
				{
					return -1;
				}

				master_matched = vh_tcps_buf_gbool(query_out);

				/*
				 * Discard the hash mode, they've only got one and we have to
				 * manually implement that anyways.
				 */

				vh_tcps_buf_gbool(query_out);
				
				if (grdbconn->ipv6Enabled)
				{
					vh_tcps_buf_gi32(query_out);
					vh_tcps_buf_gi32(query_out);
					vh_tcps_buf_gi32(query_out);
					vh_tcps_buf_gi32(query_out);
				}
				else
				{
					vh_tcps_buf_gi32(query_out);
				}

				if (master_matched)
				{
					beac->partition_count = partition_count;
				}
			}
		}
	}
}


static HashTable 
grdb_beac_htp_shard_list(void *beac, SList htp,
	   					 bool assign, bool key_htp)
{
	return 0;
}

static HashTable 
grdb_beac_td_shard_list(void *beac, SList td, bool key_td)
{
	return 0;
}

static Shard 
grdb_beac_td_shard(void *beac, TableDef td)
{
	return 0;
}

static bool 
grdb_beac_connect(void *beac)
{
	return true;
}

static bool 
grdb_beac_disconnect(void *beac)
{
	return true;
}

static void 
grdb_beac_finalize(void *beac)
{
}


static int32_t 
grdb_beac_load_schema(void *beac, TableCatalog tc)
{
	return 0;
}

