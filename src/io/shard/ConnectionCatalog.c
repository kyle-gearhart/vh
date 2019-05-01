/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/shard/ConnectionCatalog.h"
#include "io/shard/Shard.h"
#include "io/utils/kvmap.h"

/*
 * Open Issues
 * 		1)	Concurrency on all structures
 * 		2)	Shutdown procedure
 * 		3)	Connection timeout
 */



typedef struct ShardAccessEntryData
{
	ShardAccess shardam;
	BackEndConnection connlist[10];
	uint16_t cl_out;

	uint16_t total;
	uint16_t available;
} ShardAccessEntryData, *ShardAccessEntry;

struct ConnectionCatalogData
{
	MemoryContext mctx;
	KeyValueMap htbl_sa;
	uint32_t total;

	Shard shard_def;
};


static void CheckoutConnection(ConnectionCatalog catalog, 
							   BackEndConnection nconn, 
							   ShardAccessEntry saentry);
static BackEndConnection SpawnConnection(ConnectionCatalog catalog, 
									  ShardAccessEntry saentry);



ConnectionCatalog
vh_ConnectionCatalogCreate(void)
{
	CatalogContext cc = vh_ctx();
	ConnectionCatalog catalog;

	if (cc)
	{
		catalog = (ConnectionCatalog)vhmalloc(sizeof(ConnectionCatalogData));
		catalog->total = 0;
		catalog->shard_def = 0;
		catalog->mctx = vh_MemoryPoolCreate(cc->memoryTop, 
											1028, 
											"Back End Connection Catalog");

		catalog->htbl_sa = vh_kvmap_create_impl(sizeof(ShardAccess),
												sizeof(ShardAccessEntryData),
												vh_htbl_hash_ptr,
												vh_htbl_comp_ptr,
												catalog->mctx);

		return catalog;
	}
	else
	{
		elog(FATAL,
			 emsg("Unable to create Connection Catalog, Catalog Context was "
				  "not found!"));
	}

	return 0;
}

void 
vh_ConnectionCatalogSetDefault(ConnectionCatalog cc, Shard shd)
{
	cc->shard_def = shd;
}

Shard
vh_ConnectionCatalogGetDefault(ConnectionCatalog cc)
{
	return cc->shard_def;
}

BackEndConnection
vh_ConnectionGet(ConnectionCatalog catalog, ShardAccess shardam)
{
	MemoryContext mctx_old;
	ShardAccessEntry saentry;
	BackEndConnection nconn = 0;
	uint16_t i;

	mctx_old = vh_mctx_switch(catalog->mctx);

	if (vh_kvmap_value(catalog->htbl_sa, &shardam, saentry))
	{
		if (saentry->available)
		{
			/*
			 * Scan the availability list for a free connection, the totals
			 * indicate one should be available.
			 */

			for (i = 0; i < saentry->total; i++)
			{
				if (saentry->cl_out & ( 1 << (i + 1)))
				   continue;

				assert(saentry->connlist[i]);	
				nconn = saentry->connlist[i];
				break;	
			}
		}
		else
		{
			nconn = SpawnConnection(catalog, saentry);

			if (!nconn)
				saentry = 0;
		}
	}
	else
	{
		saentry->total = 0;
		saentry->available = 0;
		saentry->shardam = shardam;
		saentry->cl_out = 0;

		memset(saentry->connlist, 0, sizeof(BackEndConnection) * 10);

		nconn = SpawnConnection(catalog, saentry);

		if (!nconn)
		{
			vh_kvmap_remove(catalog->htbl_sa, &shardam);
			saentry = 0;
		}
	}

	if (saentry)
	{
		CheckoutConnection(catalog, nconn, saentry);
	}

	vh_mctx_switch(mctx_old);
	
	return nconn;
}

void
vh_ConnectionCatalogShutDown(ConnectionCatalog cc)
{
	ShardAccessEntry sae;
	ShardAccess sa;
	BackEndConnection nconn;
	uint16_t j = 0;
	BackEnd be;
	vh_beat_disconnect beat_disconnect;
	vh_beat_freeconn beat_free;
	KeyValueMapIterator it;

	/*
	 * This really needs to take a global lock to prevent connections from being
	 * taken.  We'll first iterate the cc->shard AVL tree to release all
	 * connections there.  Then we'll need to wait on the in process connections
	 * to release.  In the future we should probably pass a force flag to the
	 * ShutDown call.  If it's force, we'll wait briefly on outsanding connections
	 * to release and then kill it.
	 */

	vh_kvmap_it_init(&it, cc->htbl_sa);

	while (vh_kvmap_it_next(&it, &sa, &sae))
	{
		for (j = 0; j < sae->total; j++)
		{
			nconn = sae->connlist[j];
			assert(nconn);

			be = nconn->be;
			assert(be);

			beat_disconnect = be->at.disconnect;
			beat_free = be->at.freeconn;

			if (beat_disconnect)
				beat_disconnect(nconn);

			if (beat_free)
				beat_free(nconn);

			sae->connlist[j] = 0;
		}		
	}

	vh_kvmap_destroy(cc->htbl_sa);
}

void
vh_ConnectionReturn(ConnectionCatalog catalog, 
					BackEndConnection nconn)
{

	ShardAccessEntry sae;
	ShardAccess sa;
	KeyValueMapIterator it;
	uint16_t j = 0;
	bool found = false;

	/*
	 * Loops thru all of the outstanding ShardAccess entries and tries to
	 * find the connection.
	 */
	vh_kvmap_it_init(&it, catalog->htbl_sa);
	while (vh_kvmap_it_next(&it, &sa, &sae))
	{
		if (sae->total == sae->available)
			continue;
		
		for (j = 0; j < sae->total; j++)
		{
			if (sae->connlist[j] == nconn)
			{
				sae->cl_out &= ~( 1 << (j + 1));
				sae->available++;

				found = true;

				break;
			}
		}

		if (found)
			break;
	}

	if (!found)
	{
		elog(ERROR2,
			 emsg("ConnectionCatalog does not have a record for the desired "
				  "BackEndConnection located at %p",
				  nconn));
	}
}



static void
CheckoutConnection(ConnectionCatalog catalog, 
				   BackEndConnection nconn, 
				   ShardAccessEntry saentry)
{
	uint16_t i;
	bool slot_set = false;

	for (i = 0; i < saentry->total; i++)
	{
		if (saentry->connlist[i] == nconn)
		{
			slot_set = true;
			break;
		}
	}

	if (slot_set)
	{
		saentry->cl_out |= (1 << (i + 1));
		saentry->available--;
	}
	else
	{
		elog(ERROR2,
			 emsg("ConnectionCatalog CheckoutConnection slot could not find BackEndConnection "
				  "%p",
				  nconn));
	}
}

static BackEndConnection 
SpawnConnection(ConnectionCatalog catalog, 
				ShardAccessEntry saentry)
{
	BackEndConnection nconn;
	BackEnd be;
	BackEndCredentialVal becredval;
	ShardAccess sa;
	vh_beat_createconn beat_createconn;
	vh_beat_connect beat_connect;

	sa = saentry->shardam;

	if (!sa)
		elog(ERROR2,
			 emsg("Corrupt connection catalog, ShardAccessEntry does not have a valid "
				  "ShardAccess pointer!"));

	be = sa->be;

	if (!be)
		elog(ERROR2,
			 emsg("Corrupt back end connection catalog; no back end referenced for the ShardAccess "
				  "entry requested!"));

	beat_createconn = be->at.createconn;
	beat_connect = be->at.connect;

	if (beat_createconn)
	{
		nconn = beat_createconn();
		becredval = vh_be_cred_retrieve(sa->becred);
		
		if (beat_connect && beat_connect(nconn, &becredval, sa->database))
		{
			saentry->connlist[saentry->total] = nconn;
			
			/*
			 * Clear the out flag
			 */
			saentry->cl_out &= ~( 1 << (saentry->total + 1));

			saentry->total++;
			saentry->available++;

			elog(DEBUG2, emsg("The shard connection catalog has created a new connection"
				" for host %s on port %s; %d total with %d available"
				, &becredval.hostname[0]
				, &becredval.hostport[0]
				, saentry->total
				, saentry->available));
			
			vh_be_cred_wipe(becredval);

			return nconn;
		}
		else
		{
			/*
			 * this should really throw an error/log file to indicate
			 * a connection could not be made
			 */

			elog(ERROR2, emsg("Could not establish a connection in the shard"
				" connection catalog for host %s port %s"
				, &becredval.hostname[0]
				, &becredval.hostport[0]));

			vh_be_cred_wipe(becredval);

			vhfree(nconn);
		}
	}

	return 0;
}

