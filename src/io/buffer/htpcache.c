/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/buffer/htpcache.h"
#include "io/utils/htbl.h"


#define VH_HTPCACHE_MAGIC		(0x58de92fa)



/*
 * ============================================================================
 * HtpCache Data Structures
 * ============================================================================
 *
 * We use single hash table with a magic field to store the cache.
 */

struct HtpCacheData
{
	int32_t magic;
	HashTable htbl;
};


/*
 * ============================================================================
 * Helper Functions
 * ============================================================================
 */

static int32_t htpc_relall_impl(HashTable htbl);

static bool htpc_htbl_relall_iter(HashTable htbl, 
								  const void *key, void *entry,
								  void *data);


/*
 * ============================================================================
 * Public Interface: Create, Finalize, Free, Destroy
 * ============================================================================
 */

HtpCache 
vh_htpc_create(void)
{
	static int32_t htbl_flags = VH_HTBL_OPT_KEYSZ |
	   							VH_HTBL_OPT_VALUESZ |
								VH_HTBL_OPT_HASHFUNC |
								VH_HTBL_OPT_COMPFUNC |
								VH_HTBL_OPT_MCTX |
								VH_HTBL_OPT_MAP;	
	HashTableOpts hopts = { };
	HtpCache htpc;

	hopts.key_sz = sizeof(HeapTuplePtr);
	hopts.value_sz = sizeof(HeapTuple);
	hopts.func_hash = vh_htbl_hash_int64;
	hopts.func_compare = vh_htbl_comp_int64;
	hopts.mctx = vh_mctx_current();
	hopts.is_map = true;

	htpc = vhmalloc(sizeof(struct HtpCacheData));
	htpc->magic = VH_HTPCACHE_MAGIC;
	htpc->htbl = vh_htbl_create(&hopts, htbl_flags);

	return htpc;
}


/*
 * vh_htpc_finalize
 *
 * Releases all references and then finalizes the data structure by destroying
 * the hash table.
 */
void
vh_htpc_finalize(HtpCache htpc)
{
	if (!htpc)
	{
		elog(WARNING,
				emsg("Invalid HtpCache pointer[%p] passed to vh_htpc_finalize.  Unable to "
					 "finalize the cache as requested.",
					 htpc));

		return;
	}

	assert(htpc->magic == VH_HTPCACHE_MAGIC);

	if (htpc->htbl)
	{
		htpc_relall_impl(htpc->htbl);
		vh_htbl_destroy(htpc->htbl);

		htpc->htbl = 0;
	}
}

/*
 * vh_htpc_free
 *
 * Frees the HtpCacheData structure.
 */
void
vh_htpc_free(HtpCache htpc)
{
	if (!htpc)
	{
		elog(WARNING,
				emsg("Invalid HtpCache pointer [%p] passed to vh_htpc_free.  "
					 "Unable to free the cache as requested."));

		return;
	}

	assert(htpc->magic == VH_HTPCACHE_MAGIC);

	vhfree(htpc);
}

void
vh_htpc_destroy(HtpCache htpc)
{
	if (!htpc)
	{
		elog(WARNING,
				emsg("Invalid HtpCache pointer [%p] passed to vh_htpc_destroy.  "
					 "Unable to free the cache as requested.",
					 htpc));

		return;
	}

	vh_htpc_finalize(htpc);
	vhfree(htpc);
}



/*
 * ============================================================================
 * Public Interface: Operations
 * ============================================================================
 */

int32_t
vh_htpc_get(HtpCache htpc, HeapTuplePtr htp,
			HeapTuple *ht)
{
	HeapTuple htr, *put;
	int32_t ret;
	
	if (!htpc)
	{
		elog(WARNING,
				emsg("Invalid HtpCache pointer [%p] passed to vh_htpc_get.  "
					 "Unable to fetch from the cache as requested.",
					 htpc));

		return -1;
	}	

	if (!htp)
	{
		elog(WARNING,
				emsg("Invalid HeapTuplePtr [%llu] passed to vh_htpc_get.  "
					 "Unable to fetch a HeapTuplePtr.",
					 htp));

		return -2;
	}

	if (!ht)
	{
		elog(WARNING,
				emsg("Invalid HeapTuple pointer [%p] to store the result of "
					 "the fetch passed to vh_htpc_get.  Unable to proceeed.",
					 ht));

		return -3;
	}

	assert(htpc->magic == VH_HTPCACHE_MAGIC);

	htr = vh_htbl_get(htpc->htbl, &htp);
	*ht = htr;

	if (htr)
		return 0;

	htr = vh_htp(htp);
	*ht = htr;

	if (htr)
	{
		put = vh_htbl_put(htpc->htbl, &htp, &ret);

		assert(ret == 1 || ret == 2);

		*put = htr;

		return 0;
	}

	return 1;
}

/*
 * vh_htpc_rel
 *
 * Releases a single HeapTuplePtr from the cache.  If successful, return 0.
 *
 * When 1 is returned, the HeapTuplePtr did not exist in the cache.
 */
int32_t
vh_htpc_rel(HtpCache htpc, HeapTuplePtr htp)
{
	HeapTuple ht;

	if (!htpc)
	{
		elog(WARNING,
				emsg("Invalid HtpCache pointer [%p] passed to vh_htpc_rel.  "
					 "Unable to release the HeapTuplePtr as requested.",
					 htpc));

		return -1;
	}

	if (!htp)
	{
		elog(WARNING,
				emsg("Invalid HeapTuplePtr [%llu] passed to vh_htpc_rel.  "
					 "Unable to release the HeapTuplePtr as requested.",
					 htp));

		return -2;
	}

	assert(htpc->magic == VH_HTPCACHE_MAGIC);
	
	ht = vh_htbl_get(htpc->htbl, &htp);

	if (ht)
	{
		/*
		 * Needs to release the buffer, then we can delete it from our hash
		 * table.
		 */

		vh_htbl_del(htpc->htbl, &htp);

		return 0;
	}

	return 1;
}

/*
 * vh_htpc_relall
 *
 * Iterate the HashTable calling htpc_tbl_relall_iter and then clear the
 * Hash Table.  We use htpc_relall do to the dirty work and use the top
 * level function do to our valid pointer and magic assertations.
 */
	
int32_t
vh_htpc_relall(HtpCache htpc)
{
	if (!htpc)
	{
		elog(WARNING,
				emsg("Invalid HtpCache pointer [%p] passed to vh_htpc_relall.  "
					 "Unable to clear the release all HeapTuplePtr references "
					 "as requested.",
					 htpc));

		return -1;
	}
	
	assert(htpc->magic == VH_HTPCACHE_MAGIC);

	return htpc_relall_impl(htpc->htbl);
}

bool
vh_htpc_ispinned(HtpCache htpc, HeapTuplePtr htp)
{
	HeapTuple ht;

	if (!htpc)
	{
		elog(WARNING,
				emsg("Invalid HtpCache pointer [%p] passed to "
					 "vh_htpc_ispinned.  Unable to proceed.",
					 htpc));

		return false;
	}

	assert(htpc->magic == VH_HTPCACHE_MAGIC);

	ht = vh_htbl_get(htpc->htbl, &htp);

	return (ht != 0);
}


/*
 * ============================================================================
 * Private Functions
 * ============================================================================
 */

static int32_t 
htpc_relall_impl(HashTable htbl)
{
	vh_htbl_iterate_map(htbl, htpc_htbl_relall_iter, 0);
	vh_htbl_clear(htbl);

	return 0;
}

static bool 
htpc_htbl_relall_iter(HashTable htbl, 
					  const void *key, void *entry,
					  void *data)
{

	/*
	 * Release the HeapTuplePtr from the buffer and return true.
	 */

	return true;
}

