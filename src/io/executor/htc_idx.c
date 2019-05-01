/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/executor/htc_idx.h"

typedef struct HTCIndexData *IndexContext;

static size_t htc_idx_gen_key(HeapTuple ht, HeapField *hfs, int32_t nhfs,
			  				  unsigned char **buffer, size_t *buffer_len);

/*
 * vh_htc_idx
 *
 * This is the actual function considered to the HTC.  There are a lot of other
 * helper functions visible outside of this translation unit.  These help the
 *
 *
 * Iterate each of the |rtups| and if an index exists, let's check it for the
 * quals.  Generally we should be using a modified radix tree which is smart
 * enough to build the key all on it's own: all we have to do is hand over a
 * valid HeapTuple structure.  NOTE: this is not a HeapTuplePtr, which is 
 * nothing more than a uint64_t the buffer manager hands out.
 */

void vh_htc_idx(void *info, HeapTuple *hts, HeapTuplePtr *htps)
{
	IndexContext ic = info;
	HeapTuplePtr htp_idx;
	size_t key_len;
	int32_t i;
	bool exists;

	for (i = 0; i < ic->htci.rtups; i++)
	{
		if (ic->use_index[i])
		{
			/*
			 * Let's see if it's in the index by calling an upsert-ish routine
			 * on the index.
			 */

			key_len = htc_idx_gen_key(hts[i], ic->hfs[i], ic->nhfs[i],
  									  &ic->key_buffers[i], &ic->key_lens[i]);

			if (key_len)
			{
				htp_idx = (ic->idx_ups[i])(ic->indexes[i], ic->key_buffers[i], 
										   key_len, htps[i], &exists);
			}
			else
			{
				exists = false;
			}


			if (exists)
			{
				/*
				 * Let's do a swap and release the HeapTuplePtr and thus give
				 * HeapTuple back to the buffer for management.  Ideally the 
				 * buffer should release everything for the page, but that
				 * hasn't been implemented yet.  At the moment, the buffer will
				 * release the tuple data from the page (i.e. 
				 * HeapTupleDef->heapasize) but not the two bytes occupied by
				 * the item pointer on the page.
				 *
				 * Over big result sets where we have lots of repeating rows 
				 * these two heap page item pointer bytes can add up quick!
				 */
				assert(htp_idx);

				vh_htp_free(htps[i]);
				hts[i] = 0;
				htps[i] = 0;

				ic->ht_transfer[i] = 0;
				ic->htp_transfer[i] = htp_idx;
			}
			else
			{
				/*
				 * This thing either didn't exist in the index or we couldn't
				 * form a key.  Transfer what came to use to the pipe.
				 */

				ic->ht_transfer[i] = hts[i];
				ic->htp_transfer[i] = htps[i];
			}
		}
		else
		{
			/*
			 * No indexing required, just transfer over what we were handed.  
			 * The  indexing routines aren't really meant to form up some kind 
			 * of  resultset, just handle duplicates that come thru.  So we set
			 * this rtup in our transfer structure and move on.
			 */
			ic->ht_transfer[i] = hts[i];
			ic->htp_transfer[i] = htps[i];

		}
	}

	/*
	 * Let's call the pipe and push our rowset forward
	 */

	ic->htc_pipe_cb(ic->htc_pipe_info,
					ic->ht_transfer,
					ic->htp_transfer);
}

/*
 * vh_htc_idx_init
 *
 * We really want all this stuff aligned on a 64 byte boundary so that each
 * array is on it's own cacheline.  This is especially true for the transfer
 * arrays and vh_htc_idx_ups_cb array.  These get read and swapped on every
 * call to vh_htc_idx.
 */
void
vh_htc_idx_init(void *info,
				int32_t rtups, 
				vh_be_htc pipe_cb, void *pipe_info)
{
	IndexContext ic = info;
	const size_t init_len = (sizeof(void*) +
							 sizeof(HeapField*) +
							 sizeof(unsigned char*) +
							 sizeof(size_t) +
							 sizeof(int32_t) + 
							 sizeof(bool) +
							 sizeof(vh_htc_idx_ups_cb) +
							 sizeof(vh_htc_idx_destroy_cb)) * rtups;
	const size_t transfer_len = sizeof(HeapTuple) * rtups;
	
	assert(ic);
	assert(rtups > 0);
	assert(pipe_cb);
	assert(pipe_info);

	ic->htc_pipe_cb = pipe_cb;
	ic->htc_pipe_info = pipe_info;

	ic->htp_flags = 0;

	/*
	 * We put these in order they're accessed in the main loop for vh_htc_idx
	 * hoping to avoid cacheline misses.  We also do some alignment to ensure
	 * each member starts on a 64 byte boundary.  We take up more space but
	 * we want vh_htc_idx to be fast and a high cacheline hit ratio is 
	 * critical.
	 */
	ic->use_index = vhmalloc(init_len);
	ic->indexes = (void**)(ic->use_index + rtups);
	ic->hfs = (HeapField**)(ic->indexes + rtups);
	ic->nhfs = (int32_t*)(ic->hfs + rtups);
	ic->key_buffers = (unsigned char**)(ic->hfs + rtups);
	ic->key_lens = (size_t*)(ic->key_buffers + rtups);
	
	ic->idx_ups = (vh_htc_idx_ups_cb*)(ic->key_lens + rtups);
	ic->idx_destroy = (vh_htc_idx_destroy_cb*)(ic->idx_ups + rtups);
	
	/*
	 * Let's zero this chunk out completely.  Subsequent calls to 
	 * vh_htc_idx_add will populate everything correctly.
	 */
	memset(ic->indexes, 0, init_len);

	/*
	 * Setup the transfer arrays.  We don't worry about zero-ing these out
	 * because they get written to immediately in the HTC call stack.
	 */
	ic->ht_transfer = vhmalloc(transfer_len);
	ic->htp_transfer = vhmalloc(transfer_len);

}

/*
 * vh_htc_idx_add
 *
 * Based on the fields that are indexed, we make a decision about what 
 * underlying structure to use to do the indexing.  We'll also setup an
 * initial key scratchpad buffer.  We guess at the proper size based on
 * the fields requested to be indexed.  Keep in mind that the HeapField
 * structure uses the TAM and TOM facilities to nest multiple Types.
 */
void
vh_htc_idx_add(void *info, int32_t rtup, HeapField *hfs, int32_t nhfs)
{
	IndexContext ic = info;

	ic->use_index[rtup] = true;
	ic->nhfs[rtup] = nhfs;
	ic->hfs[rtup] = hfs;

	/*
	 * Figure out what type of index to use based on the fields requested.  Go
	 * ahead and establish a key scratch buffer.  We're going to have to get
	 * the details of the underlying TAM/TOM structure for each field so we
	 * can make an educated guess about how wide the scratch area should be.
	 */
}

void
vh_htc_idx_destroy(void *info, bool include_indexes)
{
}

/*
 * htc_idx_gen_key
 *
 * We should resize the buffer until the key fits.
 */
static size_t 
htc_idx_gen_key(HeapTuple ht, HeapField *hfs, int32_t nhfs,
				unsigned char **buffer, size_t *buffer_len)
{
	size_t key_size = 0;

	key_size = vh_ht_formkey(*buffer, *buffer_len, ht, hfs, nhfs);

	return key_size;
}

