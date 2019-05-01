/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/buffer/HeapBuffer.h"
#include "io/buffer/HeapTuplePtr.h"
#include "io/catalog/HeapTuple.h"

/*
 * vh_htp_assign_shard
 *
 * Assigns a shard to the HeapTuplePtr and it's copy if one is available.
 *
 * We start by requesting the immutable HeapTuple.
 */

bool
vh_htp_assign_shard(HeapTuplePtr htp, Shard shd, bool both)
{
	HeapTuple ht, ht_im = 0, ht_m = 0;
	HeapTuplePtr htp_cpy, htp_im = 0, htp_m = 0;

	ht = vh_hb_heaptuple(vh_hb(vh_HTP_BUFF(htp)), htp, 0);

	if (ht)
	{
		if (vh_ht_flags(ht) & VH_HT_FLAG_MUTABLE)
		{
			ht_m = ht;
			htp_m = htp;
		}
		else
		{
			ht_im = ht;
			htp_im = htp;
		}

		if (both && (htp_cpy = ht->tupcpy))
		{
			ht = vh_hb_heaptuple(vh_hb(vh_HTP_BUFF(htp_cpy)),
  								 htp_cpy,
  								 0);

			if (vh_ht_flags(ht) & VH_HT_FLAG_MUTABLE)
			{
				ht_m = ht;
				htp_m = htp_cpy;
			}
			else
			{
				ht_im = ht;
				htp_im = htp_cpy;
			}
		}
		
		if (ht_im)
		{
			if (ht_im->shard)
			{
				elog(WARNING,
					 emsg("Reassigning shard for HeapTuplePtr %ld",
						  htp_im));
			}
			
			ht_im->shard = shd;
		}

		if (ht_m)
		{
			if (ht_m->shard)
			{
				elog(WARNING,
					 emsg("Reassigning shard for mutable HeapTuplePtr %ld",
						  htp_m));
			}

			ht_m->shard = shd;
		}
	}

	return true;
}

