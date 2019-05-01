/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#include <stdio.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/executor/htc_slist.h"
#include "io/utils/SList.h"


/*
 * vh_htc_slist
 *
 * Populates an SList with a given HeapTuple array.  If there is only one
 * HeapTuple per row, then the HeapTuple will be pushed directly into the
 * SList.  Otherwise a large sequential array will be allocated at the 
 * first call and divied out as subsequent calls (rows) are processed.
 */
void
vh_htc_slist(void *info, HeapTuple *hts, HeapTuplePtr *htps)
{
	struct HTC_SListCtx *sc = info;
	HeapTuplePtr *htarr;
	int32_t i;

	if (hts)
	{
		for (i = 0; i < sc->esz; i++)
		{
			if (hts[i])
			{
				vh_ht_flags(hts[i]) |= VH_HT_FLAG_FETCHED;
			}
		}
	}

	if (sc->esz > 1)
	{
		if (!sc->head)
		{
			sc->head = vhmalloc_ctx(sc->htci.result_ctx, 
									sizeof(HeapTuple) * sc->esz *
									sc->htci.nrows);
			sc->eremain = sc->htci.nrows;
		}

		if (!sc->eremain)
		{
			if (!sc->blocks)
				sc->blocks = vh_SListCreate_ctx(sc->htci.result_ctx);

			vh_SListPush(sc->blocks, sc->head);
																							
			sc->eoffset = 0;
			sc->eremain = sc->htci.nrows - sc->htci.cursor;
			sc->head = vhmalloc_ctx(sc->htci.result_ctx,
									sizeof(HeapTuple) * sc->esz * sc->eremain);
		}

		htarr = sc->head +	sc->eoffset;
		sc->eoffset += sc->esz;
		sc->eremain--;

		memcpy(htarr, htps, sizeof(HeapTuple) * sc->esz);
		vh_SListPush(sc->tups, htarr);
	}
	else
	{
		vh_htp_SListPush(sc->tups, htps[0]);
	}
}

void
vh_htc_slist_init(void *info, int32_t rtups)
{
}

void
vh_htc_slist_destroy(void *info)
{
}

