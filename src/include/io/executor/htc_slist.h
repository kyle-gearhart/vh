/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_htc_slist_H
#define vh_datacatalog_executor_htc_slist_H

#include "io/executor/htc.h"

/*
 * Heap Tuple Collector (HTC): SList
 *
 * Pushes the entire HeapTuplePtr array received into an SList.  Since the HTC 
 * itself doesn't own the arrays, we keep a cache of arrays |rtups| wide handy.
 */

struct HTC_SListCtx
{
	struct HeapTupleCollectorInfoData htci;
	SList tups;
	SList blocks;
	HeapTuplePtr *head;
	int32_t esz;
	int32_t eoffset;
	int32_t eremain;
};


void vh_htc_slist(void *info, HeapTuple *hts, HeapTuplePtr *htps);

void vh_htc_slist_init(void *info, int32_t rtups);
void vh_htc_slist_destroy(void *info);

#endif

