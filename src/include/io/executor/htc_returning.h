/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_EXECUTOR_HTC_RETURNING_H
#define VH_EXECUTOR_HTC_RETURNING_H

#include "io/executor/htc.h"

/*
 * Heap Tuple Collector (HTC): Returning Set
 *
 * When a rowset is returned by a statement (e.g. INSERT or UPDATE) we attempt
 * to capture those values using this collector.
 */

struct HTC_ReturningCtx
{
	struct HeapTupleCollectorInfoData htci;
	SList htp_tgt;
	SList hf_src;
	SList hf_tgt;
	int32_t i;
};

void vh_htc_returning(void *info, HeapTuple *hts, HeapTuplePtr *htps);

void vh_htc_returning_init(void *info, SList tgt, Node rfields);
void vh_htc_returning_destroy(void *info);

#endif

