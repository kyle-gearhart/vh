/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/executor/htc.h"

void
vh_htc_info_init(HeapTupleCollectorInfo info)
{
	CatalogContext cc = vh_ctx();

	assert(info);

	if (cc)
	{
		info->result_ctx = vh_mctx_current();
		info->hbno = cc->hbno_general;

		info->rtups = 0;
		info->nrows = 0;
		info->cursor = 0;

		info->htc_cb = 0;
	}
	else
	{
		elog(ERROR2,
			 emsg("A catalog context could not be located when attempting to "
				  "initialize a HTC information structure!"));
	}
}

void
vh_htc_info_copy(HeapTupleCollectorInfo from, HeapTupleCollectorInfo to)
{
	assert(from);
	assert(to);

	memcpy(to, from, sizeof(struct HeapTupleCollectorInfoData));
}

