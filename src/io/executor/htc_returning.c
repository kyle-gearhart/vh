/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/HeapTupleDef.h"
#include "io/catalog/Type.h"
#include "io/executor/htc_returning.h"
#include "io/utils/SList.h"

#include "io/nodes/Node.h"
#include "io/nodes/NodeField.h"



static void htc_returning_build_hf_tgt(Node node, void *data);

void
vh_htc_returning_init(void *info, SList tgt, Node rfields)
{
	struct HTC_ReturningCtx *htc = info;

	htc->htp_tgt = tgt;
	htc->hf_src = 0;
	htc->i = 0;

	if (rfields)
	{
		htc->hf_tgt = vh_SListCreate();

		vh_nsql_visit_tree(rfields,
			   			   htc_returning_build_hf_tgt,
					   	   htc->hf_tgt);
	}
	else
	{
		htc->hf_tgt =0;
	}
}

void
vh_htc_returning_destroy(void *info)
{
	struct HTC_ReturningCtx *htc = info;

	if (htc->htp_tgt)
		vh_SListDestroy(htc->htp_tgt);

	if (htc->hf_src)
		vh_SListDestroy(htc->hf_src);

	if (htc->hf_tgt)
		vh_SListDestroy(htc->hf_tgt);
}

/*
 * vh_htc_returning
 *
 * The HeapTuple we receive in only contains the RETURNING fields, so we just
 * need to move those over to the HeapTuplePtr represened in htp_tgt.
 *
 * We make a BIG ASSUMPTION here that the backend returns tuples in the same
 * order they went in.  In theory we could make this a little smarter and try
 * to index off the primary key.  Put it on the wish list, pal!
 */

void
vh_htc_returning(void *info, HeapTuple *hts, HeapTuplePtr *htps)
{
	struct HTC_ReturningCtx *htc = info;
	HeapTuple ht;
	HeapField *hfs_head, *hfst_head;
	HeapTuplePtr *htp_head, htp;
	int32_t i, hfs_size, htps_size;

	/*
	 * We depend on late binding to setup the fields we know we're getting
	 * back due to the RETURNING statement.  We should probably check to
	 * make sure the number of fields returned matches the number of fields
	 * in our RETURNING clause built by the planner.
	 */

	if (!htc->i)
	{
		hfs_size = vh_SListIterator(hts[0]->htd->fields, hfst_head);
		
		htc->hf_src = vh_SListCreate();
		
		for (i = 0; i < hfs_size; i++)
		{
			vh_SListPush(htc->hf_src, hfst_head[i]);
		}
	}

	/*
	 * Find out where we are the set, since this is called for each row
	 * of the RETURNING set.  We also do some simple array boundary checks
	 * with our HTC iterator.
	 */

	htps_size = vh_SListIterator(htc->htp_tgt, htp_head);

	assert(htc->i < htps_size);	

	htp = htp_head[htc->i];
	ht = vh_htp(htp);

	vh_ht_flags(ht) |= VH_HT_FLAG_FETCHED;

	hfs_size = vh_SListIterator(htc->hf_tgt, hfst_head);
	vh_SListIterator(htc->hf_src, hfs_head);

	/*
	 * Move the fields over with a MemSet semantic.
	 */

	for (i = 0; i < hfs_size; i++)
	{
		vh_tam_fireh_memset_set(hfst_head[i],							/* HeapField */
								vh_ht_field(hts[0], hfs_head[i]),		/* Source */
								vh_ht_field(ht, hfst_head[i]),			/* Target */
								false);									/* Copy VarLen */
	}

	/*
	 * There needs to be a way to reset the field.  For now, just free the HTP.
	 */

	vh_htp_free(htps[0]);

	/*
	 * Increment the HTC iterator and then we're all done!
	 */

	htc->i++;
}

static void 
htc_returning_build_hf_tgt(Node node, void *data)
{
	NodeField nf = (NodeField)node;
	SList hf_tgt = data;
	
	if (node->tag == Field)
	{
		vh_SListPush(hf_tgt, nf->tf);
	}
}

