/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/executor/eresult.h"
#include "io/utils/SList.h"

ExecResult
vh_exec_result_create(int32_t rtds)
{
	ExecResult er;
	size_t sz = sizeof(struct TableDefSlot);
	int32_t i = 0;

	if (rtds > 1)
		sz *= (rtds - 1);

	er = vhmalloc(sz + sizeof(struct ExecResultData));
	er->tups = vh_SListCreate();
	er->hbno = 0;
	er->rtds = rtds;

	for (i = 0; i < rtds; i++)
		vh_slot_td_init(&er->slots[i]);

	return er;
}

void
vh_exec_result_finalize(ExecResult er, bool keep_htp)
{
	int32_t i = 0, j = 0, tup_sz;
	HeapTuplePtr *htp_head;
	HeapTuplePtr **htpm_head, *htpm;

	if (er->er_shouldreltups && !keep_htp)
	{
		if (er->rtds == 1)
		{
			tup_sz = vh_SListIterator(er->tups, htp_head);
			for (i = 0; i < tup_sz; i++)
				vh_htp_free(htp_head[i]);
		}
		else
		{
			tup_sz = vh_SListIterator(er->tups, htpm_head);
			for (i = 0; i < tup_sz; i++)
			{
				htpm = htpm_head[i];

				for (j = 0; j < er->rtds; j++)
				{
					vh_htp_free(htpm[j]);
				}
			}
		}
	}
	
	for (i = 0; i < er->rtds; i++)
		vh_slot_td_reset(&er->slots[i]);
}

HeapTuplePtr
vh_exec_result_htp(ExecResult er, uint8_t slot, uint32_t row)
{
	HeapTuplePtr *htp_head;
	uint32_t htp_sz;

	if (slot > er->rtds)
		elog(ERROR1,
			 emsg("Attempting to access slot %d but only %d slots are "
				  "available in the ExecResult!",
				  slot,
				  er->rtds));

	htp_sz = vh_SListIterator(er->tups, htp_head);

	if (row > htp_sz)
		elog(ERROR1,
			 emsg("Attempted to access row %d, but only %d rows exist "
				  "in the ExecResult!",
				  row,
				  htp_sz));

	if (er->rtds == 1)
		return htp_head[row];
	else
		return ((HeapTuplePtr**)htp_head)[row][slot];
}

HeapTuple
vh_exec_result_ht(ExecResult er, uint8_t slot, uint32_t row)
{
	HeapTuplePtr htp = vh_exec_result_htp(er, slot, row);
	
	return vh_htp(htp);
}

HeapTuple
vh_exec_result_htim(ExecResult er, uint8_t slot, uint32_t row)
{
	HeapTuplePtr htp = vh_exec_result_htp(er, slot, row);

	return vh_htp_immutable(htp);
}

bool
vh_exec_result_iter_reset(ExecResult er)
{
	uint32_t htp_sz = 0;

	if (er->tups)
		htp_sz = vh_SListSize(er->tups);

	er->iter_idx = 0;

	return (bool)(htp_sz > 0);
}

bool
vh_exec_result_iter_last(ExecResult er)
{
	uint32_t htp_sz = 0;

	if (er->tups)
		htp_sz = vh_SListSize(er->tups);

	er->iter_idx = htp_sz ? htp_sz - 1 : 0;

	return (bool)(htp_sz > 0);
}

bool
vh_exec_result_iter_next(ExecResult er)
{
	uint32_t htp_sz = 0;

	htp_sz = vh_SListSize(er->tups);

	if (er->iter_idx + 1 < htp_sz)
	{
		er->iter_idx++;
		
		return true;
	}

	return false;
}

bool
vh_exec_result_iter_prev(ExecResult er)
{
	if (er->iter_idx > 0)
	{
		er->iter_idx--;

		return true;
	}

	return false;
}

HeapTuplePtr
vh_exec_result_iter_htp(ExecResult er, uint8_t slot)
{
	HeapTuplePtr *htp_head;
	uint32_t htp_sz;

	if (slot > er->rtds)
		elog(ERROR1,
			 emsg("Attempting to access slot %d but only %d slots are "
				  "available in the ExecResult!",
				  slot,
				  er->rtds));

	htp_sz = vh_SListIterator(er->tups, htp_head);

	assert(er->iter_idx >= 0);
	assert(htp_sz > er->iter_idx);
	
	if (er->rtds == 1)
		return htp_head[er->iter_idx];
	else
		return ((HeapTuplePtr**)htp_head)[er->iter_idx][slot];
}

HeapTuple
vh_exec_result_iter_ht(ExecResult er, uint8_t slot)
{
	HeapTuplePtr htp = vh_exec_result_iter_htp(er, slot);

	return vh_htp(htp);
}

HeapTuple
vh_exec_result_iter_htim(ExecResult er, uint8_t slot)
{
	HeapTuplePtr htp = vh_exec_result_iter_htp(er, slot);

	return vh_htp_immutable(htp);
}

