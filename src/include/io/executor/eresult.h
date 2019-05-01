/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_eresult_H
#define vh_datacatalog_executor_eresult_H

#include "io/buffer/slot.h"

typedef struct ExecResultData
{
	SList tups;
	int32_t rtds;
	HeapBufferNo hbno;

	uint32_t iter_idx;

	bool er_shouldreltups;

	TableDefSlot slots[1];
} *ExecResult;

ExecResult vh_exec_result_create(int32_t rtds);

#define vh_exec_result_rows(er)			(vh_SListSize(er->tups))
#define vh_exec_result_slots(er)		(er->rtds)

HeapTuplePtr vh_exec_result_htp(ExecResult er, uint8_t slot, uint32_t row);
HeapTuple vh_exec_result_ht(ExecResult er, uint8_t slot, uint32_t row);
HeapTuple vh_exec_result_htim(ExecResult er, uint8_t slot, uint32_t row);

/*
 * Result Iterators
 */
#define vh_exec_result_iter_first(er)		vh_exec_result_iter_reset(er)
bool vh_exec_result_iter_last(ExecResult er);
bool vh_exec_result_iter_reset(ExecResult er);
bool vh_exec_result_iter_next(ExecResult er);
bool vh_exec_result_iter_prev(ExecResult er);

bool vh_exec_result_iter(ExecResult er,
						 bool (*iter_cb)(HeapTuple, void*),
						 void *data);

HeapTuple vh_exec_result_iter_ht(ExecResult er, uint8_t slot);
HeapTuple vh_exec_result_iter_htim(ExecResult er, uint8_t slot);
HeapTuplePtr vh_exec_result_iter_htp(ExecResult er, uint8_t slot);

void vh_exec_result_finalize(ExecResult er, bool keep_htps);

#endif

