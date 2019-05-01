/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_htc_H
#define vh_datacatalog_executor_htc_H

/*
 * Heap Tuple Collector (HTC)
 *
 * Allows the back ends to form HeapTuple from native result sets without
 * having to manipulate the VH result sets.  The back end should call
 * vh_be_htc with two arrays: HeapTuple and HeapTuplePtr, each |rtups| in size.  
 * The array itself is owned by the caller.  The HTC implementation owns the 
 * HeapTuple and HeapTuplePtr assigned in the arrays.  Thus the HTC shouldn't 
 * vhfree or vhrealloc the array itself, but each value in the array is subject
 * to change.
 *
 * The HeaptupleCollectorInfo structure is also passed as a pointer to the
 * back end from the executor.  It's expected the back end pass this pointer
 * unmodified to the HTC.
 */

typedef struct HeapTupleCollectorInfoData *HeapTupleCollectorInfo;
typedef void (*vh_be_htc)(void *info, HeapTuple*, HeapTuplePtr*);

struct HeapTupleCollectorInfoData
{
	MemoryContext result_ctx;
	HeapBufferNo hbno;
	
	int32_t rtups;
	int32_t nrows;
	int32_t cursor;

	vh_be_htc htc_cb;
};

void vh_htc_info_init(HeapTupleCollectorInfo info);
void vh_htc_info_copy(HeapTupleCollectorInfo from, HeapTupleCollectorInfo to);

#endif

