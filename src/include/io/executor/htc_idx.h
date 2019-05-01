/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_htc_idx_H
#define vh_datacatalog_executor_htc_idx_H

#include "io/executor/htc.h"

/*
 * Heap Tuple Collector (HTC): Indexed
 *
 * Uses local indexing to avoid keeping multiple copies of the same row.  The 
 * planner is responsible for building an ExecStep specifying what indexes to
 * generate.  The ExecStep's open function will the call the vh_htc_idx_init
 * function followed by the vh_htc_idx_add function for each index that needs
 * to track.  These two will set the HTCIndexData structure in a manner the
 * underlying functions expect. 
 *
 * The goal here is to allow this to be the receiver of the HeapTuple and
 * HeapTuplePtr array from the backend.  Indexing can then immediately call
 * another HTC (such as an SList) with the non-duplicated HeapTuple.
 *
 * For performance reasons, the planner should only use this HTC when atleast
 * one table may present duplicate rows.  Every table does not have to be 
 * indexed, but atleast one should.
 *
 * In the future, we may bootstrap a few more options in here to update a
 * HeapTuple relation.  The indexing fields must be the same as the relation.
 */



/*
 * vh_htc_idx_ups_cb
 *
 * A function defined by the underlying index that performs an upsert-ish
 * action.  Meaning, if the index key doesn't exist at all insert.  If it
 * does, don't do a replacement, just tells us such thru the |exists| 
 * parameter.
 *
 * The function should always return the HeapTuplePtr, whether it was just
 * added or found in an existing index entry.
 *
 * The index routine itself should only store the HeapTuplePtr.
 */

typedef HeapTuplePtr (*vh_htc_idx_ups_cb)(void *index,
										  void *key,
										  size_t key_len,
										  HeapTuplePtr htp,
										  bool *exists);
typedef void (*vh_htc_idx_destroy_cb)(void *index);


/*
 * HTCIndexData
 *
 * This it the superstruct for the standard HTCInfo structure.  In addition to
 * the standard HTCInfo, there's additional information needed to process each
 * rowset succesfully.  We've made this definition public so the executor may
 * stand one of these on the stack instead of dynamically allocating it with
 * vhmalloc.  Users shouldn't touch the values directly.  The _init and _add
 * functions set all of the members up correctly.
 *
 * |use_index| flag for each rtup indicating if an index should be used.
 * |indexes| pointer to the root index by rtup
 * |idx_ups| index upsert-ish callback to fire by rtup
 * |ht_transfer| local transfer array for HeapTuple
 * |htp_transfer| local transfer array for HeapTuplePtr
 * |htc_pipe_info| HTC Info for the pipe
 * |htc_pipe_cb| HTC to call once the rowset has been de-duplicated
 */ 
struct HTCIndexData
{
	struct HeapTupleCollectorInfoData htci;

	bool *use_index;
	void **indexes;
	HeapField **hfs;
	int32_t *nhfs;
	unsigned char **key_buffers;
	size_t *key_lens;

	vh_htc_idx_ups_cb *idx_ups;
	vh_htc_idx_destroy_cb *idx_destroy;

	HeapTuple *ht_transfer;
	HeapTuplePtr *htp_transfer;

	void *htc_pipe_info;
	vh_be_htc htc_pipe_cb;

	uint16_t htp_flags;
};

void vh_htc_idx(void *info, HeapTuple *hts, HeapTuplePtr *htps);

/*
 * vh_htc_idx_add_cb
 *
 * Function protoype for creating a unique index against a given set of
 * HeapField.  This allows the HTC to be index agnostic.
 *
 * The function should return a pointer to the index created along with
 * updating the vh_htc_idx_ups_cb and vh_htc_idx_destroy_cb pointers.
 */

void vh_htc_idx_init(void *info, int32_t rtups, vh_be_htc pipe_cb, void *pipe_info);
void vh_htc_idx_add(void *info, int32_t rtup, HeapField *hfs, int32_t nhfs);
void vh_htc_idx_destroy(void *info, bool include_indexes);

#endif

