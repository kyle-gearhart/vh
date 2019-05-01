/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_fetch_rel_H
#define vh_datacatlaog_executor_fetch_rel_h


/*
 * Fethes all relations for a given set of HeapTuplePtr.
 */

void vh_exec_fetchrel(HeapTuplePtr* htps, uint32_t htps_sz,
					  TableRel *rels, uint32_t rels_sz);



#endif

