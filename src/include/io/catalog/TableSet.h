/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_catalog_TableSet_H
#define vh_datacatalog_catalog_TableSet_H

/*
 * TableSet (TS)
 *
 * We use a TableSet to nest complex table relationships.  These relationships
 * may leverage the existing relationships defined on the TableDef.  Relationships
 * may also be exclusive to the TableSet itself.
 *
 * Ideally we'll set a "root" table, define the relationships between each of the
 * children (we may nest recursively).  Then the user will pass in the SList
 * containing the records from the root table.  As the child tables are accessed,
 * we go out and fetch the dataset.  A caller may explicitly request one or all
 * relationships upfront.
 *
 * When we fetch the child relationships, we actually create a temporary table on
 * the datasource containing the child.  This allows for us to only pull back
 * the child records matching what's in the parent.
 * 
 * This methodology becomes very powerful when breaking enormous volumes of root
 * records into much smaller workloads.  Several worker threads could be spun up
 * to process the each batch of root records.
 */

typedef struct TableSetData *TableSet;

TableSet vh_ts_create(TableDef root, const char* root_alias);
bool vh_ts_destroy(TableSet ts);

bool vh_ts_root(TableSet ts, SList htps);
uint32_t vh_ts_maxdepth(TableSet ts);


/*
 * TableSetRel (TSR)
 *
 * Defines relationships between the tables contained in a TableSet.  The same
 * table may have more than one relationship defined.  When we fetch, we index
 * the primary key, so we're pointing to the same record in each relationship.
 */
typedef struct TableSetRelData *TableSetRel;

TableSetRel vh_tsr_root_create_child(TableSet ts, TableDef td, const char* child_alias);
TableSetRel vh_tsr_create_child(TableSetRel tsr_parent, TableDef td, const char* child_alias);
bool vh_tsr_push_qual(TableSetRel tsr, TableField tf_parent, TableField tf_child);

bool vh_tsr_fetch_all(TableSet ts);
bool vh_tsr_fetch(TableSetRel tsr);


/*
 * TableSetIter (TSI)
 *
 * Establish an iterator by calling vh_tsi_first or vh_tsi_last.  These
 * implicitly set the direction the iterator will travel.
 *
 * Call vh_tsi_next to iterate the root HeapTuple.  The iterator will
 * automatically nest each child relation.  The root HeapTuplePtr can be
 * accessed via vh_tsi_root_htp.  An SList of child HeapTuplePtr can be
 * retrieved by call vh_tsi_child_htps with the desired TableSetRel.
 *
 * Always destroy the TableSetIter by calling vh_tsi_destroy.
 */
typedef struct TableSetIterData *TableSetIter;

TableSetIter vh_tsi_first(TableSet ts);
TableSetIter vh_tsi_last(TableSet ts);
bool vh_tsi_next(TableSetIter tsi);
bool vh_tsi_previous(TableSetIter tsi);
HeapTuplePtr vh_tsi_root_htp(TableSetIter tsi);
SList vh_tsi_child_htps(TableSetIter tsi, TableSetRel tsr);
void vh_tsi_destroy(TableSetIter tsi);
size_t vh_tsi_root_count(TableSetIter tsi);
size_t vh_tsi_child_count(TableSetIter tsi, TableSetRel tsr);

/*
 * TableSetRelIter (TSRI)
 *
 * Establishes an iterator on a single relation.  Differs from the TSI by
 * allowing caller to only work one relationship.
 *
 * We have to ways of doing this, which passes only HeapTuplePtr to the caller,
 * expecting the caller to coordinate with the buffer about pinning and
 * unpinning of the HTP.  The second passes pinned HeapTuple to the callback
 * function.  It is resonsible for coordination with the buffer.
 *
 * The iteration will stop if the callback return false.
 */

typedef bool (*vh_tsri_htp_cb)(HeapTuplePtr htp_inner, 
							   HeapTuplePtr htp_outter,
							   void *cb_data);
typedef bool (*vh_tsri_ht_cb)(HeapTuple ht_inner, HeapTuplePtr htp_inner,
							  HeapTuple ht_outter, HeapTuplePtr htp_outter,
							  void *cb_data);

size_t vh_tsri_htp(TableSetRel tsr, vh_tsri_htp_cb cb, void *cb_data);
size_t vh_tsri_ht(TableSetRel tsr, vh_tsri_ht_cb, void *cb_data);


/*
 * TableSetPath (TSP)
 *
 * We give users the ability to fetch TableSetRel structures thru an XPath
 * like query string.  We've taught the TableSet how to cache its most
 * recent path inquiries for faster evaluation of the path.  Cache misses
 * cause us to spin up a tokenizer to step thru the tree.
 *
 * Use vh_tsp_root when the path is relative to the root or vh_tsp_rel when
 * relative to the provided TableSetRel. 
 */

TableSetRel vh_tsp_root(TableSet ts, String path, bool path_is_alias);
TableSetRel vh_tsp_rel(TableSet ts, TableSetRel tsr, String path, bool path_is_alias);

#endif

