/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_exec_estep_H
#define vh_datacatalog_sql_exec_estep_H

typedef struct PlannedStmtData *PlannedStmt;
typedef struct PlannedStmtShardData *PlannedStmtShard;

typedef struct ExecStepData *ExecStep;

/*
 * ExecStep represents the steps the executor must take to gather the desired
 * resultset from the back ends.  The planner forms the ExecStep tree.  The
 * executor will work to the bottom right most node recursively to generate
 * the results required.
 */

enum ExecStepTag
{
	EST_CommitHeapTups,
	EST_Discard,
	EST_Fetch,
	EST_Funnel
};

struct ExecStepData
{
	enum ExecStepTag tag;
	ExecStep parent;
	ExecStep fwd;
	ExecStep child;
	ExecStep sibling;
};


/*
 * ExecStepDiscard
 *
 * Executes a planned statement and does not collect the result from the
 * back end.  If the back end's client library requires us to collect it
 * in the client API, we'll do so, but immediately free it.  No attempt
 * will be made to fill new HeapTuple from the result set.  This is often
 * helpful for SELECT ... FOR UPDATE statements.
 */
struct ExecStepDiscardData
{
	struct ExecStepData es;
	PlannedStmt pstmt;
	PlannedStmtShard pstmtshd;
};



/*
 * ExecStepFetch
 *
 * Executes a planned statement, forms HeapTuple, and collects the resulting
 * HeapTuplePtr.  If an ExecStepFunnel is the parent, the results will be
 * immediately forwarded to the ExecStepFunnel.
 */
struct ExecStepFetchData
{
	struct ExecStepData es;
	PlannedStmt pstmt;
	PlannedStmtShard pstmtshd;

	/*
	 * Indexing hints to be set by the planner: these are guaranteed to be 
	 * PlannedStmt->rtups wide if initialized by the vh_est_fetch_idx_init
	 * function described below.
	 * |nhfs|	Count of HeapFields to index per slot
	 * |hfs|	HeapFields to index per slot
	 *
	 * Call vh_est_fetch_idx_init with the number of return tuples, which
	 * will allocate sufficient slots.  This will allocate everything in the
	 * current memory context.  The executor may 
	 *
	 * Indexes may then be added by calling vh_est_fetch_idx_add which will
	 * allow HeapField to be pushed onto the index.
	 */
	int32_t *nhfs;
	HeapField **hfs;

	SList tups;
	HeapBufferNo hbno;

	bool indexed;
	bool returning;
};

void vh_est_fetch_idx_init(struct ExecStepFetchData *esf, uint32_t rtups);
void vh_est_fetch_idx_add(struct ExecStepFetchData *esf, uint32_t rtup,
						  uint32_t nhfs);
void vh_est_fetch_idx_push(struct ExecStepFetchData *esf, uint32_t rtup,
						   HeapField hf);


/*
 * ExecStepFunnel
 *
 * Absorbs all results from the ExecStepFetch child and it's siblings into a
 * list of HeapTuplePtr.  Does not make any attempt to eliminate duplicates.
 */
struct ExecStepFunnelData
{
	struct ExecStepData es;
	
	SList tups;
	HeapBufferNo hbno;
};

typedef void (*vh_es_visit_tree_func)(ExecStep, void*);

void* vh_es_create(enum ExecStepTag tag, ExecStep parent);
void vh_es_visit_tree(ExecStep root,
					  vh_es_visit_tree_func[2],
					  void*[2]);

void vh_es_child_lappend(ExecStep parent, ExecStep estep);

Shard vh_es_shard(ExecStep step);
SList vh_es_shard_unique(ExecStep step);

bool vh_es_pstmt(ExecStep estep, 
				 PlannedStmt *pstmt, PlannedStmtShard *pstmtshd);

#endif

