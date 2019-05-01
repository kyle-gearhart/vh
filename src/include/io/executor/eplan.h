/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_eplan_H
#define vh_datacatalog_executor_eplan_H

#include "io/executor/estep.h"
#include "io/executor/htc.h"

/*
 * The ExecPlan is formed by the planner.  A user generated
 * query may cause several actions to be executed against a series
 * of back ends to maintain the contracts provided by VH.  Actions
 * may include table partition creation and maintenance, beacon 
 * maintenance, cross shard query combinations.  After a plan is
 * created, the XAct manager will use the top level |nquery| 
 * structure to determine the appropriate time execute all of
 * the PreparedStatement contained in |statements| serially.
 *
 * Thus it is up to the planner to form the prepared statements
 * in the correct order to maintain transactional integrity.
 *
 * A MemoryContext is formed so that all allocations expected
 * to live thru the lifetime of the top level XAct disposition
 * (COMMIT or ROLLBACK) can be destroyed without individual 
 * vhfree calls.  Any allocations which are not required for this 
 * lifetime should be managed within the call stack.
 *
 * The ExecPlan also contains details about how the result
 * sets from the individual queries against a shard are combined
 * to form a single resultset for the user.  This is useful if a
 * SELECT query must go across multiple shards or even beacons,
 * to form the resulset desired by the user.
 *
 * Statistics are captured by the backend to record duration
 * expressed in nanoseconds.
 *
 * 	|stat_htform|	duration of HeapTuple formation
 * 	|stat_qexec|	duration of back end execution
 */
 
typedef struct ExecStepData *ExecStep;
 
typedef struct ExecPlanData
{
	SList shards;
	ExecStep plan;
	ExecStep on_commit;
	ExecStep on_rollback;
	MemoryContext mctx_ep;
	MemoryContext mctx_result;

	uint32_t stat_htform;
	uint32_t stat_qexec;

	bool conns_put;
} *ExecPlan;


ExecPlan vh_exec_eplan_create(void);
void vh_exec_eplan_destroy(ExecPlan ep);
bool vh_exec_eplan_ready(ExecPlan ep);

#define vh_exec_eplan_hasplan(ep)		(ep->plan ? true : false)
#define vh_exec_eplan_hascommit(ep)		(ep->on_commit ? true : false)
#define vh_exec_eplan_hasrollback(ep)	(ep->on_rollback ? true : false)
#define vh_exec_eplan_hasfulltxact(ep)	(vh_exec_eplan_hasplan(ep) && \
										 vh_exec_eplan_hascommit(ep) && \
										 vh_exec_eplan_hasrollback(ep))


/*
 * BackEndExecPlanData
 *
 * Communications structure to streamline the calling convention to the
 * back end.  The goal is to pass the PlannedStmt, PlannedStmtShard, a
 * memory context to work within, HeapTupleCollectorInfo and some statistics
 * the back end will populate.
 */

typedef struct BackEndExecPlanData
{
	PlannedStmt pstmt;
	PlannedStmtShard pstmtshd;
	MemoryContext mctx_work;
	MemoryContext mctx_result;
	HeapTupleCollectorInfo htc_info;

	uint32_t stat_htform;
	uint32_t stat_qexec;
	int64_t stat_wait_count;

	bool discard;
} *BackEndExecPlan;


/*
 * ExecPlan NodeConnection Management
 */

SList vh_exec_eplan_putconns(ExecPlan ep, ConnectionCatalog cc,
		   					KeyValueMap kvm);

void vh_exec_eplan_relconns(ExecPlan ep, ConnectionCatalog cc,
							KeyValueMap kvm_exclude);

#endif

