/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_executor_esg_H
#define vh_datacatalog_sql_executor_esg_H

typedef struct ExecStepData *ExecStep;
typedef struct PlanTreeData *PlanTree;
typedef struct ShardHeapTupleMapData *ShardHeapTupleMap;

/*
 * The Execution Step Group (ESG) is formed exclusively by internal planner
 * routines.  The ESG is intended to be merged into a larger ExecutionPlan.
 *
 * The ESG interface allows for the planner to break a single query into
 * smaller queries.  The executor then becomes responsible for merging
 * multiple result sets together.
 *
 * All NodeQuery tree's are routed to a corresponding submodule based on
 * the query's action flag. 
 *
 * Each submodule has a set of strategies is may deploy to complete the
 * original statement.  The submodule is attempts to select the optimal
 * strategy based on the ExecStepGroupOpts, properties of the NodeQuery tree,
 * and the desired top level ExecStep.
 *
 * This allows for an UPDATE query to be broken down into five operations
 * by the esg_upd.c submodule:
 * 	1)	Create temporary table on one or more shards
 * 	2)	Select for update using the temporary table on one or more shards
 * 	3)	Delete from actual table using temporary table as the qual
 * 	4)	Bulk insert to actual table
 * 	5)	Drop temporary table
 *
 * The primary entry point for plan.c is the vh_esg_generate.  This module
 * will simply route the NodeQuery submitted to the appropriate esg
 * submodule to do the heavy lifting of forming the ESG.  The submodule
 * may recursively call vh_esg_generate which may be routed to other
 * submodules to complete the query.  In the update example provided above,
 * esg_upd.c will indirectly invoke esg_ddl.c, esg_sel.c, esg_del.c,
 * esg_ins.c, and esg_ddl.c respectively via vh_esg_generate.
 *
 * This header also includes non-static functions which are used across the
 * five primary ESG modules: 
 * 	1)	esg_del.c	DELETE
 * 	2)	esg_ddl.c	CREATE/DROP TABLE
 * 	3)	esg_ins.c	INSERT
 * 	4)	esg_sel.c	SELECT
 * 	5)	esg_upd.c	UPDATE
 */

typedef struct ExecStepGroupData
{
	ExecStep top;
	ExecStep bottom;
	uint16_t depth;

	struct
	{
		ExecStep top;
		ExecStep bottom;
		uint16_t depth;
	} on_rollback;

	struct
	{
		ExecStep top;
		ExecStep bottom;
		uint16_t depth;
	} on_commit;

} *ExecStepGroup;

typedef struct ExecStepGroupOpts
{
	PlanTree pt;
	ShardHeapTupleMap shtm;

	bool detect_shards;

	HeapBufferNo hbno;

	Shard shard;
	Shard *shard_head;
	uint32_t shard_sz;
} *ExecStepGroupOpts;

ExecStepGroup vh_esg_generate(ExecStepGroupOpts opts,
							  NodeQuery nquery,
							  enum ExecStepTag est_hint);

/*
 * Common routines shared between all ESG submodules.  These are exposed here
 * but should not be called outside of the ESG submodules.
 */

ExecStepGroup vh_esg_create(void);
void vh_esg_addstep(ExecStepGroup esg, ExecStep es);
void vh_esg_addsibling(ExecStep tree, ExecStep sibling);
bool vh_esg_valid(ExecStepGroup esg);

void vh_esg_destroy(ExecStepGroup esg);

#endif

