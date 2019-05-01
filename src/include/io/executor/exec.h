/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_exec_H
#define vh_datacatalog_executor_exec_H

#include "io/executor/eresult.h"
#include "io/plan/popts.h"

typedef struct BackEndConnectionData *BackEndConnection;
typedef struct ExecPlanData *ExecPlan;

/*
 * The exec.h header is intended to be used when Nodes should be processed
 * outside of a transaction block.  All functions defined in exec.h provide
 * no transactional guarantee outside of the plan scope.  That is, all
 * operations will run autonomously without transaction commands unless
 * otherwise provided directly in the plan.  Commit actions will be run
 * and rollback operations will only take place if there is a fatal error
 * with the backends involved.
 */


/*
 * Execution Plan only, does not invoke the planner as it assumes the caller
 * already has to be passing a well formed EP.
 */
 
ExecResult vh_exec_ep(ExecPlan ep);



/*
 * Node only, allows a user to pass a well formed Node object and attempt to
 * plan and execute it within the current context.  Allows for Node objects to
 * be executed against a backend, outside of an XAct.  Will act as if in memory
 * objects are committed if succesful.  Does not open transaction scope on the 
 * BackEnds.  Thus any HeapTuple which is modified and flushed using the Node
 * only approach will be immediately visible on the BackEnd without rollback
 * support.  The on-commit action is typically only in VH and will be fired
 * if the statement is executed succesfully.
 */
 
ExecResult vh_exec_node(Node node);

#define vh_exec_node_shard(n, s)	vh_exec_node_opts(n, (PlannerOpts) { .shard = s } )
#define vh_exec_node_shardl(n, s)	vh_exec_node_opts(n, (PlannerOpts) { .shardlist = s } )


/*
 * Node with Planner Options only, same as Node only mode but allows a user 
 * to pass PlannerOpts
 */
 
ExecResult vh_exec_node_opts(Node node, PlannerOpts popts);

/*
 * vh_exec_query_str
 *
 * The down and dirty way to get a result set back.  Essentially all we do
 * is bolt on a very basic execution plan with late binding QRP and let it
 * rip against the back end connection provided.  Late binding QRP does all
 * the work of building an appropriate HeapTupleDef to hold the result set.
 */
#define vh_exec_query_str(bec, query)	vh_exec_query_str_param(bec, query, 0, 0)
ExecResult vh_exec_query_str_param(BackEndConnection bec, const char *query,
								   TypeVarSlot *params, int32_t n_params);

#endif

