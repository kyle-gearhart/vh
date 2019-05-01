/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_executor_estep_run_H
#define vh_datacatalog_executor_estep_run_H

#include "io/executor/eplan.h"

/*
 * Execution Step Runner
 *
 * The user should first call vh_es_open to create an ExecStateData
 * structure in the current memory context.  Other details such as
 * the ConnectionCatalog will be pulled from the CatalogContext.
 *
 * Users may then invoke vh_es_runtree which will recursively drop
 * to the bottom, left-most item in the tree and begin executing back
 * up the stack.
 *
 * In the event the vh_es_open is called once followed by multiple
 * invocations of vh_es_runtree, the user should call vh_es_reset
 * between each subsequent invocation of vh_es_runtree.  This resets
 * the ExecState, primarily destroying the working context and
 * creating a new one.  The executor generally depends on working
 * context destruction rather than vhfree to cleanup memory.
 *
 * After all invocations of vh_es_runtree have been called, the user
 * may call vh_es_close. 
 */

typedef struct ExecStateData
{
	ExecPlan ep;
	ConnectionCatalog cc;
	MemoryContext mctx_work;
	MemoryContext mctx_result;
	ExecResult er;
} *ExecState;

ExecState vh_es_open(void);
void vh_es_reset(ExecState estate);
void vh_es_close(ExecState estate);

int32_t vh_es_runtree(ExecState estate, ExecStep root);

#endif

