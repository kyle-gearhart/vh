/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_plan_plan_H
#define vh_datacatalog_plan_plan_H

#include "io/plan/popts.h"

/*
 * Takes a root node, forms an ExecutionPlan that can be executed.
 */

ExecPlan vh_plan_node_opts(Node root, PlannerOpts opts);
#define vh_plan_node(nquery)	vh_plan_node_opts(nquery, (PlannerOpts) {})

#endif

