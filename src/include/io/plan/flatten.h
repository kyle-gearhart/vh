/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_plan_flatten_H
#define vh_datacatalog_sql_plan_flatten_H

/*
 * vh_plan_flatten
 *
 * Flattens a NodeQuery structure, transforming library specific qual
 * representations into SQL compliant nodes.
 */

void vh_plan_flatten(NodeQuery nquery);

#endif

