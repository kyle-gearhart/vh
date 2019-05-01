/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include "vh.h"
#include "io/nodes/NodeQuery.h"

void*
vh_sqlq_create(QueryAction action,
			   const struct NodeOpsFuncs *funcs,
			   size_t size)
{
	NodeQuery nquery;

	if (size >= sizeof(NodeQueryData))
	{
		nquery = vh_nsql_create(Query, funcs, size);
		nquery->action = action;
		nquery->hasTemporaryTables = false;
		nquery->clusterPref = Master;

		return nquery;	
	}
	else
	{
		elog(ERROR1,
			 emsg("Unable to create SQL query node object, requested size of "
				  "%d is less than the size of a base query object!",
				  size));
	}

	return 0;
}

