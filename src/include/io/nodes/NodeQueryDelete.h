/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeQueryDelete_H
#define vh_datacatalog_sql_nodes_NodeQueryDelete_H

#include "io/nodes/NodeQuery.h"

typedef struct NodeQueryDeleteData
{
	struct NodeQueryData query;
	NodeFrom from;
	Node quals;
} *NodeQueryDelete;


NodeQueryDelete vh_sqlq_del_create(void);

NodeFrom vh_sqlq_del_table(NodeQueryDelete nqdel, TableDef td);
NodeQual vh_sqlq_del_qual(NodeQueryDelete nqdel, Node *qual_list);

#endif

