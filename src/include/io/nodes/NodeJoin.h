/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeJoin_H
#define vh_datacatalog_sql_nodes_NodeJoin_H

#include "io/nodes/NodeFrom.h"

typedef enum JoinType
{
	Inner,
	Left
} JoinType;

typedef struct NodeJoinData
{
	struct NodeData node;
	bool autoFormQuals;
	bool selectFields;
	JoinType join_type;
	struct NodeFromData join_table;
	struct NodeData quals;
} *NodeJoin;

NodeJoin vh_nsql_join_create(void);
NodeQual vh_nsql_join_qual_addtf(NodeJoin, TableField, TableField);

#endif

