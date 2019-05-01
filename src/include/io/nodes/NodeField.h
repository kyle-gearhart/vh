/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeField_H
#define vh_datacatalog_sql_nodes_NodeField_H


#include "io/nodes/Node.h"

typedef struct NodeFromData *NodeFrom;
typedef struct NodeJoinData *NodeJoin;

typedef struct NodeFieldData
{
	struct NodeData node;
	TableField tf;
	
	union
	{
		NodeFrom nfrom;
		NodeJoin njoin;
	};

	String alias;

	bool wildcard;
	bool isjoin;
} *NodeField;

NodeField vh_nsql_field_create(void);
String vh_nsql_field_queryname(NodeField nfield);
void vh_nsql_field_append_queryname(NodeField nfield, String target, bool fq);

#endif

