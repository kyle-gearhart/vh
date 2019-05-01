/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeOrderBy_H
#define vh_datacatalog_sql_nodes_NodeOrderBy_H

#include "io/nodes/Node.h"

typedef struct NodeFieldData *NodeField;
typedef struct NodeFromData *NodeFrom;

typedef enum OrderOperator
{
	Ascending,
	Descending
} OrderOperator;

typedef enum OrderRangeFlags
{
	Order_Scalar,
	Order_RangeLower,
	Order_RangeUpper
} OrderRangeFlags;

typedef struct NodeOrderByData
{
	struct NodeData node;
	NodeField nfield;
	TableField tfield;
	OrderOperator oop;
	OrderRangeFlags orflags;
} *NodeOrderBy;

NodeOrderBy vh_nsql_orderby_create(void);

#endif

