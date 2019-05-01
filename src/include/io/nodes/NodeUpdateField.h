/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_nodes_NodeUpdateField_H
#define vh_nodes_NodeUpdateField_H

#include "io/catalog/TypeVarSlot.h"
#include "io/nodes/Node.h"

typedef struct NodeUpdateFieldData
{
	struct NodeData node;

	TableField tf;
	TypeVarSlot tvs;
} NodeUpdateFieldData, *NodeUpdateField;

NodeUpdateField vh_nsql_updfield_create(TableField tf, HeapTuplePtr htp);

#endif

