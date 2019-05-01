/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeCreateTable_H
#define vh_datacatalog_sql_nodes_NodeCreateTable_H

#include "io/nodes/NodeQuery.h"

typedef struct NodeCreateTableData
{
	NodeQueryData query;
	TableDef td;
	TableDefVer tdv;
	bool temporary;
} NodeCreateTableData, *NodeCreateTable;

NodeCreateTable vh_sqlq_ctbl_create(void);

void vh_sqlq_ctbl_tdv(NodeCreateTable nct, TableDefVer tdv);
void vh_sqlq_ctbl_td(NodeCreateTable nct, TableDef td);

#endif

