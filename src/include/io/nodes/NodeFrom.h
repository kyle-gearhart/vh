/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeFrom_H
#define vh_datacatalog_sql_nodes_NodeFrom_H

#include "io/nodes/NodeQuery.h"

typedef struct NodeFromData
{
	struct NodeData node;
	TableDefVer tdv;
	String alias;
	SList htps;

	/*
	 * For transient tables only
	 */
	String transient_schema;
	String transient_table;
	
	LockLevel lock_level;
	LockMode lock_mode;
	
	uint16_t result_index;
	bool transient; 	// indicates if the table definition isn't stored in memory
	bool temporary;
	bool result; // indicates if the table has atleast one field in the SELECT
	
} *NodeFrom;

NodeFrom vh_nsql_from_create(void);
void vh_nsql_from_init(NodeFrom nf);
void vh_nsql_from_append_queryname(NodeFrom nfrom, String target);

#endif

