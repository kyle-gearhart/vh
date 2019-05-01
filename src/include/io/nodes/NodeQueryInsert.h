/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeQueryInsert_H
#define vh_datacatalog_sql_nodes_NodeQueryInsert_H

#include "io/nodes/NodeQuery.h"

typedef struct NodeQueryInsertData
{
	struct NodeQueryData query;
	Node fields;
	Node rfields;			/* Fields we expect the backend to provide back */
	NodeFrom into;
} NodeQueryInsertData, *NodeQueryInsert;

/*
 * SQL Insert Query Manipulation Functions
 */
NodeQueryInsert vh_sqlq_ins_create(void);

NodeField vh_sqlq_ins_field_add(NodeQueryInsert nqins, NodeFrom nfrom, HeapField hf);
NodeField vh_sqlq_ins_rfield_add(NodeQueryInsert nqins, NodeFrom nfrom, HeapField hf);
NodeFrom vh_sqlq_ins_table(NodeQueryInsert nqins, TableDef td);
NodeQual vh_sqlq_ins_qual(NodeQueryInsert nqins, Node *qual_list);
void vh_sqlq_ins_htp(NodeQueryInsert nqins, HeapTuplePtr);
void vh_sqlq_ins_htp_list(NodeQueryInsert nqins, SList);

#endif

