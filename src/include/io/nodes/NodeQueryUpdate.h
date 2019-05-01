/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeQueryUpdate_H
#define vh_datacatalog_sql_nodes_NodeQueryUpdate_H


#include "io/nodes/NodeQuery.h"

typedef struct NodeQueryUpdateData
{
	struct NodeQueryData query;
	Node nfields;
	NodeFrom nfrom;
	Node nquals;

	HeapTuplePtr htp;
} *NodeQueryUpdate;

NodeQueryUpdate vh_sqlq_upd_create(void);

NodeFrom vh_sqlq_upd_from(NodeQueryUpdate nqupd, TableDef td);
void vh_sqlq_upd_qual_add(NodeQueryUpdate nqupd, NodeQual nqual);
void vh_sqlq_upd_field_add(NodeQueryUpdate nqupd, NodeUpdateField nuf);
void vh_sqlq_upd_htp(NodeQueryUpdate nqupd, HeapTuplePtr htp);
void vh_sqlq_upd_htpl(NodeQueryUpdate nqupd, SList htps);

#endif

