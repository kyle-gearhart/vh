/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_nodes_NodeQuery_H
#define vh_datacatalog_sql_nodes_NodeQuery_H

#include "io/nodes/Node.h"

#define VH_SQL_LOCKLEVEL_SZ 4

typedef enum QueryAction
{
	BulkInsert,
	Delete,
	Insert,
	Update,
	Select,
	DDLCreateTable
} QueryAction;

typedef enum ClusterPreference
{
	Master,
	Slave
} ClusterPreference;

typedef enum LockLevel
{
	LL_None = 0x00,
	LL_ForUpdate = 0x01,
	LL_ForUpdateNoKey = 0x02,
	LL_Share = 0x03,
	LL_KeyShare = 0x04
} LockLevel;

typedef enum LockMode
{
	LM_Wait = 0x00,
	LM_NoWait = 0x01
} LockMode;

typedef struct NodeQueryData
{
	struct NodeData node;
	QueryAction action;
	ClusterPreference clusterPref;
	bool hasTemporaryTables;
} *NodeQuery;



/*
 * Forward declare all possible members of a NodeQuery and it's children
 */
typedef struct NodeFieldData 	*NodeField;
typedef struct NodeUpdateFieldData *NodeUpdateField;
typedef struct NodeFromData 	*NodeFrom;
typedef struct NodeJoinData		*NodeJoin;
typedef struct NodeOrderByData	*NodeOrderBy;
typedef struct NodeQualData 	*NodeQual;


void* vh_sqlq_create(QueryAction action,
					 const struct NodeOpsFuncs *funcs,
					 size_t size);

/*
 * We assume DDL is not transactional, so we don't include it in _iswrite
 */
#define vh_sqlq_iswrite(nq) (nq->action != Select && \
							 nq->action != DDLCreateTable )
#define vh_sqlq_isddl(nq)	(nq->action == DDLCreateTable)
#define vh_sqlq_isread(nq)	(nq->action == Select)

#endif

