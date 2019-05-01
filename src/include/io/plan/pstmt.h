/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_plan_pstmt_H
#define vh_datacatalog_plan_pstmt_H

#include "io/nodes/NodeQuery.h"
#include "io/plan/qrp.h"

/*
 * PreparedStatementData will be changing so that multiple
 * shards can be queried using a single statement.  An SList
 * will be added that contains a struct called
 * PreparedStatementShardData.  XAct will then be taught how
 * to iterate the PreparedStatementShardData struct to run
 * the executor to aggregate result sets from multiple shards.
 *
 * The Preparer is responsible for creating the
 * PreparedStatementShardData struct(s) and inserting them
 * into the SList since it has visibility to the Beacon
 * facility.
 *
 * This allows for cross-shard queries to occur.
 *
 * The preparer will be taught to temporarily rewrite the 
 * quals so that only records for the given shard are queried
 * for.  This will be achieved by using an SList of
 * PreparedStatementQualRewriteData.
 */

typedef struct BackEndConnectionData *BackEndConnection;
typedef struct NodeFieldData *NodeField;
typedef struct ParameterListData *ParameterList;
typedef struct PlannedStmtData *PlannedStmt;
typedef struct PlannedStmtShardData *PlannedStmtShard;
typedef struct ShardData *Shard;
typedef struct ShardAccessData *ShardAccess;
union TamSetUnion;

struct PlannedStmtShardData
{
	String command;	
	Shard shard;
	ShardAccess sharda;
	ParameterList parameters;
	int32_t paramcount;

	BackEndConnection nconn;
};


/*
 * We need to change this so the command is down on the Shard layer.  It could
 * vary between shards depending on the flattening that must occur.  There's not
 * a need to store the parameter count any longer either.  The parameter list will
 * do this for us.
 */
struct PlannedStmtData
{
	NodeQuery nquery;
	BackEnd be;

	int32_t qrp_ntables;
	int32_t qrp_nfields;
	QrpTableProjection qrp_table;
	QrpFieldProjection qrp_field;
	QrpBackEndProjection qrp_backend;

	bool finalize_qrp;
	bool latebinding;
	bool latebindingset;

	SList shards;

	XAct xact;
};

#define vh_pstmt_iswrite(pstmt)	(vh_sqlq_iswrite(pstmt->nquery))
#define vh_pstmt_isddl(pstmt)	(vh_sqlq_isddl(pstmt->nquery))
#define vh_pstmt_isread(pstmt)	(vh_sqlq_isread(pstmt->nquery))

void vh_pstmt_finalize(PlannedStmt pstmt);
void vh_pstmts_finalize(PlannedStmtShard pstmts);

#endif

