/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_sql_executor_qrp_H
#define vh_datacatalog_sql_executor_qrp_H

typedef struct PlannedStmtData *PlannedStmt;
typedef struct PlannedStmtShardData *PlannedStmtShard;

struct QueryResultTableProjectionData
{
	TableDefVer rtdv;
	NodeFrom nfrom;
};

struct QueryResultFieldProjectionData
{
	HeapField hf;
	Type *tys;
	int32_t ty_depth;
	int8_t td_idx;
};

struct QueryResultBackEndProjectionData
{
	union TamSetUnion *tam_func;
	void **tam_formatters;
};

typedef struct QueryResultTableProjectionData *QrpTableProjection;
typedef struct QueryResultFieldProjectionData *QrpFieldProjection;
typedef struct QueryResultBackEndProjectionData *QrpBackEndProjection;

struct QueryResultProjectionData
{
	int32_t ntables;
	int32_t nfields;

	QrpTableProjection tables;			/* Indexed By |ntables| */
	QrpFieldProjection fields;			/* Indexed by |nfields| */
	QrpBackEndProjection backend;		/* Indexed by |nfields| */
};


#define VH_PLAN_QRP_TABLES		0x02
#define VH_PLAN_QRP_FIELDS		0x04
#define VH_PLAN_QRP_BACKEND		0x08

struct QueryResultProjectionData vh_plan_qrp(NodeQuery nquery,
											 BackEnd be,
											 bool flags);

int32_t vh_plan_qrp_lb(PlannedStmt pstmt);

void vh_plan_qrp_table_finalize(QrpTableProjection);
void vh_plan_qrp_field_finalize(QrpFieldProjection);
void vh_plan_qrp_be_finalize(QrpFieldProjection, QrpBackEndProjection);

#endif

