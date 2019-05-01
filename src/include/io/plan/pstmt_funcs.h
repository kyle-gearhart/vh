/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_plan_pstmt_funcs_H
#define vh_datacatalog_plan_pstmt_funcs_H

/*
 * vh_pstmtshd_generate:
 * 	1)	shardrep.h		--- vh_plan_sr_create
 * 	2)	BackEnd.h		--- form_command
 * 	3)	exec/param.h	---	vh_param_createlist_ptm	
 *	4)	shardrep.h		--- vh_plan_sr_restore
 */

#include "io/plan/pstmt.h"

PlannedStmt vh_pstmt_generate_from_query(NodeQuery nq, BackEnd be);
PlannedStmtShard vh_pstmtshd_generate(PlannedStmt pstmt, Shard shd, ShardAccess shda);

bool vh_pstmt_generate_from_query_str(BackEndConnection bec,
									  const char *query,
									  PlannedStmt *pstmt,
									  PlannedStmtShard *pstmts,
									  TypeVarSlot *params,
									  int32_t n_params);

void vh_pstmt_init(PlannedStmt pstmt);
void vh_pstmtshard_init(PlannedStmtShard pstmts);

int32_t vh_pstmt_qrp(PlannedStmt pstmt);
int32_t vh_pstmt_qrp_be(PlannedStmt pstmt, 
						QrpTableProjection qrpt, QrpFieldProjection qrpf,
						int32_t n_qrpt, int32_t n_qrpf);
int32_t vh_pstmtshd_generate_params(PlannedStmt pstmt, PlannedStmtShard pstmtshd);

/*
 * Planned Statement Late Binding Columns
 *
 * Late binding allows for us to run a query without knowning the table structure
 * during the planning phase.  vh_pstmt_is_lb indicates if the PlannedStmt features
 * late binding of the column definition.
 *
 * Each back end should check vh_pstmt_lb_do_add_col flag after it has received a 
 * result set and a late binding scenario exists.  When the flag is set, for each 
 * column in the result the back end should call vh_pstmt_lb_add_col.
 *
 * When vh_pstmt_is_lb is set, the back end should always call vh_pstmt_lb_qrp.
 */

#define vh_pstmt_is_lb(pstmt) 			(pstmt->latebinding)
#define vh_pstmt_lb_do_add_col(pstmt)	(pstmt->latebinding && !pstmt->latebindingset)


int32_t vh_pstmt_lb_add_col(PlannedStmt pstmt, const char *fname, Type *tys);
int32_t vh_pstmt_lb_qrp(PlannedStmt pstmt);


#endif

