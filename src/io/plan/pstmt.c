/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/plan/pstmt.h"
#include "io/plan/qrp.h"

void
vh_pstmt_finalize(PlannedStmt pstmt)
{
	int32_t i;
	QrpTableProjection qrpt;
	QrpFieldProjection qrpf;
	QrpBackEndProjection qrpbe;

	qrpt = pstmt->qrp_table;
	qrpf = pstmt->qrp_field;
	qrpbe = pstmt->qrp_backend;

	if (qrpf && qrpbe)
	{
		for (i = 0; i < pstmt->qrp_nfields; i++)
		{
			vh_plan_qrp_be_finalize(&qrpf[i], &qrpbe[i]);

			if (pstmt->finalize_qrp)
				vh_plan_qrp_field_finalize(&qrpf[i]);
		}

		vhfree(pstmt->qrp_field);
		vhfree(pstmt->qrp_backend);

		pstmt->qrp_field = 0;
		pstmt->qrp_backend = 0;
	}

	if (qrpt && pstmt->finalize_qrp)
	{
		for (i = 0; i < pstmt->qrp_ntables; i++)
			vh_plan_qrp_table_finalize(&qrpt[i]);

		vhfree(pstmt->qrp_table);
		pstmt->qrp_table = 0;
	}
}

void
vh_pstmts_finalize(PlannedStmtShard pstmts)
{
}

