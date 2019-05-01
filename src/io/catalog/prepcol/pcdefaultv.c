/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/prepcol/pcdefaultv.h"

/*
 * ============================================================================
 * PrepCol Function Table Declaration
 * ============================================================================
 */

static int32_t pcdv_populate_slot(void *pc, TypeVarSlot *target,
								  TypeVarSlot **datas, int32_t ndatas);
static int32_t pcdv_finalize(void *pc);


static const struct PrepColFuncTableData pcdv_func = {
	.populate_slot = pcdv_populate_slot,	
	.finalize = pcdv_finalize
};



/*
 * ============================================================================
 * PrepCol Default Value Structure
 * ============================================================================
 */

typedef struct pcdv_data pcdv_data;

struct pcdv_data
{
	struct PrepColData pc;

	void *value;
	TypeVarOpExec tvope;

	bool null_only;
	bool cache;
};



/*
 * ============================================================================
 * PrepCol Public Interface
 * ============================================================================
 */

PrepCol
vh_pc_defaultv_create(void *typevar, bool is_null)
{
	pcdv_data *pcdv;

	if (!typevar)
	{
		elog(WARNING,
				emsg("Invalid TypeVar pointer [%p] passsed to "
					 "vh_pc_defaultv_create.  Unable to create a Default Value "
					 "PrepCol as requested.",
					 typevar));

		return 0;
	}

	pcdv = vh_pc_create(&pcdv_func, sizeof(pcdv_data));
	pcdv->null_only = is_null;
	pcdv->value = vh_typevar_makecopy(typevar);
	pcdv->cache = true;
	
	return &pcdv->pc;
}

/*
 * ============================================================================
 * PrepCol Default Value Implementation
 * ============================================================================
 */
static int32_t 
pcdv_populate_slot(void *pc, TypeVarSlot *target,
				   TypeVarSlot **datas, int32_t ndatas)
{
	static const int32_t op_flags = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
													VH_OP_DT_TVS, VH_OP_ID_INVALID,
										   			VH_OP_DT_VAR, VH_OP_ID_INVALID);
	pcdv_data *pcdv = pc;
	MemoryContext mctx_tgt, mctx_old;
	bool target_isnull;

	/*
	 * We'll assert here just to make sure our callers get this right.  There
	 * should be no ndatas here.
	 */
	assert(!ndatas);

	target_isnull = vh_tvs_isnull(target);

	if ((pcdv->null_only && target_isnull) ||
		!pcdv->null_only)		
	{
		if (pcdv->cache && !pcdv->tvope)
		{
			/*
			 * We need to make sure we're in the same Memory Context as our
			 * PrepCol, otherwise things could go haywire on us.
			 */
			mctx_tgt = vh_mctx_from_pointer(pc);
			mctx_old = vh_mctx_switch(mctx_tgt);

			pcdv->tvope = vh_typevar_op_init("=", op_flags, target, pcdv->value);

			vh_mctx_switch(mctx_old);
		}
		
		if (pcdv->tvope)
		{
			vh_typevar_op_fp(pcdv->tvope, target, pcdv->value);
		}
		else
		{
			vh_typevar_op("=", op_flags, target, pcdv->value);
		}

		return 1;
	}

	return 0;
}


static int32_t 
pcdv_finalize(void *pc)
{
	pcdv_data *pcdv = pc;

	if (pcdv->value)
	{
		vh_typevar_destroy(pcdv->value);
		pcdv->value = 0;
	}

	if (pcdv->tvope)
	{
		vh_typevar_op_destroy(pcdv->tvope);
		pcdv->tvope = 0;
	}

	return 0;
}

