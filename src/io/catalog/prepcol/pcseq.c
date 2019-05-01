/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */

#include <assert.h>

#include "vh.h"

#include "io/catalog/TypeVar.h"
#include "io/catalog/prepcol/pcseq.h"

/*
 * ============================================================================
 * PrepCol Implementation Declaration
 * ============================================================================
 */

static int32_t pcseq_populate_slot(void *pc, TypeVarSlot *slot,
								   TypeVarSlot **datas, int32_t ndatas);
static int32_t pcseq_finalize(void *pc);

static const struct PrepColFuncTableData pcseq_func = {
	.populate_slot = pcseq_populate_slot,
	.finalize = pcseq_finalize
};


/*
 * ============================================================================
 * PrepCol Sequence Data Structures
 * ============================================================================
 */

typedef struct pcseq_data pcseq_data;

struct pcseq_data
{
	struct PrepColData pc;

	void *base;

	TypeVarOpExec tvope_ass;
	TypeVarOpExec tvope_inc;
};

/*
 * ============================================================================
 * Helper Functions
 * ============================================================================
 */

static TypeVarOpExec make_tvope_ass(TypeVar *tv, TypeVarSlot *tvs);
static TypeVarOpExec make_tvope_inc(TypeVar *tv, TypeVarSlot *tvs);

/*
 * ============================================================================
 * Public Functions
 * ============================================================================
 */

PrepCol
vh_pcseq_create(TypeVar base)
{
	pcseq_data *pcseq;

	pcseq = vh_pc_create(&pcseq_func, sizeof(struct pcseq_data));

	if (base)
	{
		pcseq->base = vh_typevar_makecopy(base);
		pcseq->tvope_inc = make_tvope_inc(base, 0);
	}
	else
	{
		pcseq->base = 0;
		pcseq->tvope_inc = 0;
	}

	pcseq->tvope_ass = 0;

	return &pcseq->pc;
}


/*
 * ============================================================================
 * PrepCol Implementation
 * ============================================================================
 */
static int32_t 
pcseq_populate_slot(void *pc, TypeVarSlot *slot,
					TypeVarSlot **datas, int32_t ndatas)
{
	const int32_t flags = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
										  VH_OP_DT_VAR,
										  VH_OP_ID_INVALID,
										  VH_OP_DT_I32,
										  VH_OP_ID_INVALID);

	pcseq_data *pcseq = pc;

	/*
	 * Check for an existing base, if we don't have one, we need to create any
	 * empty one using the slot Type stack and then go from there.
	 */
	if (!pcseq->base)
	{
		pcseq->base = vh_typevar_make_tvs(slot);
		(void)vh_typevar_op("=", flags, pcseq->base, 1);
	}

	/*
	 * If we don't have an assignment operator, let's try and create one.  If we
	 * don't get a match, we should return -1 to indicate to the caller there's
	 * something seriously wrong.
	 */
	if (!pcseq->tvope_ass &&
		(pcseq->tvope_ass = make_tvope_ass(pcseq->base, slot)) == 0)
	{
		return -1;
	}

	/*
	 * We're guaranteed to have an Assignment at this point.  Fire it and do
	 * the increment behind it.
	 */
	(void)vh_typevar_op_fp(pcseq->tvope_ass, slot, pcseq->base);
	(void)vh_typevar_op_fp(pcseq->tvope_inc, pcseq->base);
	
	return 0;
}

static int32_t
pcseq_finalize(void *pc)
{
	pcseq_data *pcseq = pc;

	if (pcseq->base)
		vh_typevar_destroy(pcseq->base);

	if (pcseq->tvope_ass)
		vh_typevar_op_destroy(pcseq->tvope_ass);

	if (pcseq->tvope_inc)
		vh_typevar_op_destroy(pcseq->tvope_inc);

	/*
	 * We must reset our pointers to zero, finalize should leave the data structure
	 * in a state that can be resumed later without the risk of dangling pointers.
	 */

	pcseq->base = 0;
	pcseq->tvope_ass = 0;
	pcseq->tvope_inc = 0;

	return 0;
}


/*
 * make_tvope_ass
 *
 * Create the assignment operator.  This is a little funky because what we're
 * assiging to is the TypeVarSlot that's passed thru populate_slot.
 */

static TypeVarOpExec
make_tvope_ass(TypeVar *tv, TypeVarSlot *tvs)
{
	const int32_t flags = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
										  VH_OP_DT_TVS,
										  VH_OP_ID_INVALID,
										  VH_OP_DT_VAR,
										  VH_OP_ID_INVALID);

	return vh_typevar_op_init("=", flags, tvs, tv);
}

/*
 * make_tvope_inc
 *
 * All we want to do is make the increment function, the only trick is that
 * we take either a TypeVar or a TypeVarSlot.  We show preference to a non-null
 * TypeVar.  Thus, is a non-null TypeVar and TypeVarSlot are passed, only the
 * TypeVar will be used for the increment operation.  It's critical we get
 * a valid TypeVar with a TypeStack, so that we can do the increments.
 */

static TypeVarOpExec 
make_tvope_inc(TypeVar *tv, TypeVarSlot *tvs)
{
	const int32_t flags = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
		   								  (tv ? VH_OP_DT_VAR : VH_OP_DT_TVS),
									  	  VH_OP_ID_INVALID,
										  VH_OP_DT_INVALID,
										  VH_OP_ID_INVALID);
	void *data = (tv ? (void*)tv : (void*)tvs);

	return vh_typevar_op_init("++", flags, data);
}

