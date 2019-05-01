/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdarg.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarPage.h"
#include "io/catalog/acm/acm_impl.h"

struct acm_avg
{
	TypeVarAcmData acm;

	TypeVarOpExec op;
	TypeVarOpExec calcop;

	size_t typevar_sz;

	Type tys[VH_TAMS_MAX_DEPTH];
	int8_t ty_depth;
};

struct acm_avg_state
{
	int64_t count;

	/*
	TypeVar avg;
	*/
	struct TypeVarPageData page;
};

#define acms_avg_var(acms)			vh_tvp_varhead(&(acms)->page, 1)
#define acms_avg(acms)				vh_tvp_var(&(acms)->page, 1)




/*
 * ============================================================================
 * Average: TypeVarAcm Functions
 * ============================================================================
 */

static void acms_avg_initialize(TypeVarAcm, void*, size_t);
static void acms_avg_finalize(TypeVarAcm, TypeVarAcmState);

static int32_t acms_avg_input(TypeVarAcm, TypeVarAcmState, va_list args);
static int32_t acms_avg_result(TypeVarAcm, TypeVarAcmState, TypeVarSlot*);

static void acm_avg_finalize(TypeVarAcm);

static const struct TypeVarAcmFuncs acm_avg_funcs = {
	.acms_initialize = acms_avg_initialize,
	.acms_finalize = acms_avg_finalize,

	.input = acms_avg_input,
	.result = acms_avg_result,

	.finalize = acm_avg_finalize
};

/*
 * ============================================================================
 * Count: TypeVarAcm Functions
 * ============================================================================
 */

struct acm_count
{
	TypeVarAcmData acm;
};

struct acm_count_state
{
	int64_t count;
};


static void acms_count_initialize(TypeVarAcm, void*, size_t);
static void acms_count_finalize(TypeVarAcm, TypeVarAcmState);

static int32_t acms_count_input(TypeVarAcm, TypeVarAcmState, va_list args);
static int32_t acms_count_result(TypeVarAcm, TypeVarAcmState, TypeVarSlot*);

static void acm_count_finalize(TypeVarAcm);

static const struct TypeVarAcmFuncs acm_count_funcs = {
	.acms_initialize = acms_count_initialize,
	.acms_finalize = acms_count_finalize,

	.input = acms_count_input,
	.result = acms_count_result,

	.finalize = acm_count_finalize
};



/*
 * ============================================================================
 * Private Functions
 * ============================================================================
 */

static Type ty_int64[] = { &vh_type_int64, 0 };



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */
	
TypeVarAcm
vh_acm_avg_tys(Type *tys)
{
	struct acm_avg *acm;
	size_t typevar_sz, acms_sz;
	Type tys_accum[VH_TAMS_MAX_DEPTH];

	vh_type_stack_fillaccum(tys_accum, tys);

	typevar_sz = vh_tvp_space(tys_accum);
	vh_tvp_maxalign(typevar_sz);
	acms_sz = typevar_sz + sizeof(struct acm_avg_state);

	acm = vh_acm_create(sizeof(struct acm_avg), &acm_avg_funcs, acms_sz);

	acm->op = vh_typevar_op_init("+", VH_OP_MAKEFLAGS(VH_OP_DT_TYSVAR | VH_OP_DT_BOTH,
													  VH_OP_DT_TYSVAR,
													  VH_OP_ID_INVALID,
													  VH_OP_DT_TYSVAR,
													  VH_OP_ID_INVALID),
								 tys_accum, tys_accum, tys);

	acm->calcop = vh_typevar_op_init_tys("/", tys_accum, ty_int64, tys_accum);
	acm->ty_depth = vh_type_stack_copy(acm->tys, tys_accum);
	acm->typevar_sz = typevar_sz;

	assert(acm->calcop);
	assert(acm->ty_depth);

	if (acm->op)
		return &acm->acm;

	vhfree(acm);

	return 0;	
}

TypeVarAcm
vh_acm_count_tys(Type *tys)
{
	struct acm_count *count;
	size_t acms_sz;

	acms_sz = sizeof(struct acm_count_state);
	count = vh_acm_create(sizeof(struct acm_count), &acm_count_funcs, acms_sz);

	return &count->acm;
}



/*
 * ============================================================================
 * Average ACM Implementation
 * ============================================================================
 */

static void
acms_avg_initialize(TypeVarAcm tvacm, void *data, size_t sz)
{
	struct acm_avg *acm = (struct acm_avg*)tvacm;
	struct acm_avg_state *acms = data;

	vh_tvp_initialize(&acms->page, acm->typevar_sz);
	vh_tvp_add(&acms->page, acm->tys);
	
	acms->count = 0;
}

static void
acms_avg_finalize(TypeVarAcm tvacm, TypeVarAcmState tvacms)
{
	struct acm_avg_state *acms = (struct acm_avg_state*)tvacms;

	vh_tvp_finalize(&acms->page);
}

static int32_t
acms_avg_input(TypeVarAcm tvacm, TypeVarAcmState tvacms, va_list args)
{
	struct acm_avg *acm = (struct acm_avg*)tvacm;
	struct acm_avg_state *acms = (struct acm_avg_state*)tvacms;
	TypeVarSlot *slot;
	void *value;

	slot = va_arg(args, TypeVarSlot*);
	value = vh_tvs_value(slot);

	vh_typevar_op_fp(acm->op, acms_avg(acms), acms_avg(acms), value);
	acms->count++;

	return 0;
}

static int32_t
acms_avg_result(TypeVarAcm tvacm, TypeVarAcmState tvacms,
				TypeVarSlot *slot)
{
	struct acm_avg_state *acms = (struct acm_avg_state*)tvacms;
	struct acm_avg *acm = (struct acm_avg*)tvacm;
	void *res;

	if (acms->count > 0)
	{
		res = vh_typevar_op_fp(acm->calcop, acms_avg(acms), &acms->count);
		vh_tvs_store_var(slot, res, VH_TVS_RA_DVAR); 
	}
	else
	{
		vh_tvs_store_i32(slot, 0);
	}

	return 0;
}

static void
acm_avg_finalize(TypeVarAcm a)
{
	struct acm_avg *acm = (struct acm_avg*)a;

	if (acm->op)
	{
		vh_typevar_op_destroy(acm->op);
		acm->op = 0;
	}
}

/*
 * ============================================================================
 * Count ACM Implementation
 * ============================================================================
 */

static void
acms_count_initialize(TypeVarAcm tvacm, void *data, size_t sz)
{
	struct acm_count_state *acms = data;

	acms->count = 0;
}

static void
acms_count_finalize(TypeVarAcm tvacm, TypeVarAcmState tvacms)
{
	/*
	 * There's nothing to do here, as our only data member has no pointers.
	 */
}

static int32_t
acms_count_input(TypeVarAcm tvacm, TypeVarAcmState tvacms, va_list args)
{
	struct acm_count_state *acms = (struct acm_count_state*)tvacms;

	acms->count++;

	return 0;
}

static int32_t
acms_count_result(TypeVarAcm tvacm, TypeVarAcmState tvacms,
				TypeVarSlot *slot)
{
	struct acm_count_state *acms = (struct acm_count_state*)tvacms;
	
	vh_tvs_store_i64(slot, acms->count);

	return 0;
}

static void
acm_count_finalize(TypeVarAcm a)
{
	/*
	 * Again, nothing to do here.
	 */
}

