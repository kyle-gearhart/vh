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

struct acm_sum
{
	TypeVarAcmData acm;
	TypeVarOpExec op;

	size_t typevar_sz;

	Type tys[VH_TAMS_MAX_DEPTH];
	int8_t ty_depth;
};

struct acm_sum_state
{
	struct TypeVarPageData page;
};

#define acms_sum_var(acms)		vh_tvp_varhead(&(acms)->page, 1)
#define acms_sum(acms)			vh_tvp_var(&(acms)->page, 1)


/*
 * ============================================================================
 * TypeVar ACM Declarations
 * ============================================================================
 */

static void acms_sum_initialize(TypeVarAcm, void*, size_t);
static void acms_sum_finalize(TypeVarAcm, TypeVarAcmState);

static int32_t acms_sum_input(TypeVarAcm, TypeVarAcmState, va_list args);
static int32_t acms_sum_result(TypeVarAcm, TypeVarAcmState, TypeVarSlot*);

static void acm_sum_finalize(TypeVarAcm);


static const struct TypeVarAcmFuncs acm_sum_funcs = { 
	.acms_initialize = acms_sum_initialize,
	.acms_finalize = acms_sum_finalize,

	.input = acms_sum_input,
	.result = acms_sum_result,

	.finalize = acm_sum_finalize
};



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

TypeVarAcm
vh_acm_sum_tys(Type *tys)
{
	struct acm_sum *acm;
	size_t typevar_sz, acms_sz;

	typevar_sz = vh_tvp_space(tys);
	vh_tvp_maxalign(typevar_sz);
	acms_sz = typevar_sz + sizeof(struct acm_sum_state);

	acm = vh_acm_create(sizeof(struct acm_sum), &acm_sum_funcs, acms_sz);
	acm->op = vh_typevar_op_init_tys("+=", tys, tys, 0);
	acm->ty_depth = vh_type_stack_copy(&acm->tys[0], &tys[0]);
	acm->typevar_sz = typevar_sz;

	if (acm->op)
		return &acm->acm;

	vhfree(acm);

	return 0;	
}



/*
 * ============================================================================
 * ACMS Implementation
 * ============================================================================
 */

static void
acms_sum_initialize(TypeVarAcm tvacm, void *at, size_t sz)
{
	struct acm_sum *acm = (struct acm_sum*)tvacm;
	struct acm_sum_state *acms = at;

	vh_tvp_initialize(&acms->page, acm->typevar_sz);
	vh_tvp_add(&acms->page, acm->tys);
}

static void
acms_sum_finalize(TypeVarAcm tvacm, TypeVarAcmState tvacms)
{
	struct acm_sum_state *acms = (struct acm_sum_state*)tvacms;

	vh_tvp_finalize(&acms->page);
}

static int32_t
acms_sum_input(TypeVarAcm tvacm, TypeVarAcmState tvacms, va_list args)
{
	struct acm_sum *acm = (struct acm_sum*)tvacm;
	struct acm_sum_state *acms = (struct acm_sum_state*)tvacms;
	TypeVarSlot *slot;
	void *value;

	slot = va_arg(args, TypeVarSlot*);
	value = vh_tvs_value(slot);
	
	vh_typevar_op_fp(acm->op, acms_sum(acms), value);

	return 0;
}

static int32_t
acms_sum_result(TypeVarAcm tvacm, TypeVarAcmState tvacms, TypeVarSlot *slot)
{
	struct acm_sum_state *acms = (struct acm_sum_state*)tvacms;
	TypeVar copy;

	copy = vh_typevar_makecopy(acms_sum(acms));
	vh_tvs_store_var(slot, copy, VH_TVS_RA_DVAR);

	return 0;
}

static void
acm_sum_finalize(TypeVarAcm tvacm)
{
	struct acm_sum *acm = (struct acm_sum*)tvacm;

	if (acm->op)
		vh_typevar_op_destroy(acm->op);
}

