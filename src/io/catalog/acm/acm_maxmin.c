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

struct acm_maxmin
{
	TypeVarAcmData acm;

	TypeVarOpExec comp;
	TypeVarOpExec setter;

	size_t typevar_sz;

	Type tys[VH_TAMS_MAX_DEPTH];
	int8_t ty_depth;
};

struct acm_maxmin_state
{
	bool set;

	uintptr_t pad;


	/*
	TypeVar min;
	*/
	struct TypeVarPageData page;
};

#define acms_maxmin_var(acms)			vh_tvp_varhead(&(acms)->page, 1)
#define acms_maxmin(acms)				vh_tvp_var(&(acms)->page, 1)



/*
 * ============================================================================
 * ACM Functions
 * ============================================================================
 */

static struct acm_maxmin* acm_maxmin_create(Type *tys);

static void acms_maxmin_initialize(TypeVarAcm, void*, size_t);
static void acms_maxmin_finalize(TypeVarAcm, TypeVarAcmState);

static int32_t acms_maxmin_input(TypeVarAcm, TypeVarAcmState, va_list args);
static int32_t acms_maxmin_result(TypeVarAcm, TypeVarAcmState, TypeVarSlot*);

static void acm_maxmin_finalize(TypeVarAcm);


static const struct TypeVarAcmFuncs acm_maxmin_funcs = {
	.acms_initialize = acms_maxmin_initialize,
	.acms_finalize = acms_maxmin_finalize,

	.input = acms_maxmin_input,
	.result = acms_maxmin_result,

	.finalize = acm_maxmin_finalize
};



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */



/*
 * vh_acm_max_tys
 *
 * The only difference between vh_acm_max and vh_acm_min is the comparison
 * operator.  Call acm_maxmin_create to setup the base ACM and then set the
 * comparison function.
 */
TypeVarAcm
vh_acm_max_tys(Type *tys)
{
	struct acm_maxmin *acm;

	acm = acm_maxmin_create(tys);

	if (!acm)
	{
		elog(WARNING,
				emsg("Unable to determine the proper TypeVarOp function for the "
				     "Type stack provided to vh_acm_max_tys."));
		return 0;
	}

	acm->comp = vh_typevar_comp_init_tys(">", tys, tys);

	if (!acm->comp)
	{
		if (acm->setter)
		{
			vh_typevar_op_destroy(acm->setter);
			acm->setter = 0;
		}

		vhfree(acm);

		elog(WARNING,
				emsg("Unable to determine the proper TypeVarComp function for the "
					 "Type stack provided to vh_acm_max_tys."));

		return 0;
	}

	return &acm->acm;
}

/*
 * vh_acm_min_tys
 *
 * The only difference between vh_acm_max_tys and vh_acm_min_tys is the comparison
 * operator.
 */
TypeVarAcm
vh_acm_min_tys(Type *tys)
{
	struct acm_maxmin *acm;

	acm = acm_maxmin_create(tys);

	if (!acm)
	{
		elog(WARNING,
				emsg("Unable to determine the proper TypeVarOp function for the "
				     "Type stack provided to vh_acm_min_tys."));
		return 0;
	}

	acm->comp = vh_typevar_comp_init_tys("<", tys, tys);

	if (!acm->comp)
	{
		if (acm->setter)
		{
			vh_typevar_op_destroy(acm->setter);
			acm->setter = 0;
		}

		vhfree(acm);

		elog(WARNING,
				emsg("Unable to determine the proper TypeVarComp function for the "
					 "Type stack provided to vh_acm_min_tys."));

		return 0;
	}

	return &acm->acm;
}

static struct acm_maxmin*
acm_maxmin_create(Type *tys)
{
	struct acm_maxmin *acm;
	size_t acms_sz, typevar_sz;
	
	typevar_sz = vh_tvp_space(tys);
	vh_tvp_maxalign(typevar_sz);
	acms_sz = sizeof(struct acm_maxmin_state) + typevar_sz;

	acm = vh_acm_create(sizeof(struct acm_maxmin), &acm_maxmin_funcs, acms_sz);
	acm->setter = vh_typevar_op_init_tys("=", tys, tys, 0);

	acm->typevar_sz = typevar_sz;
	acm->ty_depth = vh_type_stack_copy(acm->tys, tys);

	if (acm->setter)
		return acm;

	vhfree(acm);

	return 0;
}

/*
 * ============================================================================
 * ACM Implementation
 * ============================================================================
 */
static void
acms_maxmin_initialize(TypeVarAcm tvacm, void *data, size_t sz)
{
	struct acm_maxmin *acm = (struct acm_maxmin*)tvacm;
	struct acm_maxmin_state *acms = data;

	acms->set = false;
	vh_tvp_initialize(&acms->page, acm->typevar_sz);
	vh_tvp_add(&acms->page, acm->tys);
}

static void
acms_maxmin_finalize(TypeVarAcm tvacm, TypeVarAcmState tvacms)
{
	struct acm_maxmin_state *acms = (struct acm_maxmin_state*)tvacms;

	vh_tvp_finalize(&acms->page);
}

static int32_t
acms_maxmin_input(TypeVarAcm tvacm, TypeVarAcmState tvacms, va_list args)
{
	TypeVarSlot *slot;
	struct acm_maxmin *acm = (struct acm_maxmin*)tvacm;
	struct acm_maxmin_state *acms = (struct acm_maxmin_state*)tvacms;
	void *value;

	slot = va_arg(args, TypeVarSlot*);
	value = vh_tvs_value(slot);

	if (!acms->set)
	{
		vh_typevar_op_fp(acm->setter, acms_maxmin(acms), value);
		acms->set = true;

		return 0;
	}

	if (vh_typevar_comp_fp(acm->comp, value, acms_maxmin(acms)))
	{
		vh_typevar_op_fp(acm->setter, acms_maxmin(acms), value);
	}

	return 0;
}

static int32_t
acms_maxmin_result(TypeVarAcm tvacm, TypeVarAcmState tvacms, TypeVarSlot *slot)
{
	struct acm_maxmin_state *acms = (struct acm_maxmin_state*)tvacms;
	TypeVar *copy;

	if (acms->set)
	{
		copy = vh_typevar_makecopy(acms_maxmin(acms));
		vh_tvs_store_var(slot, copy, VH_TVS_RA_DVAR);
	}
	else
	{
		vh_tvs_store_null(slot);
	}

	return 0;
}

static void
acm_maxmin_finalize(TypeVarAcm tvacm)
{
	struct acm_maxmin *acm = (struct acm_maxmin*)tvacm;

	if (acm->comp)
	{
		vh_typevar_comp_destroy(acm->comp);
		acm->comp = 0;
	}

	if (acm->setter)
	{
		vh_typevar_comp_destroy(acm->setter);
		acm->setter = 0;
	}
}


