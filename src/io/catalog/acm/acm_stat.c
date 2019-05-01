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

struct acm_stat_accum
{
	TypeVarAcmData acm;

	/* Used by Input */
	TypeVarOpExec accum;			/* TypeVar += TypeVar */ 

	/* Used by Result */
	TypeVarOpExec numerator;		/* TypeVar = i64 * TypeVar (RET: creates) */
	TypeVarOpExec square;			/* TypeVar = TypeVar * TypeVar (RET: passed) */
	TypeVarOpExec subtract;			/* TypeVar -= TypeVar */
	TypeVarOpExec divide;			/* TypeVar /= i64 */
	TypeVarOpExec square_root;		/* TypeVar = sqrt(TypeVar) (RET: passed) */

	TypeVarOpExec comp;

	TypeVarOpExec transition_in;
	TypeVarOpExec transition_out;

	Type tys[VH_TAMS_MAX_DEPTH];

	size_t typevar_sz;
	size_t typevar_page_sz;

	int8_t ty_depth;
	bool sample;
	bool dev;
	bool transition;
};

struct acm_stat_accum_state
{	
	int64_t count;

	/*
	TypeVar off_sum_x;
	TypeVar off_sum_x2;
	TypeVar off_temp;
	TypeVar trans;
	*/

	struct TypeVarPageData page;
};

#define acms_sum_x(acms)			vh_tvp_var(&(acms)->page, 1)
#define acms_sum_x2(acms)			vh_tvp_var(&(acms)->page, 2)
#define acms_temp(acms)				vh_tvp_var(&(acms)->page, 3)
#define acms_trans(acms)			vh_tvp_var(&(acms)->page, 4)

#define acms_sum_x_var(acms)		vh_tvp_varhead(&(acms)->page, 1)
#define acms_sum_x2_var(acms)		vh_tvp_varhead(&(acms)->page, 2)
#define acms_temp_var(acms)			vh_tvp_varhead(&(acms)->page, 3)
#define acms_trans_var(acms)		vh_tvp_varhead(&(acms)->page, 4)


#define acms_decl(var, src)				struct acm_stat_accum_state *var = (struct acm_stat_accum_state*)src
#define acm_decl(var, src)				struct acm_stat_accum *var = (struct acm_stat_accum*)src



/*
 * ============================================================================
 * TypeVarAcm Functions
 * ============================================================================
 */

static void acms_stat_initialize(TypeVarAcm tvacm, void *data, size_t sz);
static void acms_stat_finalize(TypeVarAcm tvacm, TypeVarAcmState tvacms);

static int32_t acms_stat_input(TypeVarAcm tvacm, TypeVarAcmState tvacms, 
							   va_list args);
static int32_t acms_stat_result(TypeVarAcm tvacm, TypeVarAcmState tvacms,
 								TypeVarSlot *slot);

static void acm_finalize(TypeVarAcm tvacm);

static const struct TypeVarAcmFuncs acm_stat_func = {
	.acms_initialize = acms_stat_initialize,
	.acms_finalize = acms_stat_finalize,

	.input = acms_stat_input,
	.result = acms_stat_result,

	.finalize = acm_finalize
};



/*
 * ============================================================================
 * Private Functions
 * ============================================================================
 */

static struct acm_stat_accum* acm_create(Type *tys, bool is_deviation);

static void acm_create_dev_ops(struct acm_stat_accum*, Type *tys_accum);



/*
 * ============================================================================
 * Public Interface Implementation
 * ============================================================================
 */

TypeVarAcm
vh_acm_devp_tys(Type *tys)
{
	struct acm_stat_accum *acm;

	acm = acm_create(tys, true);
	acm->sample = false;
	acm->dev = true;

	return &acm->acm;
}

TypeVarAcm
vh_acm_devs_tys(Type *tys)
{
	struct acm_stat_accum *acm;

	acm = acm_create(tys, true);
	acm->sample = true;
	acm->dev = true;

	return &acm->acm;
}

TypeVarAcm
vh_acm_varp_tys(Type *tys)
{
	struct acm_stat_accum *acm;

	acm = acm_create(tys, false);
	acm->sample = false;
	acm->dev = false;

	return &acm->acm;
}

TypeVarAcm
vh_acm_vars_tys(Type *tys)
{
	struct acm_stat_accum *acm;

	acm = acm_create(tys, false);
	acm->sample = true;
	acm->dev = false;

	return &acm->acm;
}



/*
 * ============================================================================
 * ACMS State
 * ============================================================================
 */

static void
acms_stat_initialize(TypeVarAcm tvacm, void *data, size_t sz)
{
	struct acm_stat_accum_state *acms = data;
	struct acm_stat_accum *acm = (struct acm_stat_accum*)tvacm;

	acms->count = 0;

	vh_tvp_initialize(&acms->page, acm->typevar_page_sz);
	vh_tvp_add(&acms->page, acm->tys);
	vh_tvp_add(&acms->page, acm->tys);
	vh_tvp_add(&acms->page, acm->tys);
	vh_tvp_add(&acms->page, acm->tys);
}

static void
acms_stat_finalize(TypeVarAcm tvacm, TypeVarAcmState tvacms)
{
	acms_decl(acms, tvacms);

	vh_tvp_finalize(&acms->page);
}

/*
 * acms_stat_accum_input
 *
 * Receive input TypeVarSlot, expects 1
 */
static int32_t
acms_stat_input(TypeVarAcm tvacm, TypeVarAcmState tvacms, va_list args)
{
	acms_decl(acms, tvacms);
	acm_decl(acm, tvacm);
	TypeVarSlot *slot;
	void *value;

	acms->count++;

	slot = va_arg(args, TypeVarSlot*);
	
	/*
	 * 	1)	Add the slot value to the sum_x
	 * 	2)	Multiply the slot value by itself and store it in the temp
	 * 	3)	Add the temp value to the sum_x2
	 */

	if (acm->transition)
	{
		vh_typevar_op_fp(acm->transition_in, acms_trans(acms), vh_tvs_value(slot));
		value = acms_trans(acms);
	}
	else
	{
		value = vh_tvs_value(slot);
	}

	vh_typevar_op_fp(acm->accum, acms_sum_x(acms), value);
	vh_typevar_op_fp(acm->square, acms_temp(acms), value, value);
	vh_typevar_op_fp(acm->accum, acms_sum_x2(acms), acms_temp(acms));

	return 0;
}

/*
 * acms_stat_result
 *
 * Calculates the standard deviation or variance, depending on @dev in the acm.
 * We rely on the @acm to tell us if we're doing the population or sample.
 */
static int32_t
acms_stat_result(TypeVarAcm tvacm, TypeVarAcmState tvacms, TypeVarSlot *slot)
{
	acms_decl(acms, tvacms);
	acm_decl(acm, tvacm);
	TypeVar numerator;
	int64_t count;

	count = acms->count;
	count = count * (count - (acm->sample ? 1 : 0));

	if (count < 1)
	{
		vh_tvs_store_i32(slot, 0);
		return 0;
	}

	/*
	 * 	1)	Multiply the @count by the sum_x2, returning it as a TypeVar @numerator
	 * 	2)	Square the @sum_x and store in @acms_temp
	 * 	3)	Subtract @acms_temp from the numerator, store in @numerator
	 */
	numerator = vh_typevar_op_fp(acm->numerator, acms->count, acms_sum_x2(acms));
	vh_typevar_op_fp(acm->square, acms_temp(acms), 
					 acms_sum_x(acms), acms_sum_x(acms));
	vh_typevar_op_fp(acm->subtract, numerator, acms_temp(acms));

	/*
	 * Make sure the numerator isn't less than zero
	 */
	if (vh_typevar_comp_fp(acm->comp, numerator, 0))
	{
	}

	/*
	 * First square our count and take care of if we're looking for the sample
	 * vs. the population.  Then we can call the TypeVarOpExec.
	 *
	 * 	1)	Divide the @numerator by the square of @count
	 * 	2)	For standard deviation, take the square root of @numerator
	 * 	3)	Store @numerator in the TypeVarSlot, set the fly to destroy it on release
	 */

	vh_typevar_op_fp(acm->divide, numerator, numerator, count);

	/*
	 * We're a standard deviation calculation, so figure out what do to with that
	 */
	if (acm->dev)
	{
		vh_typevar_op_fp(acm->square_root, numerator, numerator);
	}

	vh_tvs_store_var(slot, numerator, VH_TVS_RA_DVAR);

	return 0;
}

static void
acm_finalize(TypeVarAcm tvacm)
{
	acm_decl(acm, tvacm);

	if (acm->accum)
	{
		vh_typevar_op_destroy(acm->accum);
		acm->accum = 0;
	}

	if (acm->numerator)
	{
		vh_typevar_op_destroy(acm->numerator);
		acm->numerator = 0;
	}

	if (acm->square)
	{
		vh_typevar_op_destroy(acm->square);
		acm->square = 0;
	}

	if (acm->subtract)
	{
		vh_typevar_op_destroy(acm->subtract);
		acm->subtract = 0;
	}

	if (acm->divide)
	{
		vh_typevar_op_destroy(acm->divide);
		acm->divide = 0;
	}

	if (acm->square_root)
	{
		vh_typevar_op_destroy(acm->square_root);
		acm->square_root = 0;
	}

	if (acm->transition_in)
	{
		vh_typevar_op_destroy(acm->transition_in);
		acm->transition_in = 0;
	}

	if (acm->transition_out)
	{
		vh_typevar_op_destroy(acm->transition_out);
		acm->transition_out = 0;
	}
}


/*
 * ============================================================================
 * Private Functions
 * ============================================================================
 */

static struct acm_stat_accum*
acm_create(Type *tys, bool dev)
{
	struct acm_stat_accum *acm;
	size_t typevar_sz, typevar_page_sz, acms_sz;
	Type tys_accum[VH_TAMS_MAX_DEPTH];
	
	vh_type_stack_fillaccum(tys_accum, tys);
	typevar_sz = vh_tvp_space(tys_accum);
	typevar_page_sz = typevar_sz * 4;
	vh_tvp_maxalign(typevar_page_sz);

	acms_sz = sizeof(struct acm_stat_accum_state) + typevar_page_sz;

	acm = vh_acm_create(sizeof(struct acm_stat_accum), &acm_stat_func, acms_sz);
	acm->ty_depth = vh_type_stack_copy(acm->tys, tys_accum);

	/*
	 * typevar_sz		Individual TypeVar
	 * typevar_page_sz	Page size requirements
	 */
	acm->typevar_sz = typevar_sz;
	acm->typevar_page_sz = typevar_page_sz;
	vh_tvp_maxalign(acm->typevar_page_sz);

	/*
	 * Setup the TypeVarOpExec
	 */
	acm->comp = vh_typevar_comp_init("<=",
									 VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
										 			 VH_OP_DT_TYSVAR,
													 VH_OP_ID_INVALID,
													 VH_OP_DT_I64,
													 VH_OP_ID_INVALID),
									 tys_accum,
									 0);

	acm->accum = vh_typevar_op_init("+=",
		   							VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
													VH_OP_DT_TYSVAR,
													VH_OP_ID_INVALID,
													VH_OP_DT_TYSVAR,
													VH_OP_ID_INVALID),
									tys_accum,
									tys_accum);

	acm->square = vh_typevar_op_init("*",
									 VH_OP_MAKEFLAGS(VH_OP_DT_TYSVAR | VH_OP_DT_BOTH,
		  											 VH_OP_DT_TYSVAR,
  													 VH_OP_ID_INVALID,
  													 VH_OP_DT_TYSVAR,
  													 VH_OP_ID_INVALID),
									 tys_accum,
  									 tys_accum,
									 tys_accum);

	acm->numerator = vh_typevar_op_init("*",
										VH_OP_MAKEFLAGS(VH_OP_DT_VAR,
														VH_OP_DT_I64,
														VH_OP_ID_INVALID,
														VH_OP_DT_TYSVAR,
														VH_OP_ID_INVALID),
										0,
										tys_accum);

	acm->subtract = vh_typevar_op_init("-=",
									   VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
							  						   VH_OP_DT_TYSVAR,
			  										   VH_OP_ID_INVALID,
		  											   VH_OP_DT_TYSVAR,
		  											   VH_OP_ID_INVALID),
		  							   tys_accum,
		  							   tys_accum);

	acm->divide = vh_typevar_op_init("/", 
									 VH_OP_MAKEFLAGS(VH_OP_DT_TYSVAR | VH_OP_DT_BOTH,
										 			 VH_OP_DT_TYSVAR,
													 VH_OP_ID_INVALID,
													 VH_OP_DT_I64,
													 VH_OP_ID_INVALID),
									 tys_accum,
									 tys_accum,
									 1);

	if (!vh_type_stack_match(tys, tys_accum))
	{
		acm->transition = true;
		acm->transition_in = vh_typevar_op_init("=",
											 VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
												 			 VH_OP_DT_TYSVAR,
															 VH_OP_ID_INVALID,
															 VH_OP_DT_TYSVAR,
															 VH_OP_ID_INVALID),
											 tys_accum,
											 tys);
	}
	
	if (dev)
	{
		acm_create_dev_ops(acm, tys_accum);
	}

	return acm;
}

/*
 * acm_create_dev_ops
 *
 * Creates the appropriate TypeVarExecOp for a standard deviation calculation.
 */
static void 
acm_create_dev_ops(struct acm_stat_accum* acm, Type *tys_accum)
{
	acm->square_root = vh_typevar_op_init("sqrt",
										  VH_OP_MAKEFLAGS(VH_OP_DT_TYSVAR | VH_OP_DT_BOTH,
											  			  VH_OP_DT_TYSVAR,
														  VH_OP_ID_INVALID,
														  VH_OP_DT_INVALID,
														  VH_OP_ID_INVALID),
										  tys_accum,
										  tys_accum);

}

