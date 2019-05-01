/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarPage.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/analytics/ml/mlfdistance.h"

/*
 * ============================================================================
 * Euclidean Distance
 * ============================================================================
 */

typedef struct CacheEuclideanData *CacheEuclidean;

struct CacheEuclideanData
{
	TypeVarOpExec tvope_input;

	TypeVarOpExec tvope_diff;
	TypeVarOpExec tvope_power;
	TypeVarOpExec tvope_accum;
	TypeVarOpExec tvope_sqrt;

	/*
	 * Two TypeVar Follow On The Page:
	 * 	1)	Distance (accumulator)
	 * 	2)	Temporary
	 * 	3)	Input LHS (only if we need to transform the input)
	 * 	4)	Input RHS (only if we need to transform the input)
	 */
	struct TypeVarPageData page;
};

#define euclidean_page(cache)		(&((CacheEuclidean)(cache))->page)

static CacheEuclidean euclidean_cache(Type *tys);


/*
 * ============================================================================
 * Manhattan Distance
 * ============================================================================
 */

typedef struct CacheManhattanData *CacheManhattan;

struct CacheManhattanData
{
	TypeVarOpExec tvope_input;

	TypeVarOpExec tvope_sub;
	TypeVarOpExec tvope_abs;
	TypeVarOpExec tvope_accum;

	/*
	 * 3 TypeVar Follow on the Page:
	 * 	1)	Temporary
	 * 	2)	LHS
	 * 	3)	RHS
	 */

	struct TypeVarPageData page;
};

#define manhattan_page(cache)		(&((CacheManhattan)(cache))->page)

static CacheManhattan manhattan_cache(Type *tys);


/*
 * ============================================================================
 * Vector Dot Product
 * ============================================================================
 */

typedef struct CacheVectorDotData *CacheVectorDot;

struct CacheVectorDotData
{
	TypeVarOpExec tvope_input;

	TypeVarOpExec tvope_mult;
	TypeVarOpExec tvope_accum;

	/*
	 * Three TypeVar Follow On The Page:
	 * 	1)	Distance
	 * 	2)	Temp
	 * 	3)	Input LHS
	 * 	4)	Input RHS
	 */
	struct TypeVarPageData page;
};

#define vectordot_page(cache)		(&((CacheVectorDot)(cache))->page)

static CacheVectorDot vectordot_cache(Type *tys);


/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

/*
 * vh_mlf_distance_destroy
 *
 * If we've got a distance function and a cache, we need to destroy that distance's
 * particular cache layout.
 */
void
vh_mlf_distance_destroy(vh_mlf_distance_func distance, void *cache)
{
	/*
	 * Save some stack space a union of the various cache structures available
	 * to the module.  Only one is used once we figure out which one it is.
	 */
	union
	{
		CacheEuclidean euclidean;
		CacheManhattan man;
		CacheVectorDot vd;
	} u;

	u.euclidean = cache;

	if (!cache)
		return;

	if (distance == vh_mlf_distance_euclidean)
	{	
		vh_tvp_finalize(euclidean_page(cache));

		if (u.euclidean->tvope_input)
			vh_typevar_op_destroy(u.euclidean->tvope_input);

		if (u.euclidean->tvope_diff)
			vh_typevar_op_destroy(u.euclidean->tvope_diff);

		if (u.euclidean->tvope_power)
			vh_typevar_op_destroy(u.euclidean->tvope_power);

		if (u.euclidean->tvope_accum)
			vh_typevar_op_destroy(u.euclidean->tvope_accum);

		if (u.euclidean->tvope_sqrt)
			vh_typevar_op_destroy(u.euclidean->tvope_sqrt);
	}
	else if (distance == vh_mlf_distance_manhattan)
	{
		vh_tvp_finalize(manhattan_page(cache));

		if (u.man->tvope_input)
			vh_typevar_op_destroy(u.man->tvope_input);
		
		if (u.man->tvope_sub)
			vh_typevar_op_destroy(u.man->tvope_sub);

		if (u.man->tvope_abs)
			vh_typevar_op_destroy(u.man->tvope_abs);

		if (u.man->tvope_accum)
			vh_typevar_op_destroy(u.man->tvope_accum);
	}
	else if (distance == vh_mlf_distance_vectordot)
	{
		vh_tvp_finalize(vectordot_page(cache));

		if (u.vd->tvope_input)
			vh_typevar_op_destroy(u.vd->tvope_input);

		if (u.vd->tvope_mult)
			vh_typevar_op_destroy(u.vd->tvope_mult);

		if (u.vd->tvope_accum)
			vh_typevar_op_destroy(u.vd->tvope_accum);
	}

	vhfree(cache);
}


/*
 * ============================================================================
 * Euclidean Implementation
 * ============================================================================
 */

/*
 * vh_mlf_distance_euclidean
 *
 * Calculates a euclidean disance on the fly.
 */
int32_t
vh_mlf_distance_euclidean(void **cache, TypeVarSlot *result,
						  TypeVarSlot *training_set, TypeVarSlot **instance,
						  int32_t n_datas)
{
	Type tys[VH_TAMS_MAX_DEPTH];
	int32_t i;
	void *lhs, *rhs, *ret;
	CacheEuclidean ce;

	vh_tvs_fill_tys(training_set, tys);

	if (cache)
	{
		if (!(*cache))
			*cache = euclidean_cache(tys);

		ce = *cache;

		/*
		 * Run the cached algorithm.
		 *
		 * distance += (a - b)^2
		 * sqrt(distance)
		 */
		for (i = 0; i < n_datas; i++)
		{
			lhs = vh_tvs_value(&training_set[i]);
			rhs = vh_tvs_value(instance[i]);

			if (ce->tvope_input)
			{
				ret = vh_tvp_var(euclidean_page(*cache), 3);
				vh_typevar_op_fp(ce->tvope_input, ret, lhs);
				lhs = vh_tvp_var(euclidean_page(*cache), 3);

				ret = vh_tvp_var(euclidean_page(*cache), 4);
				vh_typevar_op_fp(ce->tvope_input, ret, rhs);
				rhs = vh_tvp_var(euclidean_page(*cache), 4);				
			}

			ret = vh_tvp_var(euclidean_page(*cache), 2);		/* Temp Value */
			vh_typevar_op_fp(ce->tvope_diff, ret, lhs, rhs);
			vh_typevar_op_fp(ce->tvope_power, ret, ret, ret);

			lhs = vh_tvp_var(euclidean_page(*cache), 1);		/* Distance Value */
			vh_typevar_op_fp(ce->tvope_accum, lhs, ret);
		}

		/*
		 * Take the square root of the distance value and then we should go
		 * ahead and reset the distance variable on the page.
		 */

		ret = vh_typevar_op_fp(ce->tvope_sqrt, lhs);
		vh_tvs_store_var(result, ret, VH_TVS_RA_DVAR);

		vh_typevar_reset(lhs);
	}
	else
	{
		/*
		 * No cache has been provided and it is not requested, so we'll have to
		 * be careful about cleaning up all our memory.
		 *
		 * The first priority is to get the accumulator type stack.
		 */

		return -1;
	}

	return 0;
}

static CacheEuclidean
euclidean_cache(Type *tys)
{
	Type tys_accum[VH_TAMS_MAX_DEPTH];
	size_t cache_sz, page_sz;
	CacheEuclidean cache;
	bool needs_input_transform = false;

	vh_type_stack_fillaccum(tys_accum, tys);
	needs_input_transform = !vh_type_stack_match(tys, tys_accum);

	if (needs_input_transform)
		page_sz = vh_tvp_space(tys_accum) * 4;
	else
		page_sz = vh_tvp_space(tys_accum) * 2;

	vh_tvp_maxalign(page_sz);
	cache_sz = sizeof(struct CacheEuclideanData) + page_sz;
	cache = vhmalloc(cache_sz);

	/*
	 * Initialize our TypeVarPage
	 */	
	vh_tvp_initialize(euclidean_page(cache), page_sz);
	vh_tvp_add(euclidean_page(cache), tys_accum);		/* Distance */
	vh_tvp_add(euclidean_page(cache), tys_accum);		/* Temp */

	/*
	 * Build out the TypeVarOpExec
	 */

	if (needs_input_transform)
	{
		vh_tvp_add(euclidean_page(cache), tys_accum);	/* LHS */
		vh_tvp_add(euclidean_page(cache), tys_accum);	/* RHS */

		cache->tvope_input = vh_typevar_op_init("=",
												VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
																VH_OP_DT_TYSVAR,
																VH_OP_ID_INVALID,
																VH_OP_DT_TYSVAR,
																VH_OP_ID_INVALID),
												tys_accum,
												tys);
	}
	else
	{
		cache->tvope_input = 0;
	}

	cache->tvope_diff = vh_typevar_op_init("-", 
										   VH_OP_MAKEFLAGS(VH_OP_DT_TYSVAR | VH_OP_DT_BOTH,
											   			   VH_OP_DT_TYSVAR,
														   VH_OP_ID_INVALID,
														   VH_OP_DT_TYSVAR,
														   VH_OP_ID_INVALID),
										   tys_accum,
										   tys_accum,
										   tys_accum);

	cache->tvope_power = vh_typevar_op_init("*", 
										  	VH_OP_MAKEFLAGS(VH_OP_DT_TYSVAR | VH_OP_DT_BOTH,
															VH_OP_DT_TYSVAR,
														  	VH_OP_ID_INVALID,
														  	VH_OP_DT_TYSVAR,
														  	VH_OP_ID_INVALID),
										  	tys_accum,
										  	tys_accum,
										  	tys_accum);
	
	cache->tvope_accum = vh_typevar_op_init("+=", 
										  	VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
															VH_OP_DT_TYSVAR,
														  	VH_OP_ID_INVALID,
														  	VH_OP_DT_TYSVAR,
														  	VH_OP_ID_INVALID),
										  	tys_accum,
										  	tys_accum,
										  	tys_accum);
	
	cache->tvope_sqrt = vh_typevar_op_init("sqrt",
										  	VH_OP_MAKEFLAGS(VH_OP_DT_TYSVAR,
															VH_OP_DT_TYSVAR,
														  	VH_OP_ID_INVALID,
														  	VH_OP_DT_INVALID,
														  	VH_OP_ID_INVALID),
										  	tys_accum,
										  	tys_accum);

	return cache;
}

/*
 * ============================================================================
 * Manhattan Distance
 * ============================================================================
 */

int32_t
vh_mlf_distance_manhattan(void **cache,
						  TypeVarSlot *result,
						  TypeVarSlot *training_set,
						  TypeVarSlot **instance,
						  int32_t n_datas)
{
	Type tys[VH_TAMS_MAX_DEPTH];
	int32_t i;
	void *lhs, *rhs, *ret;
	CacheManhattan man;

	/*
	 * distance += abs(lhs - rhs)
	 */

	vh_tvs_fill_tys(training_set, tys);

	if (cache)
	{
		if (!cache)
			*cache = manhattan_cache(tys);

		man = *cache;

		for (i = 0; i < n_datas; i++)
		{
			lhs = vh_tvs_value(&training_set[i]);
			rhs = vh_tvs_value(instance[i]);

			if (man->tvope_input)
			{
				ret = vh_tvp_var(manhattan_page(*cache), 2);
				vh_typevar_op_fp(man->tvope_input, ret, lhs);
				lhs = vh_tvp_var(manhattan_page(*cache), 2);

				ret = vh_tvp_var(manhattan_page(*cache), 3);
				vh_typevar_op_fp(man->tvope_input, ret, rhs);
				rhs = vh_tvp_var(manhattan_page(*cache), 3);
			}

			ret = vh_tvp_var(manhattan_page(*cache), 2);	/* Temp */
			vh_typevar_op_fp(man->tvope_sub, ret, lhs, rhs);
			vh_typevar_op_fp(man->tvope_abs, ret, ret);

			lhs = vh_tvp_var(manhattan_page(*cache), 1);	/* Distance */
			vh_typevar_op_fp(man->tvope_accum, lhs, ret);
		}

		vh_tvs_store_var(result, vh_typevar_makecopy(lhs), VH_TVS_RA_DVAR);
		vh_typevar_reset(lhs);
	}
	else
	{
		/*
		 * No cache has been provided and it is not requested, so we'll have to
		 * be careful about cleaning up all our memory.
		 */

		return -1;
	}

	return 0;
}

static CacheManhattan
manhattan_cache(Type *tys)
{
	Type tys_accum[VH_TAMS_MAX_DEPTH];
	size_t cache_sz, page_sz;
	CacheManhattan cache;
	bool needs_input_transform;

	vh_type_stack_fillaccum(tys_accum, tys);
	needs_input_transform = !vh_type_stack_match(tys, tys_accum);

	if (needs_input_transform)
		page_sz = vh_tvp_space(tys_accum) * 4;
	else
		page_sz = vh_tvp_space(tys_accum) * 2;

	cache_sz = sizeof(struct CacheManhattanData) + page_sz;
	cache = vhmalloc(cache_sz);

	vh_tvp_initialize(manhattan_page(cache), page_sz);
	vh_tvp_add(manhattan_page(cache), tys_accum);		/* Distance */
	vh_tvp_add(manhattan_page(cache), tys_accum);		/* Temp */

	if (needs_input_transform)
	{
		vh_tvp_add(manhattan_page(cache), tys_accum);	/* LHS */
		vh_tvp_add(manhattan_page(cache), tys_accum);	/* RHS */

		cache->tvope_input = vh_typevar_op_init("=",
												VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
																VH_OP_DT_TYSVAR,
																VH_OP_ID_INVALID,
																VH_OP_DT_TYSVAR,
																VH_OP_ID_INVALID),
												tys_accum,
												tys);
	}
	else
	{
		cache->tvope_input = 0;
	}

	cache->tvope_sub = vh_typevar_op_init("-",
										  VH_OP_MAKEFLAGS(VH_OP_DT_TYSVAR | VH_OP_DT_BOTH,
											  			  VH_OP_DT_TYSVAR,
														  VH_OP_ID_INVALID,
														  VH_OP_DT_TYSVAR,
														  VH_OP_ID_INVALID),
										  tys_accum,
										  tys_accum,
										  tys_accum);

	cache->tvope_abs = vh_typevar_op_init("abs",
										  VH_OP_MAKEFLAGS(VH_OP_DT_TYSVAR | VH_OP_DT_BOTH,
											  			  VH_OP_DT_TYSVAR,
														  VH_OP_ID_INVALID,
														  VH_OP_DT_INVALID,
														  VH_OP_ID_INVALID),
										  tys_accum,
										  tys_accum);

	cache->tvope_accum = vh_typevar_op_init("+=",
											VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
															VH_OP_DT_TYSVAR,
															VH_OP_ID_INVALID,
															VH_OP_DT_TYSVAR,
															VH_OP_ID_INVALID),
											tys_accum,
											tys_accum);

	return cache;
}

/*
 * ============================================================================
 * Vector Dot Implementation
 * ============================================================================
 */


int32_t
vh_mlf_distance_vectordot(void **cache,
						  TypeVarSlot *result,
						  TypeVarSlot *training_set,
						  TypeVarSlot **instance, 
						  int32_t n_datas)
{
	Type tys[VH_TAMS_MAX_DEPTH];
	int32_t i;
	void *lhs, *rhs, *ret;
	CacheVectorDot vd;

	vh_tvs_fill_tys(training_set, tys);

	if (cache)
	{
		if (!(*cache))
			*cache = vectordot_cache(tys);

		vd = *cache;

		/*
		 * distance += (lhs * rhs)
		 */

		for (i = 0; i < n_datas; i++)
		{
			lhs = vh_tvs_value(&training_set[i]);
			rhs = vh_tvs_value(instance[i]);

			if (vd->tvope_input)
			{
				ret = vh_tvp_var(vectordot_page(*cache), 3);
				vh_typevar_op_fp(vd->tvope_input, ret, lhs);
				lhs = vh_tvp_var(vectordot_page(*cache), 3);

				ret = vh_tvp_var(vectordot_page(*cache), 4);
				vh_typevar_op_fp(vd->tvope_input, ret, rhs);
				rhs = vh_tvp_var(vectordot_page(*cache), 4);
			}

			ret = vh_tvp_var(vectordot_page(*cache), 2);		/* Temp Value */
			vh_typevar_op_fp(vd->tvope_mult, ret, lhs, rhs);

			lhs = vh_tvp_var(vectordot_page(*cache), 1);		/* Distance Value */
			vh_typevar_op_fp(vd->tvope_accum, lhs, ret);
		}

		vh_tvs_store_var(result, vh_typevar_makecopy(lhs), VH_TVS_RA_DVAR);

		vh_typevar_reset(lhs);
	}
	else
	{
		/*
		 * No cache has been provided and it is not requested, so we'll have to
		 * be careful about cleaning up all our memory.
		 *
		 * The first priority is to get the accumulator type stack.
		 */

		return -1;
	}

	return 0;
}

static CacheVectorDot
vectordot_cache(Type *tys)
{
	Type tys_accum[VH_TAMS_MAX_DEPTH];
	size_t cache_sz, page_sz;
	CacheVectorDot cache;
	bool needs_input_transform;

	vh_type_stack_fillaccum(tys_accum, tys);
	needs_input_transform = !vh_type_stack_match(tys, tys_accum);

	if (needs_input_transform)
		page_sz = vh_tvp_space(tys_accum) * 4;
	else
		page_sz = vh_tvp_space(tys_accum) * 2;

	cache_sz = sizeof(struct CacheVectorDotData) + page_sz;
	cache = vhmalloc(cache_sz);

	/*
	 * Initialize our TypeVarPage
	 */
	vh_tvp_initialize(vectordot_page(cache), page_sz);
	vh_tvp_add(vectordot_page(cache), tys_accum);			/* Distance */
	vh_tvp_add(vectordot_page(cache), tys_accum);			/* Temp */

	if (needs_input_transform)
	{
		vh_tvp_add(vectordot_page(cache), tys_accum);		/* LHS */
		vh_tvp_add(vectordot_page(cache), tys_accum);		/* RHS */

		cache->tvope_input = vh_typevar_op_init("=",
												VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
																VH_OP_DT_TYSVAR,
																VH_OP_ID_INVALID,
																VH_OP_DT_TYSVAR,
																VH_OP_ID_INVALID),
												tys_accum,
												tys);
	}
	else
	{
		cache->tvope_input = 0;
	}

	cache->tvope_mult = vh_typevar_op_init("*",
										   VH_OP_MAKEFLAGS(VH_OP_DT_TYSVAR | VH_OP_DT_BOTH,
											   			   VH_OP_DT_TYSVAR,
														   VH_OP_ID_INVALID,
														   VH_OP_DT_TYSVAR,
														   VH_OP_ID_INVALID),
										   tys_accum,
										   tys_accum,
										   tys_accum);

	cache->tvope_accum = vh_typevar_op_init("+=",
											VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
															VH_OP_DT_TYSVAR,
															VH_OP_ID_INVALID,
															VH_OP_DT_TYSVAR,
															VH_OP_ID_INVALID),
											tys_accum,
											tys_accum);

	return cache;
}

