/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/types/njson.h"
#include "io/analytics/nloutput.h"


#define NLO_ROOT_METHOD_ARRAY		0x01
#define NLO_ROOT_METHOD_VALUE		0x02


typedef struct NestLevelOutputCtxData NestLevelOutputCtxData, *NestLevelOutputCtx;

struct NestLevelOutputCtxData
{
	NestLevel nl;
	Json root;

	int32_t root_method;

	bool output_keys;
};


/*
 * Private Declarations
 */
static bool nlo_json_scan(NestIdxAccess nia, void* user);

/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

Json
vh_nlo_json(NestLevel nl, int32_t flags)
{
	/*
	 * For now, just use vh_nestidx_scan_all to do our scan.
	 */
	NestIdx idx = 0;
	NestLevelOutputCtxData nlo_ctx = { };

	idx = &nl->nest->idxs[nl->idx];

	nlo_ctx.root = vh_json_make_array();
	nlo_ctx.root_method = NLO_ROOT_METHOD_ARRAY;
	nlo_ctx.output_keys = true;
	nlo_ctx.nl = nl;

	vh_nestidx_scan_all(idx, nlo_json_scan, &nlo_ctx, false);

	return nlo_ctx.root;	
}

/*
 * vh_nlo_item_json
 *
 * Returns true if successful.  Creates a new JSON structure.
 */
bool
vh_nlo_item_json(NestLevel nl, NestIdxAccess nia)
{
	return true;
}



static bool 
nlo_json_scan(NestIdxAccess nia, void* user)
{
	NestLevelOutputCtx ctx = user;
	NestLevel nl = ctx->nl;
	NestIdxValue niv = nia->data;
	NestAggCol col;
	Json j_obj, j_pair;
	TypeVarSlot agg_slot, *groupby_slot;
	TypeVarAcmState tvacms;
	TypeVar json_target;
	Type tys[VH_TAMS_MAX_DEPTH];
	int32_t i, max, acms_res;
	int8_t ty_depth;
	bool is_typevar;

	j_obj = vh_json_make_object();

	/*
	 * Output the keys and their values for this entry.
	 */
	if (ctx->output_keys)
	{
		for (i = 0; i < nl->groupby_n_cols; i++)
		{
			groupby_slot = &nia->keys[nl->groupby_cols[i].idx_slot];
			ty_depth = vh_tvs_fill_tys(groupby_slot, tys);
			j_pair = vh_json_make_pair(tys, ty_depth, nl->groupby_cols[i].name);
			json_target = vh_json_typevar(j_pair, &is_typevar);

			assert(is_typevar);
		
			vh_typevar_op("=",
					  VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
						  			  VH_OP_DT_VAR,
									  VH_OP_ID_INVALID,
									  VH_OP_DT_TVS,
									  VH_OP_ID_INVALID),
					  json_target,
					  groupby_slot);

			vh_json_obj_add_pair(j_obj, j_pair);
		}
	}

	/*
	 * Output the aggregates
	 */
	vh_tvs_init(&agg_slot);
	max = nl->agg_n_cols;
	
	for (i = 0; i < max; i++)
	{
		col = &nl->agg_cols[i];

		tvacms = vh_nestidxv_value(niv, nl->idx, i);

		if (!tvacms)
			continue;

		j_pair = vh_json_make_pair(col->tys, col->tys_depth, col->name);
		json_target = vh_json_typevar(j_pair, &is_typevar);
		vh_json_obj_add_pair(j_obj, j_pair);

		assert(is_typevar);
			
		/*
		 * Call vh_acms_result to generate a result for us to transfer over to
		 * the JSON.
		 */

		acms_res = vh_acms_result(col->acm, tvacms, &agg_slot);

		if (acms_res)
		{
		}

		vh_typevar_op("=",
					  VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
						  			  VH_OP_DT_VAR,
									  VH_OP_ID_INVALID,
									  VH_OP_DT_TVS,
									  VH_OP_ID_INVALID),
					  json_target,
					  &agg_slot);
	}

	vh_tvs_reset(&agg_slot);

	switch (ctx->root_method)
	{
		case NLO_ROOT_METHOD_ARRAY:

			vh_json_arr_push(ctx->root, j_obj);

			break;

		case NLO_ROOT_METHOD_VALUE:

			break;


		default:
			break;
	}

	return true;
}

