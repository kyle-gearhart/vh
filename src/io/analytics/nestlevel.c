/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarAcm.h"
#include "io/catalog/prepcol/prepcol.h"

#include "io/catalog/searchpath.h"
#include "io/catalog/sp/sptd.h"
#include "io/analytics/nestidx.h"
#include "io/analytics/nestlevel.h"

#include "io/catalog/types/njson.h"
#include "io/analytics/nloutput.h"

/*
 * NestLevel is responsible for the Aggregates and Trends.  From setup, inputs,
 * and generating data to populate a HeapTuple for a particular entry in the
 * index representing the NestLevel, this module is responsible for it.
 *
 * We are not responsible for deriving keys to access the index or index operations
 * beyond manipulating the leaf values.  That's the responsibility of the nest.
 */



/*
 * ============================================================================
 * GroupBy Helpers
 * ============================================================================
 */

static void nl_groupby_populate(NestLevel nla, GroupByType type, 
								const char *name,
								SearchPath sp, PrepCol pc);



/*
 * ============================================================================
 * Aggregate
 * ============================================================================
 */

/*
 * struct NestAggColData
 *
 * For each aggregate column on the nest level, we create an AggColData structure.
 * Storing the TypeVarAcm here allows us to reconstruct the ACM in memory on the fly.
 */

/*
 * ============================================================================
 * Aggregate Helpers
 * ============================================================================
 */
static NestAggCol nl_agg_create(NestLevel nl, const char *name,
								SearchPath sp, PrepCol pc, 
								vh_acm_tys_create acm);



/*
 * ============================================================================
 * Input Helpers
 * ============================================================================
 */

static int32_t nl_initialize_cols(NestLevel nl, NestIdxValue niv, HeapTuple ht);
static int32_t nl_initialize_agg_cols(NestLevel nl, NestIdxValue niv);
static int32_t nl_initialize_trend_cols(NestLevel nl, NestIdxValue niv);


/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

/*
 * vh_nl_create
 *
 * Creates a new, empty NestLevel that supports aggregates (the only option 
 * available).
 */
NestLevel
vh_nl_create(void)
{
	NestLevel nl;

	nl = vhmalloc(sizeof(struct NestLevelData));
	memset(nl, 0, sizeof(struct NestLevelData));

	return nl;
}


/*
 * vh_nl_gropuby_create
 *
 * Creates a GroupBy column on a particular NestLevel using the SearchPath sp
 * to retrieve the HeapField responsible for filling the value.
 */
int32_t
vh_nl_groupby_create(NestLevel nla, const char *name, SearchPath sp)
{
	/*
	 * Let's make sure the SearchPath gives us a HeapField.
	 */
	if (!sp)
	{
		elog(WARNING,
				emsg("Invalid SearchPath pointer [%p] passed to "
					 "vh_nla_grouby_create.  Unable to create a GroupBy column.",
					 sp));

		return -1;
	}

	if (!vh_sp_isa_tablefield(sp))
	{
		elog(WARNING,
				emsg("The SearchPath [%p] does not return a TableField.  "
					 "Cannot add the Group By column as requested.",
					 sp));

		return -2;
	}

	nl_groupby_populate(nla, GBT_COL, name, sp, 0);

	return 0;
}

int32_t
vh_nl_groupby_pc_create(NestLevel nla, const char *name,
						SearchPath sp, PrepCol pc)
{
	/*
	 * Let's make sure the SearchPath gives us a HeapField.
	 */
	if (!sp)
	{
		elog(WARNING,
				emsg("Invalid SearchPath pointer [%p] passed to "
					 "vh_nla_grouby_create.  Unable to create a GroupBy column.",
					 sp));

		return -1;
	}

	if (!vh_sp_isa_tablefield(sp))
	{
		elog(WARNING,
				emsg("The SearchPath [%p] does not return a TableField.  "
					 "Cannot add the Group By column as requested.",
					 sp));

		return -2;
	}

	/*
	 * Make sure we've got a PrepCol
	 */

	if (!pc)
	{
		elog(WARNING,
				emsg("Invalid PrepCol pointer [%p] passed to "
					 "vh_nla_groupby_create_pc.  Unable to create a GroupBy column",
					 pc));
	}

	nl_groupby_populate(nla, GBT_PREPCOL, name, sp, pc);

	return 0;
}

/*
 * nla_groupby_create
 * 
 * Creates a GroupByCol and populates with the NestLevel data required.
 */
static void 
nl_groupby_populate(NestLevel nla,
					GroupByType type,
					const char *name,
	   				SearchPath sp,
   					PrepCol pc)
{
	GroupByCol gbc;

	if (nla->groupby_n_cols)
	{
		nla->groupby_cols = vhrealloc(nla->groupby_cols,
									  sizeof(struct GroupByColData) *
									  (nla->groupby_n_cols + 1));
		gbc = &nla->groupby_cols[nla->groupby_n_cols++];
	}
	else
	{
		nla->groupby_cols = vhmalloc(sizeof(struct GroupByColData));
		gbc = &nla->groupby_cols[nla->groupby_n_cols++];
	}

	gbc->type = type;
	gbc->name = vh_cstrdup(name);
	gbc->sp_field = sp;
	gbc->pc = pc;
	gbc->idx_slot = -1;
}

int32_t
vh_nl_agg_create(NestLevel nl, const char *name,
				 SearchPath sp, vh_acm_tys_create acm)
{
	NestAggCol nac;

	if (!sp)
	{
		elog(WARNING,
				emsg("Invalid SearchPath pointer [%p] passed to "
					 "vh_nl_agg_create.  Unable to create a GroupBy column.",
					 sp));

		return -1;
	}

	if (!vh_sp_isa_tablefield(sp))
	{
		elog(WARNING,
				emsg("The SearchPath [%p] does not return a TableField.  "
					 "Cannot add the Group By column as requested.",
					 sp));

		return -2;
	}

	nac = nl_agg_create(nl, name, sp, 0, acm);

	return (nac == 0 ? -1 : 0);
}

int32_t
vh_nl_agg_create_pc(NestLevel nl, const char *name, 
					SearchPath sp, PrepCol pc,
					vh_acm_tys_create acm)
{
	if (!sp)
	{
		elog(WARNING,
				emsg("Invalid SearchPath pointer [%p] passed to "
					 "vh_nl_agg_create.  Unable to create a GroupBy column.",
					 sp));

		return -1;
	}

	if (!vh_sp_isa_tablefield(sp))
	{
		elog(WARNING,
				emsg("The SearchPath [%p] does not return a TableField.  "
					 "Cannot add the Group By column as requested.",
					 sp));

		return -2;
	}

	/*
	 * Make sure we've got a PrepCol
	 */

	if (!pc)
	{
		elog(WARNING,
				emsg("Invalid PrepCol pointer [%p] passed to "
					 "vh_nl_agg_create_pc.  Unable to create a GroupBy column",
					 pc));
	}

	nl_agg_create(nl, name, sp, pc, acm);

	return 0;
}

/*
 * nl_agg_create
 *
 * The most important part here to calculate the space requirement for the ACMS.
 * We'll store this in the index leaf, but we've got to know how to build out our
 * leaf value.
 */
static NestAggCol 
nl_agg_create(NestLevel nl, const char *name,
			  SearchPath sp, PrepCol pc, vh_acm_tys_create acm)
{
	NestAggCol ac;

	if (nl->agg_n_cols)
	{
		nl->agg_cols = vhrealloc(nl->agg_cols, 
								 (sizeof(struct NestAggColData) *
							     (++nl->agg_n_cols)));
		ac = &nl->agg_cols[nl->agg_n_cols - 1];
	}
	else
	{
		nl->agg_cols = vhmalloc(sizeof(struct NestAggColData));
		ac = nl->agg_cols;
		nl->agg_n_cols++;
	}

	ac->name = vh_cstrdup(name);
	ac->sp_field = sp;
	ac->pc = pc;
	ac->acm_create = acm;
	ac->idx = nl->agg_n_cols;
	ac->acms_sz = 0;

	return ac;
}

/*
 * vh_nl_leaf_size
 *
 * Returns the number of items and the size required for each item.  We go ahead
 * and make sure these land on a (uintptr_t) boundary, since vh_nestidxv_ is
 * going to force that when adding items.
 */
int8_t
vh_nl_leaf_size(NestLevel nl, size_t *ptr_sz)
{
	size_t sz;
	NestAggCol ac;
	int32_t i;

	sz = 0;
	
	for (i = 0; i < nl->agg_n_cols; i++)
	{
		ac = &nl->agg_cols[i];
		
		sz += ac->acms_sz;

		if (sz % sizeof(uintptr_t))
		{
			sz += sizeof(uintptr_t) - (sz % sizeof(uintptr_t));
		}
	}

	*ptr_sz = sz;

	return nl->agg_n_cols;
}

/*
 * vh_nl_input_ht
 *
 * Our heavy lifter that "inserts" this HeapTuple into the NestLevel.  Insert is
 * a bit of a myth, because we use the index key storage to hold the Group By
 * data.  The leaf of the index contains the state data for the aggregates and
 * trends.  We never store the HeapTuple or HeapTuplePtr to support the nest.
 *
 * It's all transient.  This gives us some more options to consume not just
 * HeapTuple (and its derivatives), but JSON.
 *
 * Thus, it's possible for the input to be tossed entirely as once these process.
 */
int32_t
vh_nl_input_ht(NestLevel nl, NestIdx idx,
			   NestIdxValue niv,
			   HeapTuple ht, HeapTuplePtr htp,
			   TypeVarSlot **akeys, TypeVarSlot **keys,
			   bool must_initialize)
{
	TypeVarSlot input, pc_slot;
	TypeVarSlot *pc_slots[1];
   	HeapField hf;	
	void *leaf_data;
	int32_t sp_res, pc_res, i;

	if (must_initialize)
	{
		if (nl_initialize_cols(nl, niv, ht))
		{
			elog(ERROR1,
					emsg("Error initializing columns for NestLevel [%p]",
						 nl));

			return -1;
		}
	}

	/*
	 * Spin thru each of the Aggregate columns, populating the slot with help
	 * from the search path.  Then call the aggregate.
	 */

	vh_tvs_init(&input);
	vh_tvs_init(&pc_slot);

	for (i = 0; i < nl->agg_n_cols; i++)
	{
		leaf_data = vh_nestidxv_value(niv, nl->idx, i);

		/*
		 * We should probably trigger this thing to request a bigger leaf size
		 * to fit everything on it.  For now, just skip it.  We handle the grow
		 * on a NestIdx later.
		 */
		if (!leaf_data)
			continue;

		hf = vh_sp_search(nl->agg_cols[i].sp_field, &sp_res, 2,
			   			  VH_SP_CTX_HT, ht,
					  	  VH_SP_CTX_NESTLEVEL, nl);
  		
		/*
		 * We did not find the desired SearchPath field for this particular
		 * context.  We'll just warn and continue thru the loop.
		 */
		if (!hf)
			continue;

		/*
		 * Execute a single PrepCol, in the future we'll probably allow more
		 * than one value to be passed tot he prepcol, but for now we'll just
		 * do one.  To send more than one, we'd just provide an array of 
		 * SearchPath and an array of PrepCol.  The order they sit is used to
		 * fill the @datas for vh_pc_populate_slot.
		 *
		 * If we don't have a PrepCol, just fill the slot with the HeapField
		 * value.
		 */
		if (nl->agg_cols[i].pc)
		{
			vh_tvs_store_ht_hf(&pc_slot, ht, hf);

			/*
			 * Fill our array on the stack with a TypeVarSlot from the stack.
			 * PrepCol populate_slot expects an array of pointers to TypeVarSlot
			 * with the "source" data.
			 */
			pc_slots[0] = &pc_slot;

			pc_res = vh_pc_populate_slot(nl->agg_cols[i].pc,
										 &input, pc_slots, 1);

			if (pc_res)
			{
			}
		}
		else
		{
			vh_tvs_store_ht_hf(&input, ht, hf);
		}

		/*
		 * Call the TypeVar ACMS routine to fire the input for the aggregate.
		 */

		vh_acms_input(nl->agg_cols[i].acm, leaf_data, &input);

	}

	return 0;
}


static int32_t 
nl_initialize_cols(NestLevel nl, NestIdxValue niv, HeapTuple ht)
{
	NestAggCol col;
	HeapField hf;
	size_t space;
	int32_t sp_res;
	int8_t i, items;
	
	if (!nl->acm_created)
	{
		/*
		 * We need to create the ACM, especially now that we know the type.
		 */

		for (i = 0; i < nl->agg_n_cols; i++)
		{
			col = &nl->agg_cols[i];
			hf = vh_sp_search(col->sp_field, &sp_res, 2,
							  VH_SP_CTX_HT, ht,
							  VH_SP_CTX_NESTLEVEL, nl);

			if (hf)
			{
				col->tys_depth = vh_type_stack_copy(col->tys, hf->types);
				col->acm = col->acm_create(hf->types);
				col->acms_sz = vh_acms_size(col->acm);
			}
		}

		nl->acm_created = true;
	}

	/*
	 * Check to make sure we've got enough space on this thing.
	 */
	items = vh_nl_leaf_size(nl, &space);

	if (vh_nestidxv_can_accomodate(niv, space, items))
	{

		nl_initialize_agg_cols(nl, niv);
		nl_initialize_trend_cols(nl, niv);

		return 0;
	}

	elog(WARNING,
			emsg("Insufficient space available on the NestIdxValue [%p], requires "
				 " %llu plus room for %d item pointers.",
				 niv,
				 space,
				 items));

	return -1;
}

static int32_t 
nl_initialize_agg_cols(NestLevel nl, NestIdxValue niv)
{
	void *item;
	int8_t i, max;

	max = nl->agg_n_cols;

	for (i = 0; i < max; i++)
	{
		item = vh_nestidxv_add(niv, nl->idx, i, nl->agg_cols[i].acms_sz);

		/*
		 * We should be good here, if we don't get a valid pointer back then
		 * vh_nestidxv_can_accomodate isn't working properly.
		 */

		assert(item);

		vh_acms_initialize(nl->agg_cols[i].acm, item, nl->agg_cols[i].acms_sz);	
	}

	return 0;
}

static int32_t 
nl_initialize_trend_cols(NestLevel nl, NestIdxValue niv)
{
	return 0;
}

