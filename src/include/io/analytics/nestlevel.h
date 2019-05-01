/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_analytics_nestlevel_H
#define vh_io_analytics_nestlevel_H

#include "io/analytics/nest.h"
#include "io/catalog/TypeVarAcm.h"
#include "io/catalog/TypeVarSlot.h"

/*
 * Nest Levels define each level in a nest heirarchy.  The level allow for aggregates
 * to be created based on the underlying data records.  First users should create a 
 * NestLevel for a TableDef.  Then an aggregate level may be created.  Aggregate levels
 * may have group by fields in addition to accumulator fields.
 */
typedef struct NestLevelData *NestLevel;
typedef struct NestAggColData *NestAggCol;

struct NestLevelData
{
	Nest nest;
	NestLevel parent;			/* This is the parent based on the keys */
	HeapTupleDef htd;

	GroupByCol groupby_cols;
	int32_t groupby_n_cols;

	NestAggCol agg_cols;
	int32_t agg_n_cols;

	int32_t idx;

	bool acm_created;
};

struct NestAggColData
{
	const char *name;
	SearchPath sp_field;
	PrepCol pc;

	union
	{
		TypeVarAcm acm;
		vh_acm_tys_create acm_create;
	};

	size_t acms_sz;

	Type tys[VH_TAMS_MAX_DEPTH];
	int8_t tys_depth;

	int8_t idx;
};



/*
 * ============================================================================
 * NestLevel Actions
 * ============================================================================
 */

NestLevel vh_nl_create(void);



/*
 * ============================================================================
 * Group By Columns
 * ============================================================================
 */

int32_t vh_nl_groupby_create(NestLevel nla, const char *name,
							 SearchPath sp);
int32_t vh_nl_groupby_pc_create(NestLevel nla, const char *name,
								SearchPath sp, PrepCol pc);


/*
 * ============================================================================
 * Aggregate Columns
 * ============================================================================
 *
 * We don't pass a live ACM into the creator.  Instead, we pass a factory like
 * function that will be triggered prior the first set of data arrives.  This
 * allows the NestLevel to setup the ACM, rather than having to do it prior 
 * to the NestLevel being created.
 *
 * Thus the first input into the nest will determine the data type that's on
 * the TypeVar aggregate.
 */

int32_t vh_nl_agg_create(NestLevel nla, const char *name,
						 SearchPath sp, vh_acm_tys_create acm);
int32_t vh_nl_agg_create_pc(NestLevel nla, const char *name,
							SearchPath sp, PrepCol pc, 
							vh_acm_tys_create acm);

/*
 * Note: Should the Aggregate require more than one value, we could allow an
 * array of SearchPath and PrepCol, which would indicate how to feed the 
 * TypeVarAcm.  Too complex for now.  Build the basics
 */

int8_t vh_nl_leaf_size(NestLevel nl, size_t *ptr_sz);


/*
 * ============================================================================
 * Computation Routines
 * ============================================================================
 */

int32_t vh_nl_input_ht(NestLevel nl, NestIdx nidx,
					   NestIdxValue niv,
	   				   HeapTuple ht, HeapTuplePtr htp,
					   TypeVarSlot **akeys, TypeVarSlot **keys,
					   bool must_initialize);
/*
int32_t vh_nl_input_json(NestLevel nl, NestIdx nidx,
						 NestIdxValue niv,
						 Json json,
						 TypeVarSlot **akeys, TypeVarSot **keys,
						 bool must_initialize);
*/

#endif

