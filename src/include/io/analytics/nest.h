/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_analytics_nest_H
#define vh_analytics_nest_H

#include "io/catalog/searchpath.h"
#include "io/catalog/prepcol/prepcol.h"

typedef struct GroupByColData *GroupByCol;
typedef struct NestData *Nest;
typedef struct NestIdxData *NestIdx;
typedef struct NestIdxValueData *NestIdxValue;
typedef struct NestLevelData *NestLevel;

struct NestData
{
	NestIdx idxs;
	int32_t n_idxs;
	
	NestLevel *ls;
	int32_t n_ls;
};

/*
 * In the leaf nodes of the last AggGroupByCol index, we store an array of
 * TypeVarAcmState equal to the number of aggregate columns.
 */
typedef enum
{
	GBT_COL,
	GBT_PREPCOL
} GroupByType;


/*
 * struct GroupByColdata
 *
 * For each of the GroupBy column on the nest level, we create an GroupByColData
 * structure to track how to derive the TypeVarSlot to access in the index.  We
 * don't store indices at the NestLevel because it's possible multiple NestLevels
 * could share the same index.
 */
struct GroupByColData
{
	GroupByType type;
	
	const char *name;

	SearchPath sp_field;		/* Use a search path to find the field */
	PrepCol pc;					/* PrepCol to transform the field */

	int32_t idx_slot;			/* ScanKey Column Number */
};

Nest vh_nest_create(void);

int32_t vh_nest_level_add(Nest nest, NestLevel nl);
int32_t vh_nest_input_htp(Nest nest, HeapTuplePtr htp);

#endif

