/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_analytics_nestidx_H
#define vh_analytics_nestidx_H

#include "io/analytics/nest.h"
#include "io/catalog/TypeVarSlot.h"

#define VH_NESTIDX_METH_BTREE 	0x01
#define VH_NESTIDX_METH_MAX		1

typedef struct NestIdxAccessData *NestIdxAccess;


/*
 * ============================================================================
 * NestIdx Core Datastrutures
 * ============================================================================
 */

typedef struct NestScanKeyData *NestScanKey;

struct NestIdxFuncTable
{
	int32_t (*access)(NestIdx idx, TypeVarSlot **keys, int32_t am, NestIdxAccess nia);

	/*
	 * Scan Functionality
	 */
	int32_t (*scan_begin)(NestIdx idx, NestIdxAccess nia, int32_t n_scankeys);
	bool (*scan_first)(NestIdx idx, NestIdxAccess nia, 
					   NestScanKey *skeys, int32_t n_skeys,
					   bool forward);
	bool (*scan_next)(NestIdx idx, NestIdxAccess nia, bool forward);
	void (*scan_end)(NestIdx idx, NestIdxAccess nia);

};

struct NestIdxData
{
	void *idx;				/* Opaque pointer the index */

	NestLevel *ls;
	int32_t n_ls;

	GroupByCol *cols;
	int32_t n_cols;

	int32_t method;			/* See VH_NESTIDX_METH_ defines */

	bool in_use;
};


/*
 * ============================================================================
 * Accesses
 * ============================================================================
 */

struct NestIdxAccessData
{
	int32_t am;
	
	bool exists;
	bool inserted;

	void *data;
	void *array;
	void *opaque;				/* Used for scans */
	void *extra;				/* Used for scans */

	TypeVarSlot keys[1];
};



#define VH_NESTIDX_AM_FETCH		0x01
#define VH_NESTIDX_AM_INSERT	0x02
#define VH_NESTIDX_AM_DELETE	0x04
#define VH_NESTIDX_AM_SCAN		0x08

NestIdxAccess vh_nestidx_access_create(NestIdx idx);
int32_t vh_nestidx_access_reset(NestIdx idx, NestIdxAccess nia);
int32_t vh_nestidx_access_destroy(NestIdx idx, NestIdxAccess nia);


int32_t vh_nestidx_access(NestIdx idx, 
						  GroupByCol *cols, TypeVarSlot **keys, int32_t n_vals, 
						  int32_t am, 
						  NestIdxAccess nia);

/*
 * ============================================================================
 * Scans
 * ============================================================================
 *
 * For nest scans, we want to provide opaque methods to do things like LAG and
 * exports.  These methods will setup the appropriate scankeys to access the
 * index itself thru the function table.  Thus we require a callback function
 * which will be hit at each iteration.
 */


struct NestScanKeyData
{
	TypeVarSlot tvs;
	GroupByCol col;
	int8_t oper;
};

typedef bool (*vh_nestidx_scan_cb)(NestIdxAccess nia, void *user);

int32_t vh_nestidx_scan_all(NestIdx idx,
							vh_nestidx_scan_cb cb, void *user,
							bool forward);

int32_t vh_nl_scan_all(NestLevel nl, vh_nestidx_scan_cb cb, void *user, bool fwd);

/*
 * ============================================================================
 * Leaf "Pages"
 * ============================================================================
 */

/*
 * struct NestIdxValueData
 *
 * We work this similar to a HeapPage.  At the start of each index value we'll
 * find this structure.  The goal is to index the offsets by the NestLevel's 
 * index in the index and then by the item's (NestLevelAggCol or 
 * NestLevelTrendCol) index in the NestLevel.
 *
 * The offset indicates how many bytes to offset from the the start of the
 * NestIdxValueData structure.  We can calculate the number of items in the
 * array by subtracting the d_upper from the offsetof d_lower and dividing
 * by the sizeof a single slot. 
 */

struct NestIdxValueData
{
	int16_t d_upper;
	int16_t d_lower;

	struct
	{
		int8_t nl_idx;
		int8_t item_idx;
	   	int16_t offset;	
	} slots[1];
};

#define vh_nestidxv_slot_size 			(sizeof(int8_t) + sizeof(int8_t) + sizeof(int16_t))
#define vh_nestidxv_array_size(niv)		(((niv)->d_upper - 									\
										 offsetof(struct NestIdxValueData, slots[0])) / 	\
										 vh_nestidxv_slot_size)
#define vh_nestidxv_freespace(niv)		((niv)->d_lower - (niv)->d_upper)
#define vh_nestidxv_freespace_slot(niv)	((niv)->d_lower - (niv)->d_upper - vh_nestidxv_slot_size)
#define vh_nestidxv_can_accomodate(niv, sz, items)	(vh_nestidxv_freespace(niv) >= ((sz) + (vh_nestidxv_slot_size * (items))))

NestIdxValue vh_nestidxv_initialize(void *data, size_t sz);

void* vh_nestidxv_value(NestIdxValue niv, int8_t nl_idx, int8_t item_idx);
void* vh_nestidxv_add(NestIdxValue niv, int8_t nl_idx, int8_t item_idx, size_t sz);
void vh_nestidxv_remove(NestIdxValue niv, int8_t nl_idx, int8_t item_idx);



/*
 * ============================================================================
 * Function Tables
 * ============================================================================
 */
extern const struct NestIdxFuncTable vh_nestidx_func_btree;

#endif

