/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarSlot.h"
#include "io/catalog/prepcol/pcminmax.h"



/*
 * ============================================================================
 * PrepCol Implementation Declaration
 * ============================================================================
 */

static int32_t pcminmax_populate_slot(void *pc, TypeVarSlot *slot_target,
									  TypeVarSlot **datas, int32_t ndatas);
static int32_t pcminmax_finalize(void *pc);

static const struct PrepColFuncTableData pcminmax_func = {
	.populate_slot = pcminmax_populate_slot,

	.finalize = pcminmax_finalize
};



typedef struct pcminmax_data pcminmax_data;

struct pcminmax_data
{
	struct PrepColData pc;

	TypeVarSlot minimum;
	TypeVarSlot maximum;
};



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

PrepCol
vh_pcminmax_create(TypeVarSlot* min, TypeVarSlot* max)
{
	pcminmax_data *pcminmax;
	double val;
	bool ok;

	pcminmax = vh_pc_create(&pcminmax_func, sizeof(struct pcminmax_data));
	vh_tvs_copy(&pcminmax->minimum, min);
	vh_tvs_copy(&pcminmax->maximum, max);

	/*
	 * Go ahead and convert our input min/max over to doubles, since that's what
	 * we'll be returning from populate_slot anyways.  By doing it now, once,
	 * we avoid the overhead of having to do it each and every time 
	 * populate_slot is called.
	 */
	ok = vh_tvs_double(&pcminmax->minimum, &val);

	if (ok)
		vh_tvs_store_double(&pcminmax->minimum, val);

	ok = vh_tvs_double(&pcminmax->maximum, &val);

	if (ok)
		vh_tvs_store_double(&pcminmax->maximum, val);

	return &pcminmax->pc;
}



/*
 * ============================================================================
 * PrepCol Implementation
 * ============================================================================
 */

static int32_t
pcminmax_finalize(void *pc)
{
	pcminmax_data *pcminmax = pc;

	vh_tvs_finalize(&pcminmax->minimum);
	vh_tvs_finalize(&pcminmax->maximum);

	return 0;
}

static int32_t 
pcminmax_populate_slot(void *pc, TypeVarSlot *slot_target,
					   TypeVarSlot **datas, int32_t ndatas)
{
	pcminmax_data *pcminmax = pc;
	double min, max, val;
	bool ok;

	ok = vh_tvs_double(datas[0], &val);

	if (ok)
	{
		vh_tvs_double(&pcminmax->minimum, &min);
		vh_tvs_double(&pcminmax->maximum, &max);

		val = (val - min) / (max - min);

		vh_tvs_store_double(slot_target, val);

		return 0;
	}

	return -1;
}

