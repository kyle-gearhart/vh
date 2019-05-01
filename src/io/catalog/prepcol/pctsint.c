/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/prepcol/pctsint.h"
#include "io/catalog/types/DateTime.h"

/*
 * ============================================================================
 * PrepCol Implementation Declaration
 * ============================================================================
 */

static int32_t pctsint_populate_slot(void *pc, TypeVarSlot *slot_target,
									 TypeVarSlot **datas, int32_t ndatas);
static int32_t pctsint_finalize(void *pc);

static const struct PrepColFuncTableData pctsint_func = {
	.populate_slot = pctsint_populate_slot,

	.finalize = pctsint_finalize
};



/*
 * ============================================================================
 * Time Series Interval Implementation
 * ============================================================================
 */

typedef struct pctsint_data pctsint_data;

struct pctsint_data
{
	struct PrepColData pc;

	void *base;			/* TypeVar */
	int32_t interval;
	int32_t interval_type;
	bool lower;
};



/*
 * ============================================================================
 * Helper Functions and Structures
 * ============================================================================
 */

static Type tys_datetime[] = { &vh_type_DateTime, 0 };
//static Type tys_date[] = { &vh_type_Date, 0 };


/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

PrepCol
vh_pctsint_dt_create(TypeVar base,
					 int32_t interval, int32_t interval_type,
					 bool lower)
{
	return 0;
}

PrepCol
vh_pctsint_ts_create(TypeVar base,
					 int32_t interval, int32_t interval_type,
					 bool lower)
{
	pctsint_data *pctsint;
	struct DateTimeSplit dts;
	DateTime *root;

	pctsint = vh_pc_create(&pctsint_func, sizeof(struct pctsint_data));

	if (!base)
	{
		dts.year = 1970;
		dts.month = 1;
		dts.month_day = 1;
		dts.hour = 0;
		dts.minutes = 0;
		dts.seconds = 0;

		root = pctsint->base = vh_typevar_make_tys(tys_datetime);
		*root = vh_ty_ts2datetime(&dts);
	}
	else
	{
		pctsint->base = vh_typevar_makecopy(base);
	}

	pctsint->interval = interval;
	pctsint->interval_type = interval_type;
	pctsint->lower = lower;

	return &pctsint->pc;
}



/*
 * ============================================================================
 * PrepCol Implementation
 * ============================================================================
 */

static int32_t
pctsint_populate_slot(void *pc, TypeVarSlot *slot_target,
					  TypeVarSlot **datas, int32_t ndatas)
{
	pctsint_data *pctsint = pc;
	DateTime *dt_base, *dt_value, *dt_target, dt_diff;
	int64_t factor;
	
	assert(ndatas == 1);
	assert(datas);
	assert(datas[0]);

	switch (pctsint->interval_type & (0xf0))
	{
		case 0x00:
			/*
			 * This is involves time, so we'll want to use DateTime and then use
			 * the ticks per utilities to calculate the DateTime boundary.
			 */

			dt_base = pctsint->base;
			dt_value = vh_tvs_value(datas[0]);

			switch (pctsint->interval_type & (0x0f))
			{
				case VH_PCTSINT_SECONDS:
					factor = USECS_PER_SEC;
					break;

				case VH_PCTSINT_MINUTES:
					factor = USECS_PER_MINUTE;
					break;

				case VH_PCTSINT_HOURS:
					factor = USECS_PER_HOUR;
					break;

				default:
					return -2;
			}

			/*
			dt_diff = *dt_value - *dt_base;
			dt_diff /= (factor * pctsint->interval);
			dt_diff += lower;
			*/

			dt_diff = *dt_value + (factor * pctsint->interval);
			dt_diff -= *dt_base;
			dt_diff /= (factor * pctsint->interval);

			if (pctsint->lower)
				dt_diff -= 1;

			/*
			 * dt_diff now contains the number of intervals from root
			 * that we have.
			 */

			dt_diff *= (factor * pctsint->interval);
			dt_target = vh_tvs_value(slot_target);
			*dt_target = dt_diff + *dt_base;

			return 1;


		default:
			elog(WARNING,
					emsg("Unrecognized internval type [%d]  for PrepCol Timeseries "
						 "Interval at [%p]",
						 pctsint->interval_type,
						 pctsint));

			return -2;
	}

	return -2;
}
static int32_t
pctsint_finalize(void *pc)
{
	pctsint_data *pctsint = pc;

	if (pctsint->base)
	{
		vh_typevar_destroy(pctsint->base);
		pctsint->base = 0;
	}

	return 0;
}

