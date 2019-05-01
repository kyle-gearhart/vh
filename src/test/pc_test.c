/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include <assert.h>

#include "vh.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/prepcol/pcdefaultv.h"
#include "io/catalog/prepcol/pctsint.h"
#include "io/catalog/types/DateTime.h"

static void test_pc_defaultv(void);
static void test_pc_tsint(void);


void
test_pc_entry(void)
{
	test_pc_defaultv();
	test_pc_tsint();
}

static void
test_pc_defaultv(void)
{
	int32_t *tgt = vh_makevar1(int);
	int32_t *dflt = vh_makevar1(int);
	TypeVarSlot tgt_slot;
	PrepCol pc;

	*tgt = 0;
	*dflt = 500;

	vh_tvs_init(&tgt_slot);
	vh_tvs_store_var(&tgt_slot, tgt, 0);

	pc = vh_pc_defaultv_create(dflt, false);

	vh_pc_populate_slot(pc, &tgt_slot, 0, 0);

	assert(*dflt == 500);
	assert(*tgt == *dflt);
	
	/*
	 * Switch our target back to zero and let's check to make sure the TypeVarOpExec
	 * cache stuck.
	 */
	*tgt = 0;

	vh_pc_populate_slot(pc, &tgt_slot, 0, 0);
	assert(*dflt == 500);
	assert(*tgt == *dflt);

	vh_pc_destroy(pc);
}

static void
test_pc_tsint(void)
{
	struct DateTimeSplit dts = { };
	PrepCol pc1;
	DateTime *dest, *src;
	TypeVarSlot dest_slot, src_slot;
	TypeVarSlot *slots[1];
	int32_t ret;

	slots[0] = &src_slot;

	dest = vh_makevar1(DateTime);
	src = vh_makevar1(DateTime);

	vh_tvs_init(&dest_slot);
	vh_tvs_init(&src_slot);

	dts.year = 2017;
	dts.month = 3;
	dts.month_day = 21;
	dts.hour = 12;
	dts.minutes = 37;
	dts.seconds = 7;

	*src = vh_ty_ts2datetime(&dts);

	vh_tvs_store_var(&dest_slot, dest, 0);
	vh_tvs_store_var(&src_slot, src, 0);
	
	pc1 = vh_pctsint_ts_create(0, 10, VH_PCTSINT_SECONDS, true);
	ret = vh_pc_populate_slot(pc1, &dest_slot, slots, 1);
	vh_ty_datetime2ts(&dts, *dest);
	assert(ret == 1);
	assert(dts.year == 2017);
	assert(dts.month == 3);
	assert(dts.month_day = 21);
	assert(dts.minutes == 37);
	assert(dts.seconds == 0);

	vh_ty_datetime2ts(&dts, *dest);
	assert(dest);
	vh_pc_destroy(pc1);

	/*
	 * Disable Lower Boundary
	 */
	pc1 = vh_pctsint_ts_create(0, 10, VH_PCTSINT_SECONDS, false);
	ret = vh_pc_populate_slot(pc1, &dest_slot, slots, 1);
	assert(ret == 1);

	vh_ty_datetime2ts(&dts, *dest);
	assert(dest);
	assert(dts.year == 2017);
	assert(dts.month == 3);
	assert(dts.month_day = 21);
	assert(dts.minutes == 37);
	assert(dts.seconds == 10);
	vh_pc_destroy(pc1);

	/*
	 * Do this every sixty seconds
	 */
	pc1 = vh_pctsint_ts_create(0, 60, VH_PCTSINT_SECONDS, true);
	ret = vh_pc_populate_slot(pc1, &dest_slot, slots, 1);
	assert(ret == 1);

	vh_ty_datetime2ts(&dts, *dest);
	assert(dest);
	assert(dts.year == 2017);
	assert(dts.month == 3);
	assert(dts.month_day = 21);
	assert(dts.minutes == 37);
	assert(dts.seconds == 0);
	vh_pc_destroy(pc1);
	
	/*
	 * Do this every sixty seconds, with lower disabled
	 */
	pc1 = vh_pctsint_ts_create(0, 60, VH_PCTSINT_SECONDS, false);
	ret = vh_pc_populate_slot(pc1, &dest_slot, slots, 1);
	assert(ret == 1);

	vh_ty_datetime2ts(&dts, *dest);
	assert(dest);
	assert(dts.year == 2017);
	assert(dts.month == 3);
	assert(dts.month_day = 21);
	assert(dts.minutes == 38);
	assert(dts.seconds == 0);
	vh_pc_destroy(pc1);
	
	/*
	 * Do this every one minute
	 */
	pc1 = vh_pctsint_ts_create(0, 1, VH_PCTSINT_MINUTES, true);
	ret = vh_pc_populate_slot(pc1, &dest_slot, slots, 1);
	assert(ret == 1);

	vh_ty_datetime2ts(&dts, *dest);
	assert(dest);
	assert(dts.year == 2017);
	assert(dts.month == 3);
	assert(dts.month_day = 21);
	assert(dts.minutes == 37);
	assert(dts.seconds == 0);
	vh_pc_destroy(pc1);
	
	/*
	 * Do this every one minute, not the lower boundary
	 */
	pc1 = vh_pctsint_ts_create(0, 1, VH_PCTSINT_MINUTES, false);
	ret = vh_pc_populate_slot(pc1, &dest_slot, slots, 1);
	assert(ret == 1);

	vh_ty_datetime2ts(&dts, *dest);
	assert(dest);
	assert(dts.year == 2017);
	assert(dts.month == 3);
	assert(dts.month_day = 21);
	assert(dts.minutes == 38);
	assert(dts.seconds == 0);
	vh_pc_destroy(pc1);
	
	/*
	 * Do this every one hour
	 */
	pc1 = vh_pctsint_ts_create(0, 1, VH_PCTSINT_HOURS, true);
	ret = vh_pc_populate_slot(pc1, &dest_slot, slots, 1);
	assert(ret == 1);

	vh_ty_datetime2ts(&dts, *dest);
	assert(dest);
	assert(dts.year == 2017);
	assert(dts.month == 3);
	assert(dts.month_day = 21);
	assert(dts.hour == 12);
	assert(dts.minutes == 0);
	assert(dts.seconds == 0);
	vh_pc_destroy(pc1);
	
	/*
	 * Do this every one minute, not the lower boundary
	 */
	pc1 = vh_pctsint_ts_create(0, 1, VH_PCTSINT_HOURS, false);
	ret = vh_pc_populate_slot(pc1, &dest_slot, slots, 1);
	assert(ret == 1);

	vh_ty_datetime2ts(&dts, *dest);
	assert(dest);
	assert(dts.year == 2017);
	assert(dts.month == 3);
	assert(dts.month_day = 21);
	assert(dts.hour == 13);
	assert(dts.minutes == 0);
	assert(dts.seconds == 0);
	vh_pc_destroy(pc1);
}


