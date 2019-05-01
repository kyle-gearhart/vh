/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>



#include "vh.h"
#include "io/analytics/nestlevel.h"
#include "io/catalog/HeapField.h"
#include "io/catalog/HeapTuple.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/prepcol/pctsint.h"
#include "io/catalog/sp/spht.h"
#include "io/catalog/types/DateTime.h"

#include "test.h"
#include "io/utils/stopwatch.h"

TableDef td_nest = 0;

Nest nest = 0;
NestLevel nl1 = 0;
NestLevel nl2 = 0;

static void nest_setup_td(void);
static void nest_setup(void);

static void nest_fill(void);

static HeapTuplePtr nest_create_ht(TableDef td, 
								   DateTime time, 
								   int16_t sensorid, int32_t value);

static Type tys_DateTime[] = { &vh_type_DateTime, 0 };
static Type tys_int16[] = { &vh_type_int16, 0 };
static Type tys_int32[] = { &vh_type_int32, 0 };

void test_nest_entry(void)
{
	nest_setup();
	nest_setup_td();
	
	nest_fill();
}

static void
nest_setup(void)
{
	SearchPath sp;
	PrepCol pc;

	nest = vh_nest_create();
	nl1 = vh_nl_create();
	nl2 = vh_nl_create();

	/* Nest Level 1 and 2 Group Bys */
	sp = vh_spht_tf_create("time");
	pc = vh_pctsint_ts_create(0, 1, VH_PCTSINT_MINUTES, false);
	vh_nl_groupby_pc_create(nl1, "time", sp, pc);
	vh_nl_groupby_pc_create(nl2, "time", sp, pc);

	sp = vh_spht_tf_create("sensorid");
	vh_nl_groupby_create(nl2, "sensor", sp);

	sp = vh_spht_tf_create("temperature");

	/* Nest Level 1 Aggregates */
	/*
	vh_nl_agg_create(nl1, sp, vh_acm_avg_tys);
	vh_nl_agg_create(nl1, sp, vh_acm_min_tys);
	vh_nl_agg_create(nl1, sp, vh_acm_max_tys);
	vh_nl_agg_create(nl1, sp, vh_acm_varp_tys);
	vh_nl_agg_create(nl1, sp, vh_acm_vars_tys);
	*/

	/* Nest Level 2 Aggregates */
	vh_nl_agg_create(nl2, "avg", sp, vh_acm_avg_tys);
	vh_nl_agg_create(nl2, "varp", sp, vh_acm_varp_tys);
	vh_nl_agg_create(nl2, "min", sp, vh_acm_min_tys);

	vh_nest_level_add(nest, nl1);
}

static void
nest_setup_td(void)
{
	td_nest = vh_td_create(false);
	vh_td_tf_add(td_nest, tys_DateTime, "time");
	vh_td_tf_add(td_nest, tys_int16, "sensorid");
	vh_td_tf_add(td_nest, tys_int32, "temperature");
}

static void
nest_fill(void)
{
	struct DateTimeSplit dts = { };
	HeapTuplePtr htps[10];
	DateTime dt;
	HeapTuplePtr htp;
	int32_t i, j;
	struct vh_stopwatch watch = { };

	dts.year = 2017;
	dts.month = 3;
	dts.month_day = 27;
	dts.hour = 15;
	dts.minutes = 58;
	dts.seconds = 3;

	dt = vh_ty_ts2datetime(&dts);
	htp = nest_create_ht(td_nest, dt, 1, 72);
	vh_nest_input_htp(nest, htp);
	htps[0] = htp;
	
	htp = nest_create_ht(td_nest, dt, 1, 75);
	vh_nest_input_htp(nest, htp);
	htps[1] = htp;

	htp = nest_create_ht(td_nest, dt, 1, 78);
	vh_nest_input_htp(nest, htp);
	htps[2] = htp;
	
	htp = nest_create_ht(td_nest, dt, 1, 69);
	vh_nest_input_htp(nest, htp);
	htps[3] = htp;
	
	htp = nest_create_ht(td_nest, dt, 1, 70);
	vh_nest_input_htp(nest, htp);
	htps[4] = htp;
	
	htp = nest_create_ht(td_nest, dt, 1, 81);
	vh_nest_input_htp(nest, htp);
	htps[5] = htp;

	printf("\nInputting %d HeapTuplePtr into the nest...", 165000 * 6);
	vh_stopwatch_start(&watch);

	for (i = 0; i < 165000; i++)
	{
		for (j = 0; j < 6; j++)
		{
			vh_nest_input_htp(nest, htps[j]);
		}
	}

	vh_stopwatch_end(&watch);

	printf("complete in [%ld] ms\n", vh_stopwatch_ms(&watch));
}

static HeapTuplePtr 
nest_create_ht(TableDef td, 
			   DateTime time, 
			   int16_t sensorid, 
			   int32_t value)
{
	HeapTuplePtr htp;
	DateTime *dt;

	htp = vh_allochtp_td(td);
	dt = (DateTime*)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, "time");
	*dt = time;

	vh_typevar_op("=", VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
									   VH_OP_DT_HTP,
									   VH_OP_ID_NM,
									   VH_OP_DT_I16,
									   VH_OP_ID_INVALID),
				  htp, "sensorid", sensorid);
	vh_typevar_op("=", VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
									   VH_OP_DT_HTP,
									   VH_OP_ID_NM,
									   VH_OP_DT_I32,
									   VH_OP_ID_INVALID),
				  htp, "temperature", value);

	return htp;
}

