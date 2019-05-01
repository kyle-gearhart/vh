/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "io/config/cfgjson.h"
#include "io/analytics/nestlevel.h"

static int32_t cfgj_nl_aggregate(NestLevel nl, Json jval, int32_t flags);
static int32_t cfgj_nl_groupby(NestLevel nl, Json jval, int32_t flags);


/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

/*
 * vh_cfgj_nl
 *
 * Not a lot to do here other than validate the schema and call other 
 * configuration routines.
 */
int32_t
vh_cfgj_nl(NestLevel *nl, Json jval, int32_t flags)
{
	NestLevel nll = 0;
	Json jarr, jpair;
	int32_t ret;
	bool isa;

	if (!vh_json_isa_obj(jval))
	{
		vh_cfgj_throw_error(flags, 1, "Expected a JSON Object to define a "
							"NestLevel pointed to by [%p]",
							jval);
	}

	if (!vh_cfgj_check_only(flags))
	{
		nll = vh_nl_create();
		*nl = nll;
	}
	
	jpair = vh_json_obj_key_byname(jval, "groupBy");

	if (jpair)
	{
		jarr = vh_json_objarr(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 2, "Expected an Array at key %s",
								"groupBy");
		}

		ret = cfgj_nl_groupby(nll, jarr, flags);

		if (ret)
		{
			vh_cfgj_throw_error(flags, 4, "Unable to process groupBy directive", 0);
		}
	}

	jpair = vh_json_obj_key_byname(jval, "aggregate");

	if (jpair)
	{
		jarr = vh_json_objarr(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 3, "Expected an Array at key %s",
								"aggregate");
		}

		ret = cfgj_nl_aggregate(nll, jarr, flags);

		if (ret)
		{
			vh_cfgj_throw_error(flags, 5, "Unable to process groupBy directive", 0);
		}
	}

	return 0;
}


/*
 * ============================================================================
 * Private Functions
 * ============================================================================
 */

static int32_t 
cfgj_nl_aggregate(NestLevel nl, Json jval, int32_t flags)
{
	union
	{
		void (*base)(void);
		vh_acm_tys_create acmc;
	} funcs;

	Json jobj, jpair, juservalue;
	SearchPath sp = 0;
	String str;
	int32_t i, count, ret;
	bool isa;

	assert(nl && !vh_cfgj_check_only(flags));
	assert(vh_json_isa_array(jval));

	count = vh_json_arr_count(jval);
	for (i = 0; i < count; i++)
	{
		jobj = vh_json_arr_atidx(jval, i);

		if (!vh_json_isa_obj(jobj))
		{
			vh_cfgj_throw_error(flags, 0, "Expected JSON object at NestLevel/aggregate[%d]",
								i);
			continue;
		}

		if (!vh_cfgj_check_only(flags))
		{
			ret = vh_cfgj_acm(&funcs.base, jobj, flags);

			if (ret)
			{
				vh_cfgj_throw_error(flags, ret, "Error generating accumulator at %s index %d",
									"NestLevel/aggregate",
									i);	
			}

			jpair = vh_json_obj_key_byname(jobj, "searchPath");

			if (jpair)
			{
				juservalue = vh_json_objarr(jpair, &isa);

				if (!isa)
				{
					vh_cfgj_throw_warning(flags, "Expected a JSON Object for the %s "
										"key in the NestLevel/Aggregate structure at "
										"index %d",
										"searchPath",
										i);
					continue;
				}

				ret = vh_cfgj_sp(&sp, juservalue, flags);

				if (ret)
				{
					sp = 0;

					vh_cfgj_throw_warning(flags, "SearchPath could not be created for "
										  "aggregate at index %d",
										  i);
				}
			}
			else
			{
				if (!sp)
				{
					vh_cfgj_throw_error(flags, 10, "No SearchPath specified for the "
										"aggregate at index %d",
										i);
				}
			}

			assert(sp);

			jpair = vh_json_obj_key_byname(jobj, "columnName");

			if (jpair)
			{
				str = vh_json_typevar(jpair, &isa);

				if (!isa)
				{
					vh_cfgj_throw_error(flags, 11, "Expected a scalar value at index %d "
										"for key %s in aggregate",
										i,
										"columnName");
				}
			}

			vh_nl_agg_create(nl, str ? vh_str_buffer(str) : 0, sp, funcs.acmc);
		}
		else
		{
			ret = 0;
		}

	}

	return 0;
}

static int32_t
cfgj_nl_groupby(NestLevel nl, Json jarr, int32_t flags)
{
	SearchPath sp;
	PrepCol pc;
	Json jobj, jpair, juservalue;
	String str;
	int32_t i, count, ret;
	bool isa, needs_pc = false;

	assert(nl && !vh_cfgj_check_only(flags));

	count = vh_json_arr_count(jarr);
	for (i = 0; i < count; i++)
	{
		jobj = vh_json_arr_atidx(jarr, i);

		if (!vh_json_isa_obj(jobj))
		{
			vh_cfgj_throw_error(flags, 10, "Expected a JSON Object in the Array at key %s",
								"groupBy");
		}

		jpair = vh_json_obj_key_byname(jobj, "groupByType");
		str = vh_json_typevar(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 11, "Expected a scalar value at Array index %d "
								"for the key %s",
								i,
								"groupBy");
			continue;
		}

		if (!strcmp(vh_str_buffer(str), "PrepCol"))
			needs_pc = true;

		jpair = vh_json_obj_key_byname(jobj, "columnName");

		if (jpair)
		{
			str = vh_json_typevar(jpair, &isa);

			if (!isa)
			{
				vh_cfgj_throw_error(flags, 15, "Expected a scalar value at Array index %d "
									"for the key %s",
									i,
									"columnName");
			}
		}
		else
		{
			str = 0;
		}

		jpair = vh_json_obj_key_byname(jobj, "searchPath");

		if (!jpair)
		{
			vh_cfgj_throw_error(flags, 12, "Missing %s key at Array index %d for the key %s",
								"searchPath",
								i,
								"groupBy");
			continue;
		}

		juservalue = vh_json_objarr(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 13, "Expected to find an Object at %s at Array index %d "
								"in %s",
								"searchPath",
								i,
								"groupBy");
			continue;
		}

		ret = vh_cfgj_sp(&sp, juservalue, flags);

		if (ret)
		{
			vh_cfgj_throw_error(flags, 0, "Unabled to form SearchPath for GroupBy at Array "
								"index %d",
								i);
			continue;
		}

		if (!needs_pc && !vh_cfgj_check_only(flags))
		{
			vh_nl_groupby_create(nl, str ? vh_str_buffer(str) : 0, sp);
			continue;
		}

		/*
		 * PrepCol
		 */
		
		jpair = vh_json_obj_key_byname(jobj, "prepCol");

		if (!jpair)
		{
			vh_cfgj_throw_error(flags, 12, "Missing %s key at Array index %d for the key %s",
								"prepCol",
								i,
								"groupBy");
			continue;
		}

		juservalue = vh_json_objarr(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 13, "Expected to find an Object at %s at Array index %d "
								"in %s",
								"prepCol",
								i,
								"groupBy");
			continue;
		}

		ret = vh_cfgj_pc(&pc, juservalue, flags);

		if (ret)
		{
			vh_cfgj_throw_error(flags, 0, "Unabled to form PrepCol for GroupBy at Array "
								"index %d",
								i);
			continue;
		}
		
		if (!vh_cfgj_check_only(flags))
		{
			vh_nl_groupby_pc_create(nl, str ? vh_str_buffer(str) : 0, sp, pc);
		}
	}

	return 0;
}

