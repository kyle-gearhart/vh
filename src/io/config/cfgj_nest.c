/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "io/config/cfgjson.h"
#include "io/analytics/nest.h"


/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */
int32_t
vh_cfgj_nest(Nest *nest, Json jval, int32_t flags)
{
	Json jpair, jobj, jarr;
	Nest n = 0;
	NestLevel nl;
	int32_t i, count, ret;
	bool isa;

	if (!jval)
	{
		vh_cfgj_throw_error(flags, 1, "Invalid pointer passed to vh_cfgj_nest [%p]",
							jval);
	}

	if (!vh_json_isa_obj(jval))
	{
		vh_cfgj_throw_error(flags, 2, "Expected a JSON Object for a Nest at [%p]",
							jval);
	}

	jpair = vh_json_obj_key_byname(jval, "nestLevels");

	if (jpair)
	{
		jarr = vh_json_objarr(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 3, "Expected a JSON Array for the Nest at "
								"key %s",
								"nestLevels");
		}

		if (vh_json_isa_obj(jarr))
		{
			ret = vh_cfgj_nl(&nl, jarr, flags);

			if (ret)
			{
				vh_cfgj_throw_warning(flags, "Error processing NestLevel", 0);
			}
			else
			{
				if (!vh_cfgj_check_only(flags))
				{
					if (!n)
						n = vh_nest_create();

					ret = vh_nest_level_add(n, nl);
				}
			}
		}
		else
		{
			count = vh_json_arr_count(jarr);
			for (i = 0; i < count; i++)
			{
				jobj = vh_json_arr_atidx(jarr, i);

				ret = vh_cfgj_nl(&nl, jobj, flags);

				if (ret)
				{
					vh_cfgj_throw_warning(flags, "Error process NestLevel at index %d", i);
				}
				else
				{
					if (!vh_cfgj_check_only(flags))
					{
						if (!n)
							n = vh_nest_create();

						ret = vh_nest_level_add(n, nl);
					}
				}
			}
		}
	}
	else
	{
		vh_cfgj_throw_warning(flags, "Missing %s directive on Nest",
							  "nestLevels");
	}

	if (n && nest)
		*nest = n;

	return 0;
}

