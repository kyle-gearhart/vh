/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "io/config/cfgjson.h"
#include "io/catalog/TypeVarAcm.h"


static const struct ConfigJsonMethodTable methods[] = {
	{ "vh_acm_avg", (void (*)(void)) vh_acm_avg_tys, 0 },
	/*{ "vh_acm_count", (void (*)(void)) vh_acm_count_tys, 0 }, */
	{ "vh_acm_devp", (void (*)(void)) vh_acm_devp_tys, 0 },
	{ "vh_acm_devs", (void (*)(void)) vh_acm_devs_tys, 0 },
	{ "vh_acm_max", (void (*)(void)) vh_acm_max_tys, 0 },
	{ "vh_acm_min", (void (*)(void)) vh_acm_min_tys, 0 },
	{ "vh_acm_sum", (void (*)(void)) vh_acm_sum_tys, 0 },
	{ "vh_acm_varp", (void (*)(void)) vh_acm_varp_tys, 0 },
	{ "vh_acm_vars", (void (*)(void)) vh_acm_vars_tys, 0 }
};



/*
 * ============================================================================
 * Public Functions
 * ============================================================================
 */

/*
 * vh_cfgj_acm
 *
 * We're only concerned with returning the function to create the ACM.  Most of
 * our infrastructure uses delayed creation so that type information can be
 * run at another time.
 */
int32_t
vh_cfgj_acm(void (**acm)(void), Json jval, int32_t flags)
{
	const struct ConfigJsonMethodTable *cjmt;
	Json jpair;
	String method_name = 0;
	bool isa;


	if (vh_json_isa_obj(jval))
	{
		jpair = vh_json_obj_key_byname(jval, "methodName");

		if (!jpair)
		{
			vh_cfgj_throw_error(flags, 1, "Json Object at [%p] does not have a key "
								"named %s.",
								jval,
								"methodName");
		}

		method_name = vh_json_typevar(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 2, "Expected a scalara value at key %s for "
								"the JSON Object pointed to by [%p]",
								"methodName",
								jval);
		}
	}
	else
	{
		method_name = vh_json_typevar(jval, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 3, "Expected a scalar value for the JSON Value [%p]",
								jval);
		}
	}

	if (!vh_cfgj_method_lookup(methods,
							   vh_cfgj_methods(methods),
							   vh_str_buffer(method_name),
							   &cjmt))
	{
		vh_cfgj_throw_error(flags, 4, "Unable to find ACM method named %s",
							vh_str_buffer(method_name));
	}

	if (acm)
		*acm = cjmt->method;

	return 0;
}

