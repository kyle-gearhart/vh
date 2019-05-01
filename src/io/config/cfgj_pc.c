/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "io/config/cfgjson.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/prepcol/prepcol.h"
#include "io/catalog/prepcol/pcdefaultv.h"
#include "io/catalog/prepcol/pctsint.h"

typedef enum
{
	MC_DEFAULTV,
	MC_PCTSINT,
} MethodCategory;

typedef union
{
	void (*base)(void);

	PrepCol (*pctsint)(TypeVar, int32_t, int32_t, bool);
	PrepCol (*defaultv)(TypeVar, bool);
} MethodFunc;


static const struct ConfigJsonMethodTable methods[] = {
	{ "vh_pc_defaultv_create", (void (*)(void)) vh_pc_defaultv_create, MC_DEFAULTV },
	{ "vh_pctsint_dt_create", (void (*)(void)) vh_pctsint_dt_create, MC_PCTSINT },
	{ "vh_pctsint_ts_create", (void (*)(void)) vh_pctsint_ts_create, MC_PCTSINT }
};


static int32_t cfgj_pc_defaultv(const struct ConfigJsonMethodTable *cjmt,
								PrepCol *pc, Json jval, int32_t flags);
static int32_t cfgj_pc_pctsint(const struct ConfigJsonMethodTable *cjmt,
							   PrepCol *pc, Json jval, int32_t flags);


/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */
int32_t
vh_cfgj_pc(PrepCol *pc, Json jval, int32_t flags)
{
	const struct ConfigJsonMethodTable *cjmt;
	Json methodName;
	String method_name;
	bool isa;

	if (!vh_json_isa_obj(jval))
	{
		vh_cfgj_throw_error(flags, 1, "Expected Json value at [%p] to be a JSON "
							"Object.  Unable to configure the PrepCol.",
							jval);
	}

	methodName = vh_json_obj_key_byname(jval, "methodName");

	if (!methodName)
	{
		vh_cfgj_throw_error(flags, 2, "JSON Object [%p] does not have a key named %s "
							"to declare the method to be used to create the PrepCol.",
							jval, "methodName");
	}

	method_name = vh_json_typevar(methodName, &isa);

	if (!isa)
	{
		vh_cfgj_throw_error(flags, 3, "JSON Value [%p] is incorrect, be sure the value "
							"for the key %s is a scalar JSON value and not an Array "
							"or Object.",
							methodName,
							"methodName");
	}

	if (!vh_cfgj_method_lookup(methods, 
							   vh_cfgj_methods(methods), 
							   vh_str_buffer(method_name),
							   &cjmt))
	{
		vh_cfgj_throw_error(flags, 4, "The PrepCol method %s could not be found.",
							vh_str_buffer(method_name));
	}

	switch ((MethodCategory)cjmt->category)
	{
		case MC_DEFAULTV:
			return cfgj_pc_defaultv(cjmt, pc, jval, flags);

		case MC_PCTSINT:
			return cfgj_pc_pctsint(cjmt, pc, jval, flags);
	}

	return 1;
}



/*
 * ============================================================================
 * Private Functions
 * ============================================================================
 */


static int32_t 
cfgj_pc_defaultv(const struct ConfigJsonMethodTable *cjmt,
				 PrepCol *pc, Json jval, int32_t flags)
{
	Json jpair;
	TypeVar var;
	MethodFunc func;
	PrepCol pcl;
	bool isa, *null = 0;

	jpair = vh_json_obj_key_byname(jval, "defaultValue");

	if (!jpair)
	{
		vh_cfgj_throw_error(flags, 5, "%s is required to form a Default Value PrepCol",
							"defaultValue");
	}

	var = vh_json_typevar(jpair, &isa);

	if (!isa)
	{
		vh_cfgj_throw_error(flags, 6, "Expected a scalar value for %s", "defaultValue");
	}

	jpair = vh_json_obj_key_byname(jval, "whenNull");

	if (jpair)
	{
		null = vh_json_typevar(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 6, "Expected a scalar value for %s", "whenNull");
		}
	}

	if (vh_cfgj_check_only(flags))
		return 0;

	func.base = cjmt->method;
	pcl = func.defaultv(var, (null ? *null : false));

	if (!pcl)
	{
		vh_cfgj_throw_error(flags, -1, "Internal error with PrepCol Default Value", 0);
	}

	*pc = pcl;

	return 0;
}

static int32_t 
cfgj_pc_pctsint(const struct ConfigJsonMethodTable *cjmt,
 				PrepCol *pc, Json jval, int32_t flags)
{
	Json jpair;
	TypeVar var = 0;
	MethodFunc func;
	PrepCol pcl;
	int32_t *interval, *interval_type;
	bool isa, *lower = 0;

	jpair = vh_json_obj_key_byname(jval, "base");

	if (jpair)
	{
		var = vh_json_typevar(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 6, "Expected a scalar value for %s", "defaultValue");
		}
	}

	jpair = vh_json_obj_key_byname(jval, "interval");

	if (jpair)
	{
		interval = vh_json_typevar(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 6, "Expected a scalar value for %s", "interval");
		}
	}
	
	jpair = vh_json_obj_key_byname(jval, "intervalType");

	if (jpair)
	{
		interval_type = vh_json_typevar(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 6, "Expected a scalar value for %s", "intervalType");
		}
	}
	
	jpair = vh_json_obj_key_byname(jval, "lower");

	if (jpair)
	{
		lower = vh_json_typevar(jpair, &isa);

		if (!isa)
		{
			vh_cfgj_throw_error(flags, 6, "Expected a scalar value for %s", "lower");
		}
	}


	if (vh_cfgj_check_only(flags))
		return 0;

	func.base = cjmt->method;
	pcl = func.pctsint(var, (interval ? *interval : 1), *interval_type, (lower ? *lower : true));

	if (!pcl)
	{
		vh_cfgj_throw_error(flags, -1, "Internal error with PrepCol Default Value", 0);
	}

	*pc = pcl;

	return 0;
}

