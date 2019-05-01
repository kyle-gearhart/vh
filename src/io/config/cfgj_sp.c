/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "io/config/cfgjson.h"
#include "io/catalog/searchpath.h"
#include "io/catalog/sp/spht.h"
#include "io/catalog/sp/sptd.h"


typedef enum
{
 	MC_SPTD,
	MC_SPHT
} MethodCategory;

typedef union
{
	void (*base)(void);

	SearchPath (*td)(SearchPathTableDefOpts *opts, int32_t flags);
	SearchPath (*ht)(const char*);
} MethodFunc;


static const struct ConfigJsonMethodTable methods[] = {
	{ "vh_spht_dat_create", (void (*)(void)) vh_spht_dat_create, MC_SPHT},
	{ "vh_spht_tf_create", (void (*)(void)) vh_spht_tf_create, MC_SPHT},
	{ "vh_sptd_create", (void (*)(void)) vh_sptd_create, MC_SPTD }
};


static int32_t cfgj_sp_td(const struct ConfigJsonMethodTable *cjmt,
						  SearchPath *sp, Json jval, int32_t flags);



/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */
int32_t
vh_cfgj_sp(SearchPath *sp, Json jval, int32_t flags)
{
	const struct ConfigJsonMethodTable *cjmt;
	Json methodName, fieldName;
	String method_name, field_name;
	SearchPath spl;
	MethodFunc func;
	bool isa_var;

	if (!vh_json_isa_obj(jval))
	{
		vh_cfgj_throw_error(flags, 1, "Json value at [%p] is not a JSON Object.  "
							"Unable to configure the SearchPath.",
							jval);
	}	

	methodName = vh_json_obj_key_byname(jval, "methodName");

	if (!methodName)
	{
		vh_cfgj_throw_error(flags, 2, "JSON Object [%p] does not have a key named %s "
							"to declare the method to be used to create the SearchPath.",
							jval, "methodName");
	}

	method_name = vh_json_typevar(methodName, &isa_var);

	if (!isa_var)
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
		vh_cfgj_throw_error(flags, 4, "The SearchPath method %s could not be found.",
							vh_str_buffer(method_name));
	}

	func.base = cjmt->method;

	switch ((MethodCategory)cjmt->category)
	{
		case MC_SPTD:
			return cfgj_sp_td(cjmt, sp, jval, flags);

		case MC_SPHT:
			fieldName = vh_json_obj_key_byname(jval, "fieldName");

			if (!fieldName)
			{
				vh_cfgj_throw_error(flags, 2, "JSON Object [%p] does not have a key named %s "
									"to declare the Field Name to be used to create the SearchPath.",
									jval, "fieldName");
			}

			field_name = vh_json_typevar(fieldName, &isa_var);

			if (!isa_var)
			{
				vh_cfgj_throw_error(flags, 3, "JSON Value [%p] is incorrect, be sure the value "
									"for the key %s is a scalar JSON value and not an Array "
									"or Object.",
									fieldName,
									"fieldName");
			}

			/*
			 * When we're only supposed to check the configuraion, we're in good
			 * shape at this point.
			 */
			if (vh_cfgj_check_only(flags))
				return 0;

			spl = func.ht(vh_str_buffer(field_name));

			if (!spl)
			{
				vh_cfgj_throw_error(flags, -1, "Call to SearchPath failed.", 0);
			}

			*sp = spl;

			return 0;

			break;
	}

	return 1;
}


/*
 * ============================================================================
 * Private Functions
 * ============================================================================
 */

/*
 * cfgj_sp_td
 *
 * We use the same calling convention as the main entry point.  The TableDef
 * SearchPath is a good bit more complicated than the others, so we break it out
 * into it's own function to clean things up a little bit.
 */
static int32_t 
cfgj_sp_td(const struct ConfigJsonMethodTable *cjmt, 
		   SearchPath *sp, Json jval, int32_t flags)
{
	SearchPathTableDefOpts opts = { };
	SearchPath spl;
	Json jpairName, jarr, juserval;
	String str;
	MethodFunc func;
	int32_t opts_flags = 0, i, count;
	bool isa, many_schemas = false, many_tables;

	jpairName = vh_json_obj_key_byname(jval, "schemaName");

	if (jpairName)
	{
		jarr = vh_json_objarr(jpairName, &isa);

		if (isa)
		{
			/*
			 * We have an array of schema names, so we'll need to grab the
			 * first one to create the SearchPath, then come back later and
			 * add the others.
			 */

			if (!vh_json_isa_array(jarr))
			{
				vh_cfgj_throw_error(flags, 5,
					   				"An object was found at the key %s, expected an Array",
									"schemaName");	
			}

			if (vh_json_arr_count(jarr) > 1)
				many_schemas = true;

			juserval = vh_json_arr_atidx(jarr, 0);
			str = vh_json_typevar(juserval, &isa);

			if (!isa)
			{
				vh_cfgj_throw_error(flags, 6,
									"A non-scalar value was found in the %s Array.",
									"schemaName");
			}

			if (!vh_cfgj_check_only(flags))
			{
				opts.schema_name = vh_cstrdup(vh_str_buffer(str));
				opts_flags |= VH_SPTD_OPT_SNAME;
			}
		}
		else
		{
			str = vh_json_typevar(jpairName, &isa);

			if (!vh_cfgj_check_only(flags))
			{
				opts.schema_name = vh_cstrdup(vh_str_buffer(str));
				opts_flags |= VH_SPTD_OPT_SNAME;
			}
		}
	}

	jpairName = vh_json_obj_key_byname(jval, "tableName");

	if (jpairName)
	{
		jarr = vh_json_objarr(jpairName, &isa);

		if (isa)
		{
			/*
			 * We have an array of table names, so we'll need to grab the
			 * first one to create the SearchPath, then come back later and
			 * add the others.
			 */

			if (!vh_json_isa_array(jarr))
			{
				vh_cfgj_throw_error(flags, 5,
					   				"An object was found at the key %s, expected an Array",
									"tableName");	
			}

			if (vh_json_arr_count(jarr) > 1)
				many_tables = true;

			/*
			 * Shut the compiler warnings up
			 */
			if (many_tables)
			{
			}

			juserval = vh_json_arr_atidx(jarr, 0);
			str = vh_json_typevar(juserval, &isa);

			if (!isa)
			{
				vh_cfgj_throw_error(flags, 6,
									"A non-scalar value was found in the %s Array.",
									"tableName");
			}

			if (!vh_cfgj_check_only(flags))
			{
				opts.table_name = vh_cstrdup(vh_str_buffer(str));
				opts_flags |= VH_SPTD_OPT_SNAME;
			}
		}
		else
		{
			str = vh_json_typevar(jpairName, &isa);

			if (!vh_cfgj_check_only(flags))
			{
				opts.table_name = vh_cstrdup(vh_str_buffer(str));
				opts_flags |= VH_SPTD_OPT_TNAME;
			}
		}
	}

	if (!vh_cfgj_check_only(flags))
	{
		func.base = cjmt->method;
			
		spl = func.td(&opts, opts_flags);

		if (!spl)
		{
			if (opts.schema_name)
				vhfree((void*)opts.schema_name);

			if (opts.table_name)
				vhfree((void*)opts.table_name);

			vh_cfgj_throw_error(flags, -1,
								"SearchPathTableDef was invoked improperly, check configuration "
								"directives.", 0);
		}
	}

	if (many_schemas)
	{
		jpairName = vh_json_obj_key_byname(jval, "schemaName");
		jarr = vh_json_objarr(jpairName, &isa);
		count = vh_json_arr_count(jarr);

		for (i = 1; i < count; i++)
		{
			juserval = vh_json_arr_atidx(jarr, i);
			str = vh_json_typevar(juserval, &isa);

			if (!isa)
			{
				vh_cfgj_throw_error(flags, 6, "Expected a scalar Json value in the array %s",
									"schemaName");
			}

			if (!vh_cfgj_check_only(flags))
				vh_sptd_schema_add(spl, vh_cstrdup(vh_str_buffer(str)));
		}
	}

	return 0;
}

