/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include <assert.h>
#include <stdio.h>

#include "io/config/cfgjson.h"
#include "io/catalog/types/njson_parse.h"

static void test_config_pc(void);
static void test_config_sp(void);

void
test_config_entry(void)
{
	test_config_pc();
	test_config_sp();
}

static void
test_config_pc(void)
{
	static const char json_str[] = 	"{\"methodName\": \"vh_pc_defaultv_create\","
									"\"defaultValue\": \"abc\"}";
	Json jval;
	PrepCol pc = 0;
	int32_t ret;

	jval = vh_json_strp_parser(json_str);

	if ((ret = vh_cfgj_pc(&pc, jval, VH_CFGJ_FLAG_CHECK)))
	{
		assert(!ret);
	}

	if ((ret == vh_cfgj_pc(&pc, jval, VH_CFGJ_FLAG_WARN)))
	{
		assert(!ret);
	}

	assert(pc);
}

static void
test_config_sp(void)
{
	static const char json_str[] = "{\"methodName\": \"vh_spht_tf_create\","
								   "\"fieldName\": \"firstName\"}";
	Json jval;
	SearchPath sp = 0;
	int32_t ret;

	jval = vh_json_strp_parser(json_str);

	if ((ret = vh_cfgj_sp(&sp, jval, VH_CFGJ_FLAG_CHECK)))
	{
		assert(!ret);
	}

	if ((ret = vh_cfgj_sp(&sp, jval, VH_CFGJ_FLAG_WARN)))
	{
		assert(!ret);
	}

	assert(sp);
}


