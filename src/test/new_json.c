/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/catalog/TableDef.h"
#include "io/catalog/TableField.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/types/njson.h"
#include "io/catalog/types/njson_parse.h"
#include "test.h"

static Type tys_int32[] = { &vh_type_int32, 0 };
static Type tys_int64[] = { &vh_type_int64, 0 };
static Type tys_string[] = { &vh_type_String, 0 };

static void jval_i32(void);
static void jval_i64(void);

static void jpair_i32(void);
static void jpair_i64(void);

static void jobj(void);
static void jarr(void);

static void jparse(void);

static void jht(void);

void
test_new_json_entry(void)
{
	jval_i32();
	jval_i64();

	jpair_i32();
	jpair_i64();

	jobj();
	jarr();

	jparse();

	jht();
}

static void jval_i32(void)
{
	static int32_t op_flags = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
											  VH_OP_DT_VAR,
	   										  VH_OP_ID_INVALID,
   											  VH_OP_DT_I32,
   											  VH_OP_ID_INVALID);

	Json jval;
	int32_t *val;
	bool is_typevar;

	jval = vh_json_make_value(tys_int32, 1); 
	val = vh_json_typevar(jval, &is_typevar);
	*val = 22;

	assert(is_typevar);
	assert(vh_json_isa_value(jval));

	vh_typevar_op("+=", op_flags, val, 22);
	assert(*val == 44);
	assert(vh_json_isa_value(jval));
}


static void jval_i64(void)
{
	static int32_t op_flags = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
											  VH_OP_DT_VAR,
	   										  VH_OP_ID_INVALID,
   											  VH_OP_DT_I64,
   											  VH_OP_ID_INVALID);

	Json jval;
	int64_t *val;
	bool is_typevar;

	jval = vh_json_make_value(tys_int64, 1); 
	val = vh_json_typevar(jval, &is_typevar);
	*val = 2211294014;

	assert(is_typevar);
	assert(vh_json_isa_value(jval));

	vh_typevar_op("+=", op_flags, val, 22);
	assert(*val == 2211294036);
	assert(vh_json_isa_value(jval));
}

static void
jpair_i32(void)
{
	static int32_t op_flags = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
											  VH_OP_DT_VAR,
	   										  VH_OP_ID_INVALID,
   											  VH_OP_DT_I32,
   											  VH_OP_ID_INVALID);

	Json jval;
	int32_t *val;
	bool is_typevar;

	jval = vh_json_make_pair(tys_int32, 1, "pair_name"); 
	val = vh_json_typevar(jval, &is_typevar);
	*val = 22;

	assert(is_typevar);
	assert(vh_json_isa_pair(jval));

	vh_typevar_op("+=", op_flags, val, 22);
	assert(*val == 44);
	assert(vh_json_isa_pair(jval));
}

static void
jpair_i64(void)
{
}

static void
jobj(void)
{
	Json jobj_root, jpair, jobj_customer, jobj_order;
	String stringify;
	bool is_objarr;

	jobj_root = vh_json_make_object();
	jpair = vh_json_make_pair_obj("customer");
	vh_json_obj_add_pair(jobj_root, jpair);

	jobj_customer = vh_json_objarr(jpair, &is_objarr);
	assert(is_objarr);
	assert(vh_json_isa_obj(jobj_customer));
	jpair = vh_json_make_pair(tys_string, 1, "first-name");
	vh_json_obj_add_pair(jobj_customer, jpair);

	jpair = vh_json_make_pair_obj("order");
	vh_json_obj_add_pair(jobj_root, jpair);
	jobj_order = vh_json_objarr(jpair, &is_objarr);
	assert(is_objarr);
	assert(vh_json_isa_obj(jobj_customer));

	jpair = vh_json_make_pair(tys_string, 1, "order_number");
	vh_json_obj_add_pair(jobj_order, jpair);

	jpair = vh_json_make_pair(tys_string, 1, "sold_to_party");
	vh_json_obj_add_pair(jobj_order, jpair);

	jpair = vh_json_make_pair(tys_string, 1, "payor_party");
	vh_json_obj_add_pair(jobj_order, jpair);

	jpair = vh_json_make_pair(tys_string, 1, "payor_state");
	vh_json_obj_add_pair(jobj_order, jpair);
	
	jpair = vh_json_make_pair(tys_string, 1, "payor_country");
	vh_json_obj_add_pair(jobj_order, jpair);

	stringify = vh_json_stringify(jobj_root);

	printf("\n\nJSON: %s\n\n", vh_str_buffer(stringify));

	vh_str.Destroy(stringify);
	vh_json_destroy(jobj_root);
}

/*
 * Arrays may intermingle Arrays, Objects or Values in the target array.  This
 * only gets messy when we got to stringify the array.
 */
static void
jarr(void)
{
	Json arr_root, obj_customer1, obj_customer2, jpair, jval;
	String strval;
	String stringify;

	arr_root = vh_json_make_array();

	obj_customer1 = vh_json_make_object();
	jpair = vh_json_make_pair(tys_int32, 1, "customer_id");
	vh_json_obj_add_pair(obj_customer1, jpair);

	jpair = vh_json_make_pair(tys_string, 1, "customer_firstname");
	strval = vh_json_typevar(jpair, 0);
	vh_str.Append(strval, "Kyle Gearhart");
	vh_json_obj_add_pair(obj_customer1, jpair);

	jpair = vh_json_make_pair(tys_string, 1, "customer_lastname");
	strval = vh_json_typevar(jpair, 0);
	vh_str.Append(strval, "Is A Winner");
	vh_json_obj_add_pair(obj_customer1, jpair);

	jpair = vh_json_make_pair(tys_string, 1, "customer_ssn");
	vh_json_obj_add_pair(obj_customer1, jpair);

	vh_json_arr_push(arr_root, obj_customer1);


	obj_customer2 = vh_json_make_object();

	jpair = vh_json_make_pair(tys_int32, 1, "customer_id");
	vh_json_obj_add_pair(obj_customer2, jpair);

	jpair = vh_json_make_pair(tys_string, 1, "customer_legalname");
	vh_json_obj_add_pair(obj_customer2, jpair);

	jpair = vh_json_make_pair(tys_string, 1, "customer_dbaname");
	vh_json_obj_add_pair(obj_customer2, jpair);

	jpair = vh_json_make_pair(tys_string, 1, "customer_fein");
	vh_json_obj_add_pair(obj_customer2, jpair);

	vh_json_arr_push(arr_root, obj_customer2);

	/*
	 * Let's put a scalar value in here just for S&G
	 */

	jval = vh_json_make_value(tys_string, 1);
	vh_json_arr_push(arr_root, jval);

	/*
	 * Then we'll put an empty array in the root array.
	 */
	jval = vh_json_make_array();
	vh_json_arr_push(arr_root, jval);

	stringify = vh_json_stringify(arr_root);

	printf("\n\nJSON: %s\n\n", vh_str_buffer(stringify));

	vh_str.Destroy(stringify);
	vh_json_destroy(arr_root);
}

static void
jparse(void)
{
	static const char json_p_arr[] = "[ 123, 456, 789 ]";
	static const char json_p_obj[] = "{ \"test\" : 12, \"test2\": 98 }";
	static const char json_p_com[] = "[ 123, { \"test_me\":\"bobby knight\", \"test_int\": 123 }, \"test string\" ]";

	String stringify;
	Json root;

	root = vh_json_strp_parser(json_p_arr);
	assert(vh_json_isa_array(root));
	stringify = vh_json_stringify(root);
	printf("\n\nJSON Parse: %s", vh_str_buffer(stringify));
	vh_str.Destroy(stringify);

	root = vh_json_strp_parser(json_p_obj);
	assert(vh_json_isa_obj(root));
	stringify = vh_json_stringify(root);
	printf("\nJSON Parse: %s", vh_str_buffer(stringify));
	vh_str.Destroy(stringify);
	
	root = vh_json_strp_parser(json_p_com);
	assert(vh_json_isa_array(root));
	stringify = vh_json_stringify(root);
	printf("\nJSON Parse: %s", vh_str_buffer(stringify));
	vh_str.Destroy(stringify);

	printf("\nEND JSON PARSE TESTING\n\n");
}

static void
jht(void)
{
	static int32_t typevar_op_cstr = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
		   											 VH_OP_DT_HTP,
												 	 VH_OP_ID_NM,
													 VH_OP_DT_CHR,
													 VH_OP_ID_INVALID);
	static int32_t typevar_op_i32 = VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
													VH_OP_DT_HTP,
													VH_OP_ID_NM,
													VH_OP_DT_I32,
													VH_OP_ID_INVALID);

	static const char json_literal[] = "{ \"first_name\":\"Kyle\","
				 					   "  \"last_name\":\"Gearhart\","
									   "  \"age\":29 }";
	TableDef td;
	HeapTuplePtr htp;
	String stringify;
	Json root, root_noref;
	int32_t ret, i;

	td = vh_td_create(false);
	vh_td_tf_add(td, tys_string, "first_name");
	vh_td_tf_add(td, tys_string, "last_name");
	vh_td_tf_add(td, tys_int32, "age");

	root = vh_json_strp_parser(json_literal);
	stringify = vh_json_stringify(root);
	printf("\nJSON parse: %s\n", vh_str_buffer(stringify));
	vh_str.Destroy(stringify);

	ret = vh_json_htp_tdv(vh_td_tdv_lead(td), root, &htp);
	assert(ret == 0);
	
	/*
	 * We're in HeapTuplePtr format so let's take that and serialize it back to
	 * JSON.  Then we'll stringify the JSON.
	 */
	ret = vh_htp_json(htp, false, &root_noref);
	assert(ret == 0);
	stringify = vh_json_stringify(root_noref);
	printf("\n\nJSON -> HTP -> JSON (no reference): %s",
		   vh_str_buffer(stringify));
	vh_str.Destroy(stringify);

	/*
	 * Let's to the same thing: serialize to JSON but use a reference to the
	 * HeapTuplePtr.
	 */
	ret = vh_htp_json(htp, true, &root);
	assert(ret == 0);
	stringify = vh_json_stringify(root);
	printf("\nJSON -> HTP -> JSON (with reference): %s",
		   vh_str_buffer(stringify));
	vh_str.Destroy(stringify);


	/*
	 * Change a field on the HeapTuple we're referencing and then let's check
	 * the stringify result.
	 */
	vh_typevar_op("=", typevar_op_i32, htp, "age", 30);
	stringify = vh_json_stringify(root);
	printf("\nJSON -> HTP -> JSON (with reference and changed value): %s",
		   vh_str_buffer(stringify));
	vh_str.Destroy(stringify);
	vh_json_destroy(root);
	
	stringify = vh_json_stringify(root_noref);
	printf("\n\nJSON -> HTP -> JSON (no reference): %s",
		   vh_str_buffer(stringify));
	vh_str.Destroy(stringify);
	vh_json_destroy(root_noref);


	/*
	 * Now mount a bunch of values on the HeapTuple and serialize it both with
	 * reference and without reference.
	 */
	htp = vh_allochtp_td(td);
	
	if (htp)
	{
		
		vh_typevar_op("=", typevar_op_cstr, htp, "first_name", "Bobbay Smith");
		vh_typevar_op("=", typevar_op_cstr, htp, "last_name", "Gearhart");
		vh_typevar_op("=", typevar_op_i32, htp, "age", 29);
	}

	ret = vh_htp_json(htp, true, &root);
	assert(ret == 0);
	stringify = vh_json_stringify(root);
	printf("\n\nJSON (reference): %s", vh_str_buffer(stringify));
	vh_str.Destroy(stringify);
	
	vh_typevar_op("=", typevar_op_cstr, htp, "first_name", "Bobbay Smith Gentry");
	vh_typevar_op("=", typevar_op_cstr, htp, "age", "22");

	stringify = vh_json_stringify(root);
	printf("\n\nJSON (reference, after change): %s", vh_str_buffer(stringify));
	vh_str.Destroy(stringify);
	vh_json_destroy(root);

	printf("\nBulk reference deserialization...\n");

	for (i = 0; i < 1610; i++)
	{
		ret = vh_htp_json(htp, true, &root);
		vh_json_destroy(root);
	}

	printf("\nBulk no-reference deserialization...\n");

	for (i = 0; i < 1610; i++)
	{
		ret = vh_htp_json(htp, false, &root);
		vh_json_destroy(root);
	}

	printf("tests complete\n\n");
}


