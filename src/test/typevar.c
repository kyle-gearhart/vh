/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeVar.h"

struct TypeVarPointer2
{
	TypeTag tags[2];
	union
	{
		int32_t i32;
	};
};

struct TypeVarPointer4
{
	TypeTag tags[4];
	union
	{
		int32_t i32;
	};
};

static void test_typevar_int(void);
static void test_typearray_int(void);

static void test_typecomp_int(void);
static void test_typeop_int16(void);

static void test_typeop_int64_str(void);


void test_typevar_entry(void)
{
	test_typevar_int();
	test_typearray_int();
	test_typecomp_int();
	test_typeop_int16();

	test_typeop_int64_str();
}

static void 
test_typevar_int(void)
{
	int32_t *val = vh_typevar_make(1, "int");
	struct TypeVarPointer2 *tvp2;
	struct TypeVarPointer4 *tvp4;

	*val = 124859351;
	tvp2 = (struct TypeVarPointer2*)(((TypeTag*)val) - 2);
	assert(tvp2->tags[1] == (vh_type_int32.id | VH_TYPETAG_MAGIC | VH_TYPETAG_END_FLAG));
	assert(tvp2->i32 == 124859351);
	assert(*val == tvp2->i32);

	tvp2 = vh_typevar_make(2, "int", "Array");
	tvp2 = (struct TypeVarPointer2*)(((TypeTag*)tvp2) - 2);
	assert(tvp2->tags[0] == (vh_type_Array.id | VH_TYPETAG_MAGIC | VH_TYPETAG_END_FLAG));
	assert(tvp2->tags[1] == (vh_type_int32.id | VH_TYPETAG_MAGIC));

	val = vh_makevar1(int);
	*val = 22;

	tvp2 = (struct TypeVarPointer2*)(((TypeTag*)val) - 2);
	assert(tvp2->tags[1] == (vh_type_int32.id | VH_TYPETAG_MAGIC | VH_TYPETAG_END_FLAG));
	assert(tvp2->i32 == 22);
	assert(*val == tvp2->i32);

	val = vh_makevar3(int, Range, Array);

	tvp4 = (struct TypeVarPointer4*)(((TypeTag*)val) - 4);
	assert(tvp4->tags[1] == (vh_type_Array.id | VH_TYPETAG_MAGIC | VH_TYPETAG_END_FLAG));
	assert(tvp4->tags[2] == (vh_type_Range.id | VH_TYPETAG_MAGIC));
	assert(tvp4->tags[3] == (vh_type_int32.id | VH_TYPETAG_MAGIC));
}

static void
test_typearray_int(void)
{
	void *array;
	int32_t *item_at, *item_cpy;

	array = vh_makearray1(1, int);
	item_at = vh_typearray_at(array, 0);

	if (item_at)
	{
		*item_at = 501292;
		item_cpy = vh_typevar_makecopy(item_at);

		assert(*item_cpy == 501292);
		assert(*item_cpy == *item_at);
	}

	vh_typearray_destroy(array);

	array = vh_makearray1(10000, int);
	item_at = vh_typearray_at(array, 5000);

	if (item_at)
	{
		*item_at = 1248921;
		item_cpy = vh_typevar_makecopy(item_at);

		assert(item_cpy != item_at);
		assert(*item_cpy == 1248921);
		assert(*item_cpy == *item_at);
	}

	vh_typevar_destroy(item_cpy);
	vh_typearray_finalize(array);
	vh_typearray_free(array);
}

static void
test_typecomp_int(void)
{
	TypeVarOpExec tvope;
	int32_t *lhs = vh_makevar1(int);
	int32_t *rhs = vh_makevar1(int);
	int32_t *lhs2, *rhs2;
	bool res;

	*lhs = 20;
	*rhs = 21;

	res = vh_typevar_comp("<", 
				    VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
						    VH_OP_DT_VAR,
   						    VH_OP_ID_INVALID,
   						    VH_OP_DT_VAR,
   						    VH_OP_ID_INVALID),
				    lhs,
				    rhs);

	printf("\ntest_typecomp_int: %d", res);
	assert(res);

	res = vh_typevar_comp(">", 
				    VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
						    VH_OP_DT_VAR,
   						    VH_OP_ID_INVALID,
   						    VH_OP_DT_VAR,
   						    VH_OP_ID_INVALID),
				    lhs,
				    rhs);
	assert(!res);

	tvope =	vh_typevar_comp_init(">",
			VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
											VH_OP_DT_VAR,
											VH_OP_ID_INVALID,
											VH_OP_DT_VAR,
											VH_OP_ID_INVALID),
							lhs,
							rhs);

	if (tvope)
	{
		res = vh_typevar_comp_fp(tvope, lhs, rhs);
		assert(!res);

		res = vh_typevar_comp_fp(tvope, rhs, lhs);
		assert(res);

		*lhs = 500;

		lhs2 = vh_makevar1(int);
		rhs2 = vh_makevar1(int);

		*lhs2 = 501;
		*rhs2 = 2194;

		res = vh_typevar_comp_fp(tvope, lhs, rhs2);
		assert(!res);

		res = vh_typevar_comp_fp(tvope, lhs2, rhs);
		assert(res);

		vh_typevar_comp_swap_op(tvope, "==");

		res = vh_typevar_comp_fp(tvope, lhs2, rhs2);
		assert(!res);

		*lhs2 = 1000;
		*rhs2 = 1000;

		res = vh_typevar_comp_fp(tvope, lhs2, rhs2);
		assert(res);

		vh_typevar_destroy(lhs);
		vh_typevar_destroy(rhs);
		vh_typevar_destroy(lhs2);
		vh_typevar_destroy(rhs2);

		vh_typevar_comp_destroy(tvope);
	}	
}

static void
test_typeop_int16(void)
{
	int16_t *lhs = vh_makevar1(short);
	int16_t *rhs = vh_makevar1(short);
	int16_t *ret, rets;

	*lhs = 12;
	*rhs = 25;

	ret = vh_typevar_op("+",
				VH_OP_MAKEFLAGS(VH_OP_DT_VAR,
						VH_OP_DT_VAR,
						VH_OP_ID_INVALID,
						VH_OP_DT_VAR,
						VH_OP_ID_INVALID),
				lhs,
				rhs);
	assert(ret);
	assert(*ret == 37);
	printf("\ntest_typeop_int16 (+): %d == 37", *ret);

	ret = vh_typevar_op("++",
				VH_OP_MAKEFLAGS(VH_OP_DT_VAR,
						VH_OP_DT_VAR,
						VH_OP_ID_INVALID,
						VH_OP_DT_INVALID,
						VH_OP_ID_INVALID),
				ret);
	assert(ret);
	assert(*ret == 38);
	printf("\ntest_typeop_int16 (++): %d == 38", *ret);

	/*
	 * Assign ret (38) to rhs
	 */
	vh_typevar_op("=",
				VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
						VH_OP_DT_VAR,
						VH_OP_ID_INVALID,
						VH_OP_DT_VAR,
						VH_OP_ID_INVALID),
				rhs,
				ret);
	assert(rhs);
	assert(*rhs == 38);

	rets = (int16_t)(uintptr_t)vh_typevar_op("+",
					VH_OP_MAKEFLAGS(VH_OP_DT_I16,
						VH_OP_DT_VAR,
						VH_OP_ID_INVALID,
						VH_OP_DT_I16,
						VH_OP_ID_INVALID),
					rhs,
					45);
	assert(rets == (38 + 45));
	printf("\ntest_typeop_int16(+ stack): %d == 83", rets);

	vh_typevar_op("+=",
				VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
						VH_OP_DT_VAR,
						VH_OP_ID_INVALID,
						VH_OP_DT_I16,
						VH_OP_ID_INVALID),
				lhs,
				rets);
	assert(lhs);
	assert(*lhs == (38+45+12));
	printf("\ntest_typeop_int16 (+= stack): %d == 95", *lhs);

}

static void 
test_typeop_int64_str(void)
{
	int64_t *v = vh_makevar1(int64);
	StringData str;

	vh_str_init(&str);

	*v = 1234;

	vh_typevar_op("=", VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
									   VH_OP_DT_STR,
									   VH_OP_ID_INVALID,
									   VH_OP_DT_VAR,
									   VH_OP_ID_INVALID),
				  &str, v);

	printf("\ntest_typeop_int64_str: %s", vh_str_buffer(&str));

	vh_str_finalize(&str);
	vh_str_init(&str);

	/*
	 * Trigger a buffer resize on the String target.
	 */

	*v = 12345678901234567;
	vh_typevar_op("=", VH_OP_MAKEFLAGS(VH_OP_DT_INVALID,
									   VH_OP_DT_STR,
									   VH_OP_ID_FMTSTR,
									   VH_OP_DT_VAR,
									   VH_OP_ID_INVALID),
				  &str, "%'lld", v);

	printf("\ntest_typeop_int64_str: %s\n", vh_str_buffer(&str));

}

