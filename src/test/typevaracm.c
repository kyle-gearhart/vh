/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdio.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeVar.h"
#include "io/catalog/TypeVarAcm.h"

static void test_acm_avg(void);
static void test_acm_max(void);
static void test_acm_min(void);
static void test_acm_sum(void);

static void test_acm_vars(void);

static Type tys_int16[] = { &vh_type_int16, 0 };
static Type tys_int32[] = { &vh_type_int32, 0 };
//static Type tys_int64[] = { &vh_type_int64, 0 };

void test_typevaracm_entry(void)
{
	test_acm_sum();
	test_acm_avg();
	test_acm_max();
	test_acm_min();

	test_acm_vars();
}

static void
test_acm_avg(void)
{
	int16_t *input = vh_makevar1(short);
	int16_t *res;
	TypeVarAcm acm_avg;
	TypeVarAcmState acm_state;
	TypeVarSlot slot;

	acm_avg = vh_acm_avg_tys(tys_int16);
	acm_state = vh_acms_create(acm_avg);

	assert(acm_avg);
	assert(acm_state);

	*input = 10;
	vh_tvs_init(&slot);
	vh_tvs_store_var(&slot, input, VH_TVS_RA_DVAR);

	vh_acms_input(acm_avg, acm_state, &slot);
	vh_acms_input(acm_avg, acm_state, &slot);
	vh_acms_input(acm_avg, acm_state, &slot);
	vh_acms_input(acm_avg, acm_state, &slot);
	vh_acms_input(acm_avg, acm_state, &slot);

	*input = 50;
	vh_acms_input(acm_avg, acm_state, &slot);

	vh_acms_result(acm_avg, acm_state, &slot);
	res = vh_tvs_value(&slot);
	assert(res);
	assert(*res == 16);
	printf("\ntest_acm_avg (int16): %d == 16\n", *res);
	vh_tvs_reset(&slot);
}

static void
test_acm_max(void)
{
	int16_t *input = vh_makevar1(short), *res;
	TypeVarSlot tvs, result;
	TypeVarAcm acm_max;
	TypeVarAcmState acm_state;

	vh_tvs_init(&tvs);
	vh_tvs_init(&result);
	vh_tvs_store_var(&tvs, input, 0);

	acm_max = vh_acm_max_tys(&tys_int16[0]);
	acm_state = vh_acms_create(acm_max);

	assert(acm_max);
	assert(acm_state);

	*input = 100;
	vh_acms_input(acm_max, acm_state, &tvs);

	*input = 50;
	vh_acms_input(acm_max, acm_state, &tvs);

	*input = 25;
	vh_acms_input(acm_max, acm_state, &tvs);

	vh_acms_result(acm_max, acm_state, &result);
	res = vh_tvs_value(&result);
	assert(res);
	assert(*res == 100);
	printf("\ntest_acm_max (int16): %d == 100", *res);

	*input = 30;
	vh_acms_input(acm_max, acm_state, &tvs);

	*input = 100;
	vh_acms_input(acm_max, acm_state, &tvs);

	vh_acms_result(acm_max, acm_state, &result);
	res = vh_tvs_value(&result);
	assert(res);
	assert(*res == 100);
	printf("\ntest_acm_max (int16): %d == 100", *res);

	*input = 101;
	vh_acms_input(acm_max, acm_state, &tvs);

	vh_acms_result(acm_max, acm_state, &result);
	res = vh_tvs_value(&result);
	assert(res);
	assert(*res == 101);
	printf("\ntest_acm_max (int16): %d == 101", *res);
}


static void
test_acm_min(void)
{
	int16_t *input = vh_makevar1(short);
	int16_t *res;
	TypeVarSlot tvs, result;
	TypeVarAcm acm_min;
	TypeVarAcmState acm_state;

	vh_tvs_init(&tvs);
	vh_tvs_init(&result);
	vh_tvs_store_var(&tvs, input, 0);

	acm_min = vh_acm_min_tys(&tys_int16[0]);
	acm_state = vh_acms_create(acm_min);

	assert(acm_min);
	assert(acm_state);

	*input = 100;
	vh_acms_input(acm_min, acm_state, &tvs);

	*input = 50;
	vh_acms_input(acm_min, acm_state, &tvs);

	*input = 25;
	vh_acms_input(acm_min, acm_state, &tvs);

	vh_acms_result(acm_min, acm_state, &result);
	res = vh_tvs_value(&result);
	assert(res);
	assert(*res == 25);
	printf("\ntest_acm_min (int16): %d == 25", *res);

	*input = 30;
	vh_acms_input(acm_min, acm_state, &result);

	*input = 100;
	vh_acms_input(acm_min, acm_state, &result);

	vh_acms_result(acm_min, acm_state, &result);
	res = vh_tvs_value(&result);
	assert(res);
	assert(*res == 25);
	printf("\ntest_acm_min (int16): %d == 25", *res);

	*input = 101;
	vh_acms_input(acm_min, acm_state, &result);

	vh_acms_result(acm_min, acm_state, &result);
	res = vh_tvs_value(&result);
	assert(res);
	assert(*res == 25);
	printf("\ntest_acm_min (int16): %d == 25", *res);
}


static void
test_acm_sum(void)
{
	int16_t *input = vh_makevar1(short);
	int16_t *acm_res;
	TypeVarSlot tvs, result;
	TypeVarAcm acm_sum;
	TypeVarAcmState acm_state;

	vh_tvs_init(&tvs);
	vh_tvs_init(&result);
	vh_tvs_store_var(&tvs, input, 0);

	acm_sum = vh_acm_sum_tys(&tys_int16[0]);
	acm_state = vh_acms_create(acm_sum);

	assert(acm_sum);
	assert(acm_state);

	*input = 15;

	vh_acms_input(acm_sum, acm_state, &tvs);
	vh_acms_result(acm_sum, acm_state, &result);
	acm_res = vh_tvs_value(&result);
	assert(acm_res);
	assert(*acm_res == 15);

	vh_acms_input(acm_sum, acm_state, &tvs);
	vh_acms_result(acm_sum, acm_state, &result);
	acm_res = vh_tvs_value(&result);
	assert(acm_res);
	assert(*acm_res == 30);

	*input = -10;

	vh_acms_input(acm_sum, acm_state, &tvs);
	vh_acms_result(acm_sum, acm_state, &result);
	acm_res = vh_tvs_value(&result);
	assert(acm_res);
	assert(*acm_res == 20);
	printf("\ntest_acm_sum (int16): %d == 20", *acm_res);
}

static void 
test_acm_vars(void)
{
	int32_t *input = vh_makevar1(int);
	int32_t *res;
	TypeVarAcm acm_vars;
	TypeVarAcmState acms_vars;
	TypeVarSlot slot, result;

	vh_tvs_init(&slot);
	vh_tvs_init(&result);
	vh_tvs_store_var(&slot, input, VH_TVS_RA_DVAR);

	acm_vars = vh_acm_vars_tys(tys_int32);
	acms_vars = vh_acms_create(acm_vars);

	*input = 10;
	vh_acms_input(acm_vars, acms_vars, &slot);

	*input = 20;
	vh_acms_input(acm_vars, acms_vars, &slot);

	*input = 30;
	vh_acms_input(acm_vars, acms_vars, &slot);
	
	*input = 80;
	vh_acms_input(acm_vars, acms_vars, &slot);
	
	*input = 50;
	vh_acms_input(acm_vars, acms_vars, &slot);
	
	*input = 60;
	vh_acms_input(acm_vars, acms_vars, &slot);
	
	*input = 75;
	vh_acms_input(acm_vars, acms_vars, &slot);
	
	*input = 72;
	vh_acms_input(acm_vars, acms_vars, &slot);
	
	*input = 76;
	vh_acms_input(acm_vars, acms_vars, &slot);

	vh_acms_result(acm_vars, acms_vars, &result);
	res = vh_tvs_value(&result);
	assert(res);
	assert(*res == 703);
	printf("\nvars: %d [%p]\n", *res, res);

	vh_acms_result(acm_vars, acms_vars, &result);
	printf("\nvars: %d [%p]\n", *res, res);

}


