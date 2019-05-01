/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <string.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/types/Date.h"

static void test_int32(void);
static void test_Date(void);
static void test_DateTime(void);
static void test_String(void);

static struct CStrAMOptionsData copts = { }, copts_malloc = { true };

void test_types()
{
	test_int32();
	test_String();
	test_Date();
	test_DateTime();
}

static void
test_int32(void)
{
	int val = 0, *n;
	char buffer[32], *str;
	size_t len = 32;

	vh_type_int32.tam.cstr_set(0, &copts, "5555", &val, 4, 0, 0);
	n = vh_type_int32.tam.cstr_set(0, &copts_malloc, "6666", &val, 4, 0, 0);

	assert(val == 5555);
	assert(*n == 6666);

	vh_type_int32.tam.cstr_set(0, &copts, "-55555", &val, 6, 0, 0);
	n = vh_type_int32.tam.cstr_set(0, &copts_malloc, "-123456", &val, 7, 0, 0);

	assert(val == -55555);
	assert(*n == -123456);

	vh_type_int32.tam.cstr_get(0, &copts, &val, &buffer[0], &len, 0, 0);
	str = vh_type_int32.tam.cstr_get(0, &copts_malloc, n, 0, &len, 0, 0);

	assert(strcmp("-55555", &buffer[0]) == 0);
	assert(strcmp("-123456", str) == 0);

	vhfree(n);
	vhfree(str);	
}

/*
 * Check the computations from julian day to calendar and vice versa for several date
 * scenarios.  We also check the two major epochs (UNIX and Year 2000) used by most
 * systems.
 */
static void
test_Date(void)
{
	const Date target_jd = 2440588, birthday_jd = 2446995, leapyear_jd = 2457448, bce_jd = 1268583, pgres_jd = 2451545;
	const int32_t target_year = 1970, target_month = 1, target_day = 1;
	const int32_t birthday_year = 1987, birthday_month = 7, birthday_day = 18;
	const int32_t leapyear_year = 2016, leapyear_month = 2, leapyear_day = 29;
	const int32_t bce_year = -1239, bce_month = 2, bce_day = 28;
	const int32_t pgres_year = 2000, pgres_month = 1, pgres_day = 1;
	Date computed_jd;
	int32_t computed_year, computed_month, computed_day;

	computed_jd = vh_ty_date2julian(target_year, target_month, target_day);
	assert(computed_jd == target_jd);

	vh_ty_julian2date(target_jd, &computed_year, &computed_month, &computed_day);
	assert(computed_year == target_year);
	assert(computed_month == target_month);
	assert(computed_day == target_day);

	computed_jd = vh_ty_date2julian(birthday_year, birthday_month, birthday_day);
	assert(computed_jd == birthday_jd);

	vh_ty_julian2date(birthday_jd, &computed_year, &computed_month, &computed_day);
	assert(computed_year == birthday_year);
	assert(computed_month == birthday_month);
	assert(computed_day == birthday_day);
	
	computed_jd = vh_ty_date2julian(leapyear_year, leapyear_month, leapyear_day);
	assert(computed_jd == leapyear_jd);

	vh_ty_julian2date(leapyear_jd, &computed_year, &computed_month, &computed_day);
	assert(computed_year == leapyear_year);
	assert(computed_month == leapyear_month);
	assert(computed_day == leapyear_day);
	
	computed_jd = vh_ty_date2julian(bce_year, bce_month, bce_day);
	assert(computed_jd == bce_jd);

	vh_ty_julian2date(bce_jd, &computed_year, &computed_month, &computed_day);
	assert(computed_year == bce_year);
	assert(computed_month == bce_month);
	assert(computed_day == bce_day);
	
	computed_jd = vh_ty_date2julian(pgres_year, pgres_month, pgres_day);
	assert(computed_jd == pgres_jd);

	vh_ty_julian2date(pgres_jd, &computed_year, &computed_month, &computed_day);
	assert(computed_year == pgres_year);
	assert(computed_month == pgres_month);
	assert(computed_day == pgres_day);
}

static void
test_DateTime(void)
{
}

static void
test_String(void)
{
	const char *convert_append = "inline a 10aasdf90";
	const char *convert_append_long = "this is a test string we want to manipulate"
		" because we need to add a few extra words";
	String str, cpy;

	str = vh_str.Convert("inline");
	vh_str.Append(str, " a 10aasdf90");
	assert(!strcmp(vh_str_buffer(str), convert_append));

	str = vh_str.Convert("this is a test string we want to manipulate");
	vh_str.Append(str, " because we need to add a few extra words");
	assert (!strcmp(vh_str_buffer(str), convert_append_long));

	cpy = vh_str.ConstructStr(str);
	assert(!strcmp(vh_str_buffer(cpy), convert_append_long));

	vh_type_String.tam.cstr_set(0, &copts, convert_append, str, strlen(convert_append), 0, 0);
	
	assert(strcmp(vh_str_buffer(str), convert_append) == 0);
}

