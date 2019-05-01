/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "test.h"


static void operator_int16(void);

void test_operators(void)
{
	operator_int16();
}

static void operator_int16(void)
{
	int16_t a, b, c;
	vh_tom_oper f;

	a = 12, b = 3, c = 0;

	f = vh_type_oper(&vh_type_int16, "+", &vh_type_int16, 0);
	f(0, &a, &b, &c);
	assert(a == 12);
	assert(b == 3);
	assert(c == 15);

	f = vh_type_oper(&vh_type_int16, "/", &vh_type_int16, 0);
	f(0, &a, &b, &c);
	assert(a == 12);
	assert(b == 3);
	assert(c == 4);

	f = vh_type_oper(&vh_type_int16, "*", &vh_type_int16, 0);
	f(0, &a, &c, &c);
	assert(a == 12);
	assert(b == 3);
	assert(c == 48);

	f = vh_type_oper(&vh_type_int16, "-", &vh_type_int16, 0);
	f(0, &a, &b, &a);
	assert(a == 9);
	assert(b == 3);
	assert(c == 48);
}


