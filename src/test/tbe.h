/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_TEST_TBE_H
#define VH_TEST_TBE_H

#include "vh.h"
#include "io/catalog/BackEnd.h"
#include "io/catalog/TypeVarSlot.h"

/*
 * BackEnd Testing Infrastructure
 *
 * BackEnd testing allows us to test the various components of the BackEnd
 * implementation.  We're principally concerned with serializing and
 * deserializing the individual data types with the BackEnd.  It's especially
 * important we provide a robust test suite.  Some of our backends use
 * binary transmission and as platforms change, that can make things difficult.
 *
 * The main entry point is tbe_sql_command_cycle.
 */

TypeVarSlot** tbe_alloc_expectedv(int32_t n_rows, int32_t n_columns);

int32_t tbe_sql_command_cycle(BackEndConnection bec, const char *table_name,
							  TypeVarSlot **expected_values, int32_t n_rows,
							  int32_t n_columns, int32_t flags);

#endif

