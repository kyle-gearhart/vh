/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_catalog_acm_impl_H
#define vh_io_catalog_acm_impl_H

#include "io/catalog/TypeVarAcm.h"

typedef struct TypeVarAcmData TypeVarAcmData;

/*
 * Function Table Definitions
 */
typedef void (*vh_acms_initialize_func)(TypeVarAcm, void*, size_t);
typedef void (*vh_acms_finalize_func)(TypeVarAcm, TypeVarAcmState);

typedef int32_t (*vh_acms_input_func)(TypeVarAcm, TypeVarAcmState, va_list);
typedef int32_t (*vh_acms_result_func)(TypeVarAcm, TypeVarAcmState, TypeVarSlot *slot);

typedef void (*vh_acm_finalize_func)(TypeVarAcm);

struct TypeVarAcmFuncs
{
	/* ACM State */
	vh_acms_initialize_func acms_initialize;
	vh_acms_finalize_func acms_finalize;

	/* Calculation Functions */
	vh_acms_input_func input;
	vh_acms_result_func result;
	
	/* ACM */
	vh_acm_finalize_func finalize;
};


/*
 * TypeVarAcmData
 */
struct TypeVarAcmData
{
	const struct TypeVarAcmFuncs *funcs;
	size_t acms_size;
};


void* vh_acm_create(size_t sz, const struct TypeVarAcmFuncs const *, size_t acms);

#endif

