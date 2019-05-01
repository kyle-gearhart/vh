/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <stdarg.h>

#include "vh.h"
#include "io/catalog/acm/acm_impl.h"


/*
 * ============================================================================
 * TypeVarAcm.h
 * ============================================================================
 */

size_t
vh_acms_size(TypeVarAcm tvacm)
{
	return tvacm->acms_size;
}

void
vh_acms_initialize(TypeVarAcm tvacm, void *data, size_t capacity)
{
	if (tvacm && tvacm->funcs->acms_initialize)
		tvacm->funcs->acms_initialize(tvacm, data, capacity);
}

/*
 * vh_acms_create
 */
void*
vh_acms_create(TypeVarAcm acm)
{
	TypeVarAcmState acms;

	acms = vhmalloc(acm->acms_size);
	memset(acms, 0, acm->acms_size);

	if (acm->funcs->acms_initialize)
		acm->funcs->acms_initialize(acm, acms, acm->acms_size);

	return acms;
}

void
vh_acms_destroy(TypeVarAcm acm, TypeVarAcmState acms)
{
	if (acm && acms)
	{
		if (acm->funcs->acms_finalize)
			acm->funcs->acms_finalize(acm, acms);

		vhfree(acms);
	}
}

void
vh_acms_finalize(TypeVarAcm acm, TypeVarAcmState acms)
{
	if (acm && acms)
	{
		if (acm->funcs->acms_finalize)
			acm->funcs->acms_finalize(acm, acms);
	}
}

int32_t
vh_acms_input(TypeVarAcm acm, TypeVarAcmState acms, ...)
{
	va_list ap;
	int32_t ret = -1;

	va_start(ap, acms);

	if (acm && acms && acm->funcs->input)
	{
		ret = acm->funcs->input(acm, acms, ap);
	}

	va_end(ap);

	return ret;
}

int32_t
vh_acms_result(TypeVarAcm acm, TypeVarAcmState acms, TypeVarSlot *slot)
{
	if (acm && acms && acm->funcs->result)
	{
		return acm->funcs->result(acm, acms, slot);
	}

	return -1;
}

/*
 * ============================================================================
 * acm/acm_impl.h
 * ============================================================================
 */


/*
 * vh_acm_create
 */
void*
vh_acm_create(size_t sz, 
			  const struct TypeVarAcmFuncs const *func_table,
			  size_t acms_sz)
{
	TypeVarAcm acm;

	assert(sz >= sizeof(struct TypeVarAcmData));

	acm = vhmalloc(sz);
	memset(acm, 0, sz);
	acm->funcs = func_table;
	acm->acms_size = acms_sz;

	return acm;
}



