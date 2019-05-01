/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_catalog_TypeVarAcm_H
#define vh_io_catalog_TypeVarAcm_H

#include "io/catalog/TypeVarSlot.h"

/*
 * TypeVarAcm
 *
 * Type Var Accumulators provide a common interface for performing accumulators
 * using the TypeVar interface.  Accumulators are powerful data aggregation
 * techniques exposed thru a common interface.  TypeVarAcm.h is intended to provide
 * a common interface regardless of the underlying functionality of the Acm.
 *
 * Implementation details are hidden away in acm/acm_impl.h
 *
 * We do include the launchers here, but there implementation is in their 
 * corresponding C file in the acm subdirectory.  (i.e. vh_acm_avg is implemented in
 * acm/acm_avg.c)
 *
 * Each accumulator will create two structures, a top level TypeVarAcm which 
 * contains things like the various TypeVar Fast Path Operator caching structures.
 *
 * Each TypeVarAcm can have zero, one, or more TypeVarAcmState structures, which
 * cache the local state of a particular instance of the accumulator.  This two
 * pronged approach allows us to minimize the number hefty operator caches we
 * maintain.
 *
 * Think of a nest operation where we'll perform the same aggregations on each
 * child row.  Keeping a copy of each operator cache for every single row would
 * be incredibly expensive in terms of memory usage.  TypeVarAcmState point back
 * to their originating TypeVarAcm for the operator cache information. 
 */

typedef struct TypeVarAcmData *TypeVarAcm;
typedef void *TypeVarAcmState;

size_t vh_acms_size(TypeVarAcm);
void vh_acms_initialize(TypeVarAcm, void *data, size_t capacity);
void* vh_acms_create(TypeVarAcm);
void vh_acms_finalize(TypeVarAcm, TypeVarAcmState);
void vh_acms_destroy(TypeVarAcm, TypeVarAcmState);

void vh_acm_finalize(TypeVarAcm);
void vh_acm_destroy(TypeVarAcm);

int32_t vh_acms_input(TypeVarAcm, TypeVarAcmState, ...);
int32_t vh_acms_result(TypeVarAcm, TypeVarAcmState, TypeVarSlot*);


/*
 * ============================================================================
 * Accumulators
 * ============================================================================
 *
 * There are generally two enty points for generating an accumulator.  A variadic
 * argument version and a tys version.
 *
 * The tys is helpful when many accumulators may be run against a record.  We don't
 * want the overhead of pinning and unpinning a HeapTuple for each individual
 * TypeVarOpExec call, so we do the pinning prior to call the accumulator so that
 * all acumulators only pin once per record.  The Nest infrastructure does this.
 */

typedef TypeVarAcm (*vh_acm_tys_create)(Type *tys);

TypeVarAcm vh_acm_avg_tys(Type *tys);
TypeVarAcm vh_acm_count_tys(Type *tys);
TypeVarAcm vh_acm_devp_tys(Type *tys);
TypeVarAcm vh_acm_devs_tys(Type *tys);
TypeVarAcm vh_acm_max_tys(Type *tys);
TypeVarAcm vh_acm_min_tys(Type *tys);
TypeVarAcm vh_acm_sum_tys(Type *tys);
TypeVarAcm vh_acm_varp_tys(Type *tys);
TypeVarAcm vh_acm_vars_tys(Type *tys);

#endif

