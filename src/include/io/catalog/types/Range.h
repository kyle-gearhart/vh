/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_types_Range_H
#define vh_datacatalog_types_Range_H

/*
 * The VH.IO range type is a generic type and has three attributes:
 * 	>	A lower boundary
 * 	> 	An upper boundary
 * 	> 	Flags component showing Infinity/Exclusive values
 *
 * The size of the Range type is dependent upon the TAM Stack.  The
 * range may only contain 1 nested type in the TAM stack.
 */

typedef uint8_t RangeFlags;

#define VH_TY_RANGE_LInclusive			0x02
#define VH_TY_RANGE_UInclusive			0x04
#define VH_TY_RANGE_LInfinity			0x08
#define VH_TY_RANGE_UInfinity			0x10

bool vh_ty_range_contains(Type ty_inner,
						  const void *range,
						  const void *to);
bool vh_ty_range_excludes(Type ty_inner,
						  const void *range,
						  const void *excludes);
bool vh_ty_range_includes(Type ty_iner,
						  const void *range,
						  const void *includes);
int32_t vh_ty_range_overlaps(Type ty_inner,
		   					 const void *range,
		   					 const void *to);

size_t vh_ty_range_boundary_member_sz(Type *tamstack);

#endif

