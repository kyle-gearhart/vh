/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_CATALOG_TYPEVARPAGE_H
#define VH_CATALOG_TYPEVARPAGE_H

/*
 * This isn't really a datapage that's gets flushed to disk but we use similar
 * page like access semantics to store TypeVar in a contiguous block of memory.
 *
 * TypeVarAcmState implementators are the first case we came across where
 * having infrastructure to manage the offsets made more sense than trying to
 * write macros over and over again just to access a TypeVar that's initialized
 * into a block of memory rather than having it allocated dynamically.
 */

typedef struct TypeVarPageData *TypeVarPage;

struct TypeVarPageData
{
	int16_t d_upper;
	int16_t d_lower;

	struct
	{
		int16_t varhead;
		int16_t data;
	} slots[0];
};

#define vh_tvp_header_size				offsetof(struct TypeVarPageData, slots[0])
#define vh_tvp_slot_size				(sizeof(int16_t) + sizeof(int16_t))
#define vh_tvp_array_size(tvp)			(((tvp)->d_upper -									\
										  offsetof(struct TypeVarPageData, slots[0])) / 	\
										 vh_tvp_slot_size)
#define vh_tvp_freespace(tvp)			((tvp)->d_lower - (tvp)->d_upper)
#define vh_tvp_var(tvp, off)			(((off) > 0 && (off) <= vh_tvp_array_size(tvp)) ?	\
										 (void*)(((char*)tvp) + (tvp)->slots[(off) - 1].data) : (void*)0)

#define vh_tvp_varhead(tvp, off)		(((off) > 0 && (off) <= vh_tvp_array_size(tvp)) ?	\
										 (void*)(((char*)tvp) + (tvp)->slots[(off) - 1].varhead) : (void*)0)

size_t vh_tvp_space(Type *tys);
#define vh_tvp_maxalign(sz)				((sz) = ((sz) + sizeof(struct TypeVarPageData)) % sizeof(uintptr_t) ? 						\
										 sizeof(uintptr_t) - ((sz) % sizeof(uintptr_t)) + (sz) + sizeof(struct TypeVarPageData) : \
										 (sz) + sizeof(struct TypeVarPageData))

void vh_tvp_initialize(TypeVarPage page, size_t sz);
int32_t vh_tvp_add(TypeVarPage page, Type *tys);
void vh_tvp_finalize(TypeVarPage page);

#endif

