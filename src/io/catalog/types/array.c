/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/types/Array.h"

static size_t ty_array_elem_span(const struct ArrayData *array, bool *pointer);

static void ty_array_tam_memset_set(struct TamGenStack *tamstack,
									void *source, void *target);


static int32_t ty_array_tom_comp(struct TomCompStack *tomstack,
								 const void *lhs, const void *rhs);
static void ty_array_tom_construct(struct TomConstructStack *tomstack,
								   void *target, HeapBufferNo hbno);
static void ty_array_tom_destruct(struct TomDestructStack *tomstack,
								  void *target);

const struct TypeData vh_type_Array = {
	.id = 200,
	.name = "Array",
	.size = sizeof(struct ArrayData),
	.alignment = sizeof(uintptr_t),
	.construct_forhtd = true,
	.varlen = true,
	.varlen_has_header = true,
	.include_inner_for_size = true,
	.allow_inner = true,
	.require_inner = true,
	.tam = {
		.memset_get = ty_array_tam_memset_set,
		.memset_set = ty_array_tam_memset_set
	},
	.tom = {
		.comp = ty_array_tom_comp,
		.construct = ty_array_tom_construct,
		.destruct = ty_array_tom_destruct
	}
};

static size_t ty_array_elem_span(const struct ArrayData *array, bool *pointer)
{
	size_t inner_sz = array->ty_inner->size;

	if (array->ty_inner == &vh_type_Array)
	{
		*pointer = false;
		return vh_type_Array.size;
	}

	if (inner_sz > sizeof(uintptr_t))
	{
		*pointer = true;
		return 8;
	}

	*pointer = false;

	switch (inner_sz)
	{
	case 1:
		return 1;

	case 2:
		return 2;
	case 3:
	case 4:
		return 4;

	case 5:
	case 6:
	case 7:
	case 8:
		return 8;
	}

	return 0;
}

static void 
ty_array_tam_memset_set(struct TamGenStack *tamstack,
						void *source, void *target)
{
	Array sa = source, ta = target;
	void *sc, *tc;
	size_t i, nelems = vh_ty_array_nelems(sa);
	vh_tom_construct construct_funcs[VH_TAMS_MAX_DEPTH];

	vh_toms_fill_construct_funcs(&tamstack->types[0], &construct_funcs[0]);

	ta->ty_inner = sa->ty_inner;

	for (i = 0; i < nelems; i++)
	{
		sc = vh_ty_array_elemat(sa, i);
		tc = vh_ty_array_emplace(ta);

		vh_tom_firea_construct(&tamstack->types[0], 
							   &construct_funcs[0], 
							   tc,	   
							   sa->vl.hbno);

		vh_tam_firen_memset_set(tamstack, sc, tc); 
	}
}

static int32_t
ty_array_tom_comp(struct TomCompStack *tomstack,
				  const void *lhs, const void *rhs)
{
	const struct ArrayData *la = lhs, *ra = rhs;
	void *le, *re;
	int32_t fcomp;
	size_t i, l_nelems = vh_ty_array_nelems(la),
		   r_nelems = vh_ty_array_nelems(ra);

	for (i = 0; i < l_nelems && i < r_nelems; i++)
	{
		le = vh_ty_array_elemat(la, i);
		re = vh_ty_array_elemat(ra, i);

		fcomp = vh_tom_firen_comp(tomstack, le, re);

		if (fcomp)
			break;
	}

	if (!fcomp)
	{
		return l_nelems < r_nelems ? -1 : l_nelems > r_nelems;
	}

	return fcomp;
}

static void 
ty_array_tom_construct(struct TomConstructStack *tomstack,
								   void *target, HeapBufferNo hbno)
{
	Array array = target;


	array->vl.size = 0;
	array->vl.hbno = hbno;
	array->buffer = 0;
	array->ndimensions = 1;
	array->ty_inner = tomstack->types[0];
}

/*
 * Needs to fire the destructor on each element.
 */
static void 
ty_array_tom_destruct(struct TomDestructStack *tomstack,
					  void *target)
{
	Array array = target, child;
	size_t nelems = vh_ty_array_nelems(array);
	uint32_t i;

	for (i = 0; i < nelems; i++)
	{
		child = vh_ty_array_elemat(array, i);
		vh_tom_firen_destruct(tomstack, child);
	}

	if (array->buffer)
		vhfree(array->buffer);
}

void*
vh_ty_array_elemat(const struct ArrayData* array, uint32_t idx)
{
	bool pointer;
	size_t span = ty_array_elem_span(array, &pointer);
	size_t nelems = vh_ty_array_nelems(array);
	void *elem;

	if (idx < nelems)
	{
		elem = ((char*)array->buffer) + (span * idx);
		return elem;
	}

	return 0;
}

void*
vh_ty_array_emplace(Array array)
{
	bool pointer;
	size_t span = ty_array_elem_span(array, &pointer);
	void *ptr;
	
	if (array->buffer)
	{
		array->buffer = vhrealloc(array->buffer, array->vl.size + span);
		ptr = ((char*)array->buffer) + array->vl.size;
		array->vl.size += span;
	}
	else
	{
		array->buffer = vhmalloc_ctx(vh_hb_memoryctx(array->vl.hbno), span);
		ptr = array->buffer;
		array->vl.size = span;
	}

	return ptr;
}

size_t
vh_ty_array_nelems(const struct ArrayData* array)
{
	bool pointer;
	size_t span = ty_array_elem_span(array, &pointer);
	size_t nelems = 0;

	if (span)
	{
		nelems = array->vl.size / span;
	}

	return nelems;
}

