/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeCatalog.h"
#include "io/utils/htbl.h"

static struct
{
	HashTable htbl_by_ctype;
	HashTable htbl_by_tag;
} type_catalog = { };


static inline void establish_catalog(void);


void
vh_type_add(Type ty)
{
	Type *tyh;
	const struct TypeOperRegData *regoper;
	int32_t ret;
	size_t i;

	establish_catalog();

	tyh = vh_htbl_put(type_catalog.htbl_by_ctype,
			 ty->name,
			 &ret);
	assert(ret == 1 || ret == 2);
	*tyh = ty;

	tyh = vh_htbl_put(type_catalog.htbl_by_tag,
			 &ty->id,
			 &ret);
	assert(ret == 1 || ret == 2);
	*tyh = ty;

	/*
	 * Spin thru Type Operator Registration table and set those up.  This way as
	 * types are registered, their operators can be registered as well.
	 */
	if (ty->regoper)
	{
		for (i = 0; i < ty->regoper_sz; i++)
		{
			regoper = &ty->regoper[i];
			vh_type_oper_register(regoper->lhs, regoper->oper, regoper->rhs,
								  regoper->func,
								  regoper->flags);
		}
	}
}

void
vh_type_remove(Type ty)
{
	vh_htbl_del(type_catalog.htbl_by_ctype,
		    ty->name);
	vh_htbl_del(type_catalog.htbl_by_tag,
		    (void*)((uintptr_t)ty->id));
}

/*
 * vh_type_ctype
 *
 * These should not throw an error, only return null when the requested type
 * could not be found in the catalog.
 */
Type
vh_type_ctype(const char *tname)
{
	Type *ty;

	if (type_catalog.htbl_by_ctype)
	{
		ty = vh_htbl_get(type_catalog.htbl_by_ctype,
				 tname);

		if (ty)
			return *ty;
	}

	return 0;	
}

/*
 * vh_type_tag
 *
 * These should not throw an error, only return null when the requested type
 * could not be found in the catalog.
 */
Type
vh_type_tag(TypeTag tag)
{
	Type *ty;

	if (type_catalog.htbl_by_tag)
	{
		ty = vh_htbl_get(type_catalog.htbl_by_tag,
				 &tag);

		if (ty)
			return *ty;
	}

	return 0;
}

static inline void
establish_catalog(void)
{
	HashTableOpts hopts;

	hopts.value_sz = sizeof(Type);
	hopts.mctx = vh_mctx_top();
	hopts.is_map = true;

	/*
	 * Spin up our two Hash Tables to track the types if necessary.  We want a 
	 * hash map here because of how often we're likely to do lookups.
	 */
	if (!type_catalog.htbl_by_ctype)
	{
		hopts.key_sz = sizeof(const char*);
		hopts.func_hash = vh_htbl_hash_str; 
		hopts.func_compare = vh_htbl_comp_str;
		type_catalog.htbl_by_ctype = vh_htbl_create(&hopts,
							    VH_HTBL_OPT_KEYSZ |
							    VH_HTBL_OPT_VALUESZ |
							    VH_HTBL_OPT_HASHFUNC |
							    VH_HTBL_OPT_COMPFUNC |
							    VH_HTBL_OPT_MCTX |
							    VH_HTBL_OPT_MAP);

		hopts.key_sz = sizeof(TypeTag);
		hopts.func_hash = vh_htbl_hash_int16;
		hopts.func_compare = vh_htbl_comp_int16;
		type_catalog.htbl_by_tag = vh_htbl_create(&hopts,
							  VH_HTBL_OPT_KEYSZ |
							  VH_HTBL_OPT_VALUESZ |
							  VH_HTBL_OPT_HASHFUNC |
							  VH_HTBL_OPT_COMPFUNC |
							  VH_HTBL_OPT_MCTX |
							  VH_HTBL_OPT_MAP);
	}
}



