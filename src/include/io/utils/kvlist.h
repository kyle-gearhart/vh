/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_utils_kvlist_H
#define vh_utils_kvlist_H

#include "io/utils/keys.h"
#include "io/utils/SList.h"

typedef struct KeyValueListData *KeyValueList;
typedef struct KeyIteratorData KeyValueListIterator;

struct KeyValueListData
{
	HashTable htbl;
	size_t slist_sz;
	bool deref;
};

/*
 * ============================================================================
 * KeyValueList
 * ============================================================================
 */
 
#define vh_kvlist_create_impl(ksz, fhash, fcomp, m_ctx, lsz, dref)				\
	((0 == 0) ? ( {																\
			KeyValueList kvl = vhmalloc(sizeof(struct KeyValueListData));		\
			HashTableOpts hopt = { };											\
			hopt.key_sz = (ksz);												\
			hopt.value_sz = sizeof(uintptr_t);									\
			hopt.func_hash = (fhash);											\
			hopt.func_compare = (fcomp);										\
			hopt.mctx = (m_ctx);												\
			hopt.is_map = true;													\
			kvl->slist_sz = (lsz);												\
			kvl->deref = (dref);												\
			kvl->htbl = vh_htbl_create(&hopt, VH_HTBL_OPT_ALL);					\
			kvl;																\
	} ) : 0)


#define vh_kvlist_create()				vh_kvlist_create_impl(VHB_SIZEOF_VOID,		\
															  vh_htbl_hash_ptr,		\
															  vh_htbl_comp_ptr,		\
															  vh_mctx_current(),	\
															  VHB_SIZEOF_VOID,		\
															  false)
#define vh_htp_kvlist_create()			vh_kvlist_create_impl(sizeof(HeapTuplePtr),	\
															  vh_htbl_hash_int64,	\
															  vh_htbl_comp_int64,	\
															  vh_mctx_current(),	\
															  sizeof(HeapTuplePtr),	\
															  true)

#define vh_kvlist_destroy(kvl)		vh_htbl_destroy((kvl)->htbl), vhfree((kvl))

#define vh_kvlist_value(ks, k, v)	(( {											\
										SList *getval;								\
		   								getval = vh_htbl_get((ks)->htbl, (k));		\
		   								if (getval) (v) = *getval;					\
										getval != 0;								\
									   } )	? true :								\
									 ( {											\
									   	int32_t ret;								\
									   	SList *putval;								\
										putval = vh_htbl_put((ks)->htbl, 			\
															 (k), 					\
															 &ret);					\
									    assert(ret == 1 || ret == 2);				\
									   	*putval = vh_SListCreate_ctx(vh_htbl_mctx((ks)->htbl));	\
									   	if ((ks)->slist_sz != sizeof(uintptr_t))	\
											vh_SListInit(*putval, 					\
														 (ks)->slist_sz,			\
														 true);						\
									   	else 										\
									   		(*putval)->deref = (ks)->deref;			\
									    (v) = *putval;								\
										false;										\
										} ) )

#define vh_kvlist_find(kvl, k)		(vh_htbl_get((kvl)->htbl, k))
#define vh_kvlist_count(kvl)		vh_htbl_count((kvl)->htbl)

#define vh_kvlist_it_init(it, hash)	vh_key_it_init(it, (hash)->htbl)
#define vh_kvlist_it_first(it, k, v)	vh_key_it_first(it, k, v)
#define vh_kvlist_it_next(it, k, v)		vh_key_it_next(it, k, v)
#define vh_kvlist_it_last(it, k, v)		vh_key_it_last(it, k, v)	
	
#endif

