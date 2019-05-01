/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_utils_kvmap_H
#define vh_utils_kvmap_H

#include "io/utils/keys.h"

typedef HashTable KeyValueMap;
typedef struct KeyIteratorData KeyValueMapIterator;

/*
 * ============================================================================
 * KeyValueMap
 * ============================================================================
 */

#define vh_kvmap_create_impl(ksz, vsz, fhash, fcomp, m_ctx)						\
	(0 == 0 ? ( {																\
			HashTableOpts hopt = { };											\
			hopt.key_sz = (ksz);												\
			hopt.value_sz = (vsz);												\
			hopt.func_hash = (fhash);											\
			hopt.func_compare = (fcomp);										\
			hopt.mctx = (m_ctx);												\
			hopt.is_map = true;													\
			vh_htbl_create(&hopt, VH_HTBL_OPT_ALL);								\
	} ) : 0)

#define vh_kvmap_create()			vh_kvmap_create_impl(sizeof(uintptr_t),		\
														 sizeof(uintptr_t),		\
														 vh_htbl_hash_ptr, 		\
														 vh_htbl_comp_ptr, 		\
														 vh_mctx_current())
#define vh_htp_kvmap_create()		vh_kvmap_create_impl(sizeof(HeapTuplePtr),	\
														 sizeof(uintptr_t),		\
														 vh_htbl_hash_int64,	\
														 vh_htbl_comp_int64, 	\
														 vh_mctx_current())

#define vh_kvmap_destroy(kvm)		vh_htbl_destroy((kvm))

#define vh_kvmap_exists(kvm, k)		(vh_htbl_get((kvm), (k)) ? true : false)
#define vh_kvmap_value(kvm, k, v)	(((v) = vh_htbl_get((kvm), (k))) ? true :	\
									 ( {										\
									   	int32_t ret;							\
							   			(v) = vh_htbl_put((kvm), (k), &ret);	\
									    assert(ret == 1 || ret == 2);			\
									    false;									\
									   }  ) )
#define vh_kvmap_find(kvm, k)		(vh_htbl_get((kvm), (k)))
#define vh_kvmap_remove(kvm, k)		(vh_htbl_del((kvm), (k)))


#define vh_kvmap_it_init(it, hash)	vh_key_it_init(it, hash)
#define vh_kvmap_it_first(it, k, v)	vh_key_it_first(it, k, v)
#define vh_kvmap_it_next(it, k, v)	vh_key_it_next(it, k, v)
#define vh_kvmap_it_last(it, k, v)	vh_key_it_last(it, k, v)


#endif

