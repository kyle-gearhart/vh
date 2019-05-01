/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_utils_kset_H
#define vh_utils_kset_H

#include "io/utils/keys.h"

typedef HashTable KeySet;
typedef struct KeyIteratorData KeySetIterator;

/*
 * ============================================================================
 * KeySet
 * ============================================================================
 */
 
#define vh_kset_create()														\
	vh_htbl_create(&((struct HashTableOpts) { 									\
											.key_sz = sizeof(uintptr_t), 		\
											.value_sz = 0,						\
											.func_hash = vh_htbl_hash_ptr, 		\
											.func_compare = vh_htbl_comp_ptr, 	\
											.mctx = vh_mctx_current(),			\
											.is_map = false }), 				\
				   VH_HTBL_OPT_ALL)

#define vh_htp_kset_create()													\
	vh_htbl_create(&((struct HashTableOpts) { 									\
											.key_sz = sizeof(int64_t), 			\
											.value_sz = 0,						\
											.func_hash = vh_htbl_hash_int64,	\
											.func_compare = vh_htbl_comp_int64, 	\
											.mctx = vh_mctx_current(),			\
											.is_map = false }), 				\
				   VH_HTBL_OPT_ALL)
#define vh_cstr_kset_create()													\
	vh_htbl_create(&((struct HashTableOpts) { 									\
											.key_sz = sizeof(char*), 			\
											.value_sz = 0,						\
											.func_hash = vh_htbl_hash_str,		\
											.func_compare = vh_htbl_comp_str, 	\
											.mctx = vh_mctx_current(),			\
											.is_map = false }), 				\
				   VH_HTBL_OPT_ALL)
#define vh_kset_destroy(ks)				vh_htbl_destroy((ks))

#define vh_kset_count(ks)			vh_htbl_count((ks))

#define vh_kset_exists(ks, k)		(vh_htbl_get((ks), (k)) ? true : false)
#define vh_kset_key(ks, k)			((0 == 0) ? 								\
										( {										\
										  	void *key = (k);					\
										  	((vh_htbl_get((ks), key)) ?			\
											 	true :							\
											 	( {								\
													int32_t ret;				\
													vh_htbl_put((ks), key, &ret);		\
													assert(ret == 1 || ret == 2);		\
													false; 						\
												  } ) );						\
										  	} ) : 0 )

#define vh_kset_remove(ks, k)			vh_htbl_del((ks), (k))
												
#define vh_kset_it_init(it, hash)		vh_key_it_init(it, hash)
#define vh_kset_it_first(it, k)			vh_key_it_first(it, k, 0)
#define vh_kset_it_next(it, k)			vh_key_it_next(it, k, 0)
#define vh_kset_it_last(it, k)			vh_key_it_last(it, k, 0)


#define vh_kset_to_slist(ks)			vh_key_slist(ks)

#endif

