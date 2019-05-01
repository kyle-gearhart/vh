/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef vh_io_utils_HashTable_H
#define vh_io_utils_HashTable_H

typedef int32_t (*vh_htbl_hash_func)(HashTable htbl, const void* key);
typedef int32_t (*vh_htbl_comp_func)(HashTable htbl, const void *lhs, const void *rhs);

#define VH_HTBL_OPT_KEYSZ			0x0001
#define VH_HTBL_OPT_VALUESZ			0x0002
#define VH_HTBL_OPT_HASHFUNC		0x0004
#define VH_HTBL_OPT_COMPFUNC		0x0008
#define VH_HTBL_OPT_MCTX			0x0010
#define VH_HTBL_OPT_MAP 			0x0020

#define VH_HTBL_OPT_ALL				(VH_HTBL_OPT_KEYSZ | 						\
									 VH_HTBL_OPT_VALUESZ |						\
									 VH_HTBL_OPT_HASHFUNC | 					\
									 VH_HTBL_OPT_COMPFUNC |						\
									 VH_HTBL_OPT_MCTX |							\
									 VH_HTBL_OPT_MAP)

typedef struct HashTableOpts
{
	size_t key_sz;
	size_t value_sz;						/* size of the value, only if not a set */	
	vh_htbl_hash_func func_hash;			/* hash function */
	vh_htbl_comp_func func_compare;			/* comparison function */
	MemoryContext mctx;						/* MemoryContext to use */
	bool is_map;							/* map (key and value) */
} HashTableOpts;

typedef struct HashTableData *HashTable;

HashTable vh_htbl_create(HashTableOpts *opts, int32_t flags);
void vh_htbl_destroy(HashTable htbl);
void vh_htbl_clear(HashTable htbl);

void* vh_htbl_get(HashTable htbl, const void *key);

/*
 * vh_htbl_put
 *
 * |ret| will be set to the following:
 * 	-1	Fatal error
 * 	0	Key already exists
 * 	1	Key inserted
 * 	2	Key was previously deleted, but reinserted
 */
void* vh_htbl_put(HashTable htbl, const void *key, int32_t *ret);
void vh_htbl_del(HashTable htbl, const void *key);

size_t vh_htbl_count(HashTable htbl);
MemoryContext vh_htbl_mctx(HashTable htbl);
SList vh_htbl_to_slist(HashTable htbl);

int32_t vh_htbl_iter(HashTable htbl, int32_t idx, void *key, void *value);
int32_t vh_htbl_iter_last(HashTable htbl, void *key, void *value);

/*
 * Hash Functions
 */

#if VHB_SIZEOF_VOID == 8
#define vh_htbl_comp_ptr	vh_htbl_comp_int64
#define vh_htbl_hash_ptr	vh_htbl_hash_int64
#else
#define vh_htbl_comp_ptr	vh_htbl_comp_int32
#define vh_htbl_hash_ptr	vh_htbl_hash_int32
#endif

int32_t vh_htbl_hash_int16(HashTable htbl, const void *key);
int32_t vh_htbl_comp_int16(HashTable htbl, const void *lhs, const void *rhs);
int32_t vh_htbl_hash_int32(HashTable htbl, const void *key);
int32_t vh_htbl_comp_int32(HashTable htbl, const void *lhs, const void *rhs);
int32_t vh_htbl_hash_int64(HashTable htbl, const void *key);
int32_t vh_htbl_comp_int64(HashTable htbl, const void *lhs, const void *rhs);

/*
 * Use strcmp to compare strings
 */
int32_t vh_htbl_hash_str(HashTable htbl, const void *key);
int32_t vh_htbl_comp_str(HashTable htbl, const void *lhs, const void *rhs);

/*
 * Variable Length Binary Data
 */
int32_t vh_htbl_hash_bin(HashTable htbl, const void *key);
int32_t vh_htbl_comp_bin(HashTable htbl, const void *lhs, const void *rhs);


typedef bool (*vh_htbl_iterate_map_cb)(HashTable htbl, const void *key, 
									   void *entry,
									   void *data);
typedef bool (*vh_htbl_iterate_set_cb)(HashTable htbl, const void *key,
									   void *data);

void vh_htbl_iterate_map(HashTable htbl, vh_htbl_iterate_map_cb cb,
						 void *data);
void vh_htbl_iterate_set(HashTable htbl, vh_htbl_iterate_set_cb cb,
						 void *data);

#endif

