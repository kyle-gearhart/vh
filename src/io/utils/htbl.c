/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */





/* The MIT License

   Copyright (c) 2008, 2009, 2011 by Attractive Chaos <attractor@live.co.uk>

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

#include <assert.h>

#include "vh.h"
#include "io/utils/htbl.h"
#include "io/utils/SList.h"

#define __ac_isempty(flag, i) ((flag[i>>4]>>((i&0xfU)<<1))&2)
#define __ac_isdel(flag, i) ((flag[i>>4]>>((i&0xfU)<<1))&1)
#define __ac_iseither(flag, i) ((flag[i>>4]>>((i&0xfU)<<1))&3)
#define __ac_set_isdel_false(flag, i) (flag[i>>4]&=~(1ul<<((i&0xfU)<<1)))
#define __ac_set_isempty_false(flag, i) (flag[i>>4]&=~(2ul<<((i&0xfU)<<1)))
#define __ac_set_isboth_false(flag, i) (flag[i>>4]&=~(3ul<<((i&0xfU)<<1)))
#define __ac_set_isdel_true(flag, i) (flag[i>>4]|=1ul<<((i&0xfU)<<1))

#define __ac_fsize(m) ((m) < 16? 1 : (m)>>4)

#define kroundup32(x)					(--(x), (x)|=(x)>>1, (x)|=(x)>>2, (x)|=(x)>>4,	\
										 (x)|=(x)>>8, (x)|=(x)>>16, ++(x))

static const double __ac_HASH_UPPER = 0.77;



typedef struct HashTableData
{
	vh_htbl_hash_func func_hash;
	vh_htbl_comp_func func_compare;
	MemoryContext mctx;

	size_t key_sz;
	size_t value_sz;

	int32_t *flags;
	void *keys;
	void *values;

	int32_t n_buckets;
	int32_t n_occupied;
	int32_t upper_bound;
	int32_t size;

	bool is_map;
	bool key_by_val;
} HashTableData;

static int32_t htbl_resize(HashTable htbl, int32_t new_n_buckets);
static int32_t htbl_bucket(HashTable htbl, const void *key);

static void htbl_copy(size_t sz, bool byval, const void *src, void *tgt);
#define htbl_copy_key(htbl, src, tgt)	htbl_copy((htbl)->key_sz, false, (src), (tgt))
#define htbl_copy_value(htbl, src, tgt)	htbl_copy((htbl)->value_sz, false, (src), (tgt))

#define htbl_key_at(htbl, i)		(((unsigned char*)htbl->keys) + (htbl->key_sz * i))
#define htbl_value_at(htbl, i)		(((unsigned char*)htbl->values) + (htbl->value_sz * i))


HashTable
vh_htbl_create(HashTableOpts *opts, int32_t flags)
{
	MemoryContext mctx_old;
	HashTable htbl;

	if (flags & VH_HTBL_OPT_MCTX)
	{
		mctx_old = vh_mctx_switch(opts->mctx);
		htbl = vhmalloc(sizeof(HashTableData));
		htbl->mctx = opts->mctx;
	}
	else
	{
		mctx_old = 0;
		htbl = vhmalloc(sizeof(HashTableData));
		htbl->mctx = vh_mctx_current();
	}

	if (flags & VH_HTBL_OPT_KEYSZ)
	{
		assert(opts->key_sz <= sizeof(int64_t));
		htbl->key_sz = opts->key_sz;
	}
	else
		htbl->key_sz = sizeof(void*);


	if (flags & VH_HTBL_OPT_VALUESZ)
		htbl->value_sz = opts->value_sz;
	else
		htbl->value_sz = sizeof(void*);

	
	if (flags & VH_HTBL_OPT_HASHFUNC)
	{
		htbl->func_hash = opts->func_hash;
		htbl->key_by_val = false;

		if (htbl->func_hash == vh_htbl_hash_str)
		{
			htbl->key_by_val = true;
		}
	}
	else
	{
		htbl->func_hash = vh_htbl_hash_int64;
		htbl->key_by_val = false;
	}

	
	if (flags & VH_HTBL_OPT_COMPFUNC)
		htbl->func_compare = opts->func_compare;
	else
		htbl->func_compare = vh_htbl_comp_int64;

	
	if (flags & VH_HTBL_OPT_MAP)
		htbl->is_map = opts->is_map;
	else
		htbl->is_map = true;

	/*
	 * Set all of our members required by the khash algorithm to be zero.
	 */

	htbl->n_buckets = 0;
	htbl->n_occupied = 0;
	htbl->size = 0;
	htbl->upper_bound = 0;
	htbl->flags = 0;
	htbl->keys = 0;
	htbl->values = 0;

	/*
	 * Swap back to the original memory context if needed.
	 */

	if (mctx_old)
		vh_mctx_switch(mctx_old);

	return htbl;
}

size_t
vh_htbl_count(HashTable htbl)
{
	return htbl->n_occupied;
}

MemoryContext
vh_htbl_mctx(HashTable htbl)
{
	return htbl->mctx;
}

void
vh_htbl_destroy(HashTable htbl)
{
	if (htbl)
	{
		if (htbl->keys)
			vhfree(htbl->keys);

		if (htbl->flags)
			vhfree(htbl->flags);

		if (htbl->is_map && htbl->values)
			vhfree(htbl->values);

		vhfree(htbl);
	}
}

void
vh_htbl_clear(HashTable htbl)
{
	if (htbl && htbl->flags)
	{
		memset(htbl->flags, 0xaa, __ac_fsize(htbl->n_buckets) * sizeof(int32_t));
		htbl->size = 0;
		htbl->n_occupied = 0;
	}
}

int32_t 
vh_htbl_iter(HashTable htbl, int32_t idx, void *key, void *value)
{
	int32_t i = idx;

	if (i >= htbl->n_buckets ||
		i < 0)
		return -1;

	while (__ac_iseither(htbl->flags, i) && i != htbl->n_buckets)
		i++;

	if (i == htbl->n_buckets)
		return -1;

	*((void**)key) = htbl_key_at(htbl, i);

	if (htbl->is_map && value)
		*((void**)value) = htbl_value_at(htbl, i);

	return i;
}

int32_t 
vh_htbl_iter_last(HashTable htbl, void *key, void *value)
{
	int32_t i = htbl->n_buckets - 1;

	if (i >= htbl->n_buckets ||
		i < 0)
		return -1;

	while (__ac_iseither(htbl->flags, i) && i >= 0)
		i--;

	if (i == -1)
		return -1;

	*((void**)key) = htbl_key_at(htbl, i);

	if (htbl->is_map && value)
		*((void**)value) = htbl_value_at(htbl, i);

	return i;
}

static int32_t
htbl_bucket(HashTable htbl, const void *key)
{
	int32_t k, i, last, mask, step = 0;

	if (htbl->n_buckets)
	{
		mask = htbl->n_buckets - 1;
		k = htbl->func_hash(htbl, key);
		i = k & mask;
		last = i;

		while (!__ac_isempty(htbl->flags, i) &&
			   (__ac_isdel(htbl->flags, i) ||
				!(htbl->func_compare(htbl, htbl_key_at(htbl, i), key))))
		{
			i = (i + (++step)) & mask;
			
			if (i == last)
				return htbl->n_buckets;
		}

		return __ac_iseither(htbl->flags, i) ? htbl->n_buckets : i;
	}

	return 0;
}

void*
vh_htbl_get(HashTable htbl, const void *key)
{
	int32_t bucket = htbl_bucket(htbl, key);

	if (bucket != htbl->n_buckets &&
		!__ac_iseither(htbl->flags, bucket))
	{
		if (htbl->is_map)
			return htbl_value_at(htbl, bucket);
		else
			return (void*)1;
	}

	return 0;
}

/*
 * vh_htbl_put
 *
 * Puts a key into the table and returns the value, if the HashTable is
 * also a map.  Otherwise the return value will not be a valid pointer.
 *
 * The |ret| parameter indicates the action taken.
 * 	-1	Fatal error
 * 	0	Already exists
 * 	1	Inserted succesfully
 * 	2	Previously deleted, but re-inserted
 */

void*
vh_htbl_put(HashTable htbl, 
			const void *key,  
			int32_t *ret)
{
	int32_t x, i, k, site, last, mask, step;

	if (htbl->n_occupied >= htbl->upper_bound)
	{
		if (htbl->n_occupied > (htbl->size<<1))
		{
			if (htbl_resize(htbl, htbl->n_buckets - 1) < 0)
			{
				if (ret)
					*ret = -1;

				return 0;
			}
		}
		else if (htbl_resize(htbl, htbl->n_buckets + 1) < 0)
		{
			if (ret)
				*ret = -1;

			return 0;
		}
	}

	mask = htbl->n_buckets - 1;
	step = 0;
	x = site = htbl->n_buckets;
	k = htbl->func_hash(htbl, key);
	i = k & mask;

	if (__ac_isempty(htbl->flags, i))
		x = i;
	else
	{
		last = i;
		while (!__ac_isempty(htbl->flags, i) &&
			   (__ac_isdel(htbl->flags, i) || 
				!(htbl->func_compare(htbl, htbl_key_at(htbl, i), key))))
		{
			if (__ac_isdel(htbl->flags, i))
				site = i;

			i = (i + (++step)) & mask;

			if (i == last)
			{
				x = site;
				break;
			}
		}

		if (x == htbl->n_buckets)
		{
			if (__ac_isempty(htbl->flags, i) &&
				site != htbl->n_buckets)
				x = site;
			else
				x = i;
		}
	}

	if (__ac_isempty(htbl->flags, x))
	{
		htbl_copy(htbl->key_sz, htbl->key_by_val, key, htbl_key_at(htbl, x));
	
		__ac_set_isboth_false(htbl->flags, x);
		++htbl->size;
		++htbl->n_occupied;
		
		if (ret)
			*ret = 1;
	}
	else if (__ac_isdel(htbl->flags, x))
	{
		htbl_copy(htbl->key_sz, htbl->key_by_val, key, htbl_key_at(htbl, x));

		__ac_set_isboth_false(htbl->flags, x);
		++htbl->size;

		if (ret)
			*ret = 2;
	}
	else
	{
		if (ret)
			*ret = 0;
	}

	return htbl_value_at(htbl, x);
}

void
vh_htbl_del(HashTable htbl, const void *key)
{
	int32_t bucket = htbl_bucket(htbl, key);

	if (bucket != htbl->n_buckets && !__ac_iseither(htbl->flags, bucket))
	{
		__ac_set_isdel_true(htbl->flags, bucket);
		--htbl->size;
	}
}

SList
vh_htbl_to_slist(HashTable htbl)
{
	SList list;
	int32_t i;

	list = vh_SListCreate();

	for (i = 0; i != htbl->n_buckets; i++)
	{
		if (__ac_iseither(htbl->flags, i))
			continue;

		if (htbl->is_map)
			vh_SListPush(list, htbl_key_at(htbl, i));
		else
			vh_SListPush(list, htbl_value_at(htbl, i));

	}

	return list;
}


/*
 * ============================================================================
 * Static Implementation
 * ============================================================================
 */

static int32_t 
htbl_resize(HashTable htbl, int32_t new_n_buckets)
{
	MemoryContext mctx_old;
	int32_t *new_flags = 0;
	int32_t j = 1, new_mask, k, i, step;
	void **new_keys, *new_vals;
	unsigned char *val, *key, *tmp_key, *tmp_value;

	mctx_old = vh_mctx_switch(htbl->mctx);

	kroundup32(new_n_buckets);

	if (new_n_buckets < 4)
		new_n_buckets = 4;

	if (htbl->size >= (int32_t)(new_n_buckets * __ac_HASH_UPPER + 0.5))
	{
		j = 0;
	}
	else
	{
		new_flags = vhmalloc( __ac_fsize(new_n_buckets) * sizeof(int32_t));

		if (!new_flags)
		{
			vh_mctx_switch(mctx_old);
			return -1;
		}

		memset(new_flags, 0xaa, __ac_fsize(new_n_buckets) * sizeof(int32_t));

		if (htbl->n_buckets < new_n_buckets)
		{
			if (htbl->keys)
				new_keys = vhrealloc(htbl->keys, new_n_buckets * htbl->key_sz);
			else
				new_keys = vhmalloc(new_n_buckets * htbl->key_sz);
		
			if (!new_keys)
			{
				vhfree(new_flags);
				vh_mctx_switch(mctx_old);
				return -1;
			}

			htbl->keys = new_keys;

			if (htbl->is_map)
			{
				if (htbl->values)
					new_vals = vhrealloc(htbl->values, new_n_buckets * htbl->value_sz);
				else
					new_vals = vhmalloc(new_n_buckets * htbl->value_sz);

				if (!new_vals)
				{
					vhfree(new_keys);
					vhfree(new_flags);
					vh_mctx_switch(mctx_old);
					
					return -1;
				}

				htbl->values = new_vals;
			}
		}
	}

	if (j)
	{
		key = vhmalloc(htbl->key_sz * 2);
		tmp_key = ((unsigned char*)key) + htbl->key_sz;

		if (!key)
		{
			vh_mctx_switch(mctx_old);
			return -2;
		}

		if (htbl->is_map)
			tmp_value = vhmalloc(htbl->value_sz);


		if (!tmp_value)
		{
			vh_mctx_switch(mctx_old);
			return -2;
		}

		for (j = 0; j != htbl->n_buckets; ++j)
		{
			if (__ac_iseither(htbl->flags, j) == 0)
			{
				htbl_copy_key(htbl, htbl_key_at(htbl, j), key);
				new_mask = new_n_buckets - 1;

				if (htbl->is_map)
					val = htbl_value_at(htbl, j);

				__ac_set_isdel_true(htbl->flags, j);

				while (1)
				{
					step = 0;

					if (htbl->key_by_val)
						k = htbl->func_hash(htbl, *((const char**)key));
					else
						k = htbl->func_hash(htbl, key);
					
					i = k & new_mask;

					while (!__ac_isempty(new_flags, i))
						i = (i + (++step)) & new_mask;

					__ac_set_isempty_false(new_flags, i);

					if (i < htbl->n_buckets && __ac_iseither(htbl->flags, i) == 0)
					{
						{				
							htbl_copy_key(htbl, htbl_key_at(htbl, i), tmp_key);
							htbl_copy_key(htbl, key, htbl_key_at(htbl, i));
							htbl_copy_key(htbl, tmp_key, key);
						}

						if (htbl->is_map)
						{
							htbl_copy_value(htbl, htbl_value_at(htbl, i), tmp_value);
							htbl_copy_value(htbl, val, htbl_value_at(htbl, i));
							htbl_copy_value(htbl, tmp_value, val);
						}
						
						__ac_set_isdel_true(htbl->flags, i);
					}
					else
					{
						htbl_copy_key(htbl, key, htbl_key_at(htbl, i));

						if (htbl->is_map)
							htbl_copy_value(htbl, val, htbl_value_at(htbl, i));

						break;
					}
				}
			}
		}
		
		vhfree(key);

		if (htbl->is_map)
			vhfree(tmp_value);

		/*
		 * Test if we need to shrink
		 */
		if (htbl->n_buckets > new_n_buckets)
		{
			htbl->keys = vhrealloc(htbl->keys, new_n_buckets * htbl->key_sz);

			if (htbl->is_map)
				htbl->values = vhrealloc(htbl->values, new_n_buckets * htbl->value_sz);
		}

		if (htbl->flags)
			vhfree(htbl->flags);

		htbl->flags = new_flags;
		htbl->n_buckets = new_n_buckets;
		htbl->n_occupied = htbl->size;
		htbl->upper_bound = (int32_t)(htbl->n_buckets * __ac_HASH_UPPER + 0.5);
	}

	vh_mctx_switch(mctx_old);

	return 0;
}


/*
 * Hash Key Functions
 */

int32_t
vh_htbl_hash_int16(HashTable htbl, const void *key)
{
	int16_t k = *((int16_t*)key);

	return (int32_t) k;
}

int32_t
vh_htbl_comp_int16(HashTable htbl, const void *lhs, const void *rhs)
{
	return ((*((int16_t*)lhs) == *((int16_t*)rhs)));
}

int32_t
vh_htbl_hash_int32(HashTable htbl, const void *key)
{
	return *((int32_t*)key);
}

int32_t
vh_htbl_comp_int32(HashTable htbl, const void *lhs, const void *rhs)
{
	return ((*((int32_t*)lhs) == *((int32_t*)rhs)));
}

int32_t
vh_htbl_hash_int64(HashTable htbl, const void *key)
{
	int64_t k = *((int64_t*)key);	
	return (int32_t)(k>>33^k^k<<11);
}

int32_t
vh_htbl_comp_int64(HashTable htbl, const void *lhs, const void *rhs)
{
	return ((*((int64_t*)lhs) == *((int64_t*)rhs)));
}

int32_t
vh_htbl_hash_str(HashTable htbl, const void *key)
{
	const char *str = key;
	int32_t h = *str;

	if (h)
	{
		for (++str; *str; ++str)
			h = (h << 5) - h + (int32_t)*str;
	}

	return h;
}

int32_t
vh_htbl_comp_str(HashTable htbl, const void *lhs, const void *rhs)
{
	const char * const *str_lhs = lhs;
	const char *str_rhs = rhs;

	if (*str_lhs)
		return strcmp(*str_lhs, str_rhs) == 0;

	return 0;
}

int32_t
vh_htbl_comp_bin(HashTable htbl, const void *lhs, const void *rhs)
{
	if (lhs)
		return memcmp(lhs, rhs, htbl->key_sz);

	return 0;
}

int32_t
vh_htbl_hash_bin(HashTable htbl, const void *key)
{
	const char *str = key;
	int32_t h = *str, i;

	if (h)
	{
		for (i = 0; i < htbl->key_sz; i++)
			h = (h << 5) - h + (int32_t*)str[i];
	}

	return h;
}

/*
 * htbl_copy
 *
 * We want to avoid using memcpy when possible, so if the size of the value to
 * be copied is less than or equal to eight (8) bytes then we'll use a union
 * to assign the value rather than doing a memcpy.
 */
static void 
htbl_copy(size_t sz, bool by_val, const void *source, void *target)
{
	union { char a; int16_t b; int32_t c; int64_t d; } const *src = source;
   	union { char a; int16_t b; int32_t c; int64_t d; } *tgt = target;

	if (by_val)
	{
		switch (sz)
		{
			case 1:
				tgt->a = (const int8_t)((const uintptr_t)source);
				break;

			case 2:
				tgt->b = (const int16_t)((const uintptr_t)source);
				break;

			case 4:
				tgt->c = (const int32_t)((const uintptr_t)source);
				break;
			
			case 8:
#if VHB_SIZEOF_VOID == 8
				tgt->d = (const int64_t)source;
				break;
#else
				elog(ERROR2,
						emsg("Unable to process an 8 byte by value HashTable "
							 "copy.  sizeof(void*) is not 8 bytes or greater!"));
				return;
#endif
		}
	}
	else
	{
		switch (sz)
		{
			case 1:
				tgt->a = src->a;
				break;

			case 2:
				tgt->b = src->b;
				break;

			case 4:
				tgt->c = src->c;
				break;

			case 8:
				tgt->d = src->d;
				break;

			default:
				memcpy(target, source, sz);
				break;
		}
	}
}

void
vh_htbl_iterate_set(HashTable htbl, vh_htbl_iterate_set_cb cb, void *data)
{
	int32_t i;
	bool cb_ret;

	for (i = 0; i != htbl->n_buckets; i++)
	{
		if (__ac_iseither(htbl->flags, i))
			continue;

		cb_ret = cb(htbl, htbl_key_at(htbl, i), data);

		if (!cb_ret)
			break;
	}
}

void
vh_htbl_iterate_map(HashTable htbl, vh_htbl_iterate_map_cb cb, void *data)
{
	int32_t i;
	bool cb_ret;

	for (i = 0; i != htbl->n_buckets; i++)
	{
		if (__ac_iseither(htbl->flags, i))
			continue;

		cb_ret = cb(htbl, htbl_key_at(htbl, i), htbl_value_at(htbl, i), data);

		if (!cb_ret)
			break;
	}
}

