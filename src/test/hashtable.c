/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <locale.h>
#include <stdio.h>

#include "vh.h"
#include "io/utils/htbl.h"
#include "io/utils/stopwatch.h"

HashTable htbl_set = 0;
HashTable htbl_map = 0;

static void set_int32(void);
static void set_int64(void);

void test_hashtable(void)
{
	setlocale(LC_NUMERIC, "");
	printf("\n########################################################################"
		   "\nENTERING HASH TABLE TESTS"
		   "\n########################################################################"
		   "\n");

	set_int32();
	set_int64();	
	
	printf("\n#######################################################################"
		   "\nEXITING HASH TABLE TESTS"
		   "\n#######################################################################"
		   "\n\n");
}

/*
 * Test a set with a 4 byte key
 */
static void
set_int32(void)
{
	static const int32_t loop_size = 10000;
	HashTableOpts opts;
	void *get;
	int32_t put_ret, i, j, r, *keys;
	bool quit, found;
	struct vh_stopwatch watch;

	opts.key_sz = sizeof(int32_t);
	opts.func_hash = vh_htbl_hash_int32;
	opts.func_compare = vh_htbl_comp_int32;
	opts.is_map = false;

	htbl_set = vh_htbl_create(&opts, 
							  VH_HTBL_OPT_KEYSZ |
							  VH_HTBL_OPT_HASHFUNC |
							  VH_HTBL_OPT_COMPFUNC |
							  VH_HTBL_OPT_MAP);

	get = vh_htbl_get(htbl_set, (void*)582124);
	assert(!get);

	get = vh_htbl_get(htbl_set, (void*)0);
	assert(!get);

	vh_htbl_put(htbl_set, (void*)582124, &put_ret);
	assert(put_ret == 1);

	vh_htbl_put(htbl_set, (void*)582124, &put_ret);
	assert(put_ret == 0);

	get = vh_htbl_get(htbl_set, (void*)582124);
	assert(get);

	vh_htbl_del(htbl_set, (void*)582124);
	
	get = vh_htbl_get(htbl_set, (void*)582124);
	assert(!get);

	vh_htbl_put(htbl_set, (void*)582124, &put_ret);
	assert(put_ret == 2);

	get = vh_htbl_get(htbl_set, (void*)582124);
	assert(get);

	keys = vhmalloc(sizeof(int32_t) * loop_size);
	for (i = 0; i < loop_size; i++)
	{
		keys[i] = rand();

		quit = false;
		vh_htbl_put(htbl_set, (void*)((uintptr_t)keys[i]), &put_ret);

		switch (put_ret)
		{
		case -1:
			quit = true;
			break;

		case 0:
			found = false;
			
			for (j = 0; j < i; j++)
			{
				if (keys[j] == keys[i])
				{
					found = true;
					break;
				}
			}

			//if (!found)
			//	printf("\nUnable to find key %d in the previous list, even though the "
			//		   "HashTable htbl_set indicates the key exists on the put action!",
			//		   keys[i]);

			assert(found);
			break;

		case 2:

			if (keys[i] % 3 == 0)
			{
				found = false;

				for (j = 0; j < i; j++)
				{
					if (keys[j] == keys[i])
					{
						found = true;
						break;
					}
				}

				//if (!found)
				//	printf("\nThe HashTable indicated that the key %d was previously deleted "
				//		   "however a previous entry for the key in htbl_set was not found!",
				//		   keys[i]);

				//assert(found);
			}
			else
			{
				//printf("\nThe HashTable indicated that the key %d was previously deleted "
				//	   "but reinserted.  Condition for deletion not met for the htbl_set!",
				//	   keys[i]);
				//assert(keys[i] % 3 == 0);
			}

			break;
		}

		if (quit)
		{
			printf("\nFatal error with the HashTable");
			break;
		}

		get = vh_htbl_get(htbl_set, (void*)((uintptr_t)keys[i]));
		assert(get);

		if (keys[i] % 3 == 0)
		{
			vh_htbl_del(htbl_set, (void*)((uintptr_t)keys[i]));

			get = vh_htbl_get(htbl_set, (void*)((uintptr_t)keys[i]));
			assert(!get);
		}
	}

	/*
	 * Generate random keys and see if they are in the htbl_set.  If htbl_set
	 * indicate it exists, check our local array for the key.  Throw if the key
	 * could not be found in the local set.
	 */

	printf("\nChecking %'d random values against local set with int32 key...", 1000000);
	vh_stopwatch_start(&watch);
	for (i = 0; i < 1000000; i++)
	{
		r = rand();

		get = vh_htbl_get(htbl_set, (void*)((uintptr_t)r));

		if (get)
		{
			found = false;

			for (j = 0; j < loop_size; j++)
			{
				if (keys[j] == r)
				{
					found = true;
					break;
				}
			}

			assert(found);
		}
	}
	vh_stopwatch_end(&watch);
	printf("complete in %'ld ms", vh_stopwatch_ms(&watch));

	/*
	 * Measure pure lookup performance, this is different from above because
	 * we don't check the value against the working set.
	 */
	printf("\nMeasuring vh_htbl_get performance with int32 key for %'d entries...", 1000000);
	vh_stopwatch_start(&watch);
	for (i = 0; i < 1000000; i++)
	{
		r = rand();
		get = vh_htbl_get(htbl_set, (void*)((uintptr_t)r));
	}
	vh_stopwatch_end(&watch);
	printf("complete in %'ld ms", vh_stopwatch_ms(&watch));

	vhfree(keys);
	printf("\nHashTable htbl_set with 4 byte integer key tested succesfully!");
}

/*
 * Test a set with a 8 byte key
 */
static void
set_int64(void)
{
	static const int32_t loop_size = 10000;
	HashTableOpts opts;
	void *get;
	int32_t put_ret, i, j, r;
	int64_t *keys;
	bool quit, found;
	struct vh_stopwatch watch;

	opts.key_sz = sizeof(int64_t);
	opts.func_hash = vh_htbl_hash_int64;
	opts.func_compare = vh_htbl_comp_int64;
	opts.is_map = false;

	htbl_set = vh_htbl_create(&opts, 
							  VH_HTBL_OPT_KEYSZ |
							  VH_HTBL_OPT_HASHFUNC |
							  VH_HTBL_OPT_COMPFUNC |
							  VH_HTBL_OPT_MAP);


	keys = vhmalloc(sizeof(int64_t) * loop_size);
	for (i = 0; i < loop_size; i++)
	{
		keys[i] = rand() + rand();

		quit = false;
		vh_htbl_put(htbl_set, (void*)keys[i], &put_ret);

		switch (put_ret)
		{
		case -1:
			quit = true;
			break;

		case 0:
			found = false;
			
			for (j = 0; j < i; j++)
			{
				if (keys[j] == keys[i])
				{
					found = true;
					break;
				}
			}

			//if (!found)
			//	printf("\nUnable to find key %d in the previous list, even though the "
			//		   "HashTable htbl_set indicates the key exists on the put action!",
			//		   keys[i]);

			assert(found);
			break;

		case 2:

			if (keys[i] % 3 == 0)
			{
				found = false;

				for (j = 0; j < i; j++)
				{
					if (keys[j] == keys[i])
					{
						found = true;
						break;
					}
				}

				//if (!found)
				//	printf("\nThe HashTable indicated that the key %d was previously deleted "
				//		   "however a previous entry for the key in htbl_set was not found!",
				//		   keys[i]);

				//assert(found);
			}
			else
			{
				//printf("\nThe HashTable indicated that the key %d was previously deleted "
				//	   "but reinserted.  Condition for deletion not met for the htbl_set!",
				//	   keys[i]);
				//assert(keys[i] % 3 == 0);
			}

			break;
		}

		if (quit)
		{
			printf("\nFatal error with the HashTable");
			break;
		}

		get = vh_htbl_get(htbl_set, (void*)keys[i]);
		assert(get);

		if (keys[i] % 3 == 0)
		{
			vh_htbl_del(htbl_set, (void*)keys[i]);

			get = vh_htbl_get(htbl_set, (void*)keys[i]);
			assert(!get);
		}
	}

	/*
	 * Generate random keys and see if they are in the htbl_set.  If htbl_set
	 * indicate it exists, check our local array for the key.  Throw if the key
	 * could not be found in the local set.
	 */

	printf("\nChecking %'d random values against local set for int64 key...", 1000000);
	vh_stopwatch_start(&watch);
	for (i = 0; i < 1000000; i++)
	{
		r = rand();

		get = vh_htbl_get(htbl_set, (void*)((uintptr_t)r));

		if (get)
		{
			found = false;

			for (j = 0; j < loop_size; j++)
			{
				if (keys[j] == r)
				{
					found = true;
					break;
				}
			}

			assert(found);
		}
	}
	vh_stopwatch_end(&watch);
	printf("complete in %'ld ms", vh_stopwatch_ms(&watch));

	/*
	 * Measure pure lookup performance, this is different from above because
	 * we don't check the value against the working set.
	 */
	printf("\nMeasuring vh_htbl_get performance with int64 key for %'d entries...", 1000000);
	vh_stopwatch_start(&watch);
	for (i = 0; i < 1000000; i++)
	{
		r = rand();
		get = vh_htbl_get(htbl_set, (void*)((uintptr_t)r));
	}
	vh_stopwatch_end(&watch);
	printf("complete in %'ld ms", vh_stopwatch_ms(&watch));

	vhfree(keys);
	printf("\nHashTable htbl_set with 8 byte integer key tested succesfully!");
}

