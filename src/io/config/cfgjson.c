/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>

#include "io/config/cfgjson.h"


static int32_t cfgj_method_cmp(const void *pkey, const void *pelem);

/*
 * ============================================================================
 * Public Interface
 * ============================================================================
 */

/*
 * vh_cfgj_method_lookup
 *
 * Looksup a method using binary search.  Returns true if it was found and false
 * if the table did not contain it.
 */
bool
vh_cfgj_method_lookup(const struct ConfigJsonMethodTable *table,
					  size_t n_elements,
					  const char *method_name,
					  const struct ConfigJsonMethodTable **out)
{
	const struct ConfigJsonMethodTable *found;

	found = bsearch(method_name, table, n_elements,
					sizeof(struct ConfigJsonMethodTable),
					cfgj_method_cmp);

	if (found)
	{
		*out = found;
		return true;
	}

	return false;
}



/*
 * ============================================================================
 * Private Functions
 * ============================================================================
 */

/*
 * cfgj_method_cmp
 *
 * We need a custom comparator function to accept a const char ** as the RHS.
 *
 * http://stackoverflow.com/questions/15824966/trouble-using-bsearch-with-an-array-of-strings
 */
static int32_t 
cfgj_method_cmp(const void *pkey, const void *pelem)
{
	const char *lhs = pkey;
	const char * const *rhs = pelem;

	return strcmp(lhs, *rhs);
}

