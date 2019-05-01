/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_catalog_searchpath_h
#define vh_catalog_searchpath_h

#include <stdarg.h>

/*
 * SearchPath
 *
 * We use a generic abstraction for a search path to find zero, one or more
 * data points of any type.
 *
 * Search Paths may find tables in a particular catalog or across a set of 
 * catalogs.  It may also formulate TypeVarSlots based on a variety of 
 * conditions.  The abstraction is fairly simple.  We have initialization,
 * finalization, and a search routine that must be implemented.
 *
 * SearchPaths may have run time arguments passed to their search function.
 *
 */

typedef struct SearchPathData *SearchPath;
typedef const struct SearchPathFuncTableData *SearchPathFuncTable;

struct SearchPathFuncTableData
{
	void* (*search)(SearchPath sp, int32_t *ret, int32_t nrt_args, ...);
	void* (*next)(SearchPath sp, int32_t *ret);
	int32_t (*reset)(SearchPath sp);

	void (*finalize)(SearchPath sp);
};

typedef enum
{
	SPRET_DataAt,			/* Raw Data Pointer */
	SPRET_TableDef,			/* TableDef */
	SPRET_TableDefVer,		/* TableDefVer */
	SPRET_TableField		/* TableField */
} SPRET;
 

struct SearchPathData
{
	SearchPathFuncTable funcs;
	SPRET spret;
	int32_t verbosity;
};

#define vh_sp_isa(sp, sprt)				((sp)->spret == (sprt))
#define vh_sp_isa_dataat(sp)			vh_sp_isa(sp, SPRET_DataAt)
#define vh_sp_isa_tabledef(sp)			vh_sp_isa(sp, SPRET_TableDef)
#define vh_sp_isa_tabledefver(sp)		vh_sp_isa(sp, SPRET_TableDefVer)
#define vh_sp_isa_tablefield(sp)		vh_sp_isa(sp, SPRET_TableField)


void* vh_sp_create(SearchPathFuncTable funcs, SPRET spret, size_t sz);
void vh_sp_finalize(SearchPath sp);
void vh_sp_destroy(SearchPath sp);

void vh_sp_reset(SearchPath sp);

/*
 * ============================================================================
 * vh_sp_search
 * 
 * The caller to vh_sp_search may pass "context" to the SearchPath implementation
 * via zero, one, or more varadiac arguments.  Each argument is preceeded by a 
 * 32 bit integer which identifies the underlying type being passed in as context.
 *
 * Each individual SearchPath knows how to preference the values passed in as 
 * context versus those values that are provided when the SearchPath is given.
 *
 * As a result, callers should know ahead of time what the SearchPath is returning
 * to minimize the runtime arguments that should be passed.  For many SearchPath
 * implementations, most of the available context arguments are thrown away.
 *
 * Possible @ret values:
 * 		-1		Nothing found
 * 		-2		Insufficient context
 * 		-3		Invalid iterator
 * 		0		1 value found, nothing else remains
 * 		>0		1 value found, @ret values remain
 *
 */

#define VH_SP_CTX_HT						(0x0100)		/* HeapTuple */
#define VH_SP_CTX_HTP						(0x0200)		/* HeapTuplePtr (mutable) */

#define VH_SP_CTX_TD						(0x0300)		/* TableDef */
#define VH_SP_CTX_TDV						(0x0400)		/* TableDefVer */

#define VH_SP_CTX_FNAME						(0x0500)		/* Field Name */
#define VH_SP_CTX_FIDX						(0x0600)		/* Field Index */
#define VH_SP_CTX_TVS						(0x0700)		/* TypeVarSlot */

#define VH_SP_CTX_TC						(0x0800)		/* TableCatalog */
#define VH_SP_CTX_SNAME						(0x0900)		/* Schema Name */
#define VH_SP_CTX_TNAME						(0x1000)		/* Table Name */

#define VH_SP_CTX_NESTLEVEL					(0x2000)		/* NestLevel */

#define vh_sp_search(sp, ret, nrt_args, ...) 	((sp) ? 						\
												 (sp)->funcs->search((sp),		\
																	 (ret), 	\
													 				 (nrt_args) * 2,\
																	 ## __VA_ARGS__)\
													 : 0)

int32_t vh_sp_search_dflt_ctx(SearchPath sp, void **output);


/* Utility Functions for SearchPath Implementations */
void vh_sp_pull_unk_arg(int32_t argt, va_list args);

#endif

