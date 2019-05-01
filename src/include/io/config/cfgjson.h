/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_CONFIG_CFGJSON_H
#define VH_CONFIG_CFGJSON_H

/*
 * JSON Configuration
 *
 * Configuration reads a JSON file to setup the different operations available to
 * the VH subsystems.
 *
 * The calling convention typically has two parameters, a pointer to the object
 * being created or manipulated, the JSON definition, a check-only boolean.
 *
 * The return value is an int32_t and will return negative if the call to the 
 * subsystem failed, positive if the JSON isn't correct or complete and zero
 * if everything is OK.
 */

#include "vh.h"
#include "io/catalog/types/njson.h"

#define VH_CFGJ_FLAG_CHECK				0x01
#define VH_CFGJ_FLAG_WARN				0x10
#define VH_CFGJ_FLAG_ERROR1				0x20
#define VH_CFGJ_FLAG_ERROR2				0x40

#define VH_CFGJ_MASK_THROW				0xf0


/*
 * ============================================================================
 * Analytics Configurations
 * ============================================================================
 */
typedef struct NestData *Nest;
typedef struct NestLevelData *NestLevel;

int32_t vh_cfgj_nest(Nest *nest, Json jval, int32_t flags);
int32_t vh_cfgj_nl(NestLevel *nl, Json jval, int32_t flags);



/* 
 * ============================================================================
 * Catalog Configurations 
 * ============================================================================
 */
typedef struct PrepColData *PrepCol;
typedef struct SearchPathData *SearchPath;

int32_t vh_cfgj_acm(void (**acm)(void), Json jval, int32_t flags);
int32_t vh_cfgj_pc(PrepCol *pc, Json jval, int32_t flags);
int32_t vh_cfgj_sp(SearchPath *sp, Json jval, int32_t flags);



/*
 * Helper Functions
 *
 * Intended to be called internally from the config module.
 */
struct ConfigJsonMethodTable
{
	const char *method_name;
	void (*method)(void);
	int32_t category;
};

#define vh_cfgj_methods(meth)	(sizeof((meth)) / sizeof(struct ConfigJsonMethodTable))

bool vh_cfgj_method_lookup(const struct ConfigJsonMethodTable *table, size_t sz,
						   const char* method_name,
						   const struct ConfigJsonMethodTable **out);

#define vh_cfgj_check_only(flags)		((flags) & VH_CFGJ_FLAG_CHECK)
#define vh_cfgj_throw_error(flags, ret, message, args...)						\
			do 																	\
			{																	\
				switch ((flags) & VH_CFGJ_MASK_THROW)							\
				{																\
					case VH_CFGJ_FLAG_ERROR1:									\
						elog(ERROR1, emsg((message), args));					\
						break;													\
					case VH_CFGJ_FLAG_ERROR2:									\
						elog(ERROR2, emsg((message), args));					\
						break;													\
				}																\
				if ((ret))														\
					return (ret);												\
			} while (0)

#define vh_cfgj_throw_warning(flags, message, args...)							\
			do 																	\
			{																	\
				if ((flags) & VH_CFGJ_FLAG_WARN)								\
				{																\
					elog(WARNING, emsg((message), args));						\
				}																\
			} while (0)

#endif

