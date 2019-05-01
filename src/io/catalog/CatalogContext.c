/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <uv.h>

#include "vh.h"
#include "io/buffer/BuffMgr.h"
#include "io/catalog/BackEndCatalog.h"
#include "io/catalog/TableCatalog.h"
#include "io/catalog/CatalogContext.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeCatalog.h"
#include "io/executor/xact.h"
#include "io/shard/BeaconCatalog.h"
#include "io/shard/ConnectionCatalog.h"


static bool AttachedInstance = false;
static uv_key_t UvInstance;

static void register_standard_types(void);


CatalogContext 
vh_start(void)
{
	MemoryContext top;
	CatalogContext context;

	vh_ctx_attach(0);

	top = vh_MemoryPoolCreate(0, 1024, "Top level memory context");
	context = (CatalogContext)vh_mctx_alloc(top, sizeof(CatalogContextData));

	vh_ctx_init(context);

	context->memoryCurrent = top;
	context->memoryTop = top;

	/*
	 * Reset the catalog context here in case the TableCatalog constructor
	 * attempts to get the current context to allocate for itself
	 */
	vh_ctx_attach(context);
	
	context->errorQueue = vh_err_queue_alloc(top, 1);

	/* Log all errors to the console */
	vh_err_queue_console(context->errorQueue, 0);

	context->catalogBackEnd = vh_cat_be_create();
	context->catalogBeacon = vh_BeaconCatalogCreate();
	context->catalogConnection = vh_ConnectionCatalogCreate();
	context->catalogTable = vh_cat_tbl_create("primary");

	/*
	 * Start the HeapBuffer Manager up.
	 */

	vh_hbmgr_startup(top);
	context->hbno_general = vh_hb_open(top);

	/*
	 * Get the type system rolling
	 */
	register_standard_types();

	return context;
}

void
vh_shutdown(void)
{
	CatalogContext cc = 0;

	if (AttachedInstance)
	{
		cc = vh_ctx();

		if (cc)
		{
			vh_ctx_destroy(cc);
			AttachedInstance = false;
			vh_ctx_attach(0);
		}
	}
}

static void
register_standard_types(void)
{
	vh_type_add(&vh_type_Array);
	vh_type_add(&vh_type_bool);
	vh_type_add(&vh_type_Date);
	vh_type_add(&vh_type_DateTime);
	vh_type_add(&vh_type_int16);
	vh_type_add(&vh_type_int32);
	vh_type_add(&vh_type_int64);
	vh_type_add(&vh_type_Range);
	vh_type_add(&vh_type_String);
	vh_type_add(&vh_type_dbl);
	vh_type_add(&vh_type_float);
	vh_type_add(&vh_type_numeric);
}



void 
vh_ctx_attach(CatalogContext context)
{
	if (!AttachedInstance)
	{
		uv_key_create(&UvInstance);
		AttachedInstance = true;
	}

	if (context)
	{
		uv_key_set(&UvInstance, context);
	}
}

CatalogContext 
vh_ctx(void)
{
	return (CatalogContext)uv_key_get(&UvInstance);
}

/*
 * Sets the default values to the context
 */
void 
vh_ctx_init(CatalogContext context)
{
	if (context)
	{
		context->memoryTop = 0;
		context->memoryCurrent = 0;
		context->catalogBeacon = 0;
		context->catalogConnection = 0;
		context->catalogTable = 0;
		context->xactCurrent = 0;
		context->xactTop = 0;
	}
}

void
vh_ctx_destroy(CatalogContext cc)
{
	if (cc->xactCurrent)
		vh_xact_destroy(cc->xactCurrent);

	if (cc->catalogConnection)
		vh_ConnectionCatalogShutDown(cc->catalogConnection);

	if (cc->hbno_general)
		vh_hb_close(cc->hbno_general);
}

