/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "vh.h"

#include "test.h"



CatalogContext ctx_catalog = 0;

static void test_memorycontext(void);
static void sigfault_handler(int);


int main(int argc, char** argv)
{
	signal(SIGSEGV, sigfault_handler);

	ctx_catalog = vh_start();


	test_memorycontext();
	test_typevar_entry();
	test_typevaracm_entry();
	//test_hashtable();
	test_types();
	test_operators();
	test_new_json_entry();
	test_sp_entry();
	test_pc_entry();
	test_query();
	test_be_sqlite3();	
	//test_grdb_entry();
	
	test_bt_entry();
	test_nest_entry();

	test_config_entry();

	test_ml_entry();

	vh_shutdown();

	exit(0);
}

static void test_memorycontext(void)
{
	MemoryContext mctx, old;
	char *ptr2;

	mctx = vh_MemoryPoolCreate(vh_mctx_current(),
							   8192,
							   "test memory context");
	old = vh_mctx_switch(mctx);

	vhmalloc(8192*5);
	ptr2 = vhmalloc(8192*6);
	vhmalloc(8192*8);
	vhfree(ptr2);

	vh_mctx_switch(old);
	vh_mctx_destroy(mctx);	
}

static void sigfault_handler(int sig)
{
	void *array[20];
	size_t size;

	size = backtrace(array, 20);
	fprintf(stderr, "Error, signal %d received.  Backtrace below.\n", sig);
	backtrace_symbols_fd(array, size, STDERR_FILENO);

	exit(1);
}

