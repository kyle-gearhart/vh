/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_jobs_directives_common_H
#define vh_jobs_directives_common_H

#include "vh.h"
#include "jobs/directive.h"

struct DirHeapFieldChange
{
	struct DirIns dirins;

	HeapTupleDef htd;
	HeapField hf;
};

struct DirHeapFieldCopy
{
	struct DirIns dirins;

	HeapTupleDef htd_src;
	HeapField hf_src;

	HeapTupleDef htd_tgt;
	HeapField hf_src;
};

struct DirDateStrFormat
{
	struct DirHeapFieldChange dhfchg;

	const char *format;
	char buffer[255];
};

struct DirDateCopyFormat
{
	struct DirHeapFieldCopy dhfcpy;

	const char *format;

	/*
	 * Based on the fields being transferred, we'll want to build TAM
	 * stack once.  Then we can just call the function pointer
	 * directly.
	 */
	struct TamCharGetStack get_char;

	char buffer[255];
};

#endif

