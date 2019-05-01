/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_io_catalog_PrintTup_H
#define vh_io_catalog_PrintTup_H

/*
 * Utility for printing certain fields of a HeapTuple.  We use the CStr TAM for
 * now.  There's nothing that says in the future vh_ptup_create takes a TAM
 * specifier to customize how the results come back.  That would require us
 * to change the calling conventions of vh_ptup_field and vh_ptup_field_buffer.
 */

typedef struct PrintTupCtxData *PrintTupCtx;


PrintTupCtx vh_ptup_create(HeapTupleDef htd);
void vh_ptup_finalize(PrintTupCtx ptup);

void vh_ptup_add_field(PrintTupCtx ptup, HeapField hf, const char *format);

char* vh_ptup_field(PrintTupCtx ptup, 
  					HeapTuplePtr htp, HeapField hf, 
  					size_t *length);
size_t vh_ptup_field_buffer(PrintTupCtx ptup,
							HeapTuplePtr htp, HeapField hf,
	  						char *buffer, size_t buffer_capacity);

void vh_ptup_iterate(PrintTupCtx ptup,
					 void *iter_data,
					 HeapTuplePtr (*iter_htp)(void*),
					 bool (*iter_next_cb)(void*),
					 bool (*cstr_recv)(HeapTuplePtr, HeapField, char *fval, bool more));

#endif

