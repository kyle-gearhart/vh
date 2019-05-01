/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_catalog_PrepTup_H
#define vh_catalog_PrepTup_H


typedef struct PrepTupData *PrepTup;
typedef struct PrepTupStateData *PrepTupState;


/*
 * PrepTup are formed by creating columns.  Consider these the output columns.
 * PrepTup will automatically build out a HeapTupleDef based on the columns
 * required.
 *
 * 	1)	For each column
 * 		a)	Provide a target name
 * 		b)	Provide a transfer method
 * 			i)	PrepCol
 * 				x)	For each @datas argument of the PrepCol, you must provide two
 * 					SearchPathCtx, one that provides the HeapTuple and another
 * 					that provides the HeapField argument.
 *
 * 					If the PrepCol requires no @datas arguments, then a null
 * 					array is acceptable.
 * 			ii)	CopyCol
 * 				x)	You may allow the data to be simply copied from the source
 * 					to target.
 */

typedef struct PrepTupColData *PrepTupCol;
typedef struct PrepTupData *PrepTup;


PrepTup vh_pt_create(HeapBufferNo hbno);
void vh_pt_destroy(PrepTup pt);

/*
 * vh_pt_col_add
 *
 * These are executed in the order they are added.  The first one added goes
 * first.  The @chain parameter indicates the value should use the original
 * value from the inbound HeapTuplePtr.  When it's true, we'll use the chained
 * value that has been promoted thus far.
 */
int32_t vh_pt_col_add(PrepTup pt, char *target_column,
					  SearchPath *paths, bool *chain,
					  int32_t n_paths,
					  PrepCol pc);

int32_t vh_pt_input_htp(PrepTup pt, 
						HeapTuplePtr htp_in, HeapTuple ht_in,
						HeapTuplePtr *htp_out, HeapTuple *ht_out);
						

#endif

