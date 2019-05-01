/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef VH_ANALYTICS_ML_NORMALIZE_H
#define VH_ANALYTICS_ML_NORMALIZE_H

/*
 * Helper structure to normalize a data set.  Basically we run a set of 
 * HeapTuplePtr thru to set the boundaries.  From that information, we can
 * establish a PrepTup that will normalize all inbound HTP.
 *
 *
 * 1)	vh_mln_create
 * 2)	vh_mln_add_col
 * 3)	vh_mln_input_htp
 * 4)	vh_mln_generate_preptup
 */

typedef struct MLNormalizeData *MLNormalize;

MLNormalize vh_mln_create(void);
void vh_mln_destroy(MLNormalize mln);

#define VH_MLN_FUNC_NONE		0x01
#define VH_MLN_FUNC_MINMAX 		0x02

void vh_mln_add_col(MLNormalize mln, const char *col_name, int32_t function);
int32_t vh_mln_input_htp(MLNormalize mln, HeapTuplePtr htp);
int32_t vh_mln_generate_preptup(MLNormalize mln, HeapBufferNo hbno, PrepTup *pt_out);


#endif

