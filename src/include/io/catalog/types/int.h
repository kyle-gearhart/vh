/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_catalog_types_int_H
#define vh_catalog_types_int_H

/*
 * Provide some basic integer type definitions as other types will likely rely
 * on these functions.  We should every try to directly touch these.  Rather,
 * they should be set on one or more TypeData TAM method tables.  Then the TAM
 * interface will call thru function pointers.
 */


/*
 * 1 byte integer support
 */

void* vh_ty_int8_tam_bin_get(struct TamBinGetStack *tamstack, 
							  const BinaryAMOptions bopts,
							  const void *source, void *target,
							  size_t *length, size_t *cursor);

void* vh_ty_int8_tam_bin_set(struct TamBinSetStack *tamstack, 
							  const BinaryAMOptions bopts,
			  				  const void *source, void *target,
			  				  size_t length, size_t cursor);

size_t vh_ty_int8_tam_bin_len(Type type, const void *source);

void vh_ty_int8_tam_memset_get(struct TamGenStack *tamstack, 
								void *source, void *target);
void vh_ty_int8_tam_memset_set(struct TamGenStack *tamstack, 
								void *source, void *target);

int32_t vh_ty_int8_tom_comparison(struct TomCompStack *tamstack,
								   const void *lhs, const void *rhs);


/*
 * 2 byte integer, Int16 Operations
 */
void* vh_ty_int16_tam_bin_get(struct TamBinGetStack *tamstack, 
							  const BinaryAMOptions bopts,
							  const void *source, void *target,
							  size_t *length, size_t *cursor);

void* vh_ty_int16_tam_bin_set(struct TamBinSetStack *tamstack, 
							  const BinaryAMOptions bopts,
			  				  const void *source, void *target,
			  				  size_t length, size_t cursor);

size_t vh_ty_int16_tam_bin_len(Type type, const void *source);

void vh_ty_int16_tam_memset_get(struct TamGenStack *tamstack, 
								void *source, void *target);
void vh_ty_int16_tam_memset_set(struct TamGenStack *tamstack, 
								void *source, void *target);

int32_t vh_ty_int16_tom_comparison(struct TomCompStack *tamstack,
								   const void *lhs, const void *rhs);


extern int32_t vh_ty_int16_pl_int16(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int16_sub_int16(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_mul_int16(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_div_int16(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_pl_int8(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int16_sub_int8(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_mul_int8(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_div_int8(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_pl_int32(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int16_sub_int32(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_mul_int32(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_div_int32(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_pl_int64(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int16_sub_int64(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_mul_int64(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int16_div_int64(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);



/*
 * Int32 Operations
 */
void* vh_ty_int32_tam_bin_get(struct TamBinGetStack *tamstack, 
							  const BinaryAMOptions bopts,
							  const void *source, void *target,
							  size_t *length, size_t *cursor);

void* vh_ty_int32_tam_bin_set(struct TamBinSetStack *tamstack, 
							  const BinaryAMOptions bopts,
			  				  const void *source, void *target,
			  				  size_t length, size_t cursor);

size_t vh_ty_int32_tam_bin_len(Type type, const void *source);

void vh_ty_int32_tam_memset_get(struct TamGenStack *tamstack, 
								void *source, void *target);
void vh_ty_int32_tam_memset_set(struct TamGenStack *tamstack, 
								void *source, void *target);

int32_t vh_ty_int32_tom_comparison(struct TomCompStack *tamstack,
								   const void *lhs, const void *rhs);


extern int32_t vh_ty_int32_pl_int32(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int32_sub_int32(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_mul_int32(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_div_int32(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_pl_int8(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int32_sub_int8(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_mul_int8(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_div_int8(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_pl_int16(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int32_sub_int16(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_mul_int16(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_div_int16(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_pl_int64(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int32_sub_int64(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_mul_int64(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_div_int64(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int32_assign_int64(TomOperStack *os, 
										void *data_lhs, void *data_rhs,
	 									void *data_res);
extern int32_t vh_ty_int32_sqrt(TomOperStack *os, 
								void *data_lhs, void *data_rhs,
								void *data_res);


/*
 * Int64 Operations
 */

void* vh_ty_int64_tam_bin_get(struct TamBinGetStack *tamstack, 
							  const BinaryAMOptions bopts,
							  const void *source, void *target,
							  size_t *length, size_t *cursor);

void* vh_ty_int64_tam_bin_set(struct TamBinSetStack *tamstack, 
							  const BinaryAMOptions bopts,
			  				  const void *source, void *target,
			  				  size_t length, size_t cursor);

size_t vh_ty_int64_tam_bin_len(Type type, const void *source);

void vh_ty_int64_tam_memset_get(struct TamGenStack *tamstack, 
								void *source, void *target);
void vh_ty_int64_tam_memset_set(struct TamGenStack *tamstack, 
								void *source, void *target);

int32_t vh_ty_int64_tom_comparison(struct TomCompStack *tamstack,
								   const void *lhs, const void *rhs);

extern int32_t vh_ty_int64_pl_int64(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int64_sub_int64(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_mul_int64(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_div_int64(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_pl_int8(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int64_sub_int8(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_mul_int8(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_div_int8(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_pl_int16(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int64_sub_int16(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_mul_int16(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_div_int16(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_pl_int32(TomOperStack *os, 
									void *data_lhs, void *data_rhs,
									void *data_res);

extern int32_t vh_ty_int64_sub_int32(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_mul_int32(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_div_int32(TomOperStack *os, 
									 void *data_lhs, void *data_rhs,
	 								 void *data_res);

extern int32_t vh_ty_int64_ass_int32(TomOperStack *os,
									 void *data_lhs, void *data_rhs,
									 void *data_res);
extern int32_t vh_ty_int64_sqrt(TomOperStack *os, 
								void *data_lhs, void *data_rhs,
								void *data_res);

#endif

