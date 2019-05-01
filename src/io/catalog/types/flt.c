/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include <assert.h>
#include <byteswap.h>
#include <math.h>
#include <stdio.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeCatalog.h"

typedef double dbl_t;
typedef float flt_t;


void* 
vh_ty_flt_tam_bin_get(struct TamBinGetStack *tamstack, 
						const BinaryAMOptions bopts,
			  			const void *src, void *tgt,
			  			size_t *length, size_t *cursor)
{
	const flt_t *source = src;
	flt_t *target = tgt;
	flt_t **pptarget = (flt_t**)target, *buffer, swapped;

	if (!bopts->malloc && length && *length == 0)
	{
		*length = sizeof(flt_t);
		return 0;
	}

	if (bopts->sourceBigEndian != bopts->targetBigEndian)
		swapped = __bswap_64(*source);
	else
		swapped = *source;

	if (bopts->malloc)
	{
		buffer = vhmalloc(sizeof(flt_t));
		*buffer = swapped;

		if (cursor)
			*cursor = sizeof(flt_t);

		if (target)
			*pptarget = buffer;

		if (length)
			*length = sizeof(flt_t);

		return buffer;
	}
	else
	{
		if (target)
		{
			*target = swapped;

			if (cursor)
				*cursor = sizeof(flt_t);

			if (length)
				*length = sizeof(flt_t);
		}
		else
		{
			elog(ERROR2,
				 emsg("vh_tam_bin_get for int32 has been called without a "
					  "non-null target pointer parameter!  Please see "
					  "the rules contained in catalog/Type.h and adjust the "
					  "calling convention!"));
		}
	}

	return 0;
}

void* 
vh_ty_flt_tam_bin_set(struct TamBinSetStack *tamstack, 
						const BinaryAMOptions bopts,
			  			const void *src, void *tgt,
			  			size_t length, size_t cursor)
{
	const flt_t *source = src;
	flt_t *target = tgt;
	flt_t **pptarget = (flt_t**)target, *buffer, swapped;


	if (bopts->targetBigEndian != bopts->sourceBigEndian)
		swapped = __bswap_64(*source);
	else
		swapped = *source;

	if (bopts->malloc)
	{
		buffer = vhmalloc(sizeof(flt_t));
		*buffer = swapped;

		*pptarget = buffer;

		return buffer;
	}
	else
	{
		*target = swapped;
	}

	return 0;
}

size_t 
vh_ty_flt_tam_bin_len(Type type, const void *source)
{
	return sizeof(flt_t);
}

void 
vh_ty_flt_tam_memset_get(struct TamGenStack *tamstack, 
						   void *src, void *tgt)
{
	flt_t *source = src, *target = tgt;

	if (source && target)
		*source = *target;
}

void 
vh_ty_flt_tam_memset_set(struct TamGenStack *tamstack,
						   void *src, void *tgt)
{
	flt_t *source = src, *target = tgt;

	if (source && target)
		*target = *source;
}


int32_t 
vh_ty_flt_tom_comparison(struct TomCompStack *tamstack,
						   const void *lhs, const void *rhs)
{
	const flt_t *l = lhs, *r = rhs;

	vh_tom_assert_bottom(tamstack);
	return (*l < *r) ? -1 : (*l > *r);
}

int32_t
vh_ty_flt_pl_flt(TomOperStack *os, 
					 void *data_lhs, void *data_rhs,
	  				 void *data_res)
{
	flt_t *lhs = data_lhs, *rhs = data_rhs, *res = data_res;

	*res = *lhs + *rhs;

	return 0;
}

int32_t
vh_ty_flt_sub_flt(TomOperStack *os, 
					  void *data_lhs, void *data_rhs,
	 				  void *data_res)
{
	flt_t *lhs = data_lhs, *rhs = data_rhs, *res = data_res;

	*res = *lhs - *rhs;

	return 0;
}
 
int32_t
vh_ty_flt_mul_flt(TomOperStack *os,
	   				  void *data_lhs, void *data_rhs,
	 				  void *data_res)
{
	flt_t *lhs = data_lhs, *rhs = data_rhs, *res = data_res;

	*res = (*lhs) * (*rhs);

	return 0;
}

int32_t
vh_ty_flt_div_flt(TomOperStack *os_lhs, 
					  void *data_lhs, void *data_rhs,
	 				  void *data_res)
{
	flt_t *lhs = data_lhs, *rhs = data_rhs, *res = data_res;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (*rhs);

	return 0;
}

int32_t
vh_ty_flt_pl_int8(TomOperStack *os,
					void *data_lhs, void *data_rhs,
	   				void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int8_t *rhs = data_rhs;

	*res = (*lhs) + (*rhs);

	return 0;
}

int32_t
vh_ty_flt_sub_int8(TomOperStack *os,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int8_t *rhs = data_rhs;

	*res = (*lhs) - (*rhs);

	return 0;
}

int32_t
vh_ty_flt_mul_int8(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int8_t *rhs = data_rhs;

	*res = (*lhs) * (*rhs);

	return 0;
}


int32_t
vh_ty_flt_div_int8(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int8_t *rhs = data_rhs;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (flt_t)(*rhs);

	return 0;
}

int32_t
vh_ty_flt_ass_int8(TomOperStack *os_lhs,
					  void *data_lhs, void *data_rhs,
					  void *data_res)
{
	flt_t *lhs = data_lhs;
	int8_t *rhs = data_rhs;

	*lhs = *rhs;

	return 0;
}


int32_t
vh_ty_flt_pl_int16(TomOperStack *os,
					void *data_lhs, void *data_rhs,
	   				void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int16_t *rhs = data_rhs;

	*res = (*lhs) + (*rhs);

	return 0;
}

int32_t
vh_ty_flt_sub_int16(TomOperStack *os,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int16_t *rhs = data_rhs;

	*res = (*lhs) - (*rhs);

	return 0;
}

int32_t
vh_ty_flt_mul_int16(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int16_t *rhs = data_rhs;

	*res = (*lhs) * (*rhs);

	return 0;
}


int32_t
vh_ty_flt_div_int16(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int16_t *rhs = data_rhs;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (flt_t)(*rhs);

	return 0;
}

int32_t
vh_ty_flt_ass_int16(TomOperStack *os_lhs,
					  void *data_lhs, void *data_rhs,
					  void *data_res)
{
	flt_t *lhs = data_lhs;
	int16_t *rhs = data_rhs;

	*lhs = *rhs;

	return 0;
}

int32_t
vh_ty_flt_pl_int32(TomOperStack *os,
					void *data_lhs, void *data_rhs,
	   				void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int32_t *rhs = data_rhs;

	*res = (*lhs) + (*rhs);

	return 0;
}

int32_t
vh_ty_flt_sub_int32(TomOperStack *os,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int32_t *rhs = data_rhs;

	*res = (*lhs) - (*rhs);

	return 0;
}

int32_t
vh_ty_flt_mul_int32(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int32_t *rhs = data_rhs;

	*res = (*lhs) * (*rhs);

	return 0;
}


int32_t
vh_ty_flt_div_int32(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int32_t *rhs = data_rhs;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (flt_t)(*rhs);

	return 0;
}

int32_t
vh_ty_flt_ass_int32(TomOperStack *os_lhs,
					  void *data_lhs, void *data_rhs,
					  void *data_res)
{
	flt_t *lhs = data_lhs;
	int32_t *rhs = data_rhs;

	*lhs = *rhs;

	return 0;
}

int32_t
vh_ty_flt_pl_int64(TomOperStack *os,
					void *data_lhs, void *data_rhs,
	   				void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int64_t *rhs = data_rhs;

	*res = (*lhs) + (*rhs);

	return 0;
}

int32_t
vh_ty_flt_sub_int64(TomOperStack *os,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int64_t *rhs = data_rhs;

	*res = (*lhs) - (*rhs);

	return 0;
}

int32_t
vh_ty_flt_mul_int64(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int64_t *rhs = data_rhs;

	*res = (*lhs) * (*rhs);

	return 0;
}


int32_t
vh_ty_flt_div_int64(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	int64_t *rhs = data_rhs;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (flt_t)(*rhs);

	return 0;
}

int32_t
vh_ty_flt_ass_int64(TomOperStack *os_lhs,
					  void *data_lhs, void *data_rhs,
					  void *data_res)
{
	flt_t *lhs = data_lhs;
	int64_t *rhs = data_rhs;

	*lhs = *rhs;

	return 0;
}

int32_t
vh_ty_flt_pl_dbl(TomOperStack *os,
					void *data_lhs, void *data_rhs,
	   				void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	dbl_t *rhs = data_rhs;

	*res = (*lhs) + (*rhs);

	return 0;
}

int32_t
vh_ty_flt_sub_dbl(TomOperStack *os,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	dbl_t *rhs = data_rhs;

	*res = (*lhs) - (*rhs);

	return 0;
}

int32_t
vh_ty_flt_mul_dbl(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	dbl_t *rhs = data_rhs;

	*res = (*lhs) * (*rhs);

	return 0;
}


int32_t
vh_ty_flt_div_dbl(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	flt_t *lhs = data_lhs, *res = data_res;
	dbl_t *rhs = data_rhs;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (flt_t)(*rhs);

	return 0;
}

int32_t
vh_ty_flt_ass_dbl(TomOperStack *os_lhs,
					  void *data_lhs, void *data_rhs,
					  void *data_res)
{
	flt_t *lhs = data_lhs;
	dbl_t *rhs = data_rhs;

	*lhs = *rhs;

	return 0;
}


int32_t
vh_ty_flt_sqrt(TomOperStack *os_lhs,
				 void *data_lhs, void *data_rhs,
  				 void *data_res)
{
	flt_t *res = data_res, *lhs = data_lhs;
	double dres, dlhs = *lhs;

	dres = sqrt(dlhs);
	*res = (flt_t)dres;

	return 0;
}

static char*
ty_flt_tam_cstr_get(struct TamCStrGetStack *tamstack,
					  CStrAMOptions copts,
					  const void *source, 
					  char *target,
					  size_t *length, size_t *cursor,
					  void *format)
{
	const flt_t *src = source;
	int32_t len = 0;
	char *tgt;
	char buffer[64];

	len = sprintf(&buffer[0], "%lf", *src);

	if (copts->malloc)
	{
		tgt = vhmalloc(len + 1);
		memcpy(tgt, &buffer[0], len + 1);

		if (length)
			*length = len;

		if (cursor)
			*cursor = len;

		return tgt;		
	}

	if (length)
	{
		if (*length > len)
		{
			memcpy(target, &buffer[0], len + 1);

			if (cursor)
			{
				*cursor = len;
			}
		}
		else
		{
			if (cursor)
				*cursor = 0;
		}
		
		*length = len;
	}

	return 0;
}

static void*
ty_flt_tam_cstr_set(struct TamCStrSetStack *tamstack,
					  CStrAMOptions copts,
					  const char *source,
					  void *target,
					  size_t length, size_t cursor,
					  void *format)
{
	flt_t *val = target;

	if (copts->malloc)
	{
		val = vhmalloc(sizeof(flt_t));
		*val = atof(source);

		return val;
	}

	*val = atof(source);

	return 0;
}

static const struct TypeOperRegData float_oper_reg[] = {
	/* Float to Float */
	{ &vh_type_float, "+", &vh_type_float, vh_ty_flt_pl_flt, 0 },
	{ &vh_type_float, "-", &vh_type_float, vh_ty_flt_sub_flt, 0 },
	{ &vh_type_float, "*", &vh_type_float, vh_ty_flt_mul_flt, 0 },
	{ &vh_type_float, "/", &vh_type_float, vh_ty_flt_div_flt, 0 },

	{ &vh_type_float, "sqrt", 0, vh_ty_flt_sqrt, 0 },
	
	/* Float to Double */
	{ &vh_type_float, "+", &vh_type_dbl, vh_ty_flt_pl_dbl, 0 },	
	{ &vh_type_float, "-", &vh_type_dbl, vh_ty_flt_sub_dbl, 0 },
	{ &vh_type_float, "*", &vh_type_dbl, vh_ty_flt_mul_dbl, 0 },
	{ &vh_type_float, "/", &vh_type_dbl, vh_ty_flt_div_dbl, 0 },
	{ &vh_type_float, "=", &vh_type_dbl, vh_ty_flt_ass_dbl, 0 },

	/* Float to Int8 */
	{ &vh_type_float, "+", &vh_type_int8, vh_ty_flt_pl_int8, 0 },	
	{ &vh_type_float, "-", &vh_type_int8, vh_ty_flt_sub_int8, 0 },
	{ &vh_type_float, "*", &vh_type_int8, vh_ty_flt_mul_int8, 0 },
	{ &vh_type_float, "/", &vh_type_int8, vh_ty_flt_div_int8, 0 },
	{ &vh_type_float, "=", &vh_type_int8, vh_ty_flt_ass_int8, 0 },

	/* Float to Int16 */
	{ &vh_type_float, "+", &vh_type_int16, vh_ty_flt_pl_int16, 0 },
	{ &vh_type_float, "-", &vh_type_int16, vh_ty_flt_sub_int16, 0 },
	{ &vh_type_float, "*", &vh_type_int16, vh_ty_flt_mul_int16, 0 },
	{ &vh_type_float, "/", &vh_type_int16, vh_ty_flt_div_int16, 0 },
	{ &vh_type_float, "=", &vh_type_int16, vh_ty_flt_ass_int16, 0 },

	/* Float to Int32 */
	{ &vh_type_float, "+", &vh_type_int32, vh_ty_flt_pl_int32, 0 },
	{ &vh_type_float, "-", &vh_type_int32, vh_ty_flt_sub_int32, 0},
	{ &vh_type_float, "*", &vh_type_int32, vh_ty_flt_mul_int32, 0},
	{ &vh_type_float, "/", &vh_type_int32, vh_ty_flt_div_int32, 0},
	{ &vh_type_float, "=", &vh_type_int32, vh_ty_flt_ass_int32, 0},

	/* Float to Int64 */
	{ &vh_type_float, "+", &vh_type_int64, vh_ty_flt_pl_int64, 0 },
	{ &vh_type_float, "-", &vh_type_int64, vh_ty_flt_sub_int64, 0},
	{ &vh_type_float, "*", &vh_type_int64, vh_ty_flt_mul_int64, 0},
	{ &vh_type_float, "/", &vh_type_int64, vh_ty_flt_div_int64, 0},
	{ &vh_type_float, "=", &vh_type_int64, vh_ty_flt_ass_int64, 0}
};

struct TypeData const vh_type_float =
{
	.id = 29,
	.name = "float",
	.varlen = false,
	.size = sizeof(float),
	.alignment = sizeof(flt_t),
	.construct_forhtd = false,

	.tam = {
		.bin_get = vh_ty_flt_tam_bin_get,
		.bin_set = vh_ty_flt_tam_bin_set,
		.bin_length = vh_ty_flt_tam_bin_len,

		.cstr_get = ty_flt_tam_cstr_get,
		.cstr_set = ty_flt_tam_cstr_set,

		.memset_get = vh_ty_flt_tam_memset_get,
		.memset_set = vh_ty_flt_tam_memset_set
	},
	.tom = {
		.comp = vh_ty_flt_tom_comparison
	},

	.regoper = float_oper_reg,
	.regoper_sz = sizeof(float_oper_reg) / sizeof(struct TypeOperRegData)
};

