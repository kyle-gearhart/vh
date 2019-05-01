/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */




#include <assert.h>
#include <byteswap.h>
#include <netinet/in.h>
#include <math.h>
#include <stdio.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/TypeCatalog.h"

typedef double dbl_t;
typedef float float_t;


void* 
vh_ty_dbl_tam_bin_get(struct TamBinGetStack *tamstack, 
						const BinaryAMOptions bopts,
			  			const void *src, void *tgt,
			  			size_t *length, size_t *cursor)
{
	union {
		dbl_t dbl;
		int64_t i64;
	} u;

	dbl_t *target = tgt;
	dbl_t **pptarget = (dbl_t**)target, *buffer;

	const uint32_t *vals = src;
	uint32_t h32, l32;
	uint64_t swapped;

	if (!bopts->malloc && length && *length == 0)
	{
		*length = sizeof(dbl_t);
		return 0;
	}

	if (bopts->sourceBigEndian != bopts->targetBigEndian)
	{
		h32 = htonl(vals[0]);
		l32 = htonl(vals[1]);

		swapped = h32;
		swapped <<= 32;
		swapped |= l32;

		u.i64 = swapped;
	}
	else
	{
		u.dbl = *((double*)src);
	}

	if (bopts->malloc)
	{
		buffer = vhmalloc(sizeof(dbl_t));
		*buffer = u.dbl;

		if (cursor)
			*cursor = sizeof(dbl_t);

		if (target)
			*pptarget = buffer;

		if (length)
			*length = sizeof(dbl_t);

		return buffer;
	}
	else
	{
		if (target)
		{
			*target = u.dbl;

			if (cursor)
				*cursor = sizeof(dbl_t);

			if (length)
				*length = sizeof(dbl_t);
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
vh_ty_dbl_tam_bin_set(struct TamBinSetStack *tamstack, 
						const BinaryAMOptions bopts,
			  			const void *src, void *tgt,
			  			size_t length, size_t cursor)
{
	union {
		dbl_t dbl;
		int64_t i64;
	} u;
	dbl_t *target = tgt;
	dbl_t **pptarget = (dbl_t**)target, *buffer;

	const uint32_t *vals = src;
	uint32_t h32, l32;
	uint64_t swapped;


	if (bopts->targetBigEndian != bopts->sourceBigEndian)
	{
		h32 = ntohl(vals[0]);
		l32 = ntohl(vals[1]);

		swapped = h32;
		swapped <<= 32;
		swapped |= l32;

		u.i64 = swapped;
	}
	else
	{
		u.dbl = *((double*)src);
	}

	if (bopts->malloc)
	{
		buffer = vhmalloc(sizeof(dbl_t));
		*buffer = u.dbl;

		*pptarget = buffer;

		return buffer;
	}
	else
	{
		*target = u.dbl;
	}

	return 0;
}

size_t 
vh_ty_dbl_tam_bin_len(Type type, const void *source)
{
	return sizeof(dbl_t);
}

void 
vh_ty_dbl_tam_memset_get(struct TamGenStack *tamstack, 
						   void *src, void *tgt)
{
	dbl_t *source = src, *target = tgt;

	if (source && target)
		*source = *target;
}

void 
vh_ty_dbl_tam_memset_set(struct TamGenStack *tamstack,
						   void *src, void *tgt)
{
	dbl_t *source = src, *target = tgt;

	if (source && target)
		*target = *source;
}


int32_t 
vh_ty_dbl_tom_comparison(struct TomCompStack *tamstack,
						   const void *lhs, const void *rhs)
{
	const dbl_t *l = lhs, *r = rhs;

	vh_tom_assert_bottom(tamstack);
	return (*l < *r) ? -1 : (*l > *r);
}

int32_t
vh_ty_dbl_pl_dbl(TomOperStack *os, 
					 void *data_lhs, void *data_rhs,
	  				 void *data_res)
{
	dbl_t *lhs = data_lhs, *rhs = data_rhs, *res = data_res;

	*res = *lhs + *rhs;

	return 0;
}

int32_t
vh_ty_dbl_sub_dbl(TomOperStack *os, 
					  void *data_lhs, void *data_rhs,
	 				  void *data_res)
{
	dbl_t *lhs = data_lhs, *rhs = data_rhs, *res = data_res;

	*res = *lhs - *rhs;

	return 0;
}
 
int32_t
vh_ty_dbl_mul_dbl(TomOperStack *os,
	   				  void *data_lhs, void *data_rhs,
	 				  void *data_res)
{
	dbl_t *lhs = data_lhs, *rhs = data_rhs, *res = data_res;

	*res = (*lhs) * (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_div_dbl(TomOperStack *os_lhs, 
					  void *data_lhs, void *data_rhs,
	 				  void *data_res)
{
	dbl_t *lhs = data_lhs, *rhs = data_rhs, *res = data_res;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_pl_float(TomOperStack *os,
					void *data_lhs, void *data_rhs,
	   				void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	float_t *rhs = data_rhs;

	*res = (*lhs) + (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_sub_float(TomOperStack *os,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	float_t *rhs = data_rhs;

	*res = (*lhs) - (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_mul_float(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	float_t *rhs = data_rhs;

	*res = (*lhs) * (*rhs);

	return 0;
}


int32_t
vh_ty_dbl_div_float(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	float_t *rhs = data_rhs;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (dbl_t)(*rhs);

	return 0;
}

int32_t
vh_ty_dbl_ass_float(TomOperStack *os_lhs,
					  void *data_lhs, void *data_rhs,
					  void *data_res)
{
	dbl_t *lhs = data_lhs;
	float_t *rhs = data_rhs;

	*lhs = *rhs;

	return 0;
}

int32_t
vh_ty_dbl_pl_int8(TomOperStack *os,
					void *data_lhs, void *data_rhs,
	   				void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int8_t *rhs = data_rhs;

	*res = (*lhs) + (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_sub_int8(TomOperStack *os,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int8_t *rhs = data_rhs;

	*res = (*lhs) - (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_mul_int8(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int8_t *rhs = data_rhs;

	*res = (*lhs) * (*rhs);

	return 0;
}


int32_t
vh_ty_dbl_div_int8(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int8_t *rhs = data_rhs;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (dbl_t)(*rhs);

	return 0;
}

int32_t
vh_ty_dbl_ass_int8(TomOperStack *os_lhs,
					  void *data_lhs, void *data_rhs,
					  void *data_res)
{
	dbl_t *lhs = data_lhs;
	int8_t *rhs = data_rhs;

	*lhs = *rhs;

	return 0;
}

int32_t
vh_ty_dbl_pl_int16(TomOperStack *os,
					void *data_lhs, void *data_rhs,
	   				void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int16_t *rhs = data_rhs;

	*res = (*lhs) + (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_sub_int16(TomOperStack *os,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int16_t *rhs = data_rhs;

	*res = (*lhs) - (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_mul_int16(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int16_t *rhs = data_rhs;

	*res = (*lhs) * (*rhs);

	return 0;
}


int32_t
vh_ty_dbl_div_int16(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int16_t *rhs = data_rhs;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (dbl_t)(*rhs);

	return 0;
}

int32_t
vh_ty_dbl_ass_int16(TomOperStack *os_lhs,
					  void *data_lhs, void *data_rhs,
					  void *data_res)
{
	dbl_t *lhs = data_lhs;
	int16_t *rhs = data_rhs;

	*lhs = *rhs;

	return 0;
}

int32_t
vh_ty_dbl_pl_int32(TomOperStack *os,
					void *data_lhs, void *data_rhs,
	   				void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int32_t *rhs = data_rhs;

	*res = (*lhs) + (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_sub_int32(TomOperStack *os,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int32_t *rhs = data_rhs;

	*res = (*lhs) - (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_mul_int32(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int32_t *rhs = data_rhs;

	*res = (*lhs) * (*rhs);

	return 0;
}


int32_t
vh_ty_dbl_div_int32(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int32_t *rhs = data_rhs;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (dbl_t)(*rhs);

	return 0;
}

int32_t
vh_ty_dbl_ass_int32(TomOperStack *os_lhs,
					  void *data_lhs, void *data_rhs,
					  void *data_res)
{
	dbl_t *lhs = data_lhs;
	int32_t *rhs = data_rhs;

	*lhs = *rhs;

	return 0;
}

int32_t
vh_ty_dbl_pl_int64(TomOperStack *os,
					void *data_lhs, void *data_rhs,
	   				void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int64_t *rhs = data_rhs;

	*res = (*lhs) + (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_sub_int64(TomOperStack *os,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int64_t *rhs = data_rhs;

	*res = (*lhs) - (*rhs);

	return 0;
}

int32_t
vh_ty_dbl_mul_int64(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int64_t *rhs = data_rhs;

	*res = (*lhs) * (*rhs);

	return 0;
}


int32_t
vh_ty_dbl_div_int64(TomOperStack *os_lhs,
					 void *data_lhs, void *data_rhs,
					 void *data_res)
{
	dbl_t *lhs = data_lhs, *res = data_res;
	int64_t *rhs = data_rhs;

	if (*rhs == 0)
	{
		elog(ERROR1, emsg("Unable to divide by zero!"));
		return -1;
	}

	*res = (*lhs) / (dbl_t)(*rhs);

	return 0;
}

int32_t
vh_ty_dbl_ass_int64(TomOperStack *os_lhs,
					  void *data_lhs, void *data_rhs,
					  void *data_res)
{
	dbl_t *lhs = data_lhs;
	int64_t *rhs = data_rhs;

	*lhs = *rhs;

	return 0;
}

int32_t
vh_ty_dbl_sqrt(TomOperStack *os_lhs,
				 void *data_lhs, void *data_rhs,
  				 void *data_res)
{
	dbl_t *res = data_res, *lhs = data_lhs;
	double dres, dlhs = *lhs;

	dres = sqrt(dlhs);
	*res = (dbl_t)dres;

	return 0;
}

static char*
ty_dbl_tam_cstr_get(struct TamCStrGetStack *tamstack,
					  CStrAMOptions copts,
					  const void *source, 
					  char *target,
					  size_t *length, size_t *cursor,
					  void *format)
{
	const dbl_t *src = source;
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
ty_dbl_tam_cstr_set(struct TamCStrSetStack *tamstack,
					  CStrAMOptions copts,
					  const char *source,
					  void *target,
					  size_t length, size_t cursor,
					  void *format)
{
	dbl_t *val = target;

	if (copts->malloc)
	{
		val = vhmalloc(sizeof(dbl_t));
		*val = atof(source);

		return val;
	}

	*val = atof(source);

	return 0;
}

static const struct TypeOperRegData dbl_oper_reg[] = {
	/* Double to Double */
	{ &vh_type_dbl, "+", &vh_type_dbl, vh_ty_dbl_pl_dbl, 0 },
	{ &vh_type_dbl, "-", &vh_type_dbl, vh_ty_dbl_sub_dbl, 0 },
	{ &vh_type_dbl, "*", &vh_type_dbl, vh_ty_dbl_mul_dbl, 0 },
	{ &vh_type_dbl, "/", &vh_type_dbl, vh_ty_dbl_div_dbl, 0 },

	{ &vh_type_dbl, "sqrt", 0, vh_ty_dbl_sqrt, 0 },
	
	/* Double to Float */
	{ &vh_type_dbl, "+", &vh_type_float, vh_ty_dbl_pl_float, 0 },	
	{ &vh_type_dbl, "-", &vh_type_float, vh_ty_dbl_sub_float, 0 },
	{ &vh_type_dbl, "*", &vh_type_float, vh_ty_dbl_mul_float, 0 },
	{ &vh_type_dbl, "/", &vh_type_float, vh_ty_dbl_div_float, 0 },
	{ &vh_type_dbl, "=", &vh_type_float, vh_ty_dbl_ass_float, 0 },

	/* Double to Int8 */
	{ &vh_type_dbl, "+", &vh_type_int8, vh_ty_dbl_pl_int8, 0 },	
	{ &vh_type_dbl, "-", &vh_type_int8, vh_ty_dbl_sub_int8, 0 },
	{ &vh_type_dbl, "*", &vh_type_int8, vh_ty_dbl_mul_int8, 0 },
	{ &vh_type_dbl, "/", &vh_type_int8, vh_ty_dbl_div_int8, 0 },
	{ &vh_type_dbl, "=", &vh_type_int8, vh_ty_dbl_ass_int8, 0 },

	/* Double to Int16 */
	{ &vh_type_dbl, "+", &vh_type_int16, vh_ty_dbl_pl_int16, 0 },
	{ &vh_type_dbl, "-", &vh_type_int16, vh_ty_dbl_sub_int16, 0 },
	{ &vh_type_dbl, "*", &vh_type_int16, vh_ty_dbl_mul_int16, 0 },
	{ &vh_type_dbl, "/", &vh_type_int16, vh_ty_dbl_div_int16, 0 },
	{ &vh_type_dbl, "=", &vh_type_int16, vh_ty_dbl_ass_int16, 0 },

	/* Double to Int32 */
	{ &vh_type_dbl, "+", &vh_type_int32, vh_ty_dbl_pl_int32, 0 },
	{ &vh_type_dbl, "-", &vh_type_int32, vh_ty_dbl_sub_int32, 0},
	{ &vh_type_dbl, "*", &vh_type_int32, vh_ty_dbl_mul_int32, 0},
	{ &vh_type_dbl, "/", &vh_type_int32, vh_ty_dbl_div_int32, 0},
	{ &vh_type_dbl, "=", &vh_type_int32, vh_ty_dbl_ass_int32, 0},

	/* Double to Int64 */
	{ &vh_type_dbl, "+", &vh_type_int64, vh_ty_dbl_pl_int64, 0 },
	{ &vh_type_dbl, "-", &vh_type_int64, vh_ty_dbl_sub_int64, 0},
	{ &vh_type_dbl, "*", &vh_type_int64, vh_ty_dbl_mul_int64, 0},
	{ &vh_type_dbl, "/", &vh_type_int64, vh_ty_dbl_div_int64, 0},
	{ &vh_type_dbl, "=", &vh_type_int64, vh_ty_dbl_ass_int64, 0}
};

struct TypeData const vh_type_dbl =
{
	.id = 28,
	.name = "double",
	.varlen = false,
	.size = sizeof(double),
	.alignment = sizeof(dbl_t),
	.construct_forhtd = false,

	.tam = {
		.bin_get = vh_ty_dbl_tam_bin_get,
		.bin_set = vh_ty_dbl_tam_bin_set,
		.bin_length = vh_ty_dbl_tam_bin_len,

		.cstr_get = ty_dbl_tam_cstr_get,
		.cstr_set = ty_dbl_tam_cstr_set,

		.memset_get = vh_ty_dbl_tam_memset_get,
		.memset_set = vh_ty_dbl_tam_memset_set
	},
	.tom = {
		.comp = vh_ty_dbl_tom_comparison
	},

	.regoper = dbl_oper_reg,
	.regoper_sz = sizeof(dbl_oper_reg) / sizeof(struct TypeOperRegData)
};

