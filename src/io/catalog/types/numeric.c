/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <stdlib.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/types/numeric.h"

/*
 * Numeric Support
 *
 * This is a varlen data type.  If we include the varlen structure we'll get a 
 * bunch of padding that doesn't make a lot of sense for a numeric type.
 *
 * Number of digits is not related to the number of digits that will actually 
 * print, it's just the number of NumericDigits we need to represent the value.
 */

#define VH_NUMERIC_NBASE 			10000
#define VH_NUMERIC_HALF_NBASE		5000
#define VH_NUMERIC_DEC_DIGITS		4
#define VH_NUMERIC_MUL_GUARD_DIGITS	2
#define VH_NUMERIC_DIV_GUARD_DIGITS	4


typedef int16_t NumericDigit;

#define VH_NUMERIC_HEADER_SZ 		sizeof(struct NumericData)
#define VH_NUMERIC_NDIGITS(num)		(((num)->size > VH_NUMERIC_HEADER_SZ ? 		\
		((num)->size - VH_NUMERIC_HEADER_SZ) / sizeof(NumericDigit) : 0 ))
#define VH_NUMERIC_SET_NDIGITS(num, d)	((num)->size = VH_NUMERIC_HEADER_SZ + ((d) * sizeof(NumericDigit)))
#define VH_NUMERIC_NDIGITS_SZ(d)	((d) * sizeof(NumericDigit))

#define VH_NUMERIC_POS				0x0000
#define VH_NUMERIC_NEG				0x4000
#define VH_NUMERIC_NAN				0xC000

typedef struct NumericData
{
	size_t size;
	HeapBufferNo hbno;
	
   	int8_t flags;
	int8_t dscale;
	int8_t weight;

	NumericDigit *ptr_digits;
} NumericData;


#define int16_swap(a, b, val)	( a != b ? ((val) >> 8) | ((val) << 8) : (val) )
#define alloc_ctx(num, sz)	(((struct vhvarlenm*)(num))->hbno ?					\
		vhmalloc_ctx(vh_hb_memoryctx(((struct vhvarlenm*)(num))->hbno), (sz)) :	\
		vhmalloc((sz)))


static void* type_numeric_tam_bin_get(struct TamBinGetStack *tamstack,
									  const BinaryAMOptions bopts,
									  const void *src, void *tgt,
									  size_t *length, size_t *cursor);
static void* type_numeric_tam_bin_set(struct TamBinSetStack *tamstack,
									  const BinaryAMOptions bopts,
									  const void *src, void *tgt,
									  size_t length, size_t cursor);

static char* type_numeric_tam_cstr_get(struct TamCStrGetStack *tamstack,
									   CStrAMOptions copts,
									   const void *src, char *tgt,
									   size_t *length, size_t *cursor,
									   void *formatter);
static void* type_numeric_tam_cstr_set(struct TamCStrSetStack *tamstack,
									   CStrAMOptions copts,
									   const char *src, void *tgt,
									   size_t length, size_t cursor,
									   void *formatter);
static void* type_numeric_tam_cstr_fmt(Type ty, const char **patterns, int32_t n_patterns);
#define type_numeric_tam_cstr_literal 	(0x1234u)

static void type_numeric_mset_get(struct TamGenStack *tamstack, void *src, void *tgt);
static void type_numeric_mset_set(struct TamGenStack *tamstack, void *src, void *tgt);

static int32_t type_numeric_comp(struct TomCompStack *tomstack,
								 const void *lhs, const void *rhs);
static void type_numeric_finalize(struct TomDestructStack *tomstack, void *tgt);


static bool type_numeric_tostring(const NumericData *num, char *str, size_t input_sz, 
								  size_t *output_sz);
static bool type_numeric_fromstring(const char *str, size_t len, Numeric dest);

/* 
 * ============================================================================
 * TAM Implementation
 * ============================================================================
 */

/*
 * type_numeric_tam_bin_get
 *
 * We follow Postgres' binary on the wire format for our internal routines.
 *
 * 	1)	Number of digits
 * 	2)	Weight
 * 	3)	Sign
 * 	4)	Digit scale
 * 	5)	Digits
 */

static void*
type_numeric_tam_bin_get(struct TamBinGetStack *tamstack,
						 const BinaryAMOptions bopts,
						 const void *src, void *tgt,
						 size_t *length, size_t *cursor)
{
	const struct NumericData *num = src;
	size_t size;
	uint16_t i, ndigits, *out, *outs;

	ndigits = (uint16_t)VH_NUMERIC_NDIGITS(num);
	size = (sizeof(uint16_t) * 4) + (sizeof(NumericDigit) * ndigits);

	if (bopts->malloc)
	{
		out = vhmalloc(size);
		outs = out;
	}
	else
	{
		out = tgt;
		outs = out;

		if (*length <= size)
		{
			*length = size;

			if (cursor)
				*cursor = 0;

			return 0;
		}
	}

	/* Number of Digits */
	*out = int16_swap(bopts->sourceBigEndian, 
					  bopts->targetBigEndian, 
					  ndigits);
	out++;

	/* Weight */
	*out = int16_swap(bopts->sourceBigEndian, 
					  bopts->targetBigEndian, 
					  num->weight);
	out++;

	/* Sign */
	*out = int16_swap(bopts->sourceBigEndian,
					  bopts->targetBigEndian,
					  (uint16_t)num->flags);
	out++;
	
	/* Digit Scale */
	*out = int16_swap(bopts->sourceBigEndian,
					  bopts->targetBigEndian,
					  (uint16_t)num->dscale);
	out++;

	/*
	 * Digits
	 */

	for (i = 0; i < ndigits; i++, out++)
	{
		*out = int16_swap(bopts->sourceBigEndian,
						  bopts->targetBigEndian,
						  (uint16_t)num->ptr_digits[i]);
	}

	*length = size;
	
	if (cursor)
		*cursor = size;

	return outs;
}

/*
 * type_numeric_tam_bin_set
 *
 * We follow Postgres' binary on the wire format for our internal routines.
 *
 * 	1)	Number of digits
 * 	2)	Weight
 * 	3)	Sign
 * 	4)	Digit scale
 * 	5)	Digits
 */
static void*
type_numeric_tam_bin_set(struct TamBinSetStack *tamstack,
						 const BinaryAMOptions bopts,
						 const void *src, void *tgt,
						 size_t length, size_t cursor)
{
	const uint16_t *in = src;
	Numeric num;
	int16_t ndigits;
	size_t existing_digits = 0, i = 0;

	assert(cursor == 0);

	if (bopts->malloc)
	{
		num = vhmalloc(sizeof(struct NumericData));
		memset(num, 0, sizeof(struct NumericData));
		num->size = VH_NUMERIC_HEADER_SZ;
	}
	else
	{
		num = tgt;
	}

	/* Number of Digits */
	existing_digits = VH_NUMERIC_NDIGITS(num);
	ndigits = int16_swap(bopts->targetBigEndian, bopts->sourceBigEndian, *in);
	VH_NUMERIC_SET_NDIGITS(num, ndigits);
	in++;

	/* Weight */
	num->weight = int16_swap(bopts->targetBigEndian, bopts->sourceBigEndian, *in);
	in++;

	/* Flags */
	num->flags = int16_swap(bopts->targetBigEndian, bopts->sourceBigEndian, *in);
	in++;

	/* DScale */
	num->dscale = int16_swap(bopts->targetBigEndian, bopts->sourceBigEndian, *in);
	in++;

	if (existing_digits < ndigits)
	{
		if (num->ptr_digits)
			num->ptr_digits = vhrealloc(num->ptr_digits, VH_NUMERIC_NDIGITS_SZ(ndigits));
		else
			num->ptr_digits = alloc_ctx(num, VH_NUMERIC_NDIGITS_SZ(ndigits));
	}

	for (i = 0; i < ndigits; i++, in++)
		num->ptr_digits[i] = int16_swap(bopts->targetBigEndian, bopts->sourceBigEndian, *in);

	/*
	 * Just for S&G's, let's see if the length we said was going to be here
	 * was in fact, correct.
	 */
	assert((sizeof(int16_t) * 4) + (sizeof(NumericDigit) * ndigits) == length);
	
	return num;
}

static bool
type_numeric_tostring(const NumericData *num, char *str, size_t input_sz, 
					  size_t *output_sz)
{
	int32_t dscale, i, d, ndigits;
	char *cp;
	char *endcp;
	NumericDigit dig, d1;
	size_t estimated_sz;

	dscale = num->dscale;
	ndigits = VH_NUMERIC_NDIGITS(num);

	i = (num->weight + 1) + VH_NUMERIC_DEC_DIGITS;
	if (i <= 0)
		i = 1;

	estimated_sz = (i + dscale + VH_NUMERIC_DEC_DIGITS + 2);

	if (input_sz < estimated_sz)
	{
		if (output_sz)
			*output_sz = estimated_sz;

		return false;
	}

	cp = str;

	if (num->flags & VH_NUMERIC_NEG)
		*cp++ = '-';

	if (num->weight < 0)
	{
		d = num->weight + 1;
		*cp++ = '0';
	}
	else
	{
		for (d = 0; d <= num->weight; d++)
		{
			dig = (d < ndigits) ? num->ptr_digits[d] : 0;

#if VH_NUMERIC_DEC_DIGITS == 4
			{
				bool putit = (d > 0);

				d1 = dig / 1000;
				dig -= d1 * 1000;
				putit |= (d1 > 0);
				
				if (putit)
					*cp++ = d1 + '0';

				d1 = dig / 100;
				dig -= d1 * 100;
				putit |= (d1 > 0);

				if (putit)
					*cp++ = d1 + '0';

				d1 = dig / 10;
				dig -= d1 * 10;
				putit |= (d1 > 0);

				if (putit)
					*cp++ = d1 + '0';

				*cp++ = dig + '0';
			}
#elif VH_NUMERIC_DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;

			if (d1 > 0 || d > 0)
				*cp++ = d1 + '0';

			*cp++ = dig + '0';
#elif VH_NUMERIC_DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported VH_NUMERIC_NBASE
#endif	
		}
	}
	
	endcp = cp;

	if (dscale > 0)
	{
		*cp++ = '.';
		endcp = cp + dscale;

		for (i = 0; i < dscale; d++, i += VH_NUMERIC_DEC_DIGITS)
		{
			dig = (d >= 0 && d < ndigits) ? num->ptr_digits[d] : 0;

#if VH_NUMERIC_DEC_DIGITS == 4
			d1 = dig / 1000;
			dig -= d1 * 1000;
			*cp++ = d1 + '0';

			d1 = dig / 100;
			dig -= d1 * 100;
			*cp++ = d1 + '0';

			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif VH_NUMERIC_DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif VH_NUMERIC_DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported VH_NUMERIC_NBASE
#endif
		}

		cp = endcp;
	}

	/*
	 * Put the actual size occupied, not just the estimated by simply subtracting
	 * the pointers.
	 */
	if (output_sz)
	{
		*output_sz = endcp - str;
	}

	*cp = '\0';

	return true;
}

static bool
type_numeric_fromstring(const char *str, size_t len, Numeric dest)
{
	unsigned char *decdigits;
	char *endptr;
   	const char *cp = str;
	long exponent;
	int32_t i, sign = VH_NUMERIC_POS, dweight = -1,
			ddigits, dscale = 0, weight, ndigits, offset;
	bool havedp = false;
	NumericDigit *digits;

	switch (*cp)
	{
		case '+':
			sign = VH_NUMERIC_POS;
			cp++;
			break;

		case '-':
			sign = VH_NUMERIC_NEG;
			cp++;
			break;
	}

	if (*cp)
	{
		havedp = true;
		cp++;
	}

	if (!isdigit((unsigned char) *cp))
	{
		return false;
	}

	decdigits = vhmalloc(len + VH_NUMERIC_DEC_DIGITS + 2);
	memset(decdigits, 0, VH_NUMERIC_DEC_DIGITS);
	i = VH_NUMERIC_DEC_DIGITS;

	while (*cp)
	{
		if (isdigit((unsigned char) *cp))
		{
			decdigits[i++] = *cp++ - '0';

			if (!havedp)
				dweight++;
			else
				dscale++;
		}
		else if (*cp == '.')
		{
			if (havedp)
			{
				vhfree(decdigits);
				return false;
			}

			havedp = true;
			cp++;
		}
		else
		{
			break;
		}
	}

	ddigits = i - VH_NUMERIC_DEC_DIGITS;
	memset(decdigits + 1, 0, VH_NUMERIC_DEC_DIGITS - 1);

	if (*cp == 'e' || *cp == 'E')
	{
		cp++;
		exponent = strtol(cp, &endptr, 10);

		if (endptr == cp)
		{
			vhfree(decdigits);
			return false;
		}

		cp = endptr;

		if (exponent >= INT_MAX / 2 || exponent <= -(INT_MAX / 2))
		{
			vhfree(decdigits);
			return false;
		}

		dweight += (int)exponent;
		dscale -= (int)exponent;

		if (dscale < 0)
			dscale = 0;
	}

	if (dweight >= 0)
		weight = (dweight + 1 + VH_NUMERIC_DEC_DIGITS - 1) / VH_NUMERIC_DEC_DIGITS - 1;
	else
		weight = -((-dweight - 1) / VH_NUMERIC_DEC_DIGITS + 1);

	offset = (weight + 1) * VH_NUMERIC_DEC_DIGITS - (dweight + 1);
	ndigits = (ddigits + offset + VH_NUMERIC_DEC_DIGITS - 1) / VH_NUMERIC_DEC_DIGITS;

	/*
	 * Allocate space in the target numeric for this.
	 */
	if (!dest->ptr_digits)
		dest->ptr_digits = alloc_ctx(dest, VH_NUMERIC_NDIGITS_SZ(ndigits));
	else if (VH_NUMERIC_NDIGITS(dest) < ndigits)
		dest->ptr_digits = vhrealloc(dest->ptr_digits, VH_NUMERIC_NDIGITS_SZ(ndigits));

	dest->flags = sign;
	dest->weight = weight;
	dest->dscale = dscale;

	i = VH_NUMERIC_DEC_DIGITS - offset;
	digits = dest->ptr_digits;

	while (ndigits-- > 0)
	{
#if VH_NUMERIC_DEC_DIGITS == 4
		*digits++ = ((decdigits[i] * 10 + decdigits[i + 1]) * 10 +
					 decdigits[i + 2] * 10) + decdigits[i + 3];
#elif VH_NUMERIC_DEC_DIGITS == 2
		*digits++ = decdigits[i] * 10 + decdigits[i + 1];
#elif VH_NUMERIC_DEC_DIGITS == 1
		*digits++ = decdigits[i];
#else
#error unsuported VH_NUMERIC_NBASE
#endif
		i += VH_NUMERIC_DEC_DIGITS;
	}

	vhfree(decdigits);

	return true;
}

static char* 
type_numeric_tam_cstr_get(struct TamCStrGetStack *tamstack,
						  CStrAMOptions copts,
						  const void *src, char *tgt,
						  size_t *length, size_t *cursor,
						  void *formatter)
{
	const NumericData *num = src;
	char buffer[256];
	char *cp = tgt;
	size_t outlen;
	bool quote = ((uintptr_t)formatter == (uintptr_t)type_numeric_tam_cstr_literal);
	
	if (copts->malloc)
	{
		if (quote)
		{
			buffer[0] = '\"';
			type_numeric_tostring(num, &buffer[1], 254, &outlen);
			cp = vhmalloc(outlen + 3);

			if (outlen >= 254)
			{
				/* Set the prefix quote and offset it prior to calling the guts */
				cp[0] = '\"';
				type_numeric_tostring(num, cp + 1, outlen, &outlen);
			}
			else
			{
				/* Include the prefix quote */
				memcpy(cp, buffer, outlen + 1);
			}
			
			/* Suffix quote and null terminator */
			cp[++outlen] = '\"';
			cp[++outlen] = '\0';
		}
		else
		{
			type_numeric_tostring(num, buffer, 256, &outlen);
			cp = vhmalloc(outlen + 1);

			if (outlen >= 256)
			{
				type_numeric_tostring(num, cp, outlen, &outlen);
			}
			else
			{
				memcpy(cp, buffer, outlen);
			}

			cp[outlen] = '\0';
		}

		*length = outlen;

		if (cursor)
			*cursor = outlen;

		return cp;
	}

	if (quote)
	{
		if (!(*length))
			return false;

		cp[0] = '\"';

		if (type_numeric_tostring(num, cp + 1, *length - 1, &outlen))
		{
			if (*length > outlen + 2)
			{
				cp[++outlen] = '\"';
				cp[++outlen] = '\0';

				*length = outlen;

				if (cursor)
					*cursor = outlen;

				return cp;
			}
			else
			{
				outlen += 2;
			}
		}
	}
	else
	{		
		if (type_numeric_tostring(num, cp, *length, &outlen))
		{
			*length = outlen;
			
			if (cursor)
				*cursor = outlen;

			return cp;
		}
	}

	*length = outlen;

	if (cursor)
		*cursor = 0;

	return 0;
}

static void* 
type_numeric_tam_cstr_set(struct TamCStrSetStack *tamstack,
	 					  CStrAMOptions copts,
						  const char *src, void *tgt,
						  size_t length, size_t cursor,
						  void *formatter)
{
	Numeric num;

	if (copts->malloc)
	{
		num = vhmalloc(sizeof(NumericData));
		memset(num, 0, sizeof(NumericData));
	}
	else
	{
		num = tgt;
	}

	if (type_numeric_fromstring(src, length, num))
	{
		return num;
	}

	return 0;
}

static void* 
type_numeric_tam_cstr_fmt(Type ty, const char **patterns, int32_t n_patterns)
{
	const char *pat;

	if (n_patterns)
	{
		pat = patterns[0];

		if (strcmp(pat, "\"") == 0)
			return (void*)type_numeric_tam_cstr_literal;
	}

	return 0;
}

static void 
type_numeric_mset_get(struct TamGenStack *tamstack, void *src, void *tgt)
{
	Numeric num_src = src, num_tgt = tgt;
	size_t existing_digits, copy_digits;

	if (tamstack->copy_varlendat)
		num_tgt->hbno = num_src->hbno;

	existing_digits = VH_NUMERIC_NDIGITS(num_tgt);
	copy_digits = VH_NUMERIC_NDIGITS(num_src);

	num_tgt->size = num_src->size;
	num_tgt->flags = num_src->flags;
	num_tgt->dscale = num_src->dscale;
	num_tgt->weight = num_src->weight;

	if (existing_digits < copy_digits)
	{
		if (existing_digits)
		{
			vhfree(num_tgt->ptr_digits);
		}
		
		num_tgt->ptr_digits = alloc_ctx(num_tgt, VH_NUMERIC_NDIGITS_SZ(copy_digits));
	}

	memcpy(num_tgt->ptr_digits, num_src->ptr_digits, VH_NUMERIC_NDIGITS_SZ(copy_digits));
	VH_NUMERIC_SET_NDIGITS(num_tgt, copy_digits);
}

static void
type_numeric_mset_set(struct TamGenStack *tamstack, void *src, void *tgt)
{
	type_numeric_mset_get(tamstack, tgt, src);
}

static int32_t
type_numeric_comp(struct TomCompStack *tomstack,
				  const void *lhs, const void *rhs)
{
	const NumericData *num_lhs = lhs, *num_rhs = rhs;

	/*
	 * Make our compiler quiet.
	 */
	assert(num_lhs);
	assert(num_rhs);

	return 0;
}

static void 
type_numeric_finalize(struct TomDestructStack *tomstack, void *tgt)
{
	Numeric num = tgt;

	if (num->ptr_digits)
	{
		vhfree(num->ptr_digits);
		num->ptr_digits = 0;

		VH_NUMERIC_SET_NDIGITS(num, 0);
	}
}

/*
 * ============================================================================
 * Type Definition
 * ============================================================================
 */

struct TypeData const vh_type_numeric =
{
	.id = 36,
	.name = "numeric",
	.varlen = true,
	.size = sizeof(NumericData),
	.alignment = VHB_SIZEOF_VOID,
	.construct_forhtd = false,

	.tam = {
		.bin_get = type_numeric_tam_bin_get,
		.bin_set = type_numeric_tam_bin_set,

		.cstr_get = type_numeric_tam_cstr_get,
		.cstr_set = type_numeric_tam_cstr_set,
		.cstr_fmt = type_numeric_tam_cstr_fmt,

		.memset_set = type_numeric_mset_get,
		.memset_get = type_numeric_mset_get
	},

	.tom = {
		.comp = type_numeric_comp,
		.destruct = type_numeric_finalize
	}
};

