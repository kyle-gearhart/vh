/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include <assert.h>
#include <time.h>

#include "vh.h"
#include "io/catalog/Type.h"
#include "io/catalog/types/Date.h"
#include "io/catalog/types/int.h"

#include <unicode/ucal.h>
#include <unicode/udat.h>
#include <unicode/ustring.h>

static void* type_date_cstr_fmt(Type ty, const char **patterns, int32_t n_patterns);
static void type_date_cstr_fmt_destroy(Type ty, void *fmt);
static char* type_date_cstr_get(struct TamCStrGetStack *tamstack,
 								CStrAMOptions copts,
 								const void *source,	char *target,
 								size_t *length, size_t *cursor,
							   void *formatter);
static void* type_date_cstr_set(struct TamCStrSetStack *tamstack,
	 							CStrAMOptions copts,
 								const char *source, void *target,
	 							size_t length, size_t cursor,
	 							void *formatter);
static size_t type_date_cstr_length(Type *tys, void *data, void *formatter);

const struct TypeData vh_type_Date = {
	.id = 110,
	.name = "Date",
	.size= sizeof(Date),
	.alignment = sizeof(Date),
	.varlen = false,
	.construct_forhtd = false,
	.tam = {
		.bin_get = vh_ty_int32_tam_bin_get,
		.bin_set = vh_ty_int32_tam_bin_set,
		.bin_length = vh_ty_int32_tam_bin_len,

		.cstr_fmt = type_date_cstr_fmt,
		.cstr_fmt_destroy = type_date_cstr_fmt_destroy,
		.cstr_get = type_date_cstr_get,
		.cstr_set = type_date_cstr_set,
		.cstr_length = type_date_cstr_length,
		
		.memset_get = vh_ty_int32_tam_memset_get,
		.memset_set = vh_ty_int32_tam_memset_set
	},
	.tom = { 
		.comp = vh_ty_int32_tom_comparison
	}
};

struct CStrFormatICU
{
	UDateFormat *ufmt;
	UCalendar *ucal;
};


Date 
vh_ty_date2julian(int year, int month, int day)
{
	int32_t julian;

	julian = day - 32075 + 1461 * (year + 4800 + (month - 14) / 12)
		/ 4 + 367 * (month - 2 - (month - 14) / 12 * 12) / 12 - 3 *
		((year + 4900 + (month - 14) / 12) / 100) / 4;

	return julian;
}

void 
vh_ty_julian2date(int jd, int *year, int *month, int *day)
{
	int i, j, k, l, n;

	l = jd + 68569;
	n = 4 * l / 146097;
	l = l - (146097 * n + 3) / 4;
	i = 4000 * (l + 1) / 1461001;
	l = l - 1461 * i / 4 + 31;
	j = 80 * l / 2447;
	k = l - 2447 * j / 80;
	l = j / 11;
	j = j + 2 - 12 * l;
	i = 100 * (n - 49) + i + l;

	*year = i;
	*month = j;
	*day = k;
}

Date
vh_ty_date_now(void)
{
	time_t t = time(0);
	struct tm *tm = gmtime(&t);

	return vh_ty_date2julian(1900 + tm->tm_year, 1 + tm->tm_mon, tm->tm_mday);
}

/*
 * We only want to open a UDateFormat and UCalendar object once per session.
 * There's good chance the TAM is going to get called in a loop and the loop
 * invoker should be smart enough (i.e. it's real painful if you don't) to
 * only setup the TAM stack once and then iterate over the loop with it.
 *
 * So we stand up a calendar and a formatter and keep it open.
 */
static void* 
type_date_cstr_fmt(Type ty, const char **patterns, int32_t n_patterns)
{
	UDateFormat ufmt = 0;
	UCalendar ucal = 0;
	UErrorCode uer = 0;
	UChar buffer[256];
	struct CStrFormatICU *fmt_data;

	assert(ty == &vh_type_Date);
	
	if (n_patterns)
	{
		fmt_data = vhmalloc(sizeof(struct CStrFormatICU));

		u_strFromUTF8(&buffer[0],			/* Destination */
					  256,					/* Destination capacity */
					  0,					/* Destination length */
					  patterns[0],			/* Source string */
					  -1,					/* Source length */
					  &uer);

		ufmt = udat_open(UDAT_PATTERN, 		/* timeStyle */
						 UDAT_PATTERN,		/* dateStyle */
						 uloc_getDefault(),	/* locale */
						 0,					/* timezone ID */
						 0,					/* timezone length */
						 &buffer[0],		/* pattern */
						 -1,				/* pattern length, -1 null terminated */
			 			 &uer);				/* error code */

		ucal = ucal_open(0,					/* Timezone ID, 0 for default */
						 -1,					/* Timezone ID length, -1 for null term */
						 uloc_getDefault(),		/* Locale */
						 UCAL_DEFAULT,			/* Calendar type */
						 &uer);

		fmt_data->ufmt = ufmt;
		fmt_data->ucal = ucal;

		return fmt_data;	 
	}

	return 0;
}

static void 
type_date_cstr_fmt_destroy(Type ty, void *fmt)
{
	struct CStrFormatICU *fmt_data = fmt;

	assert(ty == &vh_type_Date);

	if (fmt_data)
	{
		if (fmt_data->ufmt)
			udat_close(fmt_data->ufmt);

		if (fmt_data->ucal)
			ucal_close(fmt_data->ucal);

		vhfree(fmt_data);
	}
}

static char* 
type_date_cstr_get(struct TamCStrGetStack *tamstack,
				   CStrAMOptions copts,
				   const void *source,	char *target,
				   size_t *length, size_t *cursor,
				   void *formatter)
{
	struct CStrFormatICU *fmt_data = formatter;
	UDateFormat *ufmt = fmt_data->ufmt ? fmt_data->ufmt : 0;
	UCalendar *ucal = fmt_data->ucal ? fmt_data->ucal : 0;
	UErrorCode uer = 0;
	Date dt = *((Date*)source);
	int32_t y, m, d, fmt_len;
	UChar buffer[256];
	char *fmt;
	
	assert(ufmt);

	/*
	 * This is a lot of work for a cstr tam, but its the easiest route
	 * to getting the result we want when using ICU4C.  We open a
	 * calendar and put our date in there so we can use ICU to form the
	 * string.
	 */


	if (U_FAILURE(uer))
		elog(ERROR1, emsg("Unable to open calendar, %s", u_errorName(uer)));	 

	vh_ty_julian2date(dt, &y, &m, &d);

	ucal_setDate(ucal,							/* Calendar */
				 y,								/* Year */
				 --m,							/* Month */
				 d,								/* Day */
				 &uer);
		
	fmt_len = udat_formatCalendar(ufmt,			/* Formatter */
								  ucal,			/* UCalendar */
								  &buffer[0],	/* Result */
								  256,			/* Result length */
								  0,			/* Position */
								  &uer);				

	if (copts->malloc)
	{
		fmt = vhmalloc(fmt_len + 1);

		if (fmt_len > 256)
		{
			udat_formatCalendar(formatter, 		/* Formatter */
								ucal,			/* UCalendar */
								(UChar*)fmt,	/* Result */
								fmt_len,		/* Result length */
								0,				/* Position */
								&uer);
		}

		fmt = u_strToUTF8(fmt,
						  fmt_len + 1,
						  0,
						  &buffer[0],
						  fmt_len,
						  &uer);

		if (length)
			*length = fmt_len;

		if (cursor)
			*cursor = fmt_len;

		return fmt;
	}

	assert(length);

	if (*length >= (size_t)fmt_len)
	{
		u_strToUTF8(target,
					*length,
					0,
					&buffer[0],
					fmt_len,
					&uer);

		if (cursor)
			*cursor = fmt_len;
	}
	else if (cursor)
	{
		*cursor = 0;
	}

	*length = fmt_len;

	return 0;
}

static void* 
type_date_cstr_set(struct TamCStrSetStack *tamstack,
				   CStrAMOptions copts,
				   const char *source, void *target,
				   size_t length, size_t cursor,
				   void *formatter)
{
	struct CStrFormatICU *fmt_data = formatter;
	UDateFormat *ufmt = fmt_data->ufmt ? fmt_data->ufmt : 0;
	UCalendar *ucal = fmt_data->ucal ? fmt_data->ucal : 0;
	Date *dt = target;
	UErrorCode uer = 0;
	int32_t y, m, d, len;
	UChar buffer[128];

	u_strFromUTF8(&buffer[0],			/* Destination */
				  128,					/* Destination capacity */
				  &len,					/* Destination length */
				  source,				/* Source string */
				  (int32_t)length,		/* Source length */
				  &uer);
	
	udat_parseCalendar(ufmt,			/* Formatter */
					   ucal,			/* Calendar */
					   &buffer[0],		/* Text */
					   len,				/* Text length */
					   0,				/* Parse position */
					   &uer);

	y = ucal_get(ucal, UCAL_YEAR, &uer);
	m = ucal_get(ucal, UCAL_MONTH, &uer);
	d = ucal_get(ucal, UCAL_DATE, &uer);

	*dt = vh_ty_date2julian(y, ++m, d);

	return 0;
}

static size_t 
type_date_cstr_length(Type *tys, void *data, void *formatter)
{
	return 0;
}

