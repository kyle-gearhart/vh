/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_types_Date_H
#define vh_datacatalog_types_Date_H

#include "io/catalog/types/Range.h"

typedef int32_t Date;

typedef struct DateRangeData
{
	Date start;
	Date end;
	RangeFlags flags;
} *DateRange;

Date vh_ty_date2julian(int year, int month, int day);
void vh_ty_julian2date(int jd, int *year, int *month, int *day);
Date vh_ty_date_now(void);

#define vh_ty_date_day(dt)	( 1 == 1 ?											\
		( {																		\
		  	int32_t y, m, d;													\
		  	vh_ty_julian2date((dt), &y, &m, &d);								\
		  	d;																	\
		  } ) : 0 )
#define vh_ty_date_month(dt)	( 1 == 1 ? 										\
		( {																		\
		  	int32_t y, m, d;													\
		  	vh_ty_julian2date((dt), &y, &m, &d);								\
		  	m;																	\
		  } ) : 0 )
#define vh_ty_date_year(dt)	( 1 == 1 ? 											\
		( {																		\
		  	int32_t y, m, d;													\
		  	vh_ty_julian2date((dt), &y, &m, &d);								\
		  	y;																	\
		  } ) : 0 )

#define vh_GetDateNm(htp, fname)												\
	(*((Date*)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, fname)))
#define vh_ReadDateNm(htp, fname)												\
	(*((const Date*)vh_getptrnm(htp, 											\
								VH_HT_FLAGMUTABLE | VH_HB_HT_READONLY, 			\
								fname)))
#define vh_ReadDateImNm(htp, fname)												\
	(*((const Date*)vh_getptrnm(htp, 0, fname)))


#define vh_GetDateRangeNm(htp, fname)											\
	((DateRange)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, fname))
#define vh_ReadDateRangeNm(htp, fname)											\
	(*((const struct DateRangeData*)vh_getptrnm(htp,							\
												VH_HT_FLAGMUTABLE | VH_HB_HT_READONLY,	\
												fname)))
#define vh_ReadDateRangeImNm(htp, fname)										\
	((const struct DateRangeData*)vh_getptrnm(htp, 0, fname))

#endif

