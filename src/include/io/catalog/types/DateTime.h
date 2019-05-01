/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_datacatalog_types_DateTime_H
#define vh_datacatalog_types_DateTime_H

#include "io/catalog/types/Range.h"

#define SECS_PER_YEAR		(36525 * 864)
#define SECS_PER_DAY		86400
#define SECS_PER_HOUR		3600
#define SECS_PER_MINUTE		60
#define MINS_PER_HOUR		60

#define USECS_PER_DAY		86400000000LL
#define USECS_PER_HOUR		3600000000LL
#define USECS_PER_MINUTE	60000000LL
#define USECS_PER_SEC		1000000LL

typedef int64_t DateTime;

typedef struct DateTimeRangeData
{
	DateTime start;
	DateTime end;
	RangeFlags flags;
} *DateTimeRange;

struct DateTimeSplit
{
	int32_t seconds;
	int32_t minutes;
	int32_t hour;
	int32_t month_day;
	int32_t month;
	int32_t year;
};

DateTime vh_ty_ts2datetime(struct DateTimeSplit *dts);
void vh_ty_datetime2ts(struct DateTimeSplit *dts, DateTime dt);

DateTime vh_ty_datetime_now(void);

#define vh_GetDateTimeNm(htp, fname)											\
	(*((DateTime*)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, fname)))
#define vh_GetDateTimeRangeNm(htp, fname)										\
	((DateTimeRange)vh_getptrnm(htp, VH_HT_FLAG_MUTABLE, fname))
#define vh_GetDateTimeRangeImNm(htp, fname)										\
	((const struct DateTimeRangeData*)vh_getptrnm(htp, 0, fname))

#endif

