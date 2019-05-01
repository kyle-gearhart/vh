/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_catalog_prepcol_pctsint_h
#define vh_catalog_prepcol_pctsint_h

#include "io/catalog/prepcol/prepcol.h"

/*
 * PrepCol Timeseries Interval
 *
 * Expects a DateTime or a Date to apply an interval to.  The goal is to create
 * a bucketable data set for an index.
 */

#define VH_PCTSINT_SECONDS			0x01
#define VH_PCTSINT_MINUTES			0x02
#define VH_PCTSINT_HOURS			0x04

#define VH_PCTSINT_DAYS				0x10
#define VH_PCTSINT_DAYOFWEEK		0x11
#define VH_PCTSINT_DAYOFMONTH		0x12
#define VH_PCTSINT_DAYOFQUARTER		0x13
#define VH_PCTSINT_DAYOFYEAR		0x14

#define VH_PCTSINT_WEEKOFMONTH		0x20
#define VH_PCTSINT_WEEKOFQUARTER	0x21
#define VH_PCTSINT_WEEKOFYEAR		0x22

#define VH_PCTSINT_MONTH			0x30
#define VH_PCTSINT_MONTHOFQUARTER	0x31

#define VH_PCTSINT_YEAR				0x40



/*
 * vh_pctsint_dt_create
 *
 * The interval is created with Date resolution.  The base is considered to be the base
 * to apply the interval to.
 */

PrepCol vh_pctsint_dt_create(TypeVar base, 
							 int32_t interval, int32_t interval_type,
							 bool lower);
PrepCol vh_pctsint_ts_create(TypeVar base, 
							 int32_t interval, int32_t interval_type,
							 bool lower);


#endif

