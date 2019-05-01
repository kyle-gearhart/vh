/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_analytics_nlscan_H
#define vh_analytics_nlscan_H

#include "io/analytics/nestlevel.h"
#include "io/analytics/nestidx.h"



/*
 * ============================================================================
 * Scanning Routines
 * ============================================================================
 */

typedef bool (*vh_nl_scan_cb)(NestLevel, NestIdxAccess,
							  void *user);



#endif

