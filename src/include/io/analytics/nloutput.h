/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */


#ifndef vh_analytics_nloutput_H
#define vh_analytics_nloutput_H

#include "io/analytics/nestlevel.h"
#include "io/analytics/nestidx.h"
#include "io/catalog/types/njson.h"

/*
 * ============================================================================
 * JSON Output Routines
 * ============================================================================
 */


Json vh_nlo_json(NestLevel nl, int32_t flags);

/* Match vh_nl_scan_cb so we can pop this into a scan */
bool vh_nlo_item_json(NestLevel nl, NestIdxAccess nia);

#endif

