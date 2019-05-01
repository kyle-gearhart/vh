/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef vh_catalog_types_njson_parse_H
#define vh_catalog_types_njson_parse_H

#include "io/catalog/types/njson_scan.h"

typedef struct
{
	JsonScanExtraData scanner_yy_extra;

	Json root;
} JsonParserExtraData;

Json vh_json_strp_parser(const char *str);
Json vh_json_strp_parsern(const char *str, size_t len);

#endif

