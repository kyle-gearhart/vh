/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#ifndef vh_catalog_types_njson_scan_H
#define vh_catalog_types_njson_scan_H


typedef union vh_json_strp_YYSTYPE
{
	int32_t int_v;
	float float_v;
	bool bool_v;
	bool null_v;
	char *string_v;

	StringData sd;
} vh_json_strp_YYSTYPE;

typedef struct JsonScanExtraData
{
	char *scanbuf;
	size_t scanbuflen;
} JsonScanExtraData;

typedef void *vh_json_strp_scan_t;

extern int vh_json_strp_lex(vh_json_strp_YYSTYPE *lvalp, vh_json_strp_scan_t yyscanner);

#endif

