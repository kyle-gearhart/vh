/*
 * Copyright (c) 2011-2017, Kyle A. Gearhart
 */



#include "vh.h"
#include "io/catalog/types/njson.h"
#include "io/catalog/types/njson_parse.h"

#include "njson_gram.h"

vh_json_strp_scan_t vh_json_strp_scanner_init(const char*, size_t, struct JsonScanExtraData*);


Json
vh_json_strp_parser(const char *str)
{
	return vh_json_strp_parsern(str, strlen(str));
}

Json
vh_json_strp_parsern(const char *str, size_t len)
{
	vh_json_strp_scan_t yyscanner;
	JsonParserExtraData yyextra;
	int yyresult;

	yyscanner = vh_json_strp_scanner_init(str, len, &yyextra.scanner_yy_extra);
	yyresult = json_parse_yyparse(yyscanner);

	if (yyresult)
	{
		return 0;
	}

	return yyextra.root;
}
