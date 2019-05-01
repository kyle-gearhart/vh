%option noyywrap yylineno nodefault
%option reentrant
%option bison-bridge
%option prefix="json_parse_yy"

%{


	#include "vh.h"
	
	#include "njson_gram.h"
	#include "io/catalog/types/njson.h"
	#include "io/catalog/Type.h"

	#include "io/catalog/types/njson_scan.h"	

	#define YY_EXTRA_TYPE JsonScanExtraData*
	#define YYSTYPE vh_json_strp_YYSTYPE
%}


UnicodeEscapeSequence		u[0-9A-Fa-f]{4}
JSONEscapeCharacter			["\\/bfnrt]
JSONEscapeSequence			{JSONEscapeCharacter}|{UnicodeEscapeSequence}
JSONStringCharacter			[^"\\]\\{JSONEscapeSequence}
JSONString 					\"{JSONStringCharacter}*\"

%%



\"[^\"]*\"	 			{ yylval->string_v = yytext; return DOUBLE_QUOTED_STRING; }
\'[^\']*\'				{ yylval->string_v = yytext; return SINGLE_QUOTED_STRING; }
"["						{ return SQUARE_BRACKET_L; }
"]"						{ return SQUARE_BRACKET_R; }
"{"						{ return CURLY_BRACKET_L; }
"}"						{ return CURLY_BRACKET_R; }
","						{ return COMMA; }
":"						{ return COLON; }
\n						{ }
[ \t]					{ }
[-+]?[0-9]+				{ yylval->int_v = atoi(yytext); return NUMBER_I; }
[-+]?[0-9]*\.?[0-9]*([eE][-+]?[0-9]+)?  { yylval->float_v = atof(yytext); return NUMBER_F; }
true|false                              { yylval->bool_v = ( strcmp(yytext, "true") == 0 ? true : false); return BOOLEAN; }
null                                    { return NULL_T; }
.                                       {  }

%%


vh_json_strp_scan_t
vh_json_strp_scanner_init(const char *str, size_t len,
						  JsonScanExtraData *yyext)
{
	size_t slen = len;
	yyscan_t scanner = 0;

	if (yylex_init(&scanner) != 0)
	{
		elog(ERROR1,
			emsg("Fatal scanner error"));
	}

	json_parse_yyset_extra(yyext, scanner);

	yyext->scanbuf = vhmalloc(slen + 2);
	yyext->scanbuflen = slen;
	memcpy(yyext->scanbuf, str, slen);
	yyext->scanbuf[slen] = yyext->scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	yy_scan_buffer(yyext->scanbuf, slen + 2, scanner);

	return scanner;
}

void
vh_json_strp_scanner_finish(yyscan_t yyscanner)
{
}

