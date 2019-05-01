
%{

#include <assert.h>

#include "vh.h"
#include "io/catalog/types/njson.h"
#include "io/catalog/Type.h"
#include "io/catalog/types/njson_parse.h"

static Type tys_bool[] = { &vh_type_bool, 0 };
static Type tys_int64[] = { &vh_type_int64, 0 };
static Type tys_string[] = { &vh_type_String, 0 };

typedef void *yyscan_t;

void json_parse_yyerror(yyscan_t yyscanner, const char *msg);

#if YYBISON
union YYSTYPE;
int json_parse_yylex(union YYSTYPE *, void *);
#endif

#define YYDEBUG 1
%}

%pure-parser
%expect 0
%lex-param {void* scanner}
%parse-param {void* scanner}
%name-prefix = "json_parse_yy"
%error-verbose

%union 
{
	int32_t int_v;
	float float_v;
	bool bool_v;
	bool null_v;
	char *string_v;

	void* jval;
	StringData sd;
}

%type<string_v> DOUBLE_QUOTED_STRING SINGLE_QUOTED_STRING
%type<int_v> NUMBER_I
%type<float_v> NUMBER_F
%type<bool_v> BOOLEAN

%token COMMA COLON
%token SQUARE_BRACKET_L SQUARE_BRACKET_R
%token CURLY_BRACKET_L CURLY_BRACKET_R
%token DOUBLE_QUOTED_STRING SINGLE_QUOTED_STRING
%token NUMBER_I NUMBER_F
%token BOOLEAN
%token NULL_T

%type <jval> object assignment_list assignment_list_opt array list list_opt pair value
%type <sd> string


%start json

%%

json: value { (*((JsonParserExtraData**)scanner))->root = $1; }	
	| object { (*((JsonParserExtraData**)scanner))->root = $1; }
	| array { (*((JsonParserExtraData**)scanner))->root = $1; }
	;

object: CURLY_BRACKET_L assignment_list_opt CURLY_BRACKET_R 
		{ 
			$$ = $2;
		} 
	;
array: SQUARE_BRACKET_L list_opt SQUARE_BRACKET_R 
		{ 
			$$ = $2; 
		} 
	;

pair: string COLON NUMBER_I
		{
			int64_t *val;

			$$ = vh_json_make_pair(tys_int64, 1, vh_str_buffer(&($1)));
			val = vh_json_typevar($$, 0);
			*val = $3;
			vh_str_finalize(&($1));
		}
	| string COLON NUMBER_F
		{
			$$ = vh_json_make_pair(tys_int64, 1, vh_str_buffer(&($1)));
			vh_str_finalize(&($1));
		}
	| string COLON BOOLEAN
		{
			bool *val;

			$$ = vh_json_make_pair(tys_bool, 1, vh_str_buffer(&($1)));
			val = vh_json_typevar($$, 0);
			*val = $3;
			vh_str_finalize(&($1));
		}
	| string COLON string
		{
			String val;

			$$ = vh_json_make_pair(tys_string, 1, vh_str_buffer(&($1)));
			val = vh_json_typevar($$, 0);
			vh_str.AssignStr(val, &($3));
			vh_str_finalize(&($1));
			vh_str_finalize(&($3));
		}
	| string COLON NULL_T
		{
			$$ = vh_json_make_pair_null(vh_str_buffer(&($1)));
			vh_str_finalize(&($1));
		}
	;
			

assignment_list_opt:
		{
			$$ = vh_json_make_object();
		}
	| assignment_list
		{
			$$ = $1;
		}
	;

assignment_list: pair
		{
			$$ = vh_json_make_object();
			vh_json_obj_add_pair($$, $1);
		}
	| string COLON array
		{
			Json jpair;

			$$ = vh_json_make_object();
			jpair = vh_json_make_pair_objarr(vh_str_buffer(&($1)), $3);
			vh_json_obj_add_pair($$, jpair);
			vh_str_finalize(&($1));
		}
	| string COLON object
		{
			Json jpair;

			$$ = vh_json_make_object();
			jpair = vh_json_make_pair_objarr(vh_str_buffer(&($1)), $3);
			vh_json_obj_add_pair($$, jpair);
			vh_str_finalize(&($1));
		}
	| assignment_list COMMA pair
		{
			vh_json_obj_add_pair($1, $3);
		}
	| assignment_list COMMA string COLON array
		{
			Json jpair;

			jpair = vh_json_make_pair_objarr(vh_str_buffer(&($3)), $5);
			vh_json_obj_add_pair($1, jpair);
			vh_str_finalize(&($3));
		}
	| assignment_list COMMA string COLON object
		{
			Json jpair;

			jpair = vh_json_make_pair_objarr(vh_str_buffer(&($3)), $5);
			vh_json_obj_add_pair($1, jpair);
			vh_str_finalize(&($3));
		}
	;

list_opt: 
		{
			$$ = vh_json_make_array();
		}
	| list
		{
			$$ = $1;
		}
	;

list: value
		{
			$$ = vh_json_make_array();
			vh_json_arr_push($$, $1);
		}
	| array
		{
			$$ = vh_json_make_array();
			vh_json_arr_push($$, $1);
		}
	| object
		{
			$$ = vh_json_make_array();
			vh_json_arr_push($$, $1);
		}
	| list COMMA value
		{
			vh_json_arr_push($1, $3);
		}
	| list COMMA array
		{
			vh_json_arr_push($1, $3);
		}
	| list COMMA object
		{
			vh_json_arr_push($1, $3);
		}
	;

string: DOUBLE_QUOTED_STRING
		{
			vh_str_init(&($$));
			vh_str.AssignN(&($$), ($1 + 1), strlen($1 + 1) - 1);
		}
	| SINGLE_QUOTED_STRING
		{
			vh_str_init(&($$));
			vh_str.AssignN(&($$), ($1 + 1), strlen($1 + 1) - 1);
		}
	;

value: NUMBER_I
	 	{
			int64_t *val;

			$$ = vh_json_make_value(tys_int64, 1);
			val = vh_json_typevar($$, 0);
			*val = $1;
		}
	| BOOLEAN
		{
			bool *val;
			
			$$ = vh_json_make_value(tys_bool, 1);
			val = vh_json_typevar($$, 0);
			*val = $1;
		}
	| string
		{
			String str;

			$$ = vh_json_make_value(tys_string, 1);
			str = vh_json_typevar($$, 0);
			vh_str.AssignStr(str, &($1));			
		}
	| NULL_T
		{
			$$ = vh_json_make_value_null();
		}
	;


%%

void json_parse_yyerror(yyscan_t yyscanner, const char *msg)
{
	elog(ERROR, emsg("JSON parse error: %s/%d", msg, 0));
}

