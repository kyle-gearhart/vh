
set(vh_PATH catalog/types)


#find_package(BISON 2.4 REQUIRED)
#set(BISON_OUTPUT catalog/types/njson.tab.cc)
#set(BISON_DEFINES catalog/types/njson.tab.hh)

#if(BISON_FOUND)
#	add_custom_command(
#		OUTPUT ${BISON_OUTPUT}
#		COMMAND ${BISON_EXECUTABLE}
#			--defines=${BISON_DEFINES}
#			--output=${BISON_OUTPUT}
#			catalog/types/njson.y
#		DEPENDS catalog catalog/types/njson.y
#		COMMENT "Generating njson.tab.hh, njson.tab.cc (parser)"
#	)

#	SET_SOURCE_FILES_PROPERTIES(${BISON_OUTPUT} GENERATED)

#endif(BISON_FOUND)

#find_package(FLEX 2.5 REQUIRED)
#set(FLEX_OUTPUT catalog/types/njson.yy.cc)

#if(FLEX_FOUND)
#	add_custom_command(
#		OUTPUT ${FLEX_OUTPUT}
#		COMMAND ${FLEX_EXECUTABLE}
#			--outfile=${FLEX_OUTPUT}
#		DEPENDS catalog/types/njson.l
#		COMMENT "Generating njson.yy.cc (lexer)"
#	)
#
#	SET_SOURCE_FILES_PROPERTIES(${FLEX_OUTPUT} GENERATED)

#endif(FLEX_FOUND)

find_package(BISON)
find_package(FLEX)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

BISON_TARGET(njson_parser njson.y ${CMAKE_CURRENT_BINARY_DIR}/njson_gram.c COMPILE_FLAGS "-v")
FLEX_TARGET(njson_scanner njson.l ${CMAKE_CURRENT_BINARY_DIR}/njson_scan.c)
ADD_FLEX_BISON_DEPENDENCY(njson_scanner njson_parser)

set_source_files_properties(${BISON_njson_parser_OUTPUTS} PROPERTIES GENERATED TRUE)

set(vh_catalog_types_SRCS ${BISON_njson_parser_OUTPUTS}
						  ${FLEX_njson_scanner_OUTPUTS}
						  DateTime.c
						  Range.c
						  String.c
						  array.c
						  bool.c
						  date.c
						  dbl.c
						  flt.c
						  int16.c
						  int32.c
						  int64.c
						  int8.c
						  njson.c
						  njson_parse.c
						  numeric.c)

add_library(vh_catalog_types OBJECT ${vh_catalog_types_SRCS})

