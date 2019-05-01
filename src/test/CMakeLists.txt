
set(vhio-test-source 	bt.c
						config.c
						hashtable.c
						main.c
						new_json.c
						operators.c
						pc_test.c
						tbe_postgres.c
						sp_test.c
						tbe_sqlite3.c
						tbe.c
						tml.c
						tnest.c
						types.c
						typevar.c
						typevaracm.c )
								


add_executable(vhio-test ${vhio-test-source})

if(VHB_BE_POSTGRES)
find_library(DB_PGSQL_LIBPQ NAMES pq PATHS /usr/local/pgsql/lib /usr/local/lib)
endif()

find_library(MATH_LIB NAMES m)
find_library(PTHREAD NAMES pthread)
find_library(DL NAMES dl)
find_library(UV_LIB NAMES uv PATHS /usr/local/lib /usr/lib)
find_library(ICU4C NAMES icui18n PATHS /usr/local/lib)
find_library(ICU4CUC NAMES icuuc PATHS /usr/local/lib)

target_link_libraries(vhio-test vhio ${DB_PGSQL_LIBPQ} m uv pthread dl icui18n icuuc)

