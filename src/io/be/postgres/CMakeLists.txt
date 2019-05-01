
set(vh_PATH be/postgres)


OPTION(VHB_BE_POSTGRES "Build with Postgres back end integration" ON)

if(VHB_BE_POSTGRES)

set(vh_be_postgres_SRCS 	${vh_PATH}/Postgres.c
							${vh_PATH}/pgres_types.c PARENT_SCOPE)

endif()
