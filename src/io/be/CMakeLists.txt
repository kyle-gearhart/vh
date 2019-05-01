
add_subdirectory(griddb)
add_subdirectory(postgres)
add_subdirectory(sqlite)

set(vh_PATH be/)

set(vh_be_SRCS ${vh_be_postgres_SRCS}
			   ${vh_be_griddb_SRCS}
			   ${vh_be_sqlite_SRCS} PARENT_SCOPE)


