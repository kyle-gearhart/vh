
set(vh_PATH be/sqlite)

OPTION(VHB_BE_SQLITE "Build with SQLite back end integration" ON)

if(VHB_BE_SQLITE)

set(vh_be_sqlite_SRCS 	${vh_PATH}/sqlite3_be.c
						${vh_PATH}/amalgamation/sqlite3.c PARENT_SCOPE)

endif()

