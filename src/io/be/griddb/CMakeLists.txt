
set(vh_PATH be/griddb)

OPTION(VHB_BE_GRIDDB OFF)

if(VHB_BE_GRIDDB)

set(vh_be_griddb_SRCS 	${vh_PATH}/griddb-beacon.c
						${vh_PATH}/griddb-connect.c
						${vh_PATH}/griddb-exec.c
						${vh_PATH}/griddb-misc.c
						${vh_PATH}/griddb.c PARENT_SCOPE)
endif()

