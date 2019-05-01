
set(vh_PATH utils)
set(vh_utils_SRCS 	${vh_PATH}/EQueue.c
					${vh_PATH}/SList.c
					${vh_PATH}/art.c
					${vh_PATH}/base64.c
					${vh_PATH}/btree.c
					${vh_PATH}/htbl.c
					${vh_PATH}/stopwatch.c
					${vh_PATH}/tcpstream.c

					${vh_PATH}/crypt/aes.c
					${vh_PATH}/crypt/base64.c
					${vh_PATH}/crypt/blowfish.c
					${vh_PATH}/crypt/md5.c
					${vh_PATH}/crypt/sha1.c
					${vh_PATH}/crypt/sha256.c

					${vh_PATH}/mmgr/Alloc.c
					${vh_PATH}/mmgr/MemoryContext.c
					${vh_PATH}/mmgr/Pool.c PARENT_SCOPE)

