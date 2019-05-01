
#link_directories(/usr/lib)
#link_directories(/usr/local/pgsql/lib)

#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ")
#set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -pg")
#set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O0 -g -pg")

option( MMGR_DEBUG "Debug memory management" OFF )

if (MMGR_DEBUG)
add_definitions( -DVH_MMGR_DEBUG )
endif()

add_subdirectory(analytics)
add_subdirectory(be)
add_subdirectory(buffer)
add_subdirectory(catalog)
add_subdirectory(config)
add_subdirectory(executor)
add_subdirectory(nodes)
add_subdirectory(plan)
add_subdirectory(shard)
add_subdirectory(sql)
add_subdirectory(utils)

set(vh_SRCS ${vh_analytics_SRCS}
			${vh_be_SRCS}
			${vh_buffer_SRCS}
			${vh_catalog_SRCS}
			${vh_config_SRCS}
			${vh_executor_SRCS}
			${vh_nodes_SRCS}
			${vh_plan_SRCS}
			${vh_shard_SRCS}
			${vh_sql_SRCS}
			${vh_utils_SRCS}
			vh.c )

if(VHB_BE_POSTGRES)	
find_library(VHB_BE_POSTGRES_LIB NAMES pq PATHS /usr/local/pgsql/lib)
endif()


add_library(vhio 	${vh_SRCS}
					$<TARGET_OBJECTS:vh_catalog_types>)


target_link_libraries(vhio ${VHB_BE_POSTGRES_LIB})

