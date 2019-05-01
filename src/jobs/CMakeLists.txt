
#link_directories(/usr/lib)
#link_directories(/usr/local/pgsql/lib)

#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ")
#set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -pg")
#set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O0 -g -pg")

file(GLOB_RECURSE vhio-nest-source "*.c")


add_executable(vhio-nest ${vhio-nest-source})

find_library(POSTGRES_LIB NAMES pq PATHS /usr/local/pgsql/lib)
find_library(MATH_LIB NAMES m)
find_library(UV_LIB NAMES uv PATHS /usr/local/lib /usr/lib)
target_link_libraries(vhio-nest vhio pq m uv)
