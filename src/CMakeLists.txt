CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
set(CMAKE_INCLUDE_CURRENT_DIR OFF)

enable_language(C)

include_directories(/usr/local/include)
include_directories(/usr/include)
include_directories(/usr/local/pgsql/include)
include_directories(include)

include(CheckIncludeFiles)
include(CheckTypeSize)

# Check for Standard Int
check_include_files(stdint.h VHB_HAS_STDINT)
message( STATUS "Has stdint.h: " ${VHB_HAS_STDINT} )

# Check the sizeof(void*)
check_type_size("short" VHB_SIZEOF_SHORT)
check_type_size("int" VHB_SIZEOF_INT)
check_type_size("long long" VHB_SIZEOF_LONGLONG)
check_type_size("void*" VHB_SIZEOF_VOID)
message( STATUS "Pointer Size: " ${VHB_SIZEOF_VOID} )


add_definitions(-DVHB_SIZEOF_VOID=${VHB_SIZEOF_VOID})

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -pg -Wall -Wno-unused-function")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -Wall -Wno-unused-function")

message( STATUS "DEBUG BUILD FLAGS: " ${CMAKE_C_FLAGS_DEBUG})
message( STATUS "RELEASE BUILD FLAGS: " ${CMAKE_C_FLAGS_RELEASE})

project (vh)

add_subdirectory(io)
add_subdirectory(test)
#add_subdirectory(jobs)

