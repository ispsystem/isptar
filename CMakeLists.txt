cmake_minimum_required (VERSION 2.6) 
 
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS -std=c++11)
set (PROJECT isptar_) 
 
project (${PROJECT})
 
set (HEADERS 
	isptar_args.h
	isptar_file.h
	isptar_gzip.h
	isptar_io.h
	isptar_misc.h
	isptar_slice.h
	isptar_tar.h
	)
 
set (SOURCES 
	isptar.cpp
	isptar_args.cpp
	isptar_file.cpp
	isptar_gzip.cpp
	isptar_io.cpp
	isptar_misc.cpp
	isptar_slice.cpp
	isptar_tar.cpp
	) 
 
add_executable (${PROJECT} ${HEADERS} ${SOURCES})
target_link_libraries(${PROJECT} -lz)
