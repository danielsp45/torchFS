cmake_minimum_required(VERSION 3.12)
project(TorchFuseProject VERSION 0.1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find fuse3 using pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE REQUIRED fuse3)
include_directories(${FUSE_INCLUDE_DIRS})

# Optionally include additional header directories
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${CMAKE_SOURCE_DIR}/src/client)

# Collect all source files in the src/client directory
file(GLOB CLIENT_SOURCES "${CMAKE_SOURCE_DIR}/src/client/*.cc")

# Create the executable
add_executable(torchfuse ${CLIENT_SOURCES})

# Link with fuse libraries found by pkg-config
target_link_libraries(torchfuse ${FUSE_LIBRARIES})

# Pass any necessary compile definitions (for example, to set FUSE_USE_VERSION)
target_compile_definitions(torchfuse PRIVATE FUSE_USE_VERSION=31 _GNU_SOURCE)
