cmake_minimum_required(VERSION 3.12)
project(TorchFS VERSION 0.1.0 LANGUAGES CXX)

# Export compile commands for clangd
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Global definitions (ensures all files get it)
add_definitions(-D_FILE_OFFSET_BITS=64)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
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
add_executable(torchfs ${CLIENT_SOURCES})

# Link with fuse libraries found by pkg-config
target_link_libraries(torchfs ${FUSE_LIBRARIES})

# Pass any necessary compile definitions (for example, to set FUSE_USE_VERSION)
target_compile_definitions(torchfs PRIVATE FUSE_USE_VERSION=31 _GNU_SOURCE _FILE_OFFSET_BITS=64)
