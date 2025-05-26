cmake_minimum_required(VERSION 3.12)
project(TorchFS VERSION 0.1.0 LANGUAGES CXX)

# ──────────────────────────────────────
# Global compile flags
# ──────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS  OFF)
add_definitions(-D_FILE_OFFSET_BITS=64)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ──────────────────────────────────────
# FUSE (pkg-config)
# ──────────────────────────────────────
find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE REQUIRED fuse3)
include_directories(${FUSE_INCLUDE_DIRS})

# ──────────────────────────────────────
# liberasurecode
# ──────────────────────────────────────
find_package(liberasurecode CONFIG QUIET)
if(NOT liberasurecode_FOUND)
    # Manual detection for vcpkg manifest mode
    message(STATUS "liberasurecode CONFIG not found, trying manual detection...")
    
    # Look in vcpkg manifest mode installation paths
    set(VCPKG_INSTALLED_DIR "${CMAKE_BINARY_DIR}/vcpkg_installed")
    
    find_path(LIBERASURECODE_INCLUDE_DIR
        NAMES erasurecode.h liberasurecode.h
        PATHS 
            "${VCPKG_INSTALLED_DIR}/x64-linux/include"
            "${CMAKE_PREFIX_PATH}/include"
        PATH_SUFFIXES liberasurecode
        NO_DEFAULT_PATH
    )
    
    find_library(LIBERASURECODE_LIBRARY
        NAMES erasurecode liberasurecode
        PATHS 
            "${VCPKG_INSTALLED_DIR}/x64-linux/lib"
            "${CMAKE_PREFIX_PATH}/lib"
        NO_DEFAULT_PATH
    )
    
    # Look for additional required libraries
    find_library(XOR_HD_LIBRARY
        NAMES Xorcode xorcode
        PATHS 
            "${VCPKG_INSTALLED_DIR}/x64-linux/lib"
            "${CMAKE_PREFIX_PATH}/lib"
        NO_DEFAULT_PATH
    )
    
    find_library(JERASURE_LIBRARY
        NAMES Jerasure jerasure
        PATHS 
            "${VCPKG_INSTALLED_DIR}/x64-linux/lib"
            "${CMAKE_PREFIX_PATH}/lib"
        NO_DEFAULT_PATH
    )
    
    find_library(GF_COMPLETE_LIBRARY
        NAMES gf_complete
        PATHS 
            "${VCPKG_INSTALLED_DIR}/x64-linux/lib"
            "${CMAKE_PREFIX_PATH}/lib"
        NO_DEFAULT_PATH
    )
    
    if(LIBERASURECODE_INCLUDE_DIR AND LIBERASURECODE_LIBRARY)
        message(STATUS "Found liberasurecode:")
        message(STATUS "  Include dir: ${LIBERASURECODE_INCLUDE_DIR}")
        message(STATUS "  Library: ${LIBERASURECODE_LIBRARY}")
        
        # Collect all required libraries
        set(LIBERASURECODE_LIBRARIES ${LIBERASURECODE_LIBRARY})
        
        if(XOR_HD_LIBRARY)
            message(STATUS "  XOR HD Library: ${XOR_HD_LIBRARY}")
            list(APPEND LIBERASURECODE_LIBRARIES ${XOR_HD_LIBRARY})
        endif()
        
        if(JERASURE_LIBRARY)
            message(STATUS "  Jerasure Library: ${JERASURE_LIBRARY}")
            list(APPEND LIBERASURECODE_LIBRARIES ${JERASURE_LIBRARY})
        endif()
        
        if(GF_COMPLETE_LIBRARY)
            message(STATUS "  GF Complete Library: ${GF_COMPLETE_LIBRARY}")
            list(APPEND LIBERASURECODE_LIBRARIES ${GF_COMPLETE_LIBRARY})
        endif()
        
        # Create an imported target for consistency
        add_library(liberasurecode::liberasurecode INTERFACE IMPORTED)
        set_target_properties(liberasurecode::liberasurecode PROPERTIES
            INTERFACE_LINK_LIBRARIES "${LIBERASURECODE_LIBRARIES}"
            INTERFACE_INCLUDE_DIRECTORIES "${LIBERASURECODE_INCLUDE_DIR}"
        )
    else()
        message(FATAL_ERROR "liberasurecode not found! 
        Searched in: ${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/
        Please check your vcpkg.json and ensure liberasurecode is installed.")
    endif()
endif()

# ──────────────────────────────────────
# Dependencies from vcpkg
# ──────────────────────────────────────
find_package(gflags           CONFIG REQUIRED)
find_package(unofficial-brpc  CONFIG REQUIRED)
find_package(unofficial-braft CONFIG REQUIRED)
find_package(RocksDB          CONFIG REQUIRED)
find_package(protobuf         CONFIG REQUIRED)   # vcpkg’s protobuf 3.11.x

# ──────────────────────────────────────
# Generate protos at build-time
# ──────────────────────────────────────
file(GLOB PROTO_FILES "src/common/proto/*.proto")
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# ──────────────────────────────────────
# Source lists
# ──────────────────────────────────────
file(GLOB CLIENT_SOURCES   src/client/*.cc)
file(GLOB METADATA_SOURCES src/metadata/*.cc)
file(GLOB STORAGE_SOURCES  src/storage/*.cc)
file(GLOB COMMON_SOURCES   src/common/*.cc)

# ──────────────────────────────────────
# torchfs (FUSE client) binary
# ──────────────────────────────────────
add_executable(torchfs
    ${CLIENT_SOURCES}
    ${COMMON_SOURCES}
    ${PROTO_SRCS})

target_include_directories(torchfs PRIVATE
    src/include
    ${CMAKE_CURRENT_BINARY_DIR})  # Include path for generated protobuf headers

target_compile_definitions(torchfs PRIVATE
    FUSE_USE_VERSION=30 _GNU_SOURCE _FILE_OFFSET_BITS=64)

target_link_libraries(torchfs PRIVATE
    ${FUSE_LIBRARIES}
    gflags::gflags
    unofficial::brpc::brpc-static
    protobuf::libprotoc protobuf::libprotobuf protobuf::libprotobuf-lite
    RocksDB::rocksdb
    liberasurecode::liberasurecode
)

# ──────────────────────────────────────
# metadata (Raft KV) binary
# ──────────────────────────────────────
add_executable(metadata
    ${METADATA_SOURCES}
    ${COMMON_SOURCES}
    ${PROTO_SRCS})

target_include_directories(metadata PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    src/include)

target_link_libraries(metadata PRIVATE
    gflags::gflags
    unofficial::brpc::brpc-static
    protobuf::libprotoc protobuf::libprotobuf protobuf::libprotobuf-lite
    RocksDB::rocksdb
)

# ──────────────────────────────────────
# storage (Erasure Coding) binary
# ──────────────────────────────────────

add_executable(storage
    ${STORAGE_SOURCES}
    ${COMMON_SOURCES}
    ${PROTO_SRCS})

target_include_directories(storage PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    src/include)

target_link_libraries(storage PRIVATE
    gflags::gflags
    unofficial::brpc::brpc-static
    protobuf::libprotoc protobuf::libprotobuf protobuf::libprotobuf-lite
    liberasurecode::liberasurecode
)
