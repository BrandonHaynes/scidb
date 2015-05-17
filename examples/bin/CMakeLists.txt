set (bin_src
    bin.cpp
)

file(GLOB bin_include "*.h")

add_library(bin SHARED ${bin_src} ${bin_include})
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "libbin${CMAKE_SHARED_LIBRARY_SUFFIX}" bin)
set_target_properties(bin PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)
