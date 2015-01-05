set (json_src
    json_reader.cpp
    json_writer.cpp
    json_value.cpp
)

file(GLOB json_include "*.h")

add_library(json_lib STATIC ${json_src} ${json_include})
