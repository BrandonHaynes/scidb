if(APPLE)
    set(installed_binaries
        ${CMAKE_INSTALL_PREFIX}/bin/iquery
        ${CMAKE_INSTALL_PREFIX}/bin/scidbtestharness
    )
    
    foreach(binary ${installed_binaries})
        message(STATUS "Fixing install_name for: " ${binary})
        set(arguments "")
        set(arguments ${arguments} "-change")
        set(arguments ${arguments} "@executable_path/libscidbclient.dylib")
        set(arguments ${arguments} "${CMAKE_INSTALL_PREFIX}/lib/libscidbclient.dylib")
        set(arguments ${arguments} ${binary})
        execute_process(COMMAND install_name_tool ${arguments})
    endforeach()
endif()