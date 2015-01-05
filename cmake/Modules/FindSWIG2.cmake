INCLUDE(FindPackageHandleStandardArgs)

set(_SWIG_PATHS
  /usr/bin
  )

find_program(SWIG_EXECUTABLE
  NAMES swig2 swig2.0
  PATHS ${_SWIG_PATHS}
   NO_DEFAULT_PATH
)

find_package_handle_standard_args(SWIG2
        REQUIRED_VARS SWIG_EXECUTABLE
        VERSION_VAR Java6_VERSION
        )
mark_as_advanced(
  SWIG_EXECUTABLE
  )