INCLUDE(FindPackageHandleStandardArgs)

set(_JAVA_PATHS
  /usr/lib/jvm/java-1.6.0-openjdk-amd64/bin
  /usr/lib/jvm/java-1.6.0-openjdk.x86_64/bin
  )

find_program(Java_JAVA_EXECUTABLE
  NAMES java
   PATHS ${_JAVA_PATHS}
   NO_DEFAULT_PATH
)
find_program(Java_JAR_EXECUTABLE
  NAMES jar
  PATHS ${_JAVA_PATHS}
   NO_DEFAULT_PATH
)

find_program(Java_JAVAC_EXECUTABLE
  NAMES javac
  PATHS ${_JAVA_PATHS}
   NO_DEFAULT_PATH
)

find_program(Java_JAVAH_EXECUTABLE
  NAMES javah
  PATHS ${_JAVA_PATHS}
   NO_DEFAULT_PATH
)

find_program(Java_JAVADOC_EXECUTABLE
  NAMES javadoc
  PATHS ${_JAVA_PATHS}
   NO_DEFAULT_PATH
)

find_package_handle_standard_args(Java6
        REQUIRED_VARS Java_JAVA_EXECUTABLE Java_JAR_EXECUTABLE Java_JAVAC_EXECUTABLE
                      Java_JAVAH_EXECUTABLE Java_JAVADOC_EXECUTABLE
        VERSION_VAR Java6_VERSION
        )

string(REGEX REPLACE "/bin/javac$" "" Java6_HOME ${Java_JAVAC_EXECUTABLE})

mark_as_advanced(
  Java_JAVA_EXECUTABLE
  Java_JAR_EXECUTABLE
  Java_JAVAC_EXECUTABLE
  Java_JAVAH_EXECUTABLE
  Java_JAVADOC_EXECUTABLE
  )