find_package(Java6)

if (NOT JAVA6_FOUND)
   message(FATAL_ERROR "Java 1.6 is currently the only supported version for building JDBC!")
endif()

set (ANT_BUILD_FILE ${CMAKE_CURRENT_SOURCE_DIR}/build.xml)
set (BUILD_DIR  ${CMAKE_CURRENT_BINARY_DIR}/build)
set (DIST_DIR   ${GENERAL_OUTPUT_DIRECTORY}/jdbc)
set (DEPS_DIR   ${CMAKE_CURRENT_BINARY_DIR}/deps)
set (REPORT_DIR ${CMAKE_CURRENT_BINARY_DIR}/report)
set (BASE_DIR ${CMAKE_CURRENT_BINARY_DIR})
set (PROTO_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/../../src/network/proto/)
set (JAR_OUTS ${DIST_DIR}/jdbctest.jar
          ${DIST_DIR}/jiquery.jar
          ${DIST_DIR}/scidb4j.jar
          ${DIST_DIR}/example.jar
          ${DIST_DIR}/scidb-example.war
)

set (JAVA_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/example/org/scidb/JDBCExample.java
    ${CMAKE_CURRENT_SOURCE_DIR}/jiquery/org/scidb/JIQuery.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/util/ByteBufferExtensions.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/util/InputStreamWithReadall.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/ResultSetWrapper.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/StatementWrapper.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/IStatementWrapper.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/Statement.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/Connection.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/Driver.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/TypeException.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/ResultSetMetaData.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/ResultSet.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/jdbc/IResultSetWrapper.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/io/network/Message.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/io/network/Network.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Chunk.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Type.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Connection.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Array.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Schema.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/WarningCallback.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/IChunk.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/EmptyChunk.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Result.java
    ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j/org/scidb/client/Error.java
    ${CMAKE_CURRENT_SOURCE_DIR}/jdbctest/org/scidb/JDBCBasicTestCase.java
    ${CMAKE_CURRENT_SOURCE_DIR}/jdbctest/org/scidb/JDBCTest.java
)

set_source_files_properties(${JDBC_SRC_COPY_FILE} PROPERTIES GENERATED TRUE)

execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/lib
                                                           ${CMAKE_CURRENT_BINARY_DIR}/lib)
execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/logging.properties
                                                           ${CMAKE_CURRENT_BINARY_DIR}/logging.properties)
execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j.spec
                                                           ${CMAKE_CURRENT_BINARY_DIR}/scidb4j.spec)
set (ENV{JAVA_HOME} "${Java6_HOME}")

add_custom_target ( jdbc_build ALL DEPENDS ${JAR_OUTS})

add_custom_command (OUTPUT ${JAR_OUTS}
		   DEPENDS ${JAVA_SOURCES}
		   COMMAND cp -r -f ${CMAKE_CURRENT_SOURCE_DIR}/example ${CMAKE_CURRENT_BINARY_DIR}\; cp -r -f ${CMAKE_CURRENT_SOURCE_DIR}/jdbctest ${CMAKE_CURRENT_BINARY_DIR}\; cp -r -f ${CMAKE_CURRENT_SOURCE_DIR}/jiquery ${CMAKE_CURRENT_BINARY_DIR}\; cp -r -f ${CMAKE_CURRENT_SOURCE_DIR}/scidb4j ${CMAKE_CURRENT_BINARY_DIR}\; export JAVA_HOME=${Java6_HOME}\; ant -f ${ANT_BUILD_FILE} -Dbasedir=${BASE_DIR} -Ddist_dir=${DIST_DIR} -Dbuild_dir=${BUILD_DIR} -Ddeps_dir=${DEPS_DIR} -Dreport_dir=${REPORT_DIR} -Dproto_dir=${PROTO_DIR} -Ddoc_dir=${DOC_DIR} configure
		   COMMAND export JAVA_HOME=${Java6_HOME}\; ant -f ${ANT_BUILD_FILE} -Dbasedir=${BASE_DIR} -Ddist_dir=${DIST_DIR} -Dbuild_dir=${BUILD_DIR} -Ddeps_dir=${DEPS_DIR} -Dreport_dir=${REPORT_DIR} -Dproto_dir=${PROTO_DIR} -Ddoc_dir=${DOC_DIR}
		   )

#-----------------------------------------------------------------------------
# To make the rpm, ant has to be called with the target name rpm (e.g. ant -f ${ANT_BUILD_FILE} rpm)
#-----------------------------------------------------------------------------

	 
