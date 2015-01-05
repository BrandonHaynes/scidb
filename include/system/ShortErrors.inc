/* -*- C++ -*-
 *
 *  ShortErrors.inc
 *
 *  ATTENTION: DO NOT INCLUDE THIS FILE DIRECTLY!
 *
 *  Instead you should include some file that includes this file, like
 *  ErrorCodes.h.
 *
 *  WHEN EDITING THIS FILE:
 *  1. Remember, there is no space in "cannot".
 *  2. Code numbers are ignored and will go away soon, but for now you
 *      must still put something there.
 *
 *  See http://www.drdobbs.com/the-new-c-x-macros/184401387 .
 */ 
//Name                              Code  Message text
X(SCIDB_E_NO_ERROR,                   0,  "Success")
X(SCIDB_SE_QPROC,                     1,  "Query processor error")
X(SCIDB_SE_STORAGE,                   2,  "Storage error")
X(SCIDB_SE_SYSCAT,                    3,  "System catalog error")
X(SCIDB_SE_ERRORS_MGR,                4,  "Errors manager error")
X(SCIDB_SE_UDO,                       5,  "Error in user defined object")
X(SCIDB_SE_TYPE,                      6,  "Type error")
X(SCIDB_SE_TYPESYSTEM,                7,  "Typesystem error")
X(SCIDB_SE_INTERNAL,                  8,  "Internal SciDB error")
X(SCIDB_SE_PLUGIN_MGR,                9,  "Plugin manager error")
X(SCIDB_SE_OPERATOR,                  10, "Operator error")
X(SCIDB_SE_IO,                        11, "I/O error")
X(SCIDB_SE_METADATA,                  12, "Metadata error")
X(SCIDB_SE_NO_MEMORY,                 13, "Not enough memory")
X(SCIDB_SE_EXECUTION,                 14, "Error during query execution")
X(SCIDB_SE_CONFIG,                    15, "Error in config")
X(SCIDB_SE_NETWORK,                   16, "Network error")
X(SCIDB_SE_SYNTAX,                    17, "Query syntax error")
X(SCIDB_SE_OPTIMIZER,                 18, "Query optimizer error")
X(SCIDB_SE_THREAD,                    19, "Thread sync primitive error")
X(SCIDB_SE_PARSER,                    20, "Error during query parsing")
X(SCIDB_SE_INFER_SCHEMA,              21, "Error during schema inferring")
X(SCIDB_SE_ARRAY_WRITER,              22, "Error while saving array")
X(SCIDB_SE_INJECTED_ERROR,            23, "Injected error")
X(SCIDB_SE_IMPORT_ERROR,              24, "Import error")
X(SCIDB_SE_MERGE,                     25, "Merge error")
X(SCIDB_SE_REDISTRIBUTE,              26, "Error during redistribute")
X(SCIDB_SE_TYPE_CONVERSION,           27, "Type conversion error")
X(SCIDB_SE_REPLICATION,               28, "Replication error")
