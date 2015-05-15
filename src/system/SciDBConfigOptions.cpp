/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include "stdint.h"

#include "system/Config.h"
#include "system/Constants.h"
#include "SciDBConfigOptions.h"
#include <unistd.h>

using namespace std;

namespace scidb
{

void configHook(int32_t configOption)
{
    switch (configOption)
    {
        case CONFIG_CONFIG:
            Config::getInstance()->setConfigFileName(
                Config::getInstance()->getOption<string>(CONFIG_CONFIG));
            break;

        case CONFIG_HELP:
            cout << "Available options:" << endl
                << Config::getInstance()->getDescription() << endl;
            exit(0);
            break;

        case CONFIG_VERSION:
            cout << SCIDB_BUILD_INFO_STRING() << endl;
            exit(0);
            break;
    }
}

void initConfig(int argc, char* argv[])
{
    Config *cfg = Config::getInstance();

    // WARNING: When using the EIC multipliers (KiB, MiB, GiB, etc.)
    // or any other size_t 64-bit value, make sure to use Config::SIZE
    // rather than Config::INTEGER.  Otherwise at runtime you'll get a
    // boost::bad_any_cast exception when reading the option value
    // (cannot extract 64-bit unsigned size_t into 32-bit signed int).

    // ANOTHER WARNING: Until my advice in ticket #3533 comment:10 is
    // heeded, you'll have to also add your new option to the
    // scidb.py.in script template.  And if your boolean option's
    // default is 0/off/false, yet another scidb.py hoop: see
    // preallocate-shared-mem for an example.  Mumble....  -mjl

    cfg->addOption
        (CONFIG_PRECISION, 'w', "precision", "PRECISION", "", scidb::Config::INTEGER,
               "Precision for printing floating point numbers. Default is 6", 6, false)
        (CONFIG_CATALOG, 'c', "catalog", "CATALOG", "", Config::STRING,
            "Catalog connection string. In order to create use utils/prepare-db.sh")
        (CONFIG_LOGCONF, 'l', "logconf", "LOG_PROPERTIES", "",
            Config::STRING, "Log4cxx properties file.", string(""), false)
        (CONFIG_COORDINATOR, 'k', "coordinator", "COORDINATOR", "", Config::BOOLEAN,
            "Option to start coordinator instance. It will works on default port or on port specified by port option.",
            false, false)
        (CONFIG_PORT, 'p', "port", "PORT", "", Config::INTEGER, "Set port for server. Default - any free port, but 1239 if coodinator.",
                0, false)
        (CONFIG_INTERFACE, 'i', "interface", "INTERFACE", "", Config::STRING, "Interface for listening connections.",
                string("0.0.0.0"), false)
        (CONFIG_REGISTER, 'r', "register", "", "", Config::BOOLEAN,
            "Register instance in system catalog.", false, false)
        (CONFIG_ASYNC_REPLICATION, 0, "async-replication", "", "", Config::BOOLEAN,
            "Asynchronous replication.", true, false)
        (CONFIG_RECOVER, 0, "recover", "", "", Config::INTEGER,
            "Recover instance.", -1, false)
        (CONFIG_REDUNDANCY, 0, "redundancy", "", "", Config::INTEGER,
            "Level of redundancy.", 0, false)
        (CONFIG_INITIALIZE, 0, "initialize", "", "", Config::BOOLEAN,
            "Initialize cluster.", false, false)
        (CONFIG_STORAGE, 's', "storage", "STORAGE", "", Config::STRING, "Storage URL.",
                string("./storage.scidb"), false)
        (CONFIG_PLUGINSDIR, 'u', "pluginsdir", "PLUGINS", "", Config::STRING, "Plugins folder.",
            string(SCIDB_INSTALL_PREFIX()) + string("/lib/scidb/plugins"), false)
        (CONFIG_SMGR_CACHE_SIZE, 'm', "smgr-cache-size", "CACHE", "", Config::INTEGER,
            "Size of storage cache (Mb).", 256, false)
        (CONFIG_CONFIG, 'f', "config", "", "", Config::STRING,
                "Instance configuration file.", string(""), false)
        (CONFIG_HELP, 'h', "help", "", "", Config::BOOLEAN, "Show this text.",
                false, false)
        (CONFIG_SPARSE_CHUNK_INIT_SIZE, 0, "sparse-chunk-init-size", "SPARSE_CHUNK_INIT_SIZE", "", Config::REAL,
            "Default density for sparse arrays (0.01 corresponds to 1% density),"
            "SciDB uses this parameter to calculate size of memory which has to be preallocated in sparse chunk,",
            DEFAULT_SPARSE_CHUNK_INIT_SIZE, false)
        (CONFIG_DENSE_CHUNK_THRESHOLD, 0, "dense-chunk-threshold", "DENSE_CHUNK_THRESHOLD", "", Config::REAL,
            "Minimal ratio of filled elements of sparse chunk.", DEFAULT_DENSE_CHUNK_THRESHOLD, false)
        (CONFIG_SPARSE_CHUNK_THRESHOLD, 0, "sparse-chunk-threshold", "SPARSE_CHUNK_THRESHOLD", "", Config::REAL,
            "Maximal ratio of filled elements of sparse chunk.", 0.1, false)
        (CONFIG_STRING_SIZE_ESTIMATION, 0, "string-size-estimation", "STRING_SIZE_ESTIMATION", "", Config::INTEGER,
            "Average string size (bytes).", DEFAULT_STRING_SIZE_ESTIMATION, false)
        (CONFIG_STORAGE_MIN_ALLOC_SIZE_BYTES, 0, "storage-min-alloc-size-bytes", "STORAGE_MIN_ALLOC_SIZE_BYTES", "", Config::INTEGER,
         "Size of minimum allocation chunk in storage file.", 512, false)
        (CONFIG_READ_AHEAD_SIZE, 0, "read-ahead-size", "READ_AHEAD_SIZE", "", Config::SIZE,
         "Total size of read ahead chunks (bytes).", 64*MiB, false)
        (CONFIG_DAEMON_MODE, 'd', "daemon-mode", "", "", Config::BOOLEAN, "Run scidb in background.",
                false, false)
        (CONFIG_MEM_ARRAY_THRESHOLD, 'a', "mem-array-threshold", "MEM_ARRAY_THRESHOLD", "", Config::SIZE,
                "Maximal size of memory used by temporary in-memory array (MiB)", DEFAULT_MEM_THRESHOLD, false)
        (CONFIG_REDIM_CHUNK_OVERHEAD_LIMIT, 0, "redim-chunk-overhead-limit-mb",
         "REDIM_CHUNK_OVERHEAD_LIMIT", "", Config::SIZE,
         "Redimension memory usage for chunk headers will be limited to this "
         "value in MiB (0 disables check)", 0*MiB, false)
        (CONFIG_CHUNK_SIZE_LIMIT, 0, "chunk-size-limit-mb",
         "CHUNK_SIZE_LIMIT", "", Config::SIZE,
         "Maximum allowable chunk size in MiB (0 disables check)", 0*MiB, false)
        (CONFIG_RESULT_PREFETCH_THREADS, 't', "result-prefetch-threads", "EXEC_THREADS", "", Config::INTEGER,
                "Number of execution threads for concurrent processing of chunks of one query", 4, false)
        (CONFIG_RESULT_PREFETCH_QUEUE_SIZE, 'q', "result-prefetch-queue-size", "PREFETCHED_CHUNKS", "", Config::INTEGER,
                "Number of prefetch chunks for each query", 4, false)
        (CONFIG_EXECUTION_THREADS, 'j', "execution-threads", "MAX_JOBS", "", Config::INTEGER,
         "Max. number of queries that can be processed in parallel", 5, false)
        (CONFIG_OPERATOR_THREADS, 'x', "operator-threads", "USED_CPU_LIMIT", "", Config::INTEGER,
                "Max. number of threads for concurrent processing of one chunk", 0, false)
        (CONFIG_MERGE_SORT_BUFFER, 0, "merge-sort-buffer", "MERGE_SORT_BUFFER", "", Config::INTEGER,
                "Maximal size for in-memory sort buffer (Mb)", 128, false)
        (CONFIG_MERGE_SORT_NSTREAMS, 0, "merge-sort-nstreams", "MERGE_SORT_NSTREAMS", "", Config::INTEGER,
                "Number of streams to merge at each level of sort", 8, false)
        (CONFIG_MERGE_SORT_PIPELINE_LIMIT, 0, "merge-sort-pipeline-limit", "MERGE_SORT_PIPELINE_LIMIT", "", Config::INTEGER,
         "Max number of outstanding sorted runs before merging", 32, false)
        (CONFIG_NETWORK_BUFFER, 'n', "network-buffer", "NETWORK_BUFFER", "", Config::INTEGER,
                "Size of memory used for network buffers (Mb)", 512, false)
        (CONFIG_ASYNC_IO_BUFFER, 0, "async-io-buffer", "ASYNC_IO_BUFFER", "", Config::INTEGER,
                "Maximal size of connection output IO queue (Mb)", 64, false)
        (CONFIG_CHUNK_RESERVE, 0, "chunk-reserve", "CHUNK_RESERVE", "", Config::INTEGER, "Percent of chunks size preallocated for adding deltas", 0, false)
        (CONFIG_ENABLE_DELTA_ENCODING, 0, "enable-delta-encoding", "ENABLE_DELTA_ENCODING", "", Config::BOOLEAN, "True if system should attempt to compute delta chunk versions", false, false)
        (CONFIG_VERSION, 'V', "version", "", "", Config::BOOLEAN, "Version.",
                false, false)
        (CONFIG_STAT_MONITOR, 0, "stat-monitor", "STAT_MONITOR", "", Config::INTEGER,
                "Statistics monitor type: 0 - none, 1 - Logger, 2 - Postgres", 0, false)
        (CONFIG_STAT_MONITOR_PARAMS, 0, "stat-monitor-params", "STAT_MONITOR_PARAMS", "STAT_MONITOR_PARAMS",
            Config::STRING, "Parameters for statistics monitor: logger name or connection string", string(""), false)
        (CONFIG_LOG_LEVEL, 0, "log-level", "LOG_LEVEL", "LOG_LEVEL", Config::STRING,
         "Level for basic log4cxx logger. Ignored if log-properties option is used. Default level is ERROR", string("error"), false)
        (CONFIG_RECONNECT_TIMEOUT, 0, "reconnect-timeout", "RECONNECT_TIMEOUT", "", Config::INTEGER, "Time in seconds to wait before re-connecting to peer(s).",
       3, false)
        (CONFIG_LIVENESS_TIMEOUT, 0, "liveness-timeout", "LIVENESS_TIMEOUT", "", Config::INTEGER, "Time in seconds to wait before declaring a network-silent instance dead.",
       120, false)
        (CONFIG_DEADLOCK_TIMEOUT, 0, "deadlock-timeout", "DEADLOCK_TIMEOUT", "", Config::INTEGER,
         "Time in seconds to wait before declaring a query deadlocked.", 30, false)
        (CONFIG_NO_WATCHDOG, 0, "no-watchdog", "NO_WATCHDOG", "", Config::BOOLEAN, "Do not start a watch-dog process.",
                false, false)
        (CONFIG_TILE_SIZE, 0, "tile-size", "TILE_SIZE", "", Config::INTEGER, "Size of tile", 10000, false)
        (CONFIG_TILES_PER_CHUNK, 0, "tiles-per-chunk", "TILES_PER_CHUNK", "", Config::INTEGER, "Number of tiles per chunk", 100, false)
        (CONFIG_SYNC_IO_INTERVAL, 0, "sync-io-interval", "SYNC_IO_INTERVAL", "", Config::INTEGER, "Interval of time for io synchronization (milliseconds)", 0, false)
        (CONFIG_IO_LOG_THRESHOLD, 0, "io-log-threshold", "IO_LOG_THRESHOLD", "", Config::INTEGER, "Duration above which ios are logged (milliseconds)", -1, false)
        (CONFIG_OUTPUT_PROC_STATS, 0, "output-proc-stats", "OUTPUT_PROC_STATS", "", Config::BOOLEAN, "Output SciDB process statistics such as virtual memory usage to stderr",
                false, false)
        (CONFIG_MAX_MEMORY_LIMIT, 0, "max-memory-limit", "MAX_MEMORY_LIMIT", "", Config::INTEGER, "Maximum amount of memory the scidb process can take up (mebibytes)", -1, false)

        (CONFIG_SMALL_MEMALLOC_SIZE, 0, "small-memalloc-size", "SMALL_MEMALLOC_SIZE", "", Config::SIZE, "Maximum size of a memory allocation request which is considered small (in bytes). Larger memory allocation requests may be allocated according to a different policy.", 64*KiB, false)

        (CONFIG_LARGE_MEMALLOC_LIMIT, 0, "large-memalloc-limit", "LARGE_MEMALLOC_LIMIT", "", Config::INTEGER, "Maximum number of large  (vs. small) memory allocations. The policy for doing large memory allocations may be different from the (default) policy used for small memory allocations. This parameter limits the number of outstanding allocations performed using the (non-default) large-size allocation policy.", std::numeric_limits<int>::max(), false)

        (CONFIG_STRICT_CACHE_LIMIT, 0, "strict-cache-limit", "STRICT_CACHE_LIMIT", "", Config::BOOLEAN, "Block thread if cache is overflown", false, false)
        (CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE, 0, "replication-receive-queue-size", "REPLICATION_RECEIVE_QUEUE_SIZE", "", Config::INTEGER, "The length of incoming replication queue (across all connections)", 64, false)
        (CONFIG_REPLICATION_SEND_QUEUE_SIZE, 0, "replication-send-queue-size", "REPLICATION_SEND_QUEUE_SIZE", "", Config::INTEGER, "The length of outgoing replication queue (across all connections)", 4, false)

        (CONFIG_SG_RECEIVE_QUEUE_SIZE, 0, "sg-receive-queue-size", "SG_RECEIVE_QUEUE_SIZE", "", Config::INTEGER, "The length of incoming sg queue (across all connections)", 8, false)
        (CONFIG_SG_SEND_QUEUE_SIZE, 0, "sg-send-queue-size", "SG_SEND_QUEUE_SIZE", "", Config::INTEGER, "The length of outgoing sg queue (across all connections)", 16, false)
        (CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT, 0, "array-emptyable-by-default", "ARRAY_EMPTYABLE_BY_DEFAULT", "", Config::BOOLEAN, "Be default arrays are emptyable", true, false)
        (CONFIG_LOAD_SCAN_BUFFER, 0, "load-scan-buffer", "LOAD_SCAN_BUFFER", "", Config::INTEGER, "Number of MB for one input buffer used in InputScanner", 1, false)
        (CONFIG_MATERIALIZED_WINDOW_THRESHOLD, 0, "materialized-window-threshhold", "MATERIALIZED_WINDOW_THRESHHOLD", "", Config::INTEGER, "Size in Mebibytes above which we will not materialize the input chunk to a window(...) operation", 128, false)
        (CONFIG_MPI_DIR, 0, "mpi-dir", "MPI_DIR", "", Config::STRING, "Location of MPI installation.", DEFAULT_MPI_DIR(), false)
        (CONFIG_MPI_IF, 0, "mpi-if", "MPI_IF", "", Config::STRING, "Network interface to use for MPI traffic", string(""), false)
        (CONFIG_MPI_TYPE, 0, "mpi-type", "MPI_TYPE", "", Config::STRING, "MPI installation type [mpich2-1.2 | mpich2-1.4].", DEFAULT_MPI_TYPE(), false)
        (CONFIG_MPI_SHM_TYPE, 0, "mpi-shm-type", "MPI_SHM_TYPE", "", Config::STRING, "MPI shared memory type [SHM | FILE].", string("SHM"), false)
        (CONFIG_CATALOG_RECONNECT_TRIES, 0, "catalog-reconnect-tries", "CONFIG_CATALOG_RECONNECT_TRIES", "", Config::INTEGER, "Count of tries of catalog reconnection", 5, false)
        (CONFIG_QUERY_MAX_SIZE, 0, "query-max-size", "CONFIG_QUERY_MAX_SIZE", "", Config::SIZE, "Max number of bytes in query string", 16*MiB, false)
        (CONFIG_REQUESTS, 0, "requests", "MAX_REQUESTS", "", Config::INTEGER,
         "Max. number of client query requests queued for execution. Any requests in excess of the limit are returned to the client with an error.", 256, false)
        (CONFIG_ENABLE_CATALOG_UPGRADE, 0, "enable-catalog-upgrade", "ENABLE_CATALOG_UPGRADE", "", Config::BOOLEAN, "Set to true to enable the automatic upgrade of SciDB catalog", false, false)
        (CONFIG_REDIMENSION_CHUNKSIZE, 0, "redimension-chunksize", "REDIMENSION_CHUNKSIZE", "", Config::SIZE, "Chunksize for internal intermediate array used in operator redimension", 10*KiB, false)
        (CONFIG_MAX_OPEN_FDS, 0, "max-open-fds", "MAX_OPEN_FDS", "", Config::INTEGER, "Maximum number of fds that will be opened by the storage manager at once", 256, false)
        (CONFIG_PREALLOCATE_SHARED_MEM, 0, "preallocate-shared-mem", "PREALLOCATE_SHM", "", Config::BOOLEAN, "Make sure shared memory backing (e.g. /dev/shm) is preallocated", true, false)
        (CONFIG_INSTALL_ROOT, 0, "install_root", "INSTALL_ROOT", "", Config::STRING, "The installation directory from which SciDB runs", string(SCIDB_INSTALL_PREFIX()), false)
        (CONFIG_INPUT_DOUBLE_BUFFERING, 0, "input-double-buffering", "INPUT_DOUBLE_BUFFERING", "", Config::BOOLEAN,
         "Use double buffering where possible in input and load operators", true, false)
        ;

    cfg->addHook(configHook);

    cfg->parse(argc, argv, "");

    // By default redefine coordinator's port to 1239.
    if (!cfg->optionActivated(CONFIG_PORT) && cfg->getOption<bool>(CONFIG_COORDINATOR))
    {
        cfg->setOption(CONFIG_PORT, 1239);
    }
}

} // namespace
