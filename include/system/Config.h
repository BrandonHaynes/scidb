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


/**
 * @file Config.h
 *
 * @brief Wrapper around boost::programm_options and config parser which
 * consolidate command-line arguments, enviroment variables and config options.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef CONFIG_H_
#define CONFIG_H_

#include <string>
#include <boost/any.hpp>
#include <assert.h>
#include <map>
#include <stdint.h>
#include <iostream>
#include <vector>

#include "util/Singleton.h"
#include "system/Utils.h"

namespace scidb
{

enum
{
    // Plug-in builds depend on this enum, so please keep the order
    // stable to avoid a "flag day".  Add new values at the bottom, or
    // take one of the CONFIG_UNUSED_* slots.  Don't remove values,
    // instead rename to CONFIG_UNUSED_<integer_value_of_the_constant>.

    CONFIG_PRECISION,
    CONFIG_CATALOG,
    CONFIG_LOGCONF,
    CONFIG_PORT,
    CONFIG_INTERFACE,
    CONFIG_COORDINATOR,
    CONFIG_REGISTER,
    CONFIG_INITIALIZE,
    CONFIG_STORAGE,
    CONFIG_PLUGINSDIR,
    CONFIG_METADATA,
    CONFIG_SMGR_CACHE_SIZE,
    CONFIG_HELP,
    CONFIG_CONFIG,
    CONFIG_SPARSE_CHUNK_INIT_SIZE,
    CONFIG_SPARSE_CHUNK_THRESHOLD,
    CONFIG_DENSE_CHUNK_THRESHOLD,
    CONFIG_STRING_SIZE_ESTIMATION,
    CONFIG_UNUSED_18,           // keep enum values stable!
    CONFIG_UNUSED_19,           // ditto
    CONFIG_STORAGE_MIN_ALLOC_SIZE_BYTES,
    CONFIG_READ_AHEAD_SIZE,
    CONFIG_DAEMON_MODE,
    CONFIG_UNUSED_23,           // ditto
    CONFIG_RESULT_PREFETCH_THREADS,
    CONFIG_RESULT_PREFETCH_QUEUE_SIZE,
    CONFIG_VERSION,
    CONFIG_MERGE_SORT_BUFFER,
    CONFIG_MERGE_SORT_NSTREAMS,
    CONFIG_MERGE_SORT_PIPELINE_LIMIT,
    CONFIG_MEM_ARRAY_THRESHOLD,
    CONFIG_REDIM_CHUNK_OVERHEAD_LIMIT,
    CONFIG_CHUNK_SIZE_LIMIT,
    CONFIG_NETWORK_BUFFER,
    CONFIG_ASYNC_IO_BUFFER,
    CONFIG_STAT_MONITOR,
    CONFIG_STAT_MONITOR_PARAMS,
    CONFIG_LOG_LEVEL,
    CONFIG_CHUNK_RESERVE,
    CONFIG_EXECUTION_THREADS,
    CONFIG_OPERATOR_THREADS,
    CONFIG_RECONNECT_TIMEOUT,
    CONFIG_LIVENESS_TIMEOUT,
    CONFIG_DEADLOCK_TIMEOUT,
    CONFIG_REDUNDANCY,
    CONFIG_RECOVER,
    CONFIG_ASYNC_REPLICATION,
    CONFIG_NO_WATCHDOG,
    CONFIG_TILE_SIZE,
    CONFIG_TILES_PER_CHUNK,
    CONFIG_SYNC_IO_INTERVAL,
    CONFIG_IO_LOG_THRESHOLD,
    CONFIG_OUTPUT_PROC_STATS,
    CONFIG_MAX_MEMORY_LIMIT,
    CONFIG_STRICT_CACHE_LIMIT,
    CONFIG_REPART_SEQ_SCAN_THRESHOLD,
    CONFIG_REPART_ALGORITHM,
    CONFIG_REPART_DENSE_OPEN_ONCE,
    CONFIG_REPART_DISABLE_TILE_MODE,
    CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE,
    CONFIG_REPLICATION_SEND_QUEUE_SIZE,
    CONFIG_SG_RECEIVE_QUEUE_SIZE,
    CONFIG_SG_SEND_QUEUE_SIZE,
    CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT,
    CONFIG_SMALL_MEMALLOC_SIZE,
    CONFIG_LARGE_MEMALLOC_LIMIT,
    CONFIG_LOAD_SCAN_BUFFER,
    CONFIG_ENABLE_DELTA_ENCODING,
    CONFIG_MATERIALIZED_WINDOW_THRESHOLD,
    CONFIG_MPI_DIR,
    CONFIG_MPI_IF,
    CONFIG_MPI_TYPE,
    CONFIG_MPI_SHM_TYPE,
    CONFIG_CATALOG_RECONNECT_TRIES,
    CONFIG_QUERY_MAX_SIZE,
    CONFIG_REQUESTS,
    CONFIG_ENABLE_CATALOG_UPGRADE,
    CONFIG_REDIMENSION_CHUNKSIZE,
    CONFIG_MAX_OPEN_FDS,
    CONFIG_PREALLOCATE_SHARED_MEM,
    CONFIG_INSTALL_ROOT,
    CONFIG_INPUT_DOUBLE_BUFFERING
};

enum RepartAlgorithm
{
    RepartAuto = 0,
    RepartDense,
    RepartSparse
};

const char* toString(RepartAlgorithm);

template< typename Enum >
std::vector< std::string > getDefinition(size_t elementCount)
{
    SCIDB_ASSERT(elementCount > 0);
    std::vector< std::string > result;
    for(size_t i = 0; i < elementCount; ++i) {
        result.push_back(toString(static_cast<Enum>(i)));
    }
    return result;
}

class Config: public Singleton<Config>
{
public:
    typedef enum
    {
        STRING,
        INTEGER,
        REAL,
        BOOLEAN,
        STRING_LIST,
        SET,
        SIZE
    } ConfigOptionType;

    class ConfigAddOption
    {
    public:
        ConfigAddOption(Config *owner);

        ConfigAddOption& operator()(
            int32_t option,
            char shortCmdLineArg,
            const std::string &longCmdLineArg,
            const std::string &configOption,
            const std::string &envVariable,
            ConfigOptionType type,
            const std::string &description = "",
            const boost::any &value = boost::any(),
            bool required = true);

        ConfigAddOption& operator()(
            int32_t option,
            char shortCmdLineArg,
            const std::string &longCmdLineArg,
            const std::string &configOption,
            const std::string &envVariable,
            const std::vector< std::string > &envDefinition,
            const std::string &description = "",
            const boost::any &value = boost::any(),
            bool required = true);
    private:
        Config *_owner;
    };

    class ConfigOption
    {
    private:
        void init(const boost::any &value);
    public:
        ConfigOption(
            char shortCmdLineArg,
            const std::string &longCmdLineArg,
            const std::string &configOption,
            const std::string &envVariable,
            ConfigOptionType type,
            const std::string &description,
            const boost::any &value,
            bool required = true);

        ConfigOption(
            char shortCmdLineArg,
            const std::string &longCmdLineArg,
            const std::string &configOption,
            const std::string &envVariable,
            const std::vector< std::string >& envDefinition,
            const std::string &description,
            const boost::any &value,
            bool required = true);

        void setValue(const std::string&);
        void setValue(int);
        void setValue(size_t);
        void setValue(double);
        void setValue(bool);
        void setValue(const std::vector< std::string >&);
        void setValue(const boost::any &value);

        char getShortName() const
        {
            return _short;
        }

        const std::string& getLongName() const
        {
            return _long;
        }

        const std::string& getConfigName() const
        {
            return _config;
        }

        const std::string& getEnvName() const
        {
            return _env;
        }

        ConfigOptionType getType() const
        {
            return _type;
        }

        bool getRequired() const
        {
            return _required;
        }

        bool getActivated() const
        {
            return _activated;
        }

        void setActivated(bool value = true)
        {
            _activated = value;
        }

        const std::string& getDescription() const
        {
            return _description;
        }

        const boost::any& getValue() const
        {
            return _value;
        }

        std::string getValueAsString() const;

    private:
        char _short;
        std::string _long;
        std::string _config;
        std::string _env;
        ConfigOptionType _type;
        std::vector< std::string > _set;
        boost::any _value;
        bool _required;
        bool _activated;
        std::string _description;
    };

    ConfigAddOption addOption(
        int32_t option,
        char shortCmdLineArg,
        const std::string &longCmdLineArg,
        const std::string &configOption,
        const std::string &envVariable,
        ConfigOptionType type,
        const std::string &description = "",
        const boost::any &value = boost::any(),
        bool required = true);

    ConfigAddOption addOption(
        int32_t option,
        char shortCmdLineArg,
        const std::string &longCmdLineArg,
        const std::string &configOption,
        const std::string &envVariable,
        const std::vector< std::string > &envDefinition,
        const std::string &description = "",
        const boost::any &value = boost::any(),
        bool required = true);

    void addHook(void (*hook)(int32_t));

    void parse(int argc, char **argv, const char* configFileName);

    template<class T>
    const T& getOption(int32_t option)
    {
        assert(_values[option]);
        return boost::any_cast<const T&>(_values[option]->getValue());
    }

    /**
     * Retrieve the config-file-form name of a config option
     * @param[in] option one of the config option enum values
     * @return the command-line-style name of option
     */
    std::string const& getOptionName(int32_t option)
    {
        return _values[option]->getLongName();
    }

    /**
     * With this function it able to reinit config file path during parsing
     * command line arguments or environment variables inside config hooks before
     * opening default config.
     *
     * @param configFileName Path to config file
     */
    void setConfigFileName(const std::string& configFileName);

    const std::string& getDescription() const;

    const std::string& getConfigFileName() const;

    bool optionActivated(int32_t option);

    void setOption(int32_t option, const boost::any &value);

    std::string setOptionValue(std::string const& name, std::string const& value);

    std::string getOptionValue(std::string const& name);

    std::string toString();

private:
    ~Config();

    std::map<int32_t, ConfigOption*> _values;

    std::map<std::string, int32_t> _longArgToOption;

    std::vector<void (*)(int32_t)> _hooks;

    std::string _configFileName;

    std::string _description;

    friend class Singleton<Config>;
};

}
#endif /* CONFIG_H_ */
