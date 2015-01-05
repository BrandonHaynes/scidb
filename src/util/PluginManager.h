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

/*
 * @file PluginManager.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief A manager of plugable modules.
 *
 * Loads modules, finds symbols, manages instances.
 * May have different implementations for different OSs.
 */

#ifndef PLUGINMANAGER_H_
#define PLUGINMANAGER_H_

#include <map>

#include "util/Singleton.h"
#include "util/Mutex.h"

namespace scidb
{

struct PluginDesc
{
    void* handle;
    uint32_t major;
    uint32_t minor;
    uint32_t patch;
    uint32_t build;
};

class ListLibrariesArrayBuilder;

class PluginManager: public Singleton<PluginManager>
{
private:
    /**
     * The function finds symbol in the given module handle.
     * @param plugin a pointer of module to given by findModule method.
     * @param symbolName a name of symbol to load.
     * @return a pointer to loaded symbol.
     */
    static void* openSymbol(void* plugin, const std::string& symbolName, bool throwException = false);

    /**
     * The function finds module and symbol in it. Currently it's looking for a module in
     * plugins folder specified in config.
     * @param moduleName a name of module to load
     * @param symbolName a name of symbol to load
     * @return a pointer to loaded symbol
     */
    void* findSymbol(const std::string& moduleName, const std::string& symbolName)
    {
        return openSymbol(findModule(moduleName).handle, symbolName, true);
    }

    /**
     * The function finds module. Currently it's looking for a module in
     * plugins folder specified in config.
     * @param moduleName a name of module to load
     * @return a reference to loaded module descriptor
     */
    PluginDesc& findModule(const std::string& moduleName, bool* was = NULL);

public:
    PluginManager();

    ~PluginManager();

    /**
     * This method loads module and all user defined objects.
     * @param libraryName a name of library
     * @param registerInCatalog tells to register library in system catalog
     */
    void loadLibrary(const std::string& libraryName, bool registerInCatalog);

    /**
     * This method unloads module and all user defined objects.
     * @param libraryName a name of library
     */
    void unLoadLibrary(const std::string& libraryName);

    /**
     * Load all of the libraries that are registered in the system catalog;
     * to be called on startup.
     */
    void preLoadLibraries();

    /**
     * Iterate over all of the loaded, plugins and apply the builder to each of them. Include one entry for SciDB itself.
     * @param builder the lister object
     */
    void listPlugins(ListLibrariesArrayBuilder& builder);

    /**
     *  Get the name of the library that is currently being loaded.
     *  This is a call-back invoked by the loaded plugin, on the same thread.
     *  @return the name of the currently loaded library
     */
    const std::string loadingLibrary() {
        ScopedMutexLock cs (_mutex); //copy under lock
        return _loadingLibrary;
    }

    /**
     * Change the directory to load plugins from.
     * @param pluginsDirectory the path
     */
    void setPluginsDirectory(const std::string &pluginsDirectory);

private:
    Mutex _mutex;
    std::map<std::string, PluginDesc> _plugins;
    std::string _loadingLibrary;
    std::string _pluginsDirectory;
};


} // namespace

#endif /* PLUGINMANAGER_H_ */
