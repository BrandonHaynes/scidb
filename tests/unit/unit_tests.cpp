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

//Trickery: do not include scidbconfig.h (this is a test too)
// #define SCIDBCONFIG_H_ 1

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/TestAssert.h>

// include log4cxx header files.
#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/helpers/exception.h>

#include <libgen.h>
#include <unistd.h>

#include "system/Config.h"
#include "system/SystemCatalog.h"
#include "system/SciDBConfigOptions.h"

// enum
// {
//      CONFIG_CATALOG,
//      CONFIG_HELP,
//      CONFIG_PORT,
//      CONFIG_OPTIMIZER_TYPE,
//      CONFIG_PLUGINSDIR
//  };

// Included tests
//#include "services/catalog/catalog_unit_tests.h"

#include "query/ExpressionUnitTests.h"
//#include "smgr/delta/DeltaUnitTests.h"
#include "query/TypeLibraryUnitTests.h"
#include "query/FunctionLibraryUnitTests.h"
#include "query/optimizer/OptUnitTests.h"
#include "query/AggregateUnitTests.h"
#include "array/BitmaskUnitTests.h"
#include "query/AuxUnitTests.h"
//#include "system/ExceptionUnitTests.h"
#include "PointerRangeUnitTests.h"
#include "ArenaUnitTests.h"

using namespace std;

void configHook(int32_t configOption)
{
    switch (configOption)
    {
        case CONFIG_HELP:
            cout << "Available options:" << endl
                 << Config::getInstance()->getDescription() << endl;
            ::exit(0);
            break;
    }
}

int main(int argc,char* argv[])
{
 /* Some tests rely on the local path...*/
    {
        char s[PATH_MAX];                                // Local copy of path

        strcpy(s,argv[0]);                               // Dirname mutates s

        if (chdir(dirname(s)) != 0)                      // Failed to change dir?
        {
            cout << "WARNING: could not chdir to " << s << endl;
        }
    }
    try
    {
        log4cxx::BasicConfigurator::configure();
        log4cxx::LoggerPtr rootLogger(log4cxx::Logger::getRootLogger());
        rootLogger->setLevel(log4cxx::Level::toLevel("INFO"));

        Config *cfg = Config::getInstance();

        cfg->addOption(scidb::CONFIG_PLUGINSDIR, 'u', "pluginsdir", "PLUGINS", "", scidb::Config::STRING, "Plugins folder.",
                       string("/../../bin/plugins"), false);

        initConfig(argc, argv);
        cfg->setOption(CONFIG_PORT,0);

        SystemCatalog* catalog = SystemCatalog::getInstance();
        catalog->connect(cfg->getOption<string>(CONFIG_CATALOG), false);

        TypeLibrary::registerBuiltInTypes();
        FunctionLibrary::getInstance()->registerBuiltInFunctions();

        CppUnit::TextUi::TestRunner runner;
        CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
        runner.addTest(registry.makeTest());
        const bool wasSuccessful = runner.run("", false);
        return wasSuccessful ? 0 : 3;
    }
    catch(const std::exception& e)
    {
        cout << "Unhandled std::exception: " << e.what() << endl;
        return 1;
    }
    catch(...)
    {
        cout << "Unhandled exception" << endl;
        return 2;
    }
}
