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
 * entry.cpp
 *
 *  Created on: Dec 28, 2009
 *      Author: roman.simakov@gmail.com
 */

#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/helpers/exception.h>

#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/asio.hpp>

#include <dlfcn.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <malloc.h>
#include <fstream>

#include <dense_linear_algebra/blas/initMathLibs.h>
#include "network/NetworkManager.h"
#include "system/SciDBConfigOptions.h"
#include "system/Config.h"
#include "util/JobQueue.h"
#include "util/ThreadPool.h"
#include "system/Constants.h"
#include "query/QueryProcessor.h"
#include "util/PluginManager.h"
#include "smgr/io/Storage.h"
#include "query/Parser.h"
#include <util/InjectedError.h>
#include <util/Utility.h>
#include <smgr/io/ReplicationManager.h>
#include <system/Utils.h>

// to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.entry"));

using namespace scidb;

boost::shared_ptr<ThreadPool> messagesThreadPool;

void scidb_termination_handler(int signum)
{
    NetworkManager::shutdown();
}

void runSciDB()
{
   struct sigaction action;
   action.sa_handler = scidb_termination_handler;
   sigemptyset(&action.sa_mask);
   action.sa_flags = 0;
   sigaction (SIGINT, &action, NULL);
   sigaction (SIGTERM, &action, NULL);

   Config *cfg = Config::getInstance();
   assert(cfg);

   // Configuring loggers
   const std::string& log4cxxProperties = cfg->getOption<string>(CONFIG_LOGCONF);
   if (log4cxxProperties.empty()) {
      log4cxx::BasicConfigurator::configure();
      const std::string& log_level = cfg->getOption<string>(CONFIG_LOG_LEVEL);
      log4cxx::LoggerPtr rootLogger(log4cxx::Logger::getRootLogger());
      rootLogger->setLevel(log4cxx::Level::toLevel(log_level));
   }
   else {
      log4cxx::PropertyConfigurator::configure(log4cxxProperties.c_str());
   }

   //Initialize random number generator
   //We will try to read seed from /dev/urandom and if we can't for some reason we will take time and pid as seed
   ifstream file ("/dev/urandom", ios::in|ios::binary);
   unsigned int seed;
   if (file.is_open())
   {
       const size_t size = sizeof(unsigned int);
       char buf[size];
       file.read(buf, size);
       file.close();
       seed = *reinterpret_cast<unsigned int*>(buf);
   }
   else
   {
       seed = time(0) ^ (getpid() << 8);
       LOG4CXX_WARN(logger, "Can not open /dev/urandom. srandom will be initialized with fallback seed based on time and pid.");
   }
   srandom(seed);

   LOG4CXX_INFO(logger, "Start SciDB instance (pid="<<getpid()<<"). " << SCIDB_BUILD_INFO_STRING(". "));
   LOG4CXX_INFO(logger, "Configuration:\n" << cfg->toString());

   if (cfg->getOption<int>(CONFIG_MAX_MEMORY_LIMIT) > 0)
   {
       size_t maxMem = ((int64_t) cfg->getOption<int>(CONFIG_MAX_MEMORY_LIMIT)) * MiB;
       LOG4CXX_DEBUG(logger, "Capping maximum memory:");

       struct rlimit rlim;
       if (getrlimit(RLIMIT_AS, &rlim) != 0)
       {
           LOG4CXX_DEBUG(logger, ">getrlimit call failed: " << ::strerror(errno)
                         << " (" << errno << "); memory cap not set.");
       }
       else
       {
           if (rlim.rlim_cur == RLIM_INFINITY || rlim.rlim_cur > maxMem)
           {
               rlim.rlim_cur = maxMem;
               if (setrlimit(RLIMIT_AS, &rlim) != 0)
               {
                   LOG4CXX_DEBUG(logger, ">setrlimit call failed: " << ::strerror(errno)
                                 << " (" << errno << "); memory cap not set.");
               }
               else
               {
                   LOG4CXX_DEBUG(logger, ">memory cap set to " << rlim.rlim_cur  << " bytes.");
               }
           }
           else
           {
               LOG4CXX_DEBUG(logger, ">memory cap "<<rlim.rlim_cur<<" is already under "<<maxMem<<"; not changed.");
           }
       }
   }

   std::string tmpDir = FileManager::getInstance()->getTempDir();
   // If the tmp directory does not exist, create it.
   // Note that multiple levels of directories may need to be created.
   if (tmpDir.length() == 0 || tmpDir[tmpDir.length()-1] != '/') {
       tmpDir += '/';
   }
   if (access(tmpDir.c_str(),0) != 0) {
       size_t end = 0;
       do {
           while (end < tmpDir.length() && tmpDir[end] != '/') {
               ++ end;
           }
           if (end < tmpDir.length()) {
               ++ end;
               string subdir = tmpDir.substr(0, end);
               if (access(subdir.c_str(),0) != 0) {
                   if (mkdir(subdir.c_str(), 0755) != 0) {
                       LOG4CXX_DEBUG(logger, "Could not create temp directory "
                                     << subdir << ": " << ::strerror(errno)
                                     << " (" << errno << ')');
                       throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_OPEN_FILE)
                           << subdir.c_str() << ::strerror(errno) << errno;
                   }
                   LOG4CXX_DEBUG(logger, "Created temp directory "<<subdir);
               }
           }
       } while (end<tmpDir.length());
   }

   const size_t memThreshold = Config::getInstance()->getOption<size_t>(CONFIG_MEM_ARRAY_THRESHOLD);
   std::string memArrayBasePath = tmpDir + "/memarray";

   SharedMemCache::getInstance().initSharedMemCache(memThreshold * MiB,
                                                    memArrayBasePath.c_str());

   int largeMemLimit = cfg->getOption<int>(CONFIG_LARGE_MEMALLOC_LIMIT);
   if (largeMemLimit>0 && (0==mallopt(M_MMAP_MAX, largeMemLimit))) {

       LOG4CXX_WARN(logger, "Failed to set large-memalloc-limit");
   }

   size_t smallMemSize = cfg->getOption<size_t>(CONFIG_SMALL_MEMALLOC_SIZE);
   if (smallMemSize>0 && (0==mallopt(M_MMAP_THRESHOLD, smallMemSize))) {

       LOG4CXX_WARN(logger, "Failed to set small-memalloc-size");
   }

   boost::shared_ptr<JobQueue> messagesJobQueue = boost::make_shared<JobQueue>();

   // Here we can play with thread number
   // TODO: For SG operations probably we should have separate thread pool
   const uint32_t nJobs = std::max(cfg->getOption<int>(CONFIG_EXECUTION_THREADS),2);
   messagesThreadPool = make_shared<ThreadPool>(nJobs, messagesJobQueue);

   SystemCatalog* catalog = SystemCatalog::getInstance();
   const bool initializeCluster = Config::getInstance()->getOption<bool>(CONFIG_INITIALIZE);
   try
   {
       //Disable metadata upgrade in initialize mode
       catalog->connect(
           Config::getInstance()->getOption<string>(CONFIG_CATALOG),
           !initializeCluster);
   }
   catch (const std::exception &e)
   {
       LOG4CXX_ERROR(logger, "System catalog connection failed: " << e.what());
       scidb::exit(1);
   }
   int errorCode = 0;
   try
   {
       if (!catalog->isInitialized() || initializeCluster)
       {
           catalog->initializeCluster();
       }

       catalog->invalidateTempArrays();

       TypeLibrary::registerBuiltInTypes();

       FunctionLibrary::getInstance()->registerBuiltInFunctions();

       // Force preloading builtin operators
       OperatorLibrary::getInstance();

       PluginManager::getInstance()->preLoadLibraries();

       loadPrelude();  // load built in macros

       // Pull in the injected error library symbols
       InjectedErrorLibrary::getLibrary()->getError(0);
       PhysicalOperator::getInjectedErrorListener();
       ThreadPool::startInjectedErrorListener();

       ReplicationManager::getInstance()->start(messagesJobQueue);

       messagesThreadPool->start();
       NetworkManager::getInstance()->run(messagesJobQueue);
   }
   catch (const std::exception &e)
   {
       LOG4CXX_ERROR(logger, "Error during SciDB execution: " << e.what());
       errorCode = 1;
   }
   try
   {
      Query::freeQueries();
      if (messagesThreadPool) {
         messagesThreadPool->stop();
      }
      StorageManager::getInstance().close();
      ReplicationManager::getInstance()->stop();
   }
   catch (const std::exception &e)
   {
      LOG4CXX_ERROR(logger, "Error during SciDB exit: " << e.what());
      errorCode = 1;
   }
   LOG4CXX_INFO(logger, "SciDB instance. " << SCIDB_BUILD_INFO_STRING(". ") << " is exiting.");
   log4cxx::Logger::getRootLogger()->setLevel(log4cxx::Level::getOff());
   scidb::exit(errorCode);
}

void printPrefix(const char * msg="")
{
   time_t t = time(NULL);
   assert(t!=(time_t)-1);
   struct tm *date = localtime(&t);
   assert(date);
   if (date) {
      cerr << date->tm_year+1900<<"-"
           << date->tm_mon+1<<"-"
           << date->tm_mday<<" "
           << date->tm_hour<<":"
           << date->tm_min<<":"
           << date->tm_sec
           << " ";
   }
   cerr << "(ppid=" << getpid() << "): " << msg;
}
void handleFatalError(const int err, const char * msg)
{
   printPrefix(msg);
   cerr << ": "
        << err << ": "
        << strerror(err) << endl;
   scidb::exit(1);
}

int controlPipe[2];

void setupControlPipe()
{
   close(controlPipe[0]);
   close(controlPipe[1]);
   if (pipe(controlPipe)) {
      handleFatalError(errno,"pipe() failed");
   }
}

void checkPort()
{
    uint16_t n = 10;
    while (true) {
        try {
            boost::asio::io_service ioService;
            boost::asio::ip::tcp::acceptor
            testAcceptor(ioService,
                         boost::asio::ip::tcp::endpoint(
                             boost::asio::ip::tcp::v4(),
                             Config::getInstance()->getOption<int>(CONFIG_PORT)));
            testAcceptor.close();
            ioService.stop();
            return;
        } catch (const boost::system::system_error& e) {
            if ((n--) <= 0) {
                printPrefix();
                cerr << e.what() << ". Exiting." << endl;
                scidb::exit(1);
            }
        }
        sleep(1);
    }
}

void terminationHandler(int signum)
{
   unsigned char byte = 1;
   ssize_t ret = write(controlPipe[1], &byte, sizeof(byte));
   if (ret!=1){}
   printPrefix("Terminated.\n");
   // A signal handler should only call async signal-safe routines
   // _exit() is one, but exit() is not
   _exit(0);
}

void initControlPipe()
{
   controlPipe[0] = controlPipe[1] = -1;
}

void setupTerminationHandler()
{
   struct sigaction action;
   action.sa_handler = terminationHandler;
   sigemptyset(&action.sa_mask);
   action.sa_flags = 0;
   sigaction (SIGINT, &action, NULL);
   sigaction (SIGTERM, &action, NULL);
}

void handleExitStatus(int status, pid_t childPid)
{
   if (WIFSIGNALED(status)) {
      printPrefix();
      cerr << "SciDB child (pid="<<childPid<<") terminated by signal = "
           << WTERMSIG(status) << (WCOREDUMP(status)? ", core dumped" : "")
           << endl;
   }
   if (WIFEXITED(status)) {
      printPrefix();
      cerr << "SciDB child (pid="<<childPid<<") exited with status = "
           << WEXITSTATUS(status)
           << endl;
   }
}

void runWithWatchdog()
{
   setupTerminationHandler();

   uint32_t forkTimeout = 3; //sec
   uint32_t backOffFactor = 1;
   uint32_t maxBackOffFactor = 32;

   printPrefix("Started.\n");

   while (true)
   {
      checkPort();
      setupControlPipe();

      time_t forkTime = time(NULL);
      assert(forkTime > 0);

      pid_t pid = fork();

      if (pid < 0) { // error
         handleFatalError(errno,"fork() failed");
      } else if (pid > 0) { //parent

         // close the read end of the pipe
         close(controlPipe[0]);
         controlPipe[0] = -1;

         int status;
         pid_t p = wait(&status);
         if (p == -1) {
            handleFatalError(errno,"wait() failed");
         }

         handleExitStatus(status, pid);

         time_t exitTime = time(NULL);
         assert(exitTime > 0);

         if ((exitTime - forkTime) < forkTimeout) {
            sleep(backOffFactor*(forkTimeout - (exitTime - forkTime)));
            backOffFactor *= 2;
            backOffFactor = (backOffFactor < maxBackOffFactor) ? backOffFactor : maxBackOffFactor;
         } else {
            backOffFactor = 1;
         }

      }  else { //child

         //close the write end of the pipe
         close(controlPipe[1]);
         controlPipe[1] = -1;

         // connect stdin with the read end
         if (dup2(controlPipe[0], STDIN_FILENO) != STDIN_FILENO) {
            handleFatalError(errno,"dup2() failed");
         }
         if (controlPipe[0] != STDIN_FILENO) {
            close(controlPipe[0]);
            controlPipe[0] = -1;
         }

         runSciDB();
         assert(0);
      }
   }
   assert(0);
}

int main(int argc,char* argv[])
{
    struct LoggerControl : boost::noncopyable, scidb::stackonly
    {
       ~LoggerControl()
        {
            log4cxx::Logger::getRootLogger()->setLevel(log4cxx::Level::getOff());
        }
    }   loggerControl;

    try
    {
        earlyInitMathLibEnv();  // environ changes must precede multi-threading.
    }
    catch(const std::exception &e)
    {
        printPrefix();
        cerr << "Failed to initialize math lib environ: " << e.what() << endl;
        scidb::exit(1);
    }

    // need to adjust sigaction SIGCHLD ?
   try
   {
       initConfig(argc, argv);
   }
   catch (const std::exception &e)
   {
      printPrefix();
      cerr << "Failed to initialize server configuration: " << e.what() << endl;
      scidb::exit(1);
   }
   Config *cfg = Config::getInstance();

   if (cfg->getOption<bool>(CONFIG_DAEMON_MODE))
   {
      if (daemon(1, 0) == -1) {
         handleFatalError(errno,"daemon() failed");
      }
      // STDIN is /dev/null in a daemon process,
      // we need to fake it out in case we run without the watchdog
      initControlPipe();
      close(STDIN_FILENO);
      setupControlPipe();
      if (controlPipe[0]==STDIN_FILENO) {
          close(controlPipe[1]);
          controlPipe[1]=-1;
      } else {
          assert(controlPipe[1]==STDIN_FILENO);
          close(controlPipe[0]);
          controlPipe[0]=-1;
      }
   } else {
       initControlPipe();
   }

   if(cfg->getOption<bool>(CONFIG_REGISTER) ||
      cfg->getOption<bool>(CONFIG_NO_WATCHDOG)) {
      runSciDB();
      assert(0);
      scidb::exit(1);
   }
   runWithWatchdog();
   assert(0);
   scidb::exit(1);
}

