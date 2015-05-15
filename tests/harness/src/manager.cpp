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
 * @file manager.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <iostream>
# include <vector>
# include <string>
# include <strings.h>
# include <boost/thread/thread.hpp>
# include <boost/thread/pthread/condition_variable.hpp>
# include <boost/filesystem/operations.hpp>
# include <boost/date_time/posix_time/posix_time.hpp>
# include <log4cxx/logger.h>
# include <log4cxx/ndc.h>

# include "Exceptions.h"
# include "errdb.h"
# include "manager.h"
# include "reporter.h"
# include "global.h"
# include "helper.h"
# include "interface.h"
# include "executorfactory.h"

# define LOGGER_TAG_MANAGER  "[MANAGER]"
# define LOGGER_TAG_WORKER   "WORKER"

using namespace std;
using namespace boost;
using namespace log4cxx;
using namespace scidbtestharness;
using namespace scidbtestharness::Exceptions;
namespace harnessexceptions = scidbtestharness::Exceptions;
namespace bfs = boost::filesystem;

boost::mutex communication_var_mutex;
bool command_from_manager = 0;
bool response_from_worker = 0;
bool new_job = 0;
bool job_read = 0;
bool job_done = 0;
bool job_failed = 0;
bool invalid_executor = 0;
bool terminate_on_failure = 0;
bool whole_job_completed = 0;
bool g_terminateOnFailure = 0;
ExecutorType g_executor_type;

boost::condition_variable cond, cond1;
boost::mutex mut, mut1;

boost::mutex job_string_mutex;
string g_job_string;

boost::mutex free_workers_mutex;
int free_workers=-1;

static struct ExecutionStats complete_es;
boost::mutex complete_es_mutex;

static struct InfoForExecutor g_info_forExecutor;
struct TestcaseExecutionInfo g_test_ei;
int g_test_count=0;
REPORTER *g_rptr;

static Result execute_testcase (struct InfoForExecutor &ie)
{
	pthread_t tid = pthread_self ();
	LoggerPtr logger = Logger :: getLogger (HARNESS_LOGGER_NAME);

    string tid_str = iTos ((uint64_t)tid);
    string worker_tag = LOGGER_TAG_WORKER;
	if (ie.selftesting == false)
		worker_tag = worker_tag + '[' + tid_str + ']';
	else
		worker_tag = worker_tag + "[]";

	LogString saved_context;
    LOGGER_PUSH_NDCTAG (worker_tag);

	int retValue;
	Result result = RESULT_SYSTEM_EXCEPTION;
	string result_str = "FAILED";
	long int sTime = 0, eTime = 0;
	string failureReason("Test case execution failed, Check log file.");
	boost::posix_time::ptime time_of_epoch(gregorian::date(1970,1,1));

	scidbtestharness::executors::ExecutorFactory f;
	scidbtestharness :: executors :: Executor *caseexecutor = 0;
	try
	{
		prepare_filepaths (ie, true);

		string actualrfile_backup = ie.actual_rfile + ".bak";
		string difffile_backup = ie.diff_file + ".bak";
		string logfile_backup = ie.log_file + ".bak";
		if (ie.keepPreviousRun)
		{
			/* rename all the files with extra extension .bak */
			if (Is_regular (ie.actual_rfile))
			{
				bfs::remove (actualrfile_backup);	
				bfs::rename (ie.actual_rfile, actualrfile_backup);	
			}
			if (Is_regular (ie.diff_file))
			{
				bfs::remove (difffile_backup);	
				bfs::rename (ie.diff_file, difffile_backup);	
			}
			if (Is_regular (ie.log_file))
			{
				bfs::remove (logfile_backup);	
				bfs::rename (ie.log_file, logfile_backup);	
			}
		}
		else
		{
			/* remove actual files as well as backup files */
			bfs::remove (ie.actual_rfile);
			bfs::remove (ie.diff_file);
			bfs::remove (ie.log_file);
			bfs::remove (actualrfile_backup);	
			bfs::remove (difffile_backup);	
			bfs::remove (logfile_backup);	
		}

		if ((caseexecutor = f.getExecutor (g_executor_type)) == NULL)
		{
			throw ConfigError (FILE_LINE_FUNCTION, ERR_CONFIG_INVALID_EXECUTOR_TYPE);
		}
		sTime = (boost::posix_time::microsec_clock::local_time()-time_of_epoch).total_milliseconds();
		ie.logger_name = tid_str;

		complete_es_mutex.lock();   /* lock */
		g_test_count++;
		ie.test_sequence_number = g_test_count;
		ie.tid = tid;
		ie.testID = converttoid (ie.rootDir, ie.tcfile);
		complete_es_mutex.unlock(); /* unlock */

		/* test case execution by the executor.
		 * Now all the exceptions from 'defaultexecutor' are being locally handled by it and
		 * only return code SUCCESS/FAILURE is only being returned. Hence resolving the issue of crash
		 * during pthread_mutex_destroy() at the end of harness execution. */
		retValue = caseexecutor->execute (ie);
		eTime = (boost::posix_time::microsec_clock::local_time()-time_of_epoch).total_milliseconds();
		delete caseexecutor;
		caseexecutor = 0;

		LOG4CXX_DEBUG (logger, "Executor returned : " << (retValue == SUCCESS ? "SUCCESS" : "FAILURE"));
		/* lock */
		complete_es_mutex.lock();

		boost::posix_time::ptime t_ptime=  boost::posix_time::microsec_clock::local_time();
		stringstream t_ptime_s;
		t_ptime_s << t_ptime;

		if (retValue == SUCCESS)
		{
			if (ie.record)                 // PASS
			{
				bfs::copy_file (ie.actual_rfile, ie.expected_rfile, bfs::copy_option::overwrite_if_exists);
				bfs::remove (ie.actual_rfile);
				result = RESULT_RECORDED;
				result_str = "RECORDED";
				failureReason = "";
				complete_es.testcasesPassed++;
				if (bfs::file_size(ie.expected_rfile) == 0)
				{       // Remove empty expected file
					bfs::remove (ie.expected_rfile);
				}
			}
			else
			{
				int rv;
				LOG4CXX_DEBUG (logger, "Going to compare the files now.");
				if ((rv = diff (ie.expected_rfile, ie.actual_rfile, ie.diff_file)) == DIFF_FILES_MATCH)   // PASS
				{
					LOG4CXX_DEBUG (logger, "Files Match");
					result = RESULT_PASS;
					result_str = "PASS";
					failureReason = "";
					complete_es.testcasesPassed++;
				}
				else if (rv == DIFF_FILES_DIFFER)       // FAIL
				{
					LOG4CXX_DEBUG (logger, "Files Differ");
					result = RESULT_FILES_DIFFER;
					result_str = "FILES_DIFFER";
					failureReason = "Expected output and Actual Output differ. Check .diff file.";
					complete_es.testcasesFailed++;

					if (ie.save_failures)
					{
						string t_diffFile = ie.diff_file + "_" + t_ptime_s.str() + "_.diff";
						bfs::copy_file(ie.diff_file,t_diffFile);
						string t_logFile = ie.log_file + "_" + t_ptime_s.str() + "_.log";
						bfs::copy_file(ie.log_file,t_logFile);
						string t_outFile = ie.actual_rfile + "_" + t_ptime_s.str() + "_.out";
						bfs::copy_file(ie.actual_rfile,t_outFile);
					}
				}
				else                          // SYSTEM_EXCEPTION
				{
					LOG4CXX_DEBUG (logger, "Either \"diff\" command failed or some other problem");
					result = RESULT_SYSTEM_EXCEPTION;
					result_str = "DIFF_COMMAND_FAILED";
					failureReason = "Either \"diff\" command failed or some other problem";
					complete_es.testcasesFailed++;
				}
			}
		}
		else if (retValue == ERROR_CODES_DIFFER)
		{
			result = RESULT_ERROR_CODES_DIFFER;
			result_str = "ERROR_CODES_DIFFER";
			LOG4CXX_ERROR (logger, "Test case execution failed. ERROR CODES DIFFER.");
			failureReason = "Expected error code does not match with actual error code.";
			complete_es.testcasesFailed++;
		}
		else                                   // ANY EXCEPTION, error
		{
		    /* ANY EXCEPTION, error because of which executor failed to execute the test case
 			 * primarily because errors like, problem in opening file, .test file parsing error, failure in connecting to scidb etc.*/
			result = RESULT_SYSTEM_EXCEPTION;
			result_str = "EXECUTOR_FAILED";
			LOG4CXX_ERROR (logger, "Test case execution failed. Canceling further execution of this test case. Check respective log file.");
			failureReason = "Test case execution failed. Check log file.";
			complete_es.testcasesFailed++;

			if (ie.save_failures)
			{
				string t_logFile = ie.log_file + "_" + t_ptime_s.str() + "_.log";
				bfs::copy_file(ie.log_file,t_logFile);
				string t_outFile = ie.actual_rfile + "_" + t_ptime_s.str() + "_.out";
				bfs::copy_file(ie.actual_rfile,t_outFile);
			}
		}
	} // END try

	catch (harnessexceptions :: SystemError &e)// SYSTEM_EXCEPTION
	{
		/* errors like, .test file does not exists or could not be opened etc.*/
		eTime = (boost::posix_time::microsec_clock::local_time()-time_of_epoch).total_milliseconds();
		LOG4CXX_ERROR (logger, e.what ());
		result = RESULT_SYSTEM_EXCEPTION;
		result_str = "FAILED_ON_EXCEPTION";
		LOG4CXX_ERROR (logger, "Worker failed to execute the job completely.");
		failureReason = "Worker failed to execute the job completely. Check log file.";
		complete_es.testcasesFailed++;
	}

# if 0
	catch (harnessexceptions :: ConfigError &e)// CONFIG_EXCEPTION
	{
		/* TODO : this catch() is not required now as all the exceptions from 'defaultexecutor'
 		 * are being locally handled by it and only return code SUCCESS/FAILURE is only being returned.*/
		/* errors like, connection string is not given or port number is not valid etc.*/
		eTime = (boost::posix_time::microsec_clock::local_time()-time_of_epoch).total_milliseconds();
		LOG4CXX_ERROR (logger, e.what ());
		result = RESULT_CONFIG_EXCEPTION;
		result_str = "FAILED_ON_EXCEPTION";
		LOG4CXX_ERROR (logger, "Worker failed to execute the job completely.");
		failureReason = "Worker failed to execute the job completely. Check log file.";
		complete_es.testcasesFailed++;
	}

	catch (harnessexceptions :: ExecutorError &e)// SYSTEM_EXCEPTION
	{
		/* TODO : this catch() is not required now as all the exceptions from 'defaultexecutor'
 		 * are being locally handled by it and only return code SUCCESS/FAILURE is only being returned.*/
		/* most of the exceptions thrown by CAPIs */
		eTime = (boost::posix_time::microsec_clock::local_time()-time_of_epoch).total_milliseconds();
		if (caseexecutor)
		{
			delete caseexecutor;
			caseexecutor = 0;
		}
		LOG4CXX_ERROR (logger, e.what ());
		result = RESULT_SYSTEM_EXCEPTION;
		result_str = "FAILED_ON_EXECUTOR_EXCEPTION";
		LOG4CXX_ERROR (logger, "Test case execution failed because of some Executor exception. Canceling further execution of this test case."
                               " Check respective log file.");
		failureReason = "Test case execution failed because of some Executor exception. Check log file.";
		complete_es.testcasesFailed++;
	}
# endif

	g_test_ei.testID = ie.testID;
	g_test_ei.description = "";
	g_test_ei.sTime = sTime;
	g_test_ei.eTime = eTime;
	g_test_ei.result = result;
	g_test_ei.failureReason = failureReason;

	if (ie.expected_rfile.length() > 0 && !bfs::exists (ie.expected_rfile))
		ie.expected_rfile = "";
	if (ie.actual_rfile.length() > 0 && !bfs::exists (ie.actual_rfile))
		ie.actual_rfile = "";
	if (ie.timerfile.length() > 0 && !bfs::exists (ie.timerfile))
		ie.timerfile = "";
	if (ie.diff_file.length() > 0 && !bfs::exists (ie.diff_file))
		ie.diff_file = "";
	if (ie.log_file.length() > 0 && !bfs::exists (ie.log_file))
		ie.log_file = "";

	time_t rawtime;
	time ( &rawtime );
	std::string nowStr = ctime (&rawtime);
	nowStr = nowStr.substr(0,nowStr.size()-1); // remove newline

	/* throw this line on console only if other log is going to the harness.log file */
	double duration = (eTime - sTime)/1000.0 ;
	double testSectionDuration = (ie.endTestSectionMillisec - ie.startTestSectionMillisec)/1000.0;
 	if (strcasecmp (ie.logDestination.c_str (), LOGDESTINATION_CONSOLE) != 0)
	{
		cout << "[" << ie.test_sequence_number << "][" << nowStr << "]: " << "[end]   "
		     << ie.testID << " " << result_str << " " << duration << " " << testSectionDuration << std::endl;
	}
	else
	{
		LOG4CXX_INFO (logger, ie.testID << result_str << " " << duration << "s" );
	}

	IndividualTestInfo ti(ie, g_test_ei);
	g_rptr->writeTestcaseExecutionInfo (ti);
	g_rptr->writeIntermediateRunStat (complete_es.testcasesPassed, complete_es.testcasesFailed);

	/* unlock */
	complete_es_mutex.unlock();
	LOGGER_POP_NDCTAG;

	return result;
}

static void worker_function (void)
{
	pthread_t tid = pthread_self ();
	log4cxx :: LoggerPtr logger = log4cxx :: Logger :: getLogger (HARNESS_LOGGER_NAME);

    string tid_str = iTos ((uint64_t)tid);
    string worker_tag = LOGGER_TAG_WORKER;
    worker_tag = worker_tag + '[' + tid_str + ']';

	LogString saved_context;
    LOGGER_PUSH_NDCTAG (worker_tag);

	struct InfoForExecutor local_ie;

	LOG4CXX_TRACE (logger, "Entered ...");
	for (;;)
	{
		/* wait for the new job */
		{
			communication_var_mutex.lock();	
			{
				boost::unique_lock<boost::mutex> lock(mut);
				while (!command_from_manager)
				{
					LOG4CXX_DEBUG (logger, "Going to wait for notification from manager");
					cond.wait (lock);
					LOG4CXX_DEBUG (logger, "out of wait. command_from_mgr = " << command_from_manager);
				}
			}

			if (new_job == 1)
			{
                LOG4CXX_DEBUG (logger, "Got a new_job notification");
				new_job = 0;
				command_from_manager = 0;
				//LOG4CXX_DEBUG (logger, "command_from_manager --> 0");
			}
			else if (terminate_on_failure == 1)
			{
                LOG4CXX_DEBUG (logger, "Got terminate_on_failure notification. Returning.");
				LOGGER_POP_NDCTAG;
				communication_var_mutex.unlock();	
				return;
			}
			else if (whole_job_completed == 1)
			{
                LOG4CXX_DEBUG (logger, "Got whole_job_completed notification. Returning.");
				LOGGER_POP_NDCTAG;
				communication_var_mutex.unlock();	
				return;
			}
			communication_var_mutex.unlock();	
		}

		/* read the new job */
		job_string_mutex.lock();
		local_ie = g_info_forExecutor;
		local_ie.tcfile = g_job_string;
		g_job_string = "";
		job_string_mutex.unlock();

		/* worker got allocated to the job so decrement number of free_workers */
		free_workers_mutex.lock ();
		//LOG4CXX_DEBUG (logger, "DECREMENTing free_workers=" << free_workers);
		free_workers--;
		//LOG4CXX_DEBUG (logger, "DECREMENTed to free_workers=" << free_workers);
		free_workers_mutex.unlock ();

		/* notify manager about job read */
		{
            LOG4CXX_DEBUG (logger, "read job [" << local_ie.tcfile << "]. Sending job_read notification.");
			boost::lock_guard<boost::mutex> lock(mut1);
			job_read = 1;
			response_from_worker = 1;
			cond1.notify_one ();
            LOG4CXX_DEBUG (logger, "notification job_read sent [" << local_ie.tcfile << "]");
		}

		Result r = RESULT_FILES_DIFFER;
		try
		{
			/* execute job */
            LOG4CXX_DEBUG (logger, "Executing job [" << local_ie.tcfile << "]");
			r = execute_testcase (local_ie);
		}

		catch (harnessexceptions :: ConfigError &e)// CONFIG_EXCEPTION
		{
			LOG4CXX_FATAL (logger, e.what ());
			boost::lock_guard<boost::mutex> lock(mut1);
			invalid_executor = 1;
			//cond1.notify_one ();
		}

		/* --terminate-on-failure is applicable for the test case failure only because
         * of difference in the .out and .expected files and not because of any other failure */
		if (r == RESULT_FILES_DIFFER && g_terminateOnFailure)
		{
			boost::lock_guard<boost::mutex> lock(mut1);
			job_failed = 1;

			/* TODO : probably below 2 lines are not required */
			response_from_worker = 1;
			cond1.notify_one ();

			free_workers_mutex.lock();
            LOG4CXX_DEBUG (logger, "notification job_failed sent [" << local_ie.tcfile << "]");
			free_workers_mutex.unlock();
		}

		/* notify job done successfully */
		{
			free_workers_mutex.lock();
			/* worker is idle now so increment number of free_workers */
			//LOG4CXX_DEBUG (logger, "INCREMENTing free_workers=" << free_workers);
			free_workers++;
			//LOG4CXX_DEBUG (logger, "INCREMENTed to free_workers=" << free_workers);
			free_workers_mutex.unlock();

			boost::lock_guard<boost::mutex> lock(mut1);
			job_done = 1;
            LOG4CXX_DEBUG (logger, "Sending job_done notification.");
			response_from_worker = 1;
			cond1.notify_one ();
            LOG4CXX_DEBUG (logger, "notification job_done sent [" << local_ie.tcfile << "]");
		}
	}

	LOGGER_POP_NDCTAG;
}

void MANAGER :: cleanup (void)
{
	LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_MANAGER);

	LOG4CXX_INFO (_logger, "Cleaning up by joining all the workers."); 
	if (_nWorkers <= 0)
	{
		LOG4CXX_INFO (_logger, "There are no workers in the pool. Hence no cleanup is required. Returning..."); 
		LOGGER_POP_NDCTAG;
		return;
	}

	/* notify all workers about whole_job_completed */
	{
		boost::lock_guard<boost::mutex> lock(mut);
		command_from_manager = 1;
		whole_job_completed = 1;
		free_workers_mutex.lock();
        LOG4CXX_DEBUG (_logger, "sending whole_job_completed notification to all. Currently free_workers = " << free_workers);
		free_workers_mutex.unlock();
		cond.notify_all ();
	}

	free_workers_mutex.lock();
    LOG4CXX_INFO (_logger, "joining all. Currently free_workers = " << free_workers);
	free_workers_mutex.unlock();
	join_all ();
	free_workers_mutex.lock();
    LOG4CXX_INFO (_logger, "joined all. Currently free_workers = " << free_workers);
	free_workers_mutex.unlock();

	LOGGER_POP_NDCTAG;
}

struct ExecutionStats MANAGER :: getExecutionStats (void)
{
	return complete_es;
}

int MANAGER :: runJob (vector <string> &joblist, REPORTER *rptr)
{
	int rv=SUCCESS;
    LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_MANAGER);

	if (joblist.empty ())
	{
		LOGGER_POP_NDCTAG;
		throw SystemError (FILE_LINE_FUNCTION, ERR_SYSTEM_EMPTY_JOBLIST);
	}
	/* copy values given for commandline options into a global variable */
	g_info_forExecutor = _ie;
	g_terminateOnFailure = _terminateOnFailure;
	g_executor_type = _executorType;
	g_rptr = rptr;

	free_workers_mutex.lock();
	/* initialize it only once as run_job() is being called again and again
     * and some woker threads from previous call to this function might still be running
     * hence this counter again sets to total number of workers whereas
     * it should be equal to the number of free workers at the time of call to this function.
     */
	if (free_workers == -1)
		free_workers = _nWorkers;
	free_workers_mutex.unlock();

	vector<string> :: iterator it;
	int current_job_read = 0;
	int at_the_beginning = 1;

	for (it = joblist.begin (); it != joblist.end (); )
	{
		free_workers_mutex.lock();
		if ((free_workers > 0 && at_the_beginning == 1) ||
		    (free_workers > 0 && current_job_read == 1))
		{
			at_the_beginning = 0;
			free_workers_mutex.unlock();

			/* read the job */
			job_string_mutex.lock();
			g_job_string = *it;
			LOG4CXX_DEBUG (_logger, "Read new job from the joblist");
			current_job_read = 0;
			job_string_mutex.unlock();

			/* notify any one of the free threads about new_job */
			{
				boost::lock_guard<boost::mutex> lock(mut);
				new_job = 1;
				command_from_manager = 1;
				LOG4CXX_DEBUG (_logger, "sending new_job notification [" << g_job_string << "]");
			}
			cond.notify_one ();
            LOG4CXX_DEBUG (_logger, "new_job notification sent ");
		}
		else
			free_workers_mutex.unlock();

		/* wait for job_read, job_done notifications from any worker thread */
		{
			boost::unique_lock<boost::mutex> lock(mut1);
			while (!response_from_worker)
			{
				LOG4CXX_DEBUG (_logger, "Going to wait for notification from worker");
				//LOG4CXX_DEBUG (_logger, "waiting... response_from_worker is still 0");
				cond1.wait (lock);
			}

			//LOG4CXX_DEBUG (_logger, "response_from_worker is now 1. job_read = " << job_read << "  job_done = " << job_done);

			if (invalid_executor == 1)
			{
				LOG4CXX_DEBUG (_logger, "Got invalid_executor response from worker");
				response_from_worker = 0;
				LOGGER_POP_NDCTAG;
				throw ConfigError (FILE_LINE_FUNCTION, "Workers can not proceed.");
			}

			if (job_read == 1)
			{
				LOG4CXX_DEBUG (_logger, "Got job_read response from worker");
				job_read = 0;
				response_from_worker = 0;
				current_job_read = 1;
			}

			if (job_done == 1)
			{
				LOG4CXX_DEBUG (_logger, "Got job_done response from worker");
				job_done = 0;
				response_from_worker = 0;
			}
			//LOG4CXX_DEBUG (_logger, "response_from_worker --> 0");

			if (g_terminateOnFailure && job_failed == 1)
			{
				boost::lock_guard<boost::mutex> lock(mut);
				terminate_on_failure = 1;
				command_from_manager = 1;
				//LOG4CXX_DEBUG (_logger, "command_from_manager --> 1 because of failure");

				cond.notify_all ();
				LOG4CXX_INFO (_logger, "returning and going to wait for all the worker threads to return; "
                                       "as terminate_on_failure is SET and at least one job has failed.");
				rv = FAILURE;
				break;
			}
		}

		free_workers_mutex.lock();
		if (free_workers > 0 && current_job_read)
		{
			free_workers_mutex.unlock();
			/* fetch next job only if the current job is already read by some of the thread
			 * otherwise, as in absence of current_job_read, job will be read twice,
			 * 1. when job_read = 1
			 * 2. when job_done = 1
			 * which is a wrong behaviour
			 */
			++it;
			LOG4CXX_DEBUG (_logger, "Going to read the next job from the joblist");
		}
		else
		{
			LOG4CXX_DEBUG (_logger, "Not reading the next job just yet as the workers are not free or the current job has not yet been read");
			free_workers_mutex.unlock();
		}
	} //END for (joblist)

	LOG4CXX_DEBUG (_logger, "Job list exhausted.");
# if 0
	/* wait for job_done notifications from all the busy worker threads */
	while (1)
	{
		free_workers_mutex.lock();
		if (free_workers == nWorkers)
		{
			free_workers_mutex.unlock();
			break;
		}
		free_workers_mutex.unlock();

		boost::unique_lock<boost::mutex> lock(mut1);
		while (!response_from_worker)
		{
			cond1.wait (lock);
		}

		if (job_done == 1)
		{
			job_done = 0;
			response_from_worker = 0;
		}
	}
# endif

	LOGGER_POP_NDCTAG;
	return rv;
}

void MANAGER :: createWorkgroup (int number_of_workers)
{
    LogString saved_context;
	LOGGER_PUSH_NDCTAG (LOGGER_TAG_MANAGER);

	LOG4CXX_INFO (_logger, "Creating a pool of " << number_of_workers << " worker(s)."); 
	if (_nWorkers > 0)
	{
		LOG4CXX_INFO (_logger, "Worker pool is already created with " << _nWorkers << " worker(s). Returning..."); 
		LOGGER_POP_NDCTAG;
		return;
	}

	_nWorkers = number_of_workers;
	for (int i=0; i<_nWorkers; i++)
	{
		_G.create_thread (worker_function);
	}
	LOG4CXX_INFO (_logger, "Created a pool of " << _nWorkers << " worker(s)."); 
	LOGGER_POP_NDCTAG;
}

void MANAGER :: getInfoForExecutorFromharness (const HarnessCommandLineOptions &c, ExecutorType executor_type)
{
    _ie.connectionString   = c.scidbServer;
    _ie.scidbPort          = c.scidbPort;
    _ie.rootDir            = c.rootDir;
    _ie.sleepTime          = c.sleepTime;
    _ie.logDir             = c.logDir;
    _ie.scratchDir         = c.scratchDir;
    _ie.logDestination     = c.logDestination;
    _ie.debugLevel         = c.debugLevel;
    _ie.record             = c.record;
    _ie.keepPreviousRun    = c.keepPreviousRun;
    _ie.selftesting        = c.selfTesting;
    _ie.log_queries        = c.log_queries;
    _ie.save_failures      = c.save_failures;
    _terminateOnFailure    = c.terminateOnFailure;
    _executorType          = executor_type;
}
