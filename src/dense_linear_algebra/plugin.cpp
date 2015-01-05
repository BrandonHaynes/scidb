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
 * @file plugin.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of some plugin functions.
 */

#include <vector>

#include <SciDBAPI.h>
#include <mpi/MPIManager.h>
#include <system/ErrorsLibrary.h>
#include "DLAErrors.h"

using namespace scidb;

/**
 * EXPORT FUNCTIONS
 * Functions from this section will be used by LOAD LIBRARY operator.
 */
EXPORTED_FUNCTION void GetPluginVersion(uint32_t& major, uint32_t& minor, uint32_t& patch, uint32_t& build)
{
    // MUSTFIX: prior to Cheshire
    // TODO: this is wrong, because the version number should NEVER be a function ... it should be a constant
    //       so that the constant gets put into the C file objects that use it, at the time they are compiled.
    //       But that's not exposed, what is exposed is something that will change at link time.
    //       That's plain wrong.   It should be SCIDB_VERSION_MAJOR, not SCIDB_VERSION_MAJOR().
    //
    //       to fix this it should be include/system/Constants.h that is generated from a .in file ...
    //       not Constants.cpp
    //
    major = SCIDB_VERSION_MAJOR();
    minor = SCIDB_VERSION_MINOR();
    patch = SCIDB_VERSION_PATCH();
    build = SCIDB_VERSION_BUILD();

    // SciDB networking should already be set up, let's configure MPI and run its initialization
    scidb::MpiManager::getInstance()->init();
}

class Instance
{
public:
    Instance()
    {
        //register error messages
        _msg[DLA_ERROR1] = "Inconsistent data in array bitmap";
        _msg[DLA_ERROR2] = "Matrix must contain one attribute";
        _msg[DLA_ERROR3] = "Input arrays must have 2 dimensions";
        _msg[DLA_ERROR4] = "Matrix dimensions must match: %1%";
        _msg[DLA_ERROR5] = "Attribute should have double type";
        _msg[DLA_ERROR6] = "Corr accepts 'kendall', 'spearman' and 'pearson' parameters";
        _msg[DLA_ERROR7] = "Invalid matrix";
        _msg[DLA_ERROR8] = "Attribute should have double type";
        _msg[DLA_ERROR9] = "Unbounded arrays not supported";
        _msg[DLA_ERROR10] = "Matrix chunk size should match";
        _msg[DLA_ERROR11] = "Matrix origin must match";
        _msg[DLA_ERROR12] = "Failed to solve the system of linear equations";
        _msg[DLA_ERROR13] = "Request for unknown attribute";
        _msg[DLA_ERROR14] = "Specified attribute not found in array";
        _msg[DLA_ERROR15] = "Ranked attribute cannot be an empty indicator";
        _msg[DLA_ERROR16] = "Specified dimension not found in array";
        _msg[DLA_ERROR17] = "The number of samples passed to quantile must be at least 1";
        _msg[DLA_ERROR18] = "One of the input arrays contain missing observations";
        _msg[DLA_ERROR19] = "No complete element pairs";
        _msg[DLA_ERROR20] = "ssvdNorm: Matrix must contain 1 attribute";
        _msg[DLA_ERROR21] = "ssvdNorm: Matrix (vector) must contain 1 attribute";
        _msg[DLA_ERROR22] = "ssvdNorm: Argument #%1% must have exactly two dimensions";
        _msg[DLA_ERROR23] = "ssvdNorm: First argument must have same number of rows as first argument";
        _msg[DLA_ERROR24] = "ssvdNorm: Second argument must have one column";
        _msg[DLA_ERROR25] = "ssvdNorm: Third argument must have one row";
        _msg[DLA_ERROR26] = "ssvdNorm: Third argument must have same number of columns as first argument";
        _msg[DLA_ERROR27] = "ssvdNorm: Argument #%1% must have type double";
        _msg[DLA_ERROR28] = "ssvdNorm: Argument #%1% must not be nullable";
        _msg[DLA_ERROR29] = "ssvdNorm: Argument #%1% must be bounded";
        _msg[DLA_ERROR30] = "linregr:: final parameter must be \n'coefficients', \n'residuals', \n'multiple R2',  \n'adjusted R2', \n'F-statistic', \n'p-value', \n'residual stderror', \n'stderror', \n'tvalue', \n'P-statistic', \n'confidence intervals'";
        _msg[DLA_ERROR31] = "logistregr:: last parameter must be 'coefficients' or 'summary'";
        _msg[DLA_ERROR32] = "'use' = ['everything' | 'all.obs' | 'complete.obs' | 'nan.or.complete' | 'pairwise.complete.obs']";
        _msg[DLA_ERROR33] = "SVD accepts 'left', 'right' and 'values' parameters";
        _msg[DLA_ERROR34] = "corr accepts 'kendall', 'spearman' and 'pearson' parameters";
        _msg[DLA_ERROR35] = "Singular values overflow";
        _msg[DLA_ERROR36] = "Initial matrix is zero";
        _msg[DLA_ERROR37] = "Norm of some eigenvectors is equal to 0 during calculation of singular vectors or bidiagonal matrix";
        _msg[DLA_ERROR38] = "# of successes + # of failures can not be equal to 0";
        _msg[DLA_ERROR39] = "# of successes (failures) can not be less than 0";
        _msg[DLA_ERROR40] = "Non-zero chunk overlap is not supported %1%";
        _msg[DLA_ERROR41] = "ChunkInterval is too small" ;
        _msg[DLA_ERROR42] = "ChunkInterval is too large" ;
        _msg[DLA_ERROR43] = "array dimensions must be of equal size, temporarily" ;
        _msg[DLA_ERROR44] = "dimensions must start at 0" ;
        _msg[DLA_ERROR45] = "dimensions must be int or uint types, temporarily" ;
        _msg[DLA_ERROR46] = "the option string is malformed: %1%" ;
        _msg[DLA_WARNING1] = "convergence is not reached; iteration limit exceeded";
        _msg[DLA_WARNING2] = "rank deficient problem";
        _msg[DLA_WARNING3] = "the model is overparameterized and some coefficients are not identifiable";
        _msg[DLA_WARNING4] = "the chunkSize is outside the optimal range of %1% to %2%";

        scidb::ErrorsLibrary::getInstance()->registerErrors(DLANameSpace, &_msg);
    }

    ~Instance()
    {
        scidb::ErrorsLibrary::getInstance()->unregisterErrors(DLANameSpace);
    }

private:
    scidb::ErrorsLibrary::ErrorsMessages _msg;
} _instance;
