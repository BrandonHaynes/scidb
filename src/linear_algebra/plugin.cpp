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
 * @author james mcqueston
 *
 * @brief Implementation of some plugin functions.
 */

#include <vector>

#include <SciDBAPI.h>
#include <system/ErrorsLibrary.h>
#include "LAErrors.h"

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
}

class Instance
{
public:
    Instance()
    {
        //register error messages
        _msg[LA_ERROR1] = "Inconsistent data in array bitmap";
        _msg[LA_ERROR2] = "Matrix must contain one attribute";
        _msg[LA_ERROR3] = "Input arrays must be rectangular matrices";
        _msg[LA_ERROR4] = "Matrix dimensions must match";
        _msg[LA_ERROR5] = "Cannot specify 'left' deflation subspace argument without also supplying the 'right' argument.";
        _msg[LA_ERROR6] = "Corr accepts 'kendall', 'spearman' and 'pearson' parameters";
        _msg[LA_ERROR7] = "Invalid matrix";
        _msg[LA_ERROR8] = "Attribute should have double type";
        _msg[LA_ERROR9] = "Unbounded arrays not supported";
        _msg[LA_ERROR10] = "Matrix chunk size should match";
        _msg[LA_ERROR11] = "Matrix origin must match";
        _msg[LA_ERROR12] = "Failed to solve the system of linear equations";
        _msg[LA_ERROR13] = "Request for unknown attribute";
        _msg[LA_ERROR14] = "Specified attribute not found in array";
        _msg[LA_ERROR15] = "Ranked attribute cannot be an empty indicator";
        _msg[LA_ERROR16] = "Specified dimension not found in array";
        _msg[LA_ERROR17] = "The number of samples passed to quantile must be at least 1";
        _msg[LA_ERROR18] = "One of the input arrays contain missing observations";
        _msg[LA_ERROR19] = "No complete element pairs";
        _msg[LA_ERROR20] = "ssvdNorm: Matrix must contain 1 attribute";
        _msg[LA_ERROR21] = "ssvdNorm: Matrix (vector) must contain 1 attribute";
        _msg[LA_ERROR22] = "ssvdNorm: Argument #%1% must have exactly two dimensions";
        _msg[LA_ERROR23] = "ssvdNorm: First argument must have same number of rows as first argument";
        _msg[LA_ERROR24] = "ssvdNorm: Second argument must have one column";
        _msg[LA_ERROR25] = "ssvdNorm: Third argument must have one row";
        _msg[LA_ERROR26] = "ssvdNorm: Third argument must have same number of columns as first argument";
        _msg[LA_ERROR27] = "ssvdNorm: Argument #%1% must have type double";
        _msg[LA_ERROR28] = "ssvdNorm: Argument #%1% must not be nullable";
        _msg[LA_ERROR29] = "ssvdNorm: Argument #%1% must be bounded";
        _msg[LA_ERROR30] = "linregr:: final parameter must be \n'coefficients', \n'residuals', \n'multiple R2',  \n'adjusted R2', \n'F-statistic', \n'p-value', \n'residual stderror', \n'stderror', \n'tvalue', \n'P-statistic', \n'confidence intervals'";
        _msg[LA_ERROR31] = "logistregr:: last parameter must be 'coefficients' or 'summary'";
        _msg[LA_ERROR32] = "'use' = ['everything' | 'all.obs' | 'complete.obs' | 'nan.or.complete' | 'pairwise.complete.obs']";
        _msg[LA_ERROR33] = "SVD accepts 'left', 'right' and 'values' parameters";
        _msg[LA_ERROR34] = "corr accepts 'kendall', 'spearman' and 'pearson' parameters";
        _msg[LA_ERROR35] = "Singular values overflow";
        _msg[LA_ERROR36] = "Initial matrix is zero";
        _msg[LA_ERROR37] = "Norm of some eigenvectors is equal to 0 during calculation of singular vectors or bidiagonal matrix";
        _msg[LA_ERROR38] = "# of successes + # of failures can not be equal to 0";
        _msg[LA_ERROR39] = "# of successes (failures) can not be less than 0";
        _msg[LA_ERROR40] = "Operator '%1%' requires a real valued matrix whose column chunk size is at least as large as its number of columns for parameter %2%.";
        _msg[LA_ERROR41] = "%1%: error distribution should be one of ['gaussian' | 'poisson' | 'binomial' | 'gamma']";
        _msg[LA_ERROR42] = "%1%: link function should be one of [%2%]";
        _msg[LA_ERROR43] = "%1%: Negative values are not allowed for the Poisson distribution";
        _msg[LA_ERROR44] = "%1%: Non-positive values not allowed for the gamma family";
        _msg[LA_ERROR45] = "%1%: Cannot find a valid starting value";
        _msg[LA_ERROR46] = "%1%: Response values should fall within the interval [0,1]";
        _msg[LA_ERROR47] = "%1%: Either the model matrix is not of full rank or the weights vector contains all zeros";
        _msg[LA_ERROR48] = "%1%: The algorithm failed to converge on a solution. %2%";
        _msg[LA_ERROR49] = "The input matrix is too small; both dimensions must be 4 or larger";
        _msg[LA_ERROR50] = "The input matrix must start at {0,0}";
        _msg[LA_ERROR51] = "The input matrix must have one non-nullable double attribute";
        _msg[LA_ERROR52] = "The input matrix must be emptyable";
        _msg[LA_ERROR53] = "The input matrix must not have chunk overlap";
        _msg[LA_ERROR54] = "The input matrix must be bounded to less than 2147483648 rows and columns";
        _msg[LA_ERROR55] = "The input matrix chunk size must include the entire second dimension";
        _msg[LA_ERROR56] = "Invalid matrix transpose; must be a two-dimensional array";
        _msg[LA_ERROR57] = "The input matrix transpose is too small; both dimensions must be 4 or larger";
        _msg[LA_ERROR58] = "The input matrix transpose must start at {0,0}";
        _msg[LA_ERROR59] = "The input matrix transpose have one non-nullable double attribute";
        _msg[LA_ERROR60] = "The input matrix transpose must be emptyable";
        _msg[LA_ERROR61] = "The input matrix transpose must not have chunk overlap";
        _msg[LA_ERROR62] = "The input matrix transpose chunk size must include the entire second dimension";
        _msg[LA_ERROR63] = "The input matrix dimensions do not match the transpose";
        _msg[LA_ERROR64] = "The desired number of singular values must be greater than 0";
        _msg[LA_ERROR65] = "The desired number of singular vectors is too large; it must be less than 1/3 of the smaller matrix size";
        _msg[LA_ERROR66] = "The tolerance parameter must be nonnegative";
        _msg[LA_ERROR67] = "The maximum number of iterations must be positive";
        _msg[LA_ERROR68] = "The input initial vector must be a one-dimensional array";
        _msg[LA_ERROR69] = "The input initial vector must start at {0}";
        _msg[LA_ERROR70] = "The input initial vector must have one non-nullable double attribute";
        _msg[LA_ERROR71] = "The input initial vector length must match the second dimension of the input matrix";
        _msg[LA_ERROR72] = "The left subspace vector must be a one-dimensional array";
        _msg[LA_ERROR73] = "The left subspace vector must start at {0}";
        _msg[LA_ERROR74] = "The left subspace vector must have one non-nullable double attribute";
        _msg[LA_ERROR75] = "The left subspace vector length must match the first dimension of the input matrix";
        _msg[LA_ERROR76] = "The right subspace vector must be a one-dimensional array";
        _msg[LA_ERROR77] = "The right subspace vector must start at {0}";
        _msg[LA_ERROR78] = "The right subspace vector must have one non-nullable double attribute";
        _msg[LA_ERROR79] = "The right subspace vector length must match the second dimension of the input matrix";
        _msg[LA_ERROR80] = "The median() aggregate overflowed; it is meant to be used for group-by aggregates where each group has a small number of elements";

        _msg[LA_WARNING1] = "convergence is not reached; iteration limit exceeded";
        _msg[LA_WARNING2] = "rank deficient problem";
        _msg[LA_WARNING3] = "the model is overparameterized and some coefficients are not identifiable";
        _msg[LA_WARNING4] = "%1%: Not all of the model statistics could be computed";

        scidb::ErrorsLibrary::getInstance()->registerErrors(LANameSpace, &_msg);
    }

    ~Instance()
    {
        scidb::ErrorsLibrary::getInstance()->unregisterErrors(LANameSpace);
    }

private:
    scidb::ErrorsLibrary::ErrorsMessages _msg;
} _instance;
