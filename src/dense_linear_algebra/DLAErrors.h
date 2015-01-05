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
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Error codes for dense linear algebra plugin
 */

#ifndef DENSE_LINEAR_ERRORS_H_
#define DENSE_LINEAR_ERRORS_H_

#define DLANameSpace "DLA"

enum
{
    DLA_ERROR1 = SCIDB_USER_ERROR_CODE_START, //Empty bitmap inconsistency
    DLA_ERROR2, //Matrix must contain 1 attribute
    DLA_ERROR3, //Input arrays must have 2 dimensions
    DLA_ERROR4, //Matrix dimensions must match %1%
    DLA_ERROR5, //Attribute should have double type
    DLA_ERROR6, //Corr accepts 'kendall', 'spearman' and 'pearson' parameters
    DLA_ERROR7, //Invalid matrix
    DLA_ERROR8, //Attribute should have double type
    DLA_ERROR9, //Unbounded arrays not supported
    DLA_ERROR10,//Matrix chunk interval should match
    DLA_ERROR11,//Matrix origin must match
    DLA_ERROR12,//Failed to solve the system of linear equations
    DLA_ERROR13,//Request for unknown attribute
    DLA_ERROR14,//Specified attribute not found in array
    DLA_ERROR15,//Ranked attribute cannot be an "empty indicator"
    DLA_ERROR16,//Specified dimension not found in array
    DLA_ERROR17,//The number of samples passed to quantile must be at least 1
    DLA_ERROR18,//One of the input arrays contain missing observations
    DLA_ERROR19,//No complete element pairs
    DLA_ERROR20,//ssvdNorm: Matrix must contain 1 attribute
    DLA_ERROR21,//ssvdNorm: Matrix (vector) must contain 1 attribute
    DLA_ERROR22,//ssvdNorm: Argument #%1% must have exactly two dimensions
    DLA_ERROR23,//ssvdNorm: First argument must have same number of rows as first argument
    DLA_ERROR24,//ssvdNorm: Second argument must have one column
    DLA_ERROR25,//ssvdNorm: Third argument must have one row"
    DLA_ERROR26,//ssvdNorm: Third argument must have same number of columns as first argument
    DLA_ERROR27,//ssvdNorm: Argument #%1% must have type double
    DLA_ERROR28,//ssvdNorm: Argument #%1% must not be nullable
    DLA_ERROR29,//ssvdNorm: Argument #%1% must be bounded"
    DLA_ERROR30,//linregr:: final parameter must be \n'coefficients', \n'residuals', \n'multiple R2',  \n'adjusted R2', \n'F-statistic', \n'p-value', \n'residual stderror', \n'stderror', \n'tvalue', \n'P-statistic', \n'confidence intervals'
    DLA_ERROR31,//logistregr:: last parameter must be 'coefficients' or 'summary'
    DLA_ERROR32,//'use' = ['everything' | 'all.obs' | 'complete.obs' | 'nan.or.complete' | 'pairwise.complete.obs']
    DLA_ERROR33,//SVD accepts 'left', 'right' and 'values' parameters
    DLA_ERROR34,//corr accepts 'kendall', 'spearman' and 'pearson' parameters
    DLA_ERROR35,//Singular values overflow
    DLA_ERROR36,//Initial matrix is zero
    DLA_ERROR37,//Norm of some eigenvectors is equal to 0 during calculation of singular vectors or bidiagonal matrix
    DLA_ERROR38,//# of successes + # of failures can not be equal to 0
    DLA_ERROR39, //# of successes (failures) can not be less than 0
    DLA_ERROR40,// Non-zero chunk overlap is not supported %1%
    DLA_ERROR41,//ChunkInterval is too small
    DLA_ERROR42,//ChunkInterval is too large
    DLA_ERROR43,// array dimensions must be of equal size, temporarily
    DLA_ERROR44,// dimensions must start at 0
    DLA_ERROR45,// dimensions must be int or uint types, temporarily
    DLA_ERROR46,// the option string is malformed
    DLA_WARNING1, // convergence is not reached; iteration limit exceeded
    DLA_WARNING2, // rank deficient problem
    DLA_WARNING3, // the model is overparameterized and some coefficients are not identifiable
    DLA_WARNING4 // the chunkSize is outside the optimal range of %1% to %2%
};

#endif /* DENSE_LINEAR_ERRORS_H_ */
