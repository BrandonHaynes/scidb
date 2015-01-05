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
 * @brief Error codes for linear algebra plugin
 */

#ifndef LAERRORS_H_
#define LAERRORS_H_

#define LANameSpace "LA"

enum
{
    LA_ERROR1 = SCIDB_USER_ERROR_CODE_START, //Empty bitmap inconsistency
    LA_ERROR2, //Matrix must contain 1 attribute
    LA_ERROR3, //Input arrays must be rectangular matrices
    LA_ERROR4, //Matrix dimensions must match
    LA_ERROR5, //Cannot specify 'left' deflation subspace argument without also supplying the 'right' argument.
    LA_ERROR6, //Corr accepts 'kendall', 'spearman' and 'pearson' parameters
    LA_ERROR7, //Invalid matrix
    LA_ERROR8, //Attribute should have double type
    LA_ERROR9, //Unbounded arrays not supported
    LA_ERROR10,//Matrix chunk interval should match
    LA_ERROR11,//Matrix origin must match
    LA_ERROR12,//Failed to solve the system of linerar equations
    LA_ERROR13,//Request for unknown attribute
    LA_ERROR14,//Specified attribute not found in array
    LA_ERROR15, //Ranked attribute cannot be an empty indicator
    LA_ERROR16,//Specified dimension not found in array
    LA_ERROR17,//The number of samples passed to quantile must be at least 1
    LA_ERROR18,//One of the input arrays contain missing observations
    LA_ERROR19,//No complete element pairs
    LA_ERROR20,//ssvdNorm: Matrix must contain 1 attribute
    LA_ERROR21,//ssvdNorm: Matrix (vector) must contain 1 attribute
    LA_ERROR22,//ssvdNorm: Argument #%1% must have exactly two dimensions
    LA_ERROR23,//ssvdNorm: First argument must have same number of rows as first argument
    LA_ERROR24,//ssvdNorm: Second argument must have one column
    LA_ERROR25,//ssvdNorm: Third argument must have one row"
    LA_ERROR26,//ssvdNorm: Third argument must have same number of columns as first argument
    LA_ERROR27,//ssvdNorm: Argument #%1% must have type double
    LA_ERROR28,//ssvdNorm: Argument #%1% must not be nullable
    LA_ERROR29,//ssvdNorm: Argument #%1% must be bounded"
    LA_ERROR30,//linregr:: final parameter must be \n'coefficients', \n'residuals', \n'multiple R2',  \n'adjusted R2', \n'F-statistic', \n'p-value', \n'residual stderror', \n'stderror', \n'tvalue', \n'P-statistic', \n'confidence intervals'
    LA_ERROR31,//logistregr:: last parameter must be 'coefficients' or 'summary'
    LA_ERROR32,//'use' = ['everything' | 'all.obs' | 'complete.obs' | 'nan.or.complete' | 'pairwise.complete.obs']
    LA_ERROR33,//SVD accepts 'left', 'right' and 'values' parameters
    LA_ERROR34,//corr accepts 'kendall', 'spearman' and 'pearson' parameters
    LA_ERROR35,//Singular values overflow
    LA_ERROR36,//Initial matrix is zero
    LA_ERROR37,//Norm of some eigenvectors is equal to 0 during calculation of singular vectors or bidiagonal matrix
    LA_ERROR38,//# of successes + # of failures can not be equal to 0
    LA_ERROR39, //# of successes (failures) can not be less than 0
    LA_ERROR40,  // Operator '%1%' requires a real valued matrix whose column chunk size is at least as large as its number of columns for parameter %2%.
    LA_ERROR41,  // %1%: error distribution should be one of ['gaussian' | 'poisson' | 'binomial' | 'gamma']
    LA_ERROR42,  // %1%: link function should be one of [%2%]
    LA_ERROR43,  // %1%: Negative values are not allowed for the Poisson distribution
    LA_ERROR44,  // %1%: Non-positive values not allowed for the gamma family
    LA_ERROR45,  // %1%: Cannot find a valid starting value
    LA_ERROR46,  // %1%: Response values should fall within the interval [0,1]
    LA_ERROR47,  // %1%: Either the model matrix is not of full rank or the weights vector contains all zeros.
    LA_ERROR48,  // %1%: The problem instance does not have a well defined solution.
    LA_ERROR49,//The input matrix is too small; both dimensions must be 4 or larger
    LA_ERROR50,//The input matrix must start at {0,0}
    LA_ERROR51,//The input matrix must have one non-nullable double attribute
    LA_ERROR52,//The input matrix must be emptyable
    LA_ERROR53,//The input matrix must not have chunk overlap
    LA_ERROR54,//The input matrix must be bounded to less than 2147483648 rows and columns
    LA_ERROR55,//The input matrix chunk size must include the entire second dimension
    LA_ERROR56,//Invalid matrix transpose; must be a two-dimensional array
    LA_ERROR57,//The input matrix transpose is too small; both dimensions must be 4 or larger
    LA_ERROR58,//The input matrix transpose must start at {0,0}
    LA_ERROR59,//The input matrix transpose have one non-nullable double attribute
    LA_ERROR60,//The input matrix transpose must be emptyable
    LA_ERROR61,//The input matrix transpose must not have chunk overlap
    LA_ERROR62,//The input matrix transpose chunk size must include the entire second dimension
    LA_ERROR63,//The input matrix dimensions do not match the transpose
    LA_ERROR64,//The desired number of singular values must be greater than 0
    LA_ERROR65,//The desired number of singular vectors is too large; it must be less than 1/3 of the smaller matrix size
    LA_ERROR66,//The tolerance parameter must be nonnegative
    LA_ERROR67,//The maximum number of iterations must be positive
    LA_ERROR68,//The input initial vector must be a one-dimensional array
    LA_ERROR69,//The input initial vector must start at {0}
    LA_ERROR70,//The input initial vector must have one non-nullable double attribute
    LA_ERROR71,//The input initial vector length must match the second dimension of the input matrix
    LA_ERROR72,//The left subspace vector must be a one-dimensional array
    LA_ERROR73,//The left subspace vector must start at {0}
    LA_ERROR74,//The left subspace vector must have one non-nullable double attribute
    LA_ERROR75,//The left subspace vector length must match the first dimension of the input matrix
    LA_ERROR76,//The right subspace vector must be a one-dimensional array
    LA_ERROR77,//The right subspace vector must start at {0}
    LA_ERROR78,//The right subspace vector must have one non-nullable double attribute
    LA_ERROR79,//The right subspace vector length must match the second dimension of the input matrix
    LA_ERROR80,//The median() aggregate overflowed; it is meant to be used for group-by aggregates where each group has a small number of elements

    LA_WARNING1, // convergence is not reached; iteration limit exceeded
    LA_WARNING2, // rank deficient problem
    LA_WARNING3, // the model is overparameterized and some coefficients are not identifiable
    LA_WARNING4  // %1%: Failed to compute the 'aic' model statistic
};

#endif /* LAERRORS_H_ */
