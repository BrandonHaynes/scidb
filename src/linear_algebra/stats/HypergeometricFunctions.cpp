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
 * @file hypergeometric_functions.cpp
 *
 * @author Dave Gosselin
 */

#ifndef __HYPERGEOMETRIC_H__
#define __HYPERGEOMETRIC_H__

#include <boost/math/distributions.hpp>
#include <boost/assign.hpp>
#include <boost/version.hpp>

#include <query/Operator.h>
#include <query/FunctionLibrary.h>
#include <query/FunctionDescription.h>
#include <query/TypeSystem.h>

using namespace std;
using namespace scidb;
using namespace boost::math;
using namespace boost::math::policies;
using namespace boost::assign;

namespace scidb
{

/*
 * @brief hypergeometric cumulative distribution
 * @param x (double) The number of white marbles drawn from a bag, without replacement,
 *           which contains both black and white marbles.
 * @param m (double) The number of white marbles in the bag.
 * @param n (double) The number of black marbles in the bag.
 * @param k (double) The number of marbles drawn from the bag. 
 * @param lower_tail (boolean) TRUE for lower tail, FALSE for upper.
 * @return The hypergeometric cumulative distribution up to x
 */
static void
stats_hygecdf(double x, double m, double n, double k, bool lower_tail, Value* res)
{

  if (boost::math::isnan(x) || boost::math::isnan(m) ||
      boost::math::isnan(n) || boost::math::isnan(k)) {
      res->setDouble(NAN);
      return;
  }

  if (k > m+n || m < 0 || n < 0 || k < 0) {
    res->setDouble(NAN);
    return;
  }

  if (x < 0 || n - k + x + 1 <= 0) {
    res->setDouble(0);
    return;
  }

  if (x >= k || x >= n) {
    res->setDouble(1);
    return;
  }

  hypergeometric_distribution<double> h(m, k, m+n);

  if (lower_tail) {
    res->setDouble(boost::math::cdf(h, x));
  }
  else {
    res->setDouble(boost::math::cdf(complement(h, x)));
  }
}

/*
 * @brief hypergeometric cumulative distribution
 * @param x (double) The number of white marbles drawn from a bag, without replacement,
 *           which contains both black and white marbles.
 * @param m (double) The number of white marbles in the bag.
 * @param n (double) The number of black marbles in the bag.
 * @param k (double) The number of marbles drawn from the bag. 
 * @param lower_tail (boolean) TRUE for lower tail, FALSE for upper.
 * @return The hypergeometric cumulative distribution up to x
 */
static void
stats_hygecdf_default(const Value** args, Value* res, void*)
{
  stats_hygecdf(floor(args[0]->getDouble()),
		args[1]->getDouble(),
		args[2]->getDouble(),
		args[3]->getDouble(),
		true,
		res);
}

/*
 * @brief hypergeometric cumulative distribution
 * @param x (double) The number of white marbles drawn from a bag, without replacement,
 *           which contains both black and white marbles.
 * @param m (double) The number of white marbles in the bag.
 * @param n (double) The number of black marbles in the bag.
 * @param k (double) The number of marbles drawn from the bag. 
 * @param lower_tail (boolean) TRUE for lower tail, FALSE for upper.
 * @return The hypergeometric cumulative distribution up to x
 */
static void
stats_hygecdf_override(const Value** args, Value* res, void*)
{
  stats_hygecdf(floor(args[0]->getDouble()),
		args[1]->getDouble(),
		args[2]->getDouble(),
		args[3]->getDouble(),
		args[4]->getBool(),
		res);
}

/*
 * @brief Hypergeometric probability mass function
 * @param x (double) The number of white marbles drawn from a bag, without replacement,
 *           which contains both black and white marbles.
 * @param m (double) The number of white marbles in the bag.
 * @param n (double) The number of black marbles in the bag.
 * @param k (double) The number of marbles drawn from the bag. 
 * @return The hypergeometric mass at x.
 */
static void
stats_hygepmf(const Value** args, Value *res, void*)
{
  double x = args[0]->getDouble();
  double m = args[1]->getDouble();
  double n = args[2]->getDouble();
  double k = args[3]->getDouble();
  hypergeometric_distribution <> h(m, k, m+n);

  res->setDouble(boost::math::pdf(h, x));
}

/*
 * @brief hypergeometric quantile function
 * @param p (double) The probability (0 <= p <= 1)
 * @param m (double) The number of white marbles in the bag.
 * @param n (double) The number of black marbles in the bag.
 * @param k (double) The number of marbles drawn from the bag. 
 * @param lower_tail (boolean) TRUE for lowe tail quantile, FALSE for upper.
 * @return The number of white marbles drawn from a bag, without replacement,
 *          which contains both black and white marbles.
 */
static void
stats_hygequant(const Value** args, Value *res, void*)
{
  double p = args[0]->getDouble();
  double m = args[1]->getDouble();
  double n = args[2]->getDouble();
  double k = args[3]->getDouble();
  bool lower_tail = args[4]->getBool();
  hypergeometric_distribution <> h(m, k, m+n);

  if(lower_tail) {
    res->setDouble(boost::math::quantile(h, p));
  }
  else {
    res->setDouble(boost::math::quantile(complement(h, p)));
  }
}

  REGISTER_FUNCTION(hygepmf, list_of(TID_DOUBLE)(TID_DOUBLE)(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_DOUBLE), stats_hygepmf);
  REGISTER_FUNCTION(hygecdf, list_of(TID_DOUBLE)(TID_DOUBLE)(TID_DOUBLE)(TID_DOUBLE), TypeId(TID_DOUBLE), stats_hygecdf_default);
  REGISTER_FUNCTION(hygecdf, list_of(TID_DOUBLE)(TID_DOUBLE)(TID_DOUBLE)(TID_DOUBLE)(TID_BOOL), TypeId(TID_DOUBLE), stats_hygecdf_override);
  REGISTER_FUNCTION(hygequant, list_of(TID_DOUBLE)(TID_DOUBLE)(TID_DOUBLE)(TID_DOUBLE)(TID_BOOL), TypeId(TID_DOUBLE), stats_hygequant);
}  // namespace scidb

#endif  // __HYPERGEOMETRIC_H__
