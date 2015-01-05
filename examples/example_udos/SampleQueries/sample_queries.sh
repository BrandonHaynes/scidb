#!/bin/bash

##
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
##

#Some sample queries with the trading data.
#These are not too exciting but they do demonstrate the utility of uniq and index_lookup when integer are preferred to NIDs.
#For some really exciting queries with this data, watch http://www.paradigm4.com/watch-the-using-scidb-for-computational-finance-webinar-form/

echo
iquery -aq "hello_instances()"
echo
iquery -aq "instance_stats(project(trades_flat, price))"
echo
iquery -aq "instance_stats(project(trades_flat, price), 'global=true')"
iquery -aq "remove(stock_symbols)" > /dev/null 2>&1

#Create an index of stock symbols
iquery -anq "
store(
 cast(
  uniq(
   sort(
    project(
     trades_flat, 
     symbol
    )
   )
  ),
  <symbol:string> [symbol_id=0:*,1000000,0]
 ),
 stock_symbols
)" > /dev/null 
echo
iquery -ocsv -aq "aggregate(stock_symbols, count(*) as num_unique_symbols)"

iquery -aq "remove(trades)" > /dev/null 2>&1
iquery -aq "create array trades
<price:double,
 volume:uint64>
[symbol_id=0:*,100,0,
 ms=0:*,86400000,0,
 trade_id=0:99,100,0]" > /dev/null

#Use the index to redimension trades into a new integer dimension symbol_id
#Note: the lookup array stock_symbols does not have to come from the same data set.
iquery -anq "store(redimension(
 index_lookup(trades_flat, stock_symbols, trades_flat.symbol, symbol_id),
 trades), trades
)" > /dev/null

echo
echo "Top 5 most traded stocks:"
iquery -ocsv -aq "
join(
 stock_symbols,
 redimension(
  between(
   sort(
    unpack(
     aggregate(trades, count(*) as num_trades, symbol_id),
     i
    ),
    num_trades desc
   ),
   0, 4
  ),
  <num_trades:uint64 null> [symbol_id=0:*,1000000,0]
 )
)"

iquery -aq "remove(summaries)" > /dev/null 2>&1

iquery -anq "
store(
 aggregate(
  trades,
  min(price) as low,
  max(price) as high,
  sum(volume) as t_volume,
  symbol_id
 ),
 summaries
)" > /dev/null

echo
echo "Summaries for interesting stocks:"
iquery -ocsv -aq "
project(
 cross_join(
  summaries as A,
  repart(
   filter(
    stock_symbols,
    symbol = 'AAPL' or
    symbol = 'GOOG' or
    symbol = 'FB'
   ),
   <symbol:string> [symbol_id=0:*,100,0]
  ) as B,
  A.symbol_id,
  B.symbol_id
 ),
 symbol, low, high, t_volume
)"

