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

#include <boost/array.hpp>
#include <boost/foreach.hpp>
#include <system/SystemCatalog.h>
#include <query/Operator.h>
#include <array/TransientCache.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

struct PhysicalCreateArray : PhysicalOperator
{
    PhysicalCreateArray(const string& logicalName,const string& physicalName,const Parameters& parameters,const ArrayDesc& schema)
     : PhysicalOperator(logicalName,physicalName,parameters,schema)
    {}

    virtual void
    fixDimensions(PointerRange<shared_ptr<Array> >,PointerRange<DimensionDesc>)
    {}

    virtual shared_ptr<Array>
    execute(vector<shared_ptr<Array> >& in,shared_ptr<Query> query)
    {
        bool const temp(param<OperatorParamPhysicalExpression>(2)->getExpression()->evaluate().getBool());

        if (query->isCoordinator())
        {
            string    n(param<OperatorParamArrayReference>(0)->getObjectName());
            ArrayDesc s(param<OperatorParamSchema>        (1)->getSchema());

            s.setName(n);
            s.setTransient(temp);

         /* Give our subclass a chance to compute missing dimension details
            such as a wild-carded chunk interval, for example...*/

            this->fixDimensions(in,s.getDimensions());

            SystemCatalog::getInstance()->addArray(s,psHashPartitioned);
        }

        syncBarrier(0,query);                            // Workers wait here

        if (temp)                                        // 'temp' flag given?
        {
            string    n(param<OperatorParamArrayReference>(0)->getObjectName());
            ArrayDesc s;

            SystemCatalog::getInstance()->getArrayDesc(n,s,false);

            transient::record(make_shared<MemArray>(s,query));
        }

        return shared_ptr<Array>();
    }

    template<class t>
    shared_ptr<t>& param(size_t i) const
    {
        assert(i < _parameters.size());

        return (shared_ptr<t>&)_parameters[i];
    }
};

/**
 *  Implements the create_array_as() operator (a variant of create_array that
 *  accepts additional statistics from which missing dimension sizes can then
 *  be computed and filled in) as a subclass of PhysicalCreateArray.
 *
 *  The goal here is to override the virtual fixDimensions() member() to fill
 *  in missing dimension details with sizes computed from the array of stats
 *  supplied as our initial input array, which has the following shape:
 *
 *      <loBound,hiBound,interval,overlap,minimum,maximum,distinct>[dimension]
 *
 *  where the first four components are boolean flags indicating whether the
 *  corresponding component of the target shcema was set by the user (true) or
 *  is to be computed here (false).
 *
 *  @see [wiki:Development/components/Client_Tools/ChunkLengthCalculator_NewPythonVersion]
 *  for the basic chunk selection algorithm we are implementing here.
 */
struct PhysicalCreateArrayUsing : PhysicalCreateArray
{
    enum statistic
    {
        loBound,
        hiBound,
        interval,
        overlap,
        minimum,
        maximum,
        distinct
    };

    typedef boost::array<Value,distinct+1> statistics;

    PhysicalCreateArrayUsing(const string& logicalName,const string& physicalName,const Parameters& parameters,const ArrayDesc& schema)
      : PhysicalCreateArray(logicalName,physicalName,parameters,schema)
    {}

    /**
     *
     */
    virtual void fixDimensions(PointerRange<shared_ptr<Array> > in,
                               PointerRange<DimensionDesc>      dims)
    {
        assert(in.size()==2 && !dims.empty());           // Validate arguments

        size_t const desiredValuesPerChunk(getDesiredValuesPerChunk(*in[1]));
        size_t const  overallDistinctCount(getOverallDistinctCount (*in[1]));
        size_t              numChunksFromN(max(overallDistinctCount / desiredValuesPerChunk,1UL));
        size_t                           N(0);           // Inferred intervals
        vector<statistics>               S(dims.size()); // Array of statistics
        DimensionDesc*                   d(dims.begin());// Dimension iterator
        int64_t                  remain(INFINITE_LENGTH);// Cells to play with

        getStatistics(S,*in[0]);                         // Read in statistics

        BOOST_FOREACH(statistics& s,S)                   // For each dimension
        {
            if (!s[interval].getBool())                  // ...infer interval?
            {
                N += 1;                                  // ....seen another
            }
            else                                         // ...user specified
            {
                numChunksFromN *= d->getChunkInterval();
                numChunksFromN /= s[distinct].getInt64();

                remain /= (d->getChunkInterval() + d->getChunkOverlap());
            }

            ++d;
        }

        double const chunksPerDim = max(pow(numChunksFromN,1.0/N),1.0);

        d = dims.begin();                                // Reset dim iterator

        BOOST_FOREACH(statistics& s,S)                   // For each dimension
        {
            if (!s[loBound].getBool())                   // ...infer loBound?
            {
                d->setStartMin(s[minimum].getInt64());   // ....assign default
            }

            if (!s[hiBound].getBool())                   // ...infer hiBound?
            {
                d->setEndMax(s[maximum].getInt64());     // ....assign default
            }

            if (!s[overlap].getBool())                   // ...infer overlap?
            {
                d->setChunkOverlap(0);                   // ....assign default
            }

            if (!s[interval].getBool())                   // ...infer interval?
            {
                int64_t hi = s[maximum].getInt64();      // ....highest actual
                int64_t lo = s[minimum].getInt64();      // ....lowest  actual
                int64_t ci  = (hi - lo + 1)/chunksPerDim;// ....chunk interval

                ci = max(ci,1L);                         // ....clamp at one
                ci = roundLog2(ci);                      // ....nearest power
                ci = min(ci,remain);                     // ....clamp between
                ci = max(ci,1L);                         // ....one and remain

                d->setChunkInterval(ci);                 // ....update schema

                remain /= (d->getChunkInterval() + d->getChunkOverlap());
            }

            ++d;                                         // ...next dimension
        }
    }

    /**
     *  Round the proposed chunk interval to the nearest power of two,  where
     *  'nearest' means that the base two logarithm is rounded to the nearest
     *  integer.
     */
    static int64_t roundLog2(int64_t ci)
    {
        assert(ci != 0);                                 // Validate argument

        return pow(2,round(log2(ci)));                   // Nearest power of 2
    }

    /**
     *  Input array A is a list of 7-tuples, each a (possibly null) integer,
     *  one record for each dimension in the target schema.
     *
     *  Read the rows of A into a vector of 'statistics' objects.
     */
    void getStatistics(vector<statistics>& v,Array& A)
    {
        for (size_t a=0; a!=statistics::size(); ++a)     // For each attribute
        {
            shared_ptr<ConstArrayIterator> ai(A.getConstIterator(a));

            for (size_t d=0; d!=v.size(); ++d)           // ...for each dimension
            {
                v[d][a] = ai->getChunk().getConstIterator()->getItem();
                ai->operator++();                        // ....next chunk
            }
        }
    }

    /**
     *  Return the first integer from the input array A, the overall distinct
     *  count of values found in the load array.
     */
    int64_t getOverallDistinctCount(Array& A) const
    {
        return A.getConstIterator(0)->getChunk().getConstIterator()->getItem().getInt64();
    }

    /**
     *  Return the second integer from the input array A, the desired number
     *  of values per chunk.
     */
    int64_t getDesiredValuesPerChunk(Array& A) const
    {
        return A.getConstIterator(1)->getChunk().getConstIterator()->getItem().getInt64();
    }
};

/****************************************************************************/

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCreateArray,     "create_array",      "impl_create_array")
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCreateArrayUsing,"create_array_using","impl_create_array_using")

/****************************************************************************/
}
/****************************************************************************/
