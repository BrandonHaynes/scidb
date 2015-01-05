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
 * DistinctCounter.h
 *
 *  Created on: Feb 14, 2012
 *      Author: egor.pugin@gmail.com
 */

#ifndef _DISTINCT_COUNT_H_
#define _DISTINCT_COUNT_H_

/*
 * We use here HyperLogLog algorithm by P. Flajolet, E. Fusy, O. Gandouet, F. Meunier (2007)
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 */

class DistinctCounter
{
private:
    size_t k;
    size_t m;
    boost::shared_array<uint8_t> M;

    size_t k_comp;
    double alpha_m;
private:
    inline uint8_t p(uint32_t hash)
    {
        uint8_t r = 1;
        while ((hash & 1) == 0 && r <= k_comp)
        {
            hash >>= 1;
            r++;
        }
        return r;
    }

    double log2(double arg)
    {
        return log(arg) / log(2.);
    }
public:
    DistinctCounter(double std_error = 0.005)
    {
        double err = 1.05 / std_error;
        k = ceil(log2(err * err));
        k_comp = 32 - k;
        m = (size_t)pow(2, (double)k);

        switch (m)
        {
        case 16:
            alpha_m = 0.673;
            break;
        case 32:
            alpha_m = 0.697;
            break;
        case 64:
            alpha_m = 0.709;
            break;
        default:
            alpha_m = 0.7213 / (1 + 1.079 / (double)m);
            break;
        }

        M = boost::shared_array<uint8_t>(new uint8_t[m]);
        memset(M.get(), 0, m);
    }

    inline void addValue(uint32_t hash)
    {
        size_t j = hash >> k_comp;
        M[j] = max(M[j], p(hash));
    }

    uint64_t getCount()
    {
        double c = 0;
        for (size_t i = 0; i < m; i++)
        {
            c += 1 / pow(2., (double)M[i]);
        }
        double E = alpha_m * m * m / c;

        const double pow_2_32 = 0xffffffff;

        //corrections
        if (E <= (5 / 2. * m))
        {
            double V = 0;
            for (size_t i = 0; i < m; i++)
            {
                if (M[i] == 0) V++;
            }

            if (V > 0)
            {
                E = m * log(m / V);
            }
        }
        else if (E > (1 / 30. * pow_2_32))
        {
            E = -pow_2_32 * log(1 - E / pow_2_32);
        }

        return (uint64_t)E;
    }

    void mergeDC(uint8_t *dc, size_t size)
    {
        size = min(size, m);
        for (size_t i = 0; i < size; i++)
        {
            M[i] = max(M[i], dc[i]);
        }
    }

    boost::shared_array<uint8_t>& getDC(size_t *_m)
    {
        *_m = m;
        return M;
    }
};


#endif
