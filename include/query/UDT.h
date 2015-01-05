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
 * @file UDT.h
 *
 * @author knizhnik@garret.ru
 *
 * @brief C++ wrapper for Value class allowing t use it in expressions
 */

#ifndef UDT_H_
#define UDT_H_

#include "TypeSystem.h"
#include "FunctionLibrary.h"

namespace scidb
{
    class UDT { 
      protected:
        enum OpCodes { 
            #define UDT_BIN_OP(opcode, mnem, cpp) opcode,
            #define UDT_UNARY_OP UDT_BIN_OP
            #define UDT_CMP_OP UDT_BIN_OP
            #define UDT_CNV(src_type, method, cpp_type) FROM_##src_type, TO_##src_type,
            #include "UDT.d"
            LAST
        };
        FunctionPointer fptrs[LAST];

      public:
        const TypeId tid;
        const Type   type;
        const size_t size;

        class Val { 
            friend class UDT;
            
            UDT*  udt;
            void* dst;
            Value value;

          public:
            #define UDT_BIN_OP(opcode, mnem, cpp_op)   Val  cpp_op(Val const& other) const;
            #define UDT_UNARY_OP(opcode, mnem, cpp_op) Val  cpp_op() const;
            #define UDT_CMP_OP(opcode, mnem, cpp_op)   bool cpp_op(Val const& other) const;
            #define UDT_CNV(tid, method, cpp_type)     Val operator=(cpp_type other); operator cpp_type() const;
            #include "UDT.d"

            Val operator=(Val const& other);
            Val operator=(Value const& other);
                
            friend Val min(Val const& v1, Val const& v2);
            friend Val max(Val const& v1, Val const& v2);
            friend Val abs(Val const& v);

            Val(UDT* u, Type const& type) : udt(u), dst(NULL), value(TypeLibrary::getDefaultValue(type.typeId()))
            {
            }

            Val(UDT* u = NULL) : udt(u), dst(NULL) {}

            Val(UDT* u, void* lval) : udt(u), dst(lval) {
                value.setData(lval, u->size);
            }
        };

        const Val ZERO;

        UDT(TypeId typeId) : tid(typeId), type(TypeLibrary::getType(typeId)), size(type.byteSize()), ZERO(this, type)
        {
            memset(fptrs, 0, sizeof fptrs);
        }
    };

    class MatrixOfUDT : public UDT 
    {
        char* data;
        bool  deallocate;

      public:
        const size_t nRows;
        const size_t nColumns;

        MatrixOfUDT(TypeId elemType, size_t rows, size_t columns, void* chunk) 
        : UDT(elemType), data((char*)chunk), deallocate(false), nRows(rows), nColumns(columns) {}

        MatrixOfUDT(TypeId elemType, size_t rows, size_t columns) 
        : UDT(elemType), data(new char[columns*rows*size]), deallocate(true), nRows(rows), nColumns(columns)
        {
            if (!data)
                throw USER_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_NO_MEMORY_TO_ALLOCATE_MATRIX);
        }

        Val operator()(size_t x, size_t y) {
            assert(x < nRows && y < nColumns);
            return Val(this, &data[(x*nColumns + y)*size]);
        }

        ~MatrixOfUDT() { 
            if (deallocate) { 
                delete[] data;
            }
        }
    };

    class VectorOfUDT : public UDT 
    {
        char* data;
        bool  deallocate;

      public:
        const size_t nElems;
        
        VectorOfUDT(TypeId elemType, size_t length, void* chunk) : UDT(elemType), data((char*)chunk), deallocate(false), nElems(length) {}

        VectorOfUDT(TypeId elemType, size_t length) : UDT(elemType), data(new char[length*size]), deallocate(true), nElems(length)
        {
            if (!data)
                throw USER_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_NO_MEMORY_TO_ALLOCATE_VECTOR);
        }

        Val operator()(size_t i) {
            assert(i < nElems);
            return Val(this, &data[i*size]);
        }

        ~VectorOfUDT() { 
            if (deallocate) { 
                delete[] data;
            }
        }
    };

    template<class T>
    class Matrix 
    {
        T* data;
        bool deallocate;
        
      public:
        const size_t nRows;
        const size_t nColumns;
        const T ZERO;
                
        Matrix(size_t rows, size_t columns, void* chunk) 
        : data((T*)chunk), deallocate(false), nRows(rows), nColumns(columns), ZERO((T)0) {}
        
        Matrix(size_t rows, size_t columns) 
        : data(new T[columns*rows]), deallocate(true), nRows(rows), nColumns(columns), ZERO((T)0)
        {
            if (!data)
                throw USER_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_NO_MEMORY_TO_ALLOCATE_MATRIX);
        }
        
        ~Matrix() { 
            if (deallocate) { 
                delete[] data;
            }
        }
        
        T& operator()(size_t x, size_t y) {
            assert(x < nRows && y < nColumns);
            return data[x*nColumns + y];
        }
    };
    

    template<class T>
    class Vector
    {
        T* data;
        bool deallocate;

      public:
        const size_t nElems;
        const T ZERO;
        
        Vector(size_t length, void* chunk) : data((T*)chunk), deallocate(false), nElems(length), ZERO((T)0) {}

        Vector(size_t length) : data(new T[length]), deallocate(true), nElems(length), ZERO((T)0)
        {
            if (!data)
                throw USER_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_NO_MEMORY_TO_ALLOCATE_VECTOR);
        }
        
        T& operator()(size_t i) {
            assert(i < nElems);
            return data[i];
        }

        ~Vector() { 
            if (deallocate) { 
                delete[] data;
            }
        }
    };

    /**
     * Example of usage
     *

    template<class Matrix, class Element>
    void someGenericAlgorithm(Matrix& matrix) { 
        for (size_t i = 0; i < matrix.nRows; i++) { 
            for (size_t j = 0; j < matrix.nColumns; j++) { 
                matrix(i,j) = matrix.ZERO;
                matrix(i,j) = i*10.0+j;
                Element e = matrix(i,j);
                matrix(i,j) = e * e;
            }
        }
        for (size_t i = 0; i < matrix.nRows; i++) { 
            for (size_t j = 0; j < matrix.nColumns; j++) { 
                matrix(i,j) = matrix(j,i);
                printf("%f ", (double)matrix(i,j));
            }
            printf("\n");
        }
    }

    
    
    class MyPhysicalOperator : public PhysicalOperator { 
      public:
         boost::shared_ptr<Array> execute(vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query)
         {
             TypeId attrType = inoutArrays[0]->getArrayDesc().getAttributes()[0].getType();
             if (attrType == TID_DOUBLE) {
                 Matrix<double> m(10, 10);
                 someGenericAlgorithm<Matrix<double>, double>(m);
             } else {  
                 MatrixOfUDT mUDT(attrType, 10, 10);
                 someGenericAlgorithm<MatrixOfUDT, UDT::Val>(mUDT);
             }
         }
    };

    */

}
#endif
