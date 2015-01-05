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

#include "TemplateParser.h"

namespace scidb
{
    
using namespace std;

ExchangeTemplate TemplateParser::parse(ArrayDesc const& desc, string const& format, bool isImport)
{
    ExchangeTemplate templ;
    TemplateScanner scanner(format);
    TemplateScanner::Token tkn = scanner.get();
    Attributes const& attrs = desc.getAttributes(true);
    FunctionLibrary* lib = FunctionLibrary::getInstance();

    if (tkn == TemplateScanner::TKN_IDENT) { 
        if (compareStringsIgnoreCase(scanner.getIdent(), "opaque") == 0) { 
            templ.opaque = true;
            if (scanner.get() != TemplateScanner::TKN_EOF) { 
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << scanner.getPosition();
            }
            return templ;
        } 
    } else if (tkn == TemplateScanner::TKN_LPAR) { 
        templ.opaque = false;
        size_t nAttrs = 0;
        do { 
            tkn = scanner.get();
            ExchangeTemplate::Column c;
            c.nullable = false;
            c.converter = NULL;
            if (tkn == TemplateScanner::TKN_COMMA || tkn == TemplateScanner::TKN_RPAR) { 
                // field is skipped
                c.skip = true;
                c.fixedSize = 0;
                if (!isImport) { // skip attribute in case of export 
                    nAttrs += 1;
                }
            } else { 
                if (tkn != TemplateScanner::TKN_IDENT) { 
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << scanner.getPosition();
                }
                string ident = scanner.getIdent();
                if (compareStringsIgnoreCase(ident, "dummy") == 0 || compareStringsIgnoreCase(ident, "void") == 0 || compareStringsIgnoreCase(ident, "skip") == 0) { 
                    c.skip = true;
                    if (!isImport) { // skip attribute in case of export 
                        nAttrs += 1;
                    }
                } else {
                    if (nAttrs >= attrs.size()) { 
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ATTRIBUTES_MISMATCH);
                    }
                    c.externalType = TypeLibrary::getType(ident);
                    c.internalType = TypeLibrary::getType(attrs[nAttrs++].getType());
                    c.skip = false;
                    if (c.internalType != c.externalType) { 
                        c.converter = isImport 
                            ? lib->findConverter(c.externalType.typeId(), c.internalType.typeId()) 
                            : lib->findConverter(c.internalType.typeId(), c.externalType.typeId());
                    }
                }
                tkn = scanner.get();
                if (tkn == TemplateScanner::TKN_LPAR) { 
                    tkn = scanner.get();
                    if (tkn != TemplateScanner::TKN_NUMBER) { 
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << scanner.getPosition();
                    }
                    if (!c.skip && !c.externalType.variableSize()) { 
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_FIXED_SIZE_TYPE) << c.externalType.typeId();
                    }
                    c.fixedSize = scanner.getNumber();
                    if (c.fixedSize == 0 || (uint32_t)c.fixedSize != c.fixedSize) { 
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << scanner.getPosition();
                    }
                    tkn = scanner.get();
                    if (tkn != TemplateScanner::TKN_RPAR) { 
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << scanner.getPosition();
                    }
                    tkn = scanner.get();
                } else {
                    c.fixedSize = c.skip ? 0 : c.externalType.byteSize();
                }
                if (tkn == TemplateScanner::TKN_IDENT) { 
                    if (compareStringsIgnoreCase(scanner.getIdent(), "null") == 0) { 
                        c.nullable = true;
                        tkn = scanner.get();
                    } else { 
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << scanner.getPosition();
                    } 
                }
            }
            templ.columns.push_back(c);
        } while (tkn == TemplateScanner::TKN_COMMA);

        if (tkn != TemplateScanner::TKN_RPAR) { 
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << scanner.getPosition();
        }   
        if (nAttrs != attrs.size()) { 
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_ATTRIBUTES_MISMATCH);
        } 
        tkn = scanner.get();
        if (tkn != TemplateScanner::TKN_EOF) { 
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << scanner.getPosition();
        }
        return templ;
    }
    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << scanner.getPosition();
}

uint32_t OpaqueChunkHeader::calculateSignature(ArrayDesc const& desc)
{
    Dimensions const& dims = desc.getDimensions();
    Attributes const& attrs = desc.getAttributes();
    size_t nDims = dims.size();
    size_t nAttrs = attrs.size();
    uint32_t signature = static_cast<uint32_t>(nDims ^ nAttrs);
    for (size_t i = 0; i < nDims; i++) {
        //OK to lose some information here
        signature ^= static_cast<uint32_t>(dims[i].getChunkInterval());
        signature ^= static_cast<uint32_t>(dims[i].getChunkOverlap());
    }
    for (size_t i = 0; i < nAttrs; i++) {
        signature ^= static_cast<uint32_t>(TypeLibrary::getType(attrs[i].getType()).bitSize());
    }
    return signature;
}
            

}
