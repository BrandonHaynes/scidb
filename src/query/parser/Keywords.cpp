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

/****************************************************************************/

#include <boost/tuple/tuple.hpp>                         // For boost::tie()
#include "Lexer.h"                                       // For Lexer

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

/**
 *  Class 'keyword' represents a single entry of a lexicon - essentially, just
 *  a fixed map from strings to their corresponding token numbers, implemented
 *  as an array of pairs sorted by their first component.
 */
typedef Keyed<chars,int,less_strcasecmp> keyword;        // Keyword name/token

/**
 *  Return true if the string 'text' matches an entry in the given lexicon. If
 *  so, return both the statically allocated keyword string and its associated
 *  token number through the 'lexeme' and 'token' output parameters.
 */
bool isKeyword(PointerRange<const keyword> lexicon,chars text,chars& lexeme,int& token)
{
    const keyword *f,*l;                                 // First+last matches

    boost::tie(f,l) = equal_range(lexicon.begin(),lexicon.end(),text);

    if (f != l)                                          // We found a match?
    {
        assert(l - f == 1);                              // ...match is unique
        lexeme = f->key;                                 // ...save the name
        token  = f->value;                               // ...save the token
        return true;                                     // ...yes, it sure is
    }

    return false;                                        // Not in the lexicon
}

/**
 *  Return true if the character string 'text' is an AFL keyword.
 */
bool isAFLKeyword(chars text,chars& lexeme,int& token)
{
    static const keyword lexicon[] =                     // Must remain sorted
    {
        {"and",         Token::AND        },             // ...reserved
        {"array",       Token::ARRAY      },
        {"as",          Token::AS         },
        {"asc",         Token::ASC        },
        {"between",     Token::BETWEEN    },
        {"compression", Token::COMPRESSION},
        {"create",      Token::CREATE     },
        {"default",     Token::DEFAULT    },
        {"desc",        Token::DESC       },
        {"fn",          Token::FN         },             // ...reserved
        {"in",          Token::IN         },             // ...reserved
        {"is",          Token::IS         },
        {"let",         Token::LET        },             // ...reserved
        {"not",         Token::NOT        },             // ...reserved
        {"null",        Token::NULL_VALUE },             // ...reserved
        {"or",          Token::OR         },             // ...reserved
        {"reserve",     Token::RESERVE    },
        {"select",      Token::SELECT     },             // ...reserved
        {"temp",        Token::TEMP       },
        {"using",       Token::USING      },
        {"where",       Token::WHERE      },             // ...reserved
    };

    return isKeyword(lexicon,text,lexeme,token);         // Is it a keyword?
}

/**
 *  Return true if the character string 'text' is an AQL keyword.
 */
bool isAQLKeyword(chars text,chars& lexeme,int& token)
{
    static const keyword lexicon[] =                     // Must remain sorted
    {
        {"all",         Token::ALL        },
        {"and",         Token::AND        },             // ...reserved
        {"array",       Token::ARRAY      },
        {"as",          Token::AS         },
        {"asc",         Token::ASC        },
        {"between",     Token::BETWEEN    },
        {"by",          Token::BY         },
        {"cancel",      Token::CANCEL     },             // ...reserved
        {"compression", Token::COMPRESSION},
        {"create",      Token::CREATE     },
        {"cross",       Token::CROSS      },             // ...reserved
        {"current",     Token::CURRENT    },
        {"default",     Token::DEFAULT    },
        {"desc",        Token::DESC       },
        {"drop",        Token::DROP       },
        {"errors",      Token::ERRORS     },
        {"fixed",       Token::FIXED      },             // ...reserved
        {"fn",          Token::FN         },             // ...reserved
        {"following",   Token::FOLLOWING  },
        {"from",        Token::FROM       },             // ...reserved
        {"group",       Token::GROUP      },             // ...reserved
        {"in",          Token::IN         },             // ...reserved
        {"insert",      Token::INSERT     },             // ...reserved
        {"instance",    Token::INSTANCE   },
        {"instances",   Token::INSTANCES  },
        {"into",        Token::INTO       },             // ...reserved
        {"is",          Token::IS         },
        {"join",        Token::JOIN       },             // ...reserved
        {"let",         Token::LET        },             // ...reserved
        {"library",     Token::LIBRARY    },
        {"load",        Token::LOAD       },
        {"not",         Token::NOT        },             // ...reserved
        {"null",        Token::NULL_VALUE },             // ...reserved
        {"on",          Token::ON         },             // ...reserved
        {"or",          Token::OR         },             // ...reserved
        {"order",       Token::ORDER      },             // ...reserved
        {"over",        Token::OVER       },
        {"partition",   Token::PARTITION  },
        {"preceding",   Token::PRECEDING  },
        {"query",       Token::QUERY      },
        {"redimension", Token::REDIMENSION},             // ...reserved
        {"regrid",      Token::REGRID     },             // ...reserved
        {"rename",      Token::RENAME     },             // ...reserved
        {"reserve",     Token::RESERVE    },
        {"save",        Token::SAVE       },
        {"select",      Token::SELECT     },             // ...reserved
        {"set",         Token::SET        },             // ...reserved
        {"shadow",      Token::SHADOW     },
        {"start",       Token::START      },
        {"step",        Token::STEP       },
        {"temp",        Token::TEMP       },
        {"thin",        Token::THIN       },
        {"to",          Token::TO         },
        {"unbound",     Token::UNBOUND    },
        {"unload",      Token::UNLOAD     },             // ...reserved
        {"update",      Token::UPDATE     },             // ...reserved
        {"using",       Token::USING      },
        {"variable",    Token::VARIABLE   },             // ...reserved
        {"where",       Token::WHERE      },             // ...reserved
        {"window",      Token::WINDOW     }              // ...reserved
    };

    return isKeyword(lexicon,text,lexeme,token);         // Is it a keyword?
}

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Return a flag to indicate which of the lexicons we are currently searching
 *  for keywords in.
 */
lexicon Lexer::getLexicon() const
{
    return _isKeyword==isAQLKeyword ? AQL : AFL;         // Return the lexicon
}

/**
 *  Switch to using the new lexicon 'n' to search for keywords in, and return
 *  a flag to indicate the lexicon that we were previously using.
 */
lexicon Lexer::setLexicon(lexicon n)
{
    lexicon o  = getLexicon();                           // Remember old value
    _isKeyword = n==AQL ? isAQLKeyword : isAFLKeyword;   // Assign new lexicon
    return  o;                                           // Return saved value
}

/****************************************************************************/
}}
/****************************************************************************/
