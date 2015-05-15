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
/* Bison Grammar: see http://www.gnu.org/software/bison/manual/html_node/...*/

%require            "2.4"                                // Min Bison version
%language           "c++"                                // Emit a C++ parser
%skeleton           "lalr1.cc"                           // Standard skeleton
%define             namespace           "scidb::parser"  // Parser namespace
%define             parser_class_name   "Parser"         // Parser class name
%expect             45                                   // Expected conflicts
%verbose                                                 // For parser details
%debug                                                   // For parser tracing

/****************************************************************************/
/* Additional data members for class Parser...*/

%parse-param        {Factory&  _fac}                     // The node factory
%parse-param        {Log&      _log}                     // The error log
%parse-param        {Node*&    _ast}                     // The resulting AST
%parse-param        {Lexer&   yylex}                     // The source lexer

/****************************************************************************/
/* Possible types for the semantic values $$, $1, $2 etc...*/

%union
{
    parser::Node*   node;                                // Arena allocated
    parser::real    real;                                // Passed by value
    parser::chars   string;                              // Arena allocated
    parser::chars   keyword;                             // Arena allocated
    parser::boolean boolean;                             // Passed by value
    parser::integer integer;                             // Passed by value
    parser::lexicon lexicon;                             // Passed by value
    size_t          list;                                // Shadow stack size
}

/****************************************************************************/
/* Tokens are listed from lowest precedence...*/

%left               MIN
%left               IN
%left               AS
%left               '@'
%left               OR
%left               AND
%right              NOT
%left               '=' '<' '>' NEQ LSEQ GTEQ
%nonassoc           BETWEEN
%left               '+' '-'
%left               '*' '/' '%'
%right              '^'
%left               MAX

/* ...to highest precedence. */
/****************************************************************************/
/* Control tokens...*/

%token              AQL_STATEMENT
%token              AFL_STATEMENT
%token              AFL_EXPRESSION
%token              AFL_MODULE
%token              LEXICAL_ERROR

/* Constants...*/

%token<real>        REAL
%token<string>      STRING
%token<boolean>     BOOLEAN
%token<integer>     INTEGER
%token<string>      IDENTIFIER

/* Keywords (ascending order, please)...*/

%token<keyword>     ALL
%token<keyword>     AND
%token<keyword>     ARRAY
%token<keyword>     AS
%token<keyword>     ASC
%token<keyword>     BETWEEN
%token<keyword>     BY
%token<keyword>     CANCEL
%token<keyword>     COMPRESSION
%token<keyword>     CREATE
%token<keyword>     CROSS
%token<keyword>     CURRENT
%token<keyword>     DEFAULT
%token<keyword>     DESC
%token<keyword>     DROP
%token<keyword>     ERRORS
%token<keyword>     FIXED
%token<keyword>     FN
%token<keyword>     FOLLOWING
%token<keyword>     FROM
%token<keyword>     GROUP
%token<keyword>     INSERT
%token<keyword>     INSTANCE
%token<keyword>     INSTANCES
%token<keyword>     INTO
%token<keyword>     IN
%token<keyword>     IS
%token<keyword>     JOIN
%token<keyword>     LET
%token<keyword>     LIBRARY
%token<keyword>     LOAD
%token<keyword>     NOT
%token<keyword>     NULL_VALUE
%token<keyword>     ON
%token<keyword>     OR
%token<keyword>     ORDER
%token<keyword>     OVER
%token<keyword>     PARTITION
%token<keyword>     PRECEDING
%token<keyword>     QUERY
%token<keyword>     REDIMENSION
%token<keyword>     REGRID
%token<keyword>     RENAME
%token<keyword>     RESERVE
%token<keyword>     SAVE
%token<keyword>     SELECT
%token<keyword>     SET
%token<keyword>     SHADOW
%token<keyword>     START
%token<keyword>     STEP
%token<keyword>     TEMP
%token<keyword>     THIN
%token<keyword>     TO
%token<keyword>     UNBOUND
%token<keyword>     UNLOAD
%token<keyword>     UPDATE
%token<keyword>     USING
%token<keyword>     VARIABLE
%token<keyword>     WHERE
%token<keyword>     WINDOW

/* Non terminals...*/

%type<node>         aql_statement
%type<node>         afl_statement
%type<node>         afl_expression
%type<node>         afl_module
%type<node>         attribute
%type<node>         dimension dexp cells
%type<node>         nullable
%type<node>         default
%type<node>         default_value
%type<node>         compression
%type<node>         reserve
%type<node>         temp
%type<node>         schema
%type<node>         asterisk
%type<node>         order
%type<node>         exp
%type<node>         binding where_group
%type<node>         parameter
%type<node>         reduced_exp
%type<node>         atomic_exp
%type<node>         identifier
%type<node>         version_clause
%type<node>         application
%type<node>         operand
%type<node>         reference
%type<node>         select_exp
%type<node>         grw_as_clause
%type<node>         reference_input
%type<node>         joined_input
%type<node>         named_exp
%type<node>         update
%type<node>         where_clause
%type<node>         select_item
%type<node>         instances
%type<node>         save_format
%type<node>         array_literal
%type<node>         array_literal_schema
%type<node>         window_clause
%type<node>         window_noise
%type<node>         window_dimension
%type<node>         window_value
%type<node>         regrid_clause
%type<node>         regrid_dimension
%type<node>         thin_clause
%type<node>         thin_dimension
%type<node>         array_source
%type<node>         group_by_clause
%type<node>         redimension_clause
%type<node>         order_by_clause
%type<node>         constant
%type<node>         constant_null
%type<node>         constant_real
%type<node>         constant_string
%type<node>         constant_boolean
%type<node>         constant_integer
%type<node>         create_array_statement
%type<node>         load_array_statement
%type<node>         load_format load_errors load_shadow
%type<node>         save_array_statement
%type<node>         drop_array_statement
%type<node>         rename_array_statement
%type<node>         load_library_statement
%type<node>         unload_library_statement
%type<node>         cancel_query_statement
%type<node>         insert_array_statement
%type<node>         update_array_statement
%type<node>         select_array_statement
%type<node>         select_into_statement

/* Various sequences and lists...*/

%type<list>         bindings
%type<list>         operands
%type<list>         parameters
%type<list>         references
%type<list>         attributes
%type<list>         dimensions
%type<list>         updates
%type<list>         select_items
%type<list>         reference_inputs
%type<list>         thin_dimensions
%type<list>         regrid_dimensions
%type<list>         window_clauses
%type<list>         window_dimensions

/****************************************************************************/
%code requires                                           // Head of Parser.hpp
{
#include "query/parser/ParserDetails.h"                  // For parser details
}
/****************************************************************************/
%code provides                                           // Foot of Parser.hpp
{
namespace scidb {namespace parser {
    typedef Parser::token           Token;               // For token numbers
    typedef Parser::semantic_type   semantic_type;       // The type of $i
    typedef Parser::location_type   location_type;       // The type of @i
}}
}
/****************************************************************************/
%code top                                                // Head of Parser.cpp
{
#include <query/ParsingContext.h>                        // For parsing context
#include "query/parser/Lexer.h"                          // For lexical analyzer
#include "query/parser/AST.h"                            // For abstract syntax
}
/****************************************************************************/
%%
/****************************************************************************/

start:
      AQL_STATEMENT  aql_statement                       {_ast = $2;}
    | AFL_STATEMENT  afl_statement                       {_ast = $2;}
    | AFL_EXPRESSION afl_expression                      {_ast = $2;}
    | AFL_MODULE     afl_module                          {_ast = $2;}
    ;

aql_statement:
      create_array_statement                             {$$ = $1;}
    | load_array_statement                               {$$ = $1;}
    | save_array_statement                               {$$ = $1;}
    | drop_array_statement                               {$$ = $1;}
    | rename_array_statement                             {$$ = $1;}
    | load_library_statement                             {$$ = $1;}
    | unload_library_statement                           {$$ = $1;}
    | cancel_query_statement                             {$$ = $1;}
    | insert_array_statement                             {$$ = $1;}
    | update_array_statement                             {$$ = $1;}
    | select_array_statement                             {$$ = $1;}
    | select_into_statement                              {$$ = $1;}
    ;

afl_statement:
      create_array_statement                             {$$ = $1;}
    | application                                        {$$ = $1;}
    ;

afl_expression:
      exp                                                {$$ = $1;}
    ;

afl_module:
      bindings                                           {$$ = _fac.newNode(module,@$,_fac.newList(@$,$1));}
    ;

/****************************************************************************/

exp:
      atomic_exp                                         {$$ = $1;}
    |     '+'  exp                             %prec MAX {$$ = $2;}
    |     '-'  exp                             %prec MAX {$$ = _fac.newApp(@1,"-",  $2);}
    | exp '+'  exp                                       {$$ = _fac.newApp(@2,"+",  $1,$3);}
    | exp '-'  exp                                       {$$ = _fac.newApp(@2,"-",  $1,$3);}
    | exp '*'  exp                                       {$$ = _fac.newApp(@2,"*",  $1,$3);}
    | exp '^'  exp                                       {$$ = _fac.newApp(@2,"^",  $1,$3);}
    | exp '='  exp                                       {$$ = _fac.newApp(@2,"=",  $1,$3);}
    | exp '/'  exp                                       {$$ = _fac.newApp(@2,"/",  $1,$3);}
    | exp '%'  exp                                       {$$ = _fac.newApp(@2,"%",  $1,$3);}
    | exp '<'  exp                                       {$$ = _fac.newApp(@2,"<",  $1,$3);}
    | exp '>'  exp                                       {$$ = _fac.newApp(@2,">",  $1,$3);}
    | exp  NEQ exp                                       {$$ = _fac.newApp(@2,"<>", $1,$3);}
    | exp LSEQ exp                                       {$$ = _fac.newApp(@2,"<=", $1,$3);}
    | exp GTEQ exp                                       {$$ = _fac.newApp(@2,">=", $1,$3);}
    |     NOT  exp                                       {$$ = _fac.newApp(@1,"not",$2);}
    | exp AND  exp                                       {$$ = _fac.newApp(@2,"and",$1,$3);}
    | exp OR   exp                                       {$$ = _fac.newApp(@2,"or", $1,$3);}
    | exp IS NULL_VALUE                                  {$$ = _fac.newApp(@3,"is_null",$1);}
    | exp IS NOT NULL_VALUE                              {$$ = _fac.newApp(@3,"not",_fac.newApp(@4,"is_null",$1));}
    | exp BETWEEN reduced_exp AND reduced_exp %prec BETWEEN
    {
        $$ = _fac.newApp(@$,"and",
             _fac.newApp(@$,">=" ,$1,$3),
             _fac.newApp(@$,"<=" ,_fac.newCopy($1),$5));
    }
    | exp AS identifier                                  {$$ = setAlias($1,_fac.newCopy($3));}
    | LET '{' bindings   '}' IN  exp                     {$$ = _fac.newFix(@$,_fac.newList(@3,$3),$6);}
    | FN  '(' parameters ')' '{' exp '}'                 {$$ = _fac.newAbs(@$,_fac.newList(@3,$3),$6);}
    ;

binding:
      identifier                    '=' exp where_group  {$$ = _fac.newNode(binding,@$,$1,_fac.newFix(@4,$4,$3));}
    | identifier '(' parameters ')' '=' exp where_group  {$$ = _fac.newNode(binding,@$,$1,_fac.newAbs(@3,_fac.newList(@3,$3),_fac.newFix(@7,$7,$6)));}
    ;

parameter:
      identifier                                         {$$ = _fac.newNode(binding,@$,$1,0);}
    ;

where_group:
      WHERE '{' bindings '}'                             {$$ = _fac.newList(@3,$3);}
    | blank                                              {$$ = _fac.newList(@$,0);}
    ;

/**
 *  A restricted infix epxression that omits the boolean operators in order to
 *  avoid reduce-reduce conflict on the BETWEEN token.
 */
reduced_exp:
      atomic_exp                                         {$$ = $1;}
    |             '+'  reduced_exp             %prec MAX {$$ = $2;}
    |             '-'  reduced_exp             %prec MAX {$$ = _fac.newApp(@1,"-",  $2);}
    | reduced_exp '+'  reduced_exp                       {$$ = _fac.newApp(@2,"+",  $1,$3);}
    | reduced_exp '-'  reduced_exp                       {$$ = _fac.newApp(@2,"-",  $1,$3);}
    | reduced_exp '*'  reduced_exp                       {$$ = _fac.newApp(@2,"*",  $1,$3);}
    | reduced_exp '^'  reduced_exp                       {$$ = _fac.newApp(@2,"^",  $1,$3);}
    | reduced_exp '='  reduced_exp                       {$$ = _fac.newApp(@2,"=",  $1,$3);}
    | reduced_exp '/'  reduced_exp                       {$$ = _fac.newApp(@2,"/",  $1,$3);}
    | reduced_exp '%'  reduced_exp                       {$$ = _fac.newApp(@2,"%",  $1,$3);}
    | reduced_exp '<'  reduced_exp                       {$$ = _fac.newApp(@2,"<",  $1,$3);}
    | reduced_exp '>'  reduced_exp                       {$$ = _fac.newApp(@2,">",  $1,$3);}
    | reduced_exp  NEQ reduced_exp                       {$$ = _fac.newApp(@2,"<>", $1,$3);}
    | reduced_exp LSEQ reduced_exp                       {$$ = _fac.newApp(@2,"<=", $1,$3);}
    | reduced_exp GTEQ reduced_exp                       {$$ = _fac.newApp(@2,">=", $1,$3);}
    | reduced_exp IS NULL_VALUE                          {$$ = _fac.newApp(@3,"is_null",$1);}
    | reduced_exp IS NOT NULL_VALUE                      {$$ = _fac.newApp(@3,"not",_fac.newApp(@4,"is_null",$1));}
    | reduced_exp AS identifier                          {$$ = setAlias($1,_fac.newCopy($3));}
    ;

atomic_exp:
      constant                                           {$$ = $1;}
    | reference                                %prec MIN {$$ = $1;}
    | application                                        {$$ = $1;}
    | application OVER identifier                        {$$ = _fac.newNode(olapAggregate,@$,$1,$3);}
    | '(' exp ')'                                        {$$ = $2;}
    ;

reference:
      identifier version_clause order                    {$$ = _fac.newRef(@$,$1,$2,$3);}
    | identifier '.' identifier order                    {$$ = _fac.newRef(@$,$3,$1,$4);}
    ;

application:
      identifier '(' asterisk ')'                        {$$ = _fac.newApp(@$,$1,$3);}
    | identifier '(' operands ')'                        {$$ = _fac.newApp(@$,$1,_fac.pop($3));}
    ;

operand:
      exp                                                {$$ = $1;}
    | schema                                             {$$ = $1;}
    | select_exp                                         {$$ = $1;}
    ;

version_clause:
                                                         {$$ = 0;}
    | '@' exp                                            {$$ = $2;}
    | '@' asterisk                                       {$$ = $2;}
    ;

select_exp:
      '('                                                {$<lexicon>$ = yylex.setLexicon(AQL);}
      select_array_statement ')'                         {$$ = $3;      yylex.setLexicon($<lexicon>2);}
    ;

order:
                                                         {$$ = 0;}
    | ASC                                                {$$ = _fac.newInteger(@$,ascending);}
    | DESC                                               {$$ = _fac.newInteger(@$,descending);}
    ;

asterisk:
    '*'                                                  {$$ = _fac.newNode(asterisk,@$);}
    ;

/**
 *  For locating the approximate position of an optional token.
 */
blank:                                                   {}
    ;

/****************************************************************************/

constant:
      constant_null                                      {$$ = $1;}
    | constant_real                                      {$$ = $1;}
    | constant_string                                    {$$ = $1;}
    | constant_boolean                                   {$$ = $1;}
    | constant_integer                                   {$$ = $1;}
    ;

constant_null:
    NULL_VALUE                                           {$$ = _fac.newNull(@$);}
    ;

constant_real:
    REAL                                                 {$$ = _fac.newReal(@$,$1);}
    ;

constant_string:
    STRING                                               {$$ = _fac.newString(@$,$1);}
    ;

constant_boolean:
    BOOLEAN                                              {$$ = _fac.newBoolean(@$,$1);}
    ;

constant_integer:
    INTEGER                                              {$$ = _fac.newInteger(@$,$1);}
    ;

/****************************************************************************/

identifier:
      IDENTIFIER                                         {$$ = _fac.newString(@$,$1);}
/* Context sensitive keywords common to both AFL and AQL: */
    | ARRAY                                              {$$ = _fac.newString(@$,$1);}
    | AS                                                 {$$ = _fac.newString(@$,$1);}
    | ASC                                                {$$ = _fac.newString(@$,$1);}
    | BETWEEN                                            {$$ = _fac.newString(@$,$1);}
    | COMPRESSION                                        {$$ = _fac.newString(@$,$1);}
    | CREATE                                             {$$ = _fac.newString(@$,$1);}
    | DESC                                               {$$ = _fac.newString(@$,$1);}
    | DEFAULT                                            {$$ = _fac.newString(@$,$1);}
    | IS                                                 {$$ = _fac.newString(@$,$1);}
    | RESERVE                                            {$$ = _fac.newString(@$,$1);}
    | TEMP                                               {$$ = _fac.newString(@$,$1);}
    | USING                                              {$$ = _fac.newString(@$,$1);}
/* Context sensitive keywords specific to AQL: */
    | ALL                                                {$$ = _fac.newString(@$,$1);}
    | BY                                                 {$$ = _fac.newString(@$,$1);}
    | CURRENT                                            {$$ = _fac.newString(@$,$1);}
    | DROP                                               {$$ = _fac.newString(@$,$1);}
    | ERRORS                                             {$$ = _fac.newString(@$,$1);}
    | FOLLOWING                                          {$$ = _fac.newString(@$,$1);}
    | INSTANCE                                           {$$ = _fac.newString(@$,$1);}
    | INSTANCES                                          {$$ = _fac.newString(@$,$1);}
    | LIBRARY                                            {$$ = _fac.newString(@$,$1);}
    | LOAD                                               {$$ = _fac.newString(@$,$1);}
    | OVER                                               {$$ = _fac.newString(@$,$1);}
    | PARTITION                                          {$$ = _fac.newString(@$,$1);}
    | PRECEDING                                          {$$ = _fac.newString(@$,$1);}
    | QUERY                                              {$$ = _fac.newString(@$,$1);}
    | SAVE                                               {$$ = _fac.newString(@$,$1);}
    | SHADOW                                             {$$ = _fac.newString(@$,$1);}
    | START                                              {$$ = _fac.newString(@$,$1);}
    | STEP                                               {$$ = _fac.newString(@$,$1);}
    | THIN                                               {$$ = _fac.newString(@$,$1);}
    | TO                                                 {$$ = _fac.newString(@$,$1);}
    | UNBOUND                                            {$$ = _fac.newString(@$,$1);}
    ;

/****************************************************************************/

create_array_statement:
      CREATE temp ARRAY reference schema                       {$$ = _fac.newApp(@$,"Create_Array",$4,$5,$2);}
    | CREATE temp ARRAY reference schema cells USING reference {$$ = _fac.newApp(@$,"Create_Array",$4,$5,$2,$8,$6);}
    ;

schema:
      '<' attributes '>' '[' dimensions ']'              {$$ = _fac.newNode(schema,@$,_fac.newList(@2,$2),_fac.newList(@5,$5),0);}
    ;

attribute:
     identifier ':' identifier nullable default compression reserve {$$ = _fac.newNode(attribute,@$,$1,$3,$4,$5,$6,$7);}
    ;

dimension:
      identifier '=' dexp ':' dexp     ',' dexp ',' dexp {$$ = _fac.newNode(dimension,@$,$1,$3,$5,$7,$9);}
    | identifier '=' dexp ':' asterisk ',' dexp ',' dexp {$$ = _fac.newNode(dimension,@$,$1,$3,$5,$7,$9);}
    | identifier                                         {$$ = _fac.newNode(dimension,@$,$1,0,0,0,0);}
    ;

dexp:
      exp                                                {$$ = $1;}
    | '?'                                                {$$ = 0;}
    ;

default_value:
          constant                                       {$$ = $1;}
    | '+' constant                                       {$$ = $2;}
    | '-' constant                                       {$$ = _fac.newApp(@1,"-",$2);}
    |     application                                    {$$ = $1;}
    | '+' application                                    {$$ = $2;}
    | '-' application                                    {$$ = _fac.newApp(@1,"-",$2);}
    | '(' exp ')'                                        {$$ = $2;}
    ;

nullable:
          NULL_VALUE                                     {$$ = _fac.newBoolean(@$,true);}
    | NOT NULL_VALUE                                     {$$ = _fac.newBoolean(@$,false);}
    | blank                                              {$$ = _fac.newBoolean(@$,false);}
    ;

default:
      DEFAULT default_value                              {$$ = $2;}
    | blank                                              {$$ = 0;}
    ;

compression:
      COMPRESSION constant_string                        {$$ = $2;}
    | blank                                              {$$ = 0;}
    ;

reserve:
      RESERVE constant_integer                           {$$ = $2;}
    | blank                                              {$$ = 0;}

temp:
      TEMP                                               {$$ = _fac.newBoolean(@$,true);}
    | blank                                              {$$ = _fac.newBoolean(@$,false);}
    ;
    
cells:
      '[' exp ']'                                        {$$ = $2;}
    | blank                                              {$$ = _fac.newInteger(@$,1000000);}
    ;
    
/****************************************************************************/

load_array_statement:
      LOAD reference FROM instances constant_string load_format load_errors load_shadow
    {
        if ($8 == 0)
        {
            $$ = _fac.newApp(@$,"Load",$2,$5,$4,$6,$7);
        }
        else
        {
            $$ = _fac.newApp(@$,"Load",$2,$5,$4,$6,$7,$8);
        }
    }
    ;

load_format:
      AS constant_string                                 {$$ = $2;}
    | blank                                              {$$ = _fac.newString(@$,"");}
    ;

load_errors:
      ERRORS constant_integer                            {$$ = $2;}
    | blank                                              {$$ = _fac.newInteger(@$,0);}
    ;

load_shadow:
      SHADOW ARRAY reference                             {$$ = $3;}
    |                                                    {$$ = 0;}
    ;

save_array_statement:
      SAVE reference INTO instances constant_string save_format
    {
        $$ = _fac.newApp(@$,"Save",$2,$5,$4,$6);
    }
    ;

save_format:
      AS constant_string                                 {$$ = $2;}
    | blank                                              {$$ = _fac.newString(@$,"auto");}
    ;

instances:
               constant_integer                          {$$ = $1;}
    | INSTANCE constant_integer                          {$$ = $2;}
    | ALL      INSTANCES                                 {$$ = _fac.newInteger(@$,allInstances);}
    | CURRENT  INSTANCE                                  {$$ = _fac.newInteger(@$,thisInstance);}
    | blank                                              {$$ = _fac.newInteger(@$,thisInstance);}
    ;

update_array_statement:
    UPDATE reference SET updates where_clause            {$$ = _fac.newNode(updateArray,@$,$2,_fac.newList(@4,$4),$5);}
    ;

update:
      identifier '=' exp                                 {$$ = _fac.newNode(update,@$,$1,$3);}
    ;

drop_array_statement:
      DROP ARRAY reference                               {$$ = _fac.newApp(@$,"Remove",$3);}
    ;

rename_array_statement:
      RENAME ARRAY reference TO reference                {$$ = _fac.newApp(@$,"Rename",$3,$5);}
    ;

load_library_statement:
      LOAD LIBRARY constant_string                       {$$ = _fac.newApp(@$,"Load_Library",$3);}
    ;

unload_library_statement:
      UNLOAD LIBRARY constant_string                     {$$ = _fac.newApp(@$,"Unload_Library",$3);}
    ;

cancel_query_statement:
      CANCEL QUERY constant_integer                      {$$ = _fac.newApp(@$,"Cancel",$3);}
    ;

insert_array_statement:
      INSERT INTO identifier constant_string             {$$ = _fac.newNode(insertArray,@$,$3,$4);}
    | INSERT INTO identifier select_array_statement      {$$ = _fac.newNode(insertArray,@$,$3,$4);}
    ;

/****************************************************************************/

select_array_statement:
      SELECT select_items
    {
        $$ = _fac.newNode(selectArray,@$,_fac.newList(@2,$2),0,0,0,0,0);
    }
    | SELECT select_items                 FROM reference_inputs where_clause grw_as_clause order_by_clause
    {
        $$ = _fac.newNode(selectArray,@$,_fac.newList(@2,$2),0,_fac.newList(@4,$4),$5,$6,$7);
    }
    ;

select_into_statement:
      SELECT select_items INTO identifier
    {
        $$ = _fac.newNode(selectArray,@$,_fac.newList(@2,$2),$4,0,0,0,0);
    }
    | SELECT select_items INTO identifier FROM reference_inputs where_clause grw_as_clause order_by_clause
    {
        $$ = _fac.newNode(selectArray,@$,_fac.newList(@2,$2),$4,_fac.newList(@6,$6),$7,$8,$9);
    }
    ;

select_item:
      named_exp                                          {$$ = $1;}
    | asterisk                                           {$$ = $1;}
    ;

where_clause:
                                                         {$$ =  0;}
    | WHERE exp                                          {$$ = $2;}
    ;

grw_as_clause:
                                                         {$$ =  0;}
    | group_by_clause                                    {$$ = $1;}
    | redimension_clause                                 {$$ = $1;}
    | regrid_clause                                      {$$ = $1;}
    | window_clauses                                     {$$ = _fac.newList(@$,$1);}
    ;

group_by_clause:
      GROUP BY references                                {$$ = _fac.newNode(groupByClause,@$,_fac.newList(@3,$3));}
    ;

redimension_clause:
      REDIMENSION BY '[' dimensions ']'                  {$$ = _fac.newNode(redimensionClause,@$,_fac.newList(@4,$4));}
    ;

regrid_clause:
      REGRID AS '(' PARTITION BY regrid_dimensions ')'   {$$ = _fac.newNode(regridClause,@$,_fac.newList(@6,$6));}
    ;

regrid_dimension:
      reference constant_integer                         {$$ = _fac.newNode(regridDimension,@$,$1,$2);}
    | reference CURRENT                                  {$$ = _fac.newNode(regridDimension,@$,$1,_fac.newInteger(@$,1));}
    ;

window_clause:
      window_noise WINDOW            AS '(' PARTITION BY window_dimensions ')'
    {
        $$ = _fac.newNode(windowClause,@$,_fac.newString(@$,""),_fac.newList(@7,$7),$1);
    }
    | window_noise WINDOW identifier AS '(' PARTITION BY window_dimensions ')'
    {
        $$ = _fac.newNode(windowClause,@$,$3,_fac.newList(@8,$8),$1);
    }
    ;

window_noise:
      VARIABLE                                           {$$ = _fac.newBoolean(@$,true);}
    | FIXED                                              {$$ = _fac.newBoolean(@$,false);}
    | blank                                              {$$ = _fac.newBoolean(@$,false);}
    ;

window_dimension:
      reference window_value PRECEDING AND window_value FOLLOWING
    {
        $$ = _fac.newNode(windowDimensionRange,@$,$1,$2,$5);
    }
    | reference CURRENT
    {
        $$ = _fac.newNode(windowDimensionRange,@$,$1,_fac.newInteger(@2,0),_fac.newInteger(@2,0));
    }
    ;

window_value:
      constant_integer                                   {$$ = $1;}
    | UNBOUND                                            {$$ = _fac.newInteger(@$,-1);}
    ;

order_by_clause:
                                                         {$$ =  0;}
    | ORDER BY references                                {$$ = _fac.newList(@3,$3);}
    ;

named_exp:
      exp
    {
     // lift an optional alias up from the expression to the namedExpr - todo: jab: move this into the translator
        $$ = _fac.newNode(namedExpr,@$,$1,_fac.newCopy(getAlias($1)));
    }
    ;

reference_input:
      array_source                                       {$$ = _fac.newNode(namedExpr,@$,$1,0);}
    | array_source AS identifier                         {$$ = _fac.newNode(namedExpr,@$,$1,$3);}
    | joined_input                                       {$$ = $1;}
    | thin_clause                                        {$$ = $1;}
    ;

array_source:
      reference                                %prec MIN {$$ = $1;}
    | application                                        {$$ = $1;}
    | array_literal                                      {$$ = $1;}
    | select_exp                               %prec MAX {$$ = $1;}
    ;

array_literal:
      ARRAY '(' array_literal_schema ',' constant_string ')'
    {
        $$ = _fac.newApp(@$,"Build",$3,$5,_fac.newBoolean(@1,true));
    }
    ;

array_literal_schema:
      reference                                          {$$ = $1;}
    | schema                                             {$$ = $1;}
    ;

joined_input:
      reference_input       JOIN reference_input ON exp  {$$ = _fac.newNode(joinClause,@$,$1,$3,$5);}
    | reference_input CROSS JOIN reference_input         {$$ = _fac.newNode(joinClause,@$,$1,$4,0);}
    ;

thin_clause:
      THIN array_source BY '(' thin_dimensions ')'
    {
        $$ = _fac.newNode(thinClause,@$,$2,_fac.newList(@5,$5));
    }
    ;

thin_dimension:
      reference START constant_integer STEP constant_integer
    {
        $$ = _fac.newNode(thinDimension,@$,$1,$3,$5);
    }
    ;

/****************************************************************************/
/* Lists and sequences of various kinds, all handled in the same basic manner:
 * each item is pushed onto a 'shadow' stack managed by the factory - we track
 * the number of items pushed in the semantic value '$$', which thus indicates
 * how many items the newList() factory function must later pop off the shadow
 * stack...*/

bindings:
                                                         {$$ = 0;                 }
    | binding                                            {$$ = 1;   _fac.push($1);}
    | bindings ';'                                       {$$ = $1;                }
    | bindings ';' binding                               {$$ = $1+1;_fac.push($3);}
    ;

operands:
                                                         {$$ = 0;                 }
    | operand                                            {$$ = 1;   _fac.push($1);}
    | operands ',' operand                               {$$ = $1+1;_fac.push($3);}
    ;

parameters:
                                                         {$$ = 0;                 }
    | parameter                                          {$$ = 1;   _fac.push($1);}
    | parameters ',' parameter                           {$$ = $1+1;_fac.push($3);}
    ;

references:
      reference                                          {$$ = 1;   _fac.push($1);}
    | references ',' reference                           {$$ = $1+1;_fac.push($3);}
    ;

attributes:
      attribute                                          {$$ = 1;   _fac.push($1);}
    | attributes ',' attribute                           {$$ = $1+1;_fac.push($3);}
    ;

dimensions:
      dimension                                          {$$ = 1;   _fac.push($1);}
    | dimensions ',' dimension                           {$$ = $1+1;_fac.push($3);}
    ;

updates:
      update                                             {$$ =  1;  _fac.push($1);}
    | updates ',' update                                 {$$ = $1+1;_fac.push($3);}
    ;

select_items:
      select_item                                        {$$ = 1;   _fac.push($1);}
    | select_items ',' select_item                       {$$ = $1+1;_fac.push($3);}
    ;

reference_inputs:
      reference_input                                    {$$ = 1;   _fac.push($1);}
    | reference_inputs ',' reference_input               {$$ = $1+1;_fac.push($3);}
    ;

regrid_dimensions:
      regrid_dimension                                   {$$ = 1;   _fac.push($1);}
    | regrid_dimensions ',' regrid_dimension             {$$ = $1+1;_fac.push($3);}
    ;

thin_dimensions:
      thin_dimension                                     {$$ = 1;   _fac.push($1);}
    | thin_dimensions ',' thin_dimension                 {$$ = $1+1;_fac.push($3);}
    ;

window_dimensions:
      window_dimension                                   {$$ = 1;   _fac.push($1);}
    | window_dimensions ',' window_dimension             {$$ = $1+1;_fac.push($3);}
    ;

window_clauses:
      window_clause                                      {$$ = 1;   _fac.push($1);}
    | window_clauses ',' window_clause                   {$$ = $1+1;_fac.push($3);}
    ;

/****************************************************************************/
%%
/****************************************************************************/
namespace scidb { namespace parser {
/****************************************************************************/

void Parser::error(const location_type& w,const string& s)
{
    _log.fail(SCIDB_LE_QUERY_PARSING_ERROR,w,s.c_str()); // Log a syntax error
}

/****************************************************************************/
}}
/****************************************************************************/
