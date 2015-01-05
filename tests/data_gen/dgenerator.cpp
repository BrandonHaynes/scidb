/*
**  File:   datagenerator.cpp
**
*
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
* 
** 
** About: 
**
** How to Build:

all: clean dgenerator.o
	g++ -o dgenerator dgenerator.o

clean:
	rm -f dgenerator dgenerator.o

dgenerator.o: dgenerator.cpp
	g++ -c -o dgenerator.o -g dgenerator.cpp

**
** How to use.
**
The new arguments are:

	-U - When this option is provided, create an updated array as in 
		 "create updatable array foobar....", if not provided a 
		 non-updatedable array is created.

	-A - where as is the dimension syntax associated with the array you 
	     want to create.  Program will only support two dimensions, 
		 but will support up to 128 dimensions with no change once the 
	     data generation code has been fixed to support it 

			(e.g. -A-Ab=0:99,10,0 -Ac=0:99,10,0). 
		
		The syntax is the same as used by scidb, but almost no checking 
		is done.

	-Tfilename - filename to store the table create syntax. A file named 
	    "filename.txt" will be created.

	-R - random data generation for each cell

	-D - generate data based off the current cell number (give nice 
	     incremental data for numerics and testing. 

	-P - Probability, 1.0 is a dense array with all cells containing data, 
		 less than 1 means that some cells will not contain data.  
		 Probabilty equals percentage of cells to fill with data.

	-I, -J - exist for compatability with existing data generation 
	    code.  Basically sets nRowChunks and nColChunks respectively.  
		Control size of data file.  These have to change when 
		multi-dimensional array creation is supported.

	-C - A list of attribute types created for your array. This is a 
		string made of of the characters NSCGR. This allows the creation 
		of up to 128 attributes for an array by simply including as 
		many of the 4 letters as desired in any combination. 

					C is a char attribute. 
					N is an int32 attribute. 
					S is a string attribute. 
					G is a double attribute.
					R is a rational attribute.
					M is a int8 
					O is a int16

Running dgenerator with the following paramaters:

./dgenerator -Aa=0:99,10,0 -CNC  -Ab=0:99,10,0 -Ac=0:99,10,0 -Tfoobar -U -D -P1.0 -I10 -J10  > foobar.data

	Will generate the following create array syntax:

"CREATE  UPDATABLE ARRAY foobar <  N1804289383: int32, C846930886: char > [a=0:99,10,0,b=0:99,10,0]"

The generated data will be redirected to foobar.data

** 
*/
#include <vector>
#include <iostream>
#include <string.h>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>

#include <assert.h>

#define MAX_DIMENSION_CNT 128 
#define MAX_ATTRIBUTES_CNT 128
#define MAX_FILENAME_SIZE 128
#define MAX_LGBUFFER_SIZE 2048
#define MAX_MEDBUFFER_SIZE 1024

struct arrayInfo {
	int nStart;
	int nFinish;
	int nDataItemsPerChunck;
	int nOverlap;
	char szArrayDefinition[MAX_ATTRIBUTES_CNT];
} arrayDimensionInfo [MAX_DIMENSION_CNT];

int columnNumber = 0;

void
int2str ( const int v, 
		  char * out ) 
{
	int lv = v;
	int off = 0;

	assert((NULL != out));
	assert((0 <= v));

	do { 
		out[off]=('A'+(lv%26));
		lv/=26;
		off++;
	} while (0 < lv);

	out[off]=0;
}

void
usage(char * szProg)
{
	printf("%s [-t arraynamef] -[rd] int int int int int prob string\n", szProg);
	printf("%s -t arrayname is optional\n",szProg);
	printf("%s [-t arrayname] (-[r]andom or -[d]calculated) #rowchunks #colchunks #rowsperchunk #coldperchunk xoverlap yoverlap probability[0.0->1.0] string\n", szProg);
	printf("   The string is some combination of D - double, N - integer, C - char or S - string\n");
	printf("   argc must equal 11 for -T and -D options or argc must equal 9 for -D alone\n");
	printf("   arrayname will be the name of the array in the generated CREATE ARRAY statement , the syntax will be written to arrayname.txt\n");
}

void
print_random_attr ( const int nTypeCnt, const char * szTypesListStr) 
{
	char szString[32];
	printf("(");
	for(int c = 0; c < nTypeCnt; c++) { 
		if (c) printf(", ");

		switch(toupper(szTypesListStr[c])) {
			case 'G':	/* double */
				printf("%g", ((double)random()/(double)INT_MAX));
			 break;

			case 'N':	/* integer */
				printf("%ld", random());
			break;

			case 'S':	/* string */
				int2str((random()%20000), szString);
				printf("\"%s\"", szString);
			break;

			case 'C':  /* char */
				int2str((random()%26), szString);
				printf("\"%s\"", szString);
			break;
			case 'R':	/* rational */
				printf("\"( %ld / %ld )", random(), random());
			break;
                        case 'M':       /* int8_t */
				printf("%d", (int) (random()%128));
                        break; 
                        case 'O':       /* int16_t */
                                printf("%d", (int) (random()%32768));
                        break; 
			default:	/* nothing */
		    break;
		}
	}
	printf(")");
}

void
print_det_attr ( const int nTypeCnt, const char * szTypesListStr, 
				 const int nCellNum, const int nCellMax ) 
{
	char szString[32]; 
	
	printf("(");
	for(int c = 0; c < nTypeCnt; c++) { 
		if (c) printf(", ");

		switch(toupper(szTypesListStr[c])) {
			case 'G':	/* double */
				printf("%g", ((double)nCellNum/(double)nCellMax));
			 break;

			case 'N':	/* integer */
				printf("%d", nCellNum);
			break;

			case 'S':	/* string */
				int2str(nCellNum, szString);
				printf("\"%s\"", szString);
			break;

			case 'C':	/* char */
				int2str((nCellNum%26), szString);
				printf("\"%s\"", szString);
			break;
			case 'R':	/* rational */
				printf("\"(%d / %d)\"", nCellNum, nCellMax);
			break;
                        case 'M':       /* int8_t */
                                printf("%d", (nCellNum%128));
                        break; 
                        case 'O':       /* int16_t */
                                printf("%d", (nCellNum%32768));
                        break;
                        default:        /* nothing */
		   	break;
		}
	}
	printf(")");
}

void
print_empty_attr()
{
	printf("()");
}

void
initialize_syntax(char * szString,char * szTableCreateSyntax, bool bupdatable) {
        //Remove " so we can use create statement in an input file to iquery
    	//szTableCreateSyntax = strncat( szTableCreateSyntax, "\"", 1);
	szTableCreateSyntax = strncat( szTableCreateSyntax, "CREATE ", 7 );
	if (bupdatable) strncat( szTableCreateSyntax, " UPDATABLE " , 11);
	szTableCreateSyntax = strncat( szTableCreateSyntax, "ARRAY ", 6 );
	szTableCreateSyntax = strncat( szTableCreateSyntax, szString,strlen(szString) );
}

void
create_attributes( const char * szTypesListStr, const int nTypeCnt, char * szTableSyntax) {
	int  nWritten		= 0;
        char columnPrefix[]     = "COL";
	char szPtr[128];
	szTableSyntax = strncat( szTableSyntax, " < ",3);
	
	for(int c = 0; c < nTypeCnt && columnNumber < 128; c++) { 
    		if (c) szTableSyntax = strncat( szTableSyntax, ",",1); 
		nWritten = sprintf(szPtr, "%s", columnPrefix);
		nWritten += sprintf(&szPtr[nWritten], "%3.3d", columnNumber);
                columnNumber++;
		switch(toupper(szTypesListStr[c])) {
			case 'G':	/* double */
				nWritten += sprintf(&szPtr[nWritten], "%s", "G");
				nWritten += sprintf(&szPtr[nWritten], "%s", ": double ");
				//printf("%s\n",&szPtr[0]);
				szTableSyntax = strncat( szTableSyntax, szPtr, (strlen(szPtr)-1) );				
				//printf("%s\n",szTableSyntax);
			 break;

			case 'N':	/* integer */
				nWritten += sprintf(&szPtr[nWritten], "%s", "N");
				nWritten += sprintf(&szPtr[nWritten], "%s", ": int32 ");
				szTableSyntax = strncat( szTableSyntax, szPtr, (strlen(szPtr)-1) );				
				//printf("%s\n",szTableSyntax);
			break;

 			case 'S':	/* string */
				nWritten += sprintf(&szPtr[nWritten], "%s", "S");
				nWritten += sprintf(&szPtr[nWritten], "%s", ": string ");
				szTableSyntax = strncat( szTableSyntax, szPtr, (strlen(szPtr)-1) );
				//printf("%s\n",szTableSyntax);
 			break;
 
 			case 'C':	/* char */
				nWritten += sprintf(&szPtr[nWritten], "%s", "C");
				nWritten += sprintf(&szPtr[nWritten], "%s", ": char ");
				szTableSyntax = strncat( szTableSyntax, szPtr, (strlen(szPtr)-1) );
 			break; 
			case 'R':	/* rational */
				nWritten += sprintf(&szPtr[nWritten], "%s", "R");
				nWritten += sprintf(&szPtr[nWritten], "%s", ": rational ");
				szTableSyntax = strncat( szTableSyntax, szPtr, (strlen(szPtr)-1) );				
				//printf("%s\n",szTableSyntax);
			break;
			case 'M':       /* rational */
                                nWritten += sprintf(&szPtr[nWritten], "%s", "M");
                                nWritten += sprintf(&szPtr[nWritten], "%s", ": int8 ");
                                szTableSyntax = strncat( szTableSyntax, szPtr, (strlen(szPtr)-1) );
                                //printf("%s\n",szTableSyntax);
                        break;
			case 'O':       /* rational */
                                nWritten += sprintf(&szPtr[nWritten], "%s", "O");
                                nWritten += sprintf(&szPtr[nWritten], "%s", ": int16 ");
                                szTableSyntax = strncat( szTableSyntax, szPtr, (strlen(szPtr)-1) );
                                //printf("%s\n",szTableSyntax);
                        break;


			default:	/* nothing */
		    	break;

		}
	}

	//printf("TableSyntax is: %s\n",szPtr2);

}
void
finish_attributes(char * szTableSyntax) {
	szTableSyntax = strncat( szTableSyntax, " > ", 3);
 

}
void
finish_arrays(char * szTableSyntax) {

	szTableSyntax = strncat( szTableSyntax, "]", 1);
}
void
finish_statement(char * szTableSyntax) {
//      remove " so we can use create statement in file as -f option to iquery	
//	szTableSyntax = strncat( szTableSyntax, "\"", 1);
}
void
write_tablesyntax_file (const char * szString, const char * szTableSyntax) {
	const char * szConcat = {".txt"};
	char  szFileName [MAX_FILENAME_SIZE];
	char  szSyntax[MAX_LGBUFFER_SIZE];
	char  * szPtr = szFileName;
	char  * szPtr2 = szSyntax;
	int	  nResult = 0;
	FILE * pFile;
	
        memset(szPtr, 0, (sizeof(szFileName)));
	strncpy(szFileName, szString, strlen(szString) );
	strcpy(szSyntax, szTableSyntax );
	szPtr = strcat( szPtr, szConcat );
	pFile = fopen (szPtr,"w");
	if (pFile == NULL) {
		printf ("%s%s\n", "Failed to open:", szPtr);
		exit(1);
	}

	nResult = fputs(szPtr2, pFile);
	if (nResult <= 0) {
		printf ("%s%s\n", "Failed writing create table string:", szTableSyntax);
		exit(1);
	}
        fclose(pFile); 
}


int processArrayInfo(char * szArrayInfo, int nDimensionCnt) {
	
    char * szPtr = NULL;
	
	arrayInfo * aInfo = &arrayDimensionInfo[nDimensionCnt];

   	//Initialize struct string
        memset(arrayDimensionInfo[nDimensionCnt].szArrayDefinition, 0, (sizeof(arrayDimensionInfo[nDimensionCnt].szArrayDefinition)));
	//Save the attribute type information

        //Now put the array definition into the struct.
        strcpy(&arrayDimensionInfo[nDimensionCnt].szArrayDefinition[0], &szArrayInfo[0]);
        if (!isalpha(arrayDimensionInfo[nDimensionCnt].szArrayDefinition[0])) {
           printf("firstchar is %s\n",&arrayDimensionInfo[nDimensionCnt].szArrayDefinition[0]);
           printf("Failure 1 in processArrayInfo: Array name does not start with a letter: %s\n",&arrayDimensionInfo[nDimensionCnt].szArrayDefinition[0]);
           exit(1);
        }

	//printf("The array definition is: %s\n",&arrayDimensionInfo[nDimensionCnt].szArrayDefinition[0]);
  
    //Now convert the array dimensions into integer format so they can be used later.
    szPtr = strchr( &arrayDimensionInfo[nDimensionCnt].szArrayDefinition[0], '=');
    szPtr++;
	aInfo->nStart = atoi(szPtr);
	szPtr = strchr(szPtr, ':');
	szPtr++;
    aInfo->nFinish = atoi(szPtr);
    szPtr = strchr(szPtr, ',');
    szPtr++;
    aInfo->nDataItemsPerChunck = atoi(szPtr);
 	szPtr = strchr(szPtr, ',');
    szPtr++;
	aInfo->nOverlap = atoi(szPtr);
	//printf("struct content is: %d, %d, %d, %d\n", aInfo->nStart, aInfo->nFinish, aInfo->nDataItemsPerChunck, aInfo->nOverlap);      
    //Check the numbers
    if (aInfo->nStart != 0) {
       printf("Failure 2 in processArrayInfo: Array start must be 0.\n");
       exit(1);
    }
	assert((0 < aInfo->nFinish));
	assert((0 < aInfo->nDataItemsPerChunck));
	assert((-1 < aInfo->nOverlap));       
 
	return ++nDimensionCnt;
}

void 
coords2Buffer ( int * coord, size_t len, char * szBuffer ) 
{
	assert ((NULL != coord));
	assert ((NULL != szBuffer));
	assert (( len < MAX_DIMENSION_CNT));

	char szLocal[48];

	strcpy ( szBuffer, "{ ");

	for ( size_t i = 0; i < len; i++) { 
		if ( i ) 
			sprintf ( szLocal, ", %d", coord[i]);
		else 
			sprintf ( szLocal, "%d", coord[i]);

		strcat ( szBuffer, szLocal );
	}

	strcat ( szBuffer, "}");
}

int
main (int argc, char ** argv ) 
{
    /*
}   ** Loop over the input until EOF, placing a line's worth of tokens
    ** into the list as you go.
    */
	int	nRowChunks	=   	0;
	int	nColChunks  	=   	0;
    int nRowsInChunk   	=   	0;
	int	nColsInChunk  	=   	0;
	int	nCount          =   	0;
	int	nTypeCnt 	= 	0;
	int	nIsDense        = 	0;
	int	nIsRandom       = 	0;
	int  	nCellNum	= 	0;
	int 	nCellMax	=  	0;
    int     nDimensionCnt	=	0;
	double  dbProb          =       0.0;    
   	char *  szPtr 		= 	NULL;
	bool    bUpdatable		= false;
	bool    bTableCreateSyntax 	= false;
	char    szTableSyntax[MAX_LGBUFFER_SIZE] =	{"  "}; 
    char    szFileName[MAX_FILENAME_SIZE];
	char    szString[MAX_MEDBUFFER_SIZE];
   	char   	szTypesListStr[MAX_MEDBUFFER_SIZE];
	int 	coord[MAX_DIMENSION_CNT];
	int 	chunkPos[MAX_DIMENSION_CNT];
 


	//printf("%s%u\n", "argc is:", argc);

	memset(coord,0,sizeof(coord));
	memset(chunkPos,0,sizeof(chunkPos));
        memset(szFileName, 0, (sizeof(szFileName)));
        memset(szTableSyntax, 0, (sizeof(szTableSyntax)));
        memset(szTypesListStr, 0, (sizeof(szTypesListStr)));
	
	if (argc > 1 ) {
		for (nCount = 1; nCount < argc; nCount++) {
                        memset(szString, 0, sizeof(szString));
            		strncpy(szString, argv[nCount], strlen(argv[nCount]));
			//printf("argv is: %s\n", argv[nCount]);
            		if ((szString[0]) != '-' ) {
					usage(argv[0]);
                			exit(0);

            		} 
            //printf("arg length is: %i\n", (int) strlen(argv[nCount])); 
		switch(toupper(szString[1])) { 
				case 'R':
					nIsRandom = 1; 
		 		break;
				case 'D':
					nIsRandom = 0; 
		 		break;
				case 'T':
					bTableCreateSyntax = 1;
 					strcpy(szFileName, &szString[2]);
		 		break;
				case 'H':
					usage(argv[0]);
					exit(0);
		 		break;
				case 'A'://Array dimensions
					nDimensionCnt = processArrayInfo(&szString[2], nDimensionCnt);
					//printf("totalarrays are: %d\n", nDimensionCnt);
                                break;
                                case 'P':
					dbProb = atof(&szString[2]);
					assert((0.0 < dbProb));
					assert((1.0 >= dbProb));	
				break;
				case 'I':
					nRowChunks = atoi(&szString[2]);
				        assert((0 < nRowChunks));
               			break;
				case 'J':
					nColChunks = atoi(&szString[2]);
					assert((0 < nColChunks));
                		break;
				case 'C': //Data definitions (aka array attributes)
					if ((strlen(&szString[2])) > 128) {
						printf("main failure: more than 128 attributes requested\n");
						exit(1);
					}
                    			strcpy(&szTypesListStr[0],&szString[2]);
					//printf("Data Definitions are: %s\n",&szString[2]);
                		break;
				case 'U': //make array updatable.
					bUpdatable = 1; 
                		break;
				default:
					usage(argv[0]);
					exit(0);
		 		break;

			}
		}
	}
	else {
		usage(argv[0]);
		exit(0);
	}
#ifdef  __UNDEFINED__ 
 	int count;

 	printf ("This program, \"%s\", was called with:\n",argv[0]);

 	if (argc > 1) {
      		for (count = 1; count < argc; count++) {
	  		printf("argv[%d] = %s\n", count, argv[count]);
		}
    	}	
  	else
    	{
      		printf("The command had no other arguments.\n");
    	}
#endif
	initialize_syntax(szFileName,szTableSyntax,bUpdatable);
	nTypeCnt = strlen(szTypesListStr);
	

	if ( bTableCreateSyntax ) {
		szPtr = &szTypesListStr[0];
		create_attributes(szTypesListStr, nTypeCnt, szTableSyntax);
		finish_attributes(szTableSyntax);
		szPtr = strncat( szTableSyntax, "[", 1);
		for (int i = 0; i < nDimensionCnt; i++) {
			size_t x = strlen(&arrayDimensionInfo[i].szArrayDefinition[0]);
			szPtr = strncat(szPtr, &arrayDimensionInfo[i].szArrayDefinition[0], x);
                        if ( i != (nDimensionCnt - 1)) {
				szPtr = strncat( szTableSyntax, ",", 1);
			}
		}
		finish_arrays(szPtr);
		finish_statement(szPtr);
		write_tablesyntax_file(szFileName, szTableSyntax);
	}	
	
	if (0.1 <= dbProb) 
		nIsDense = 1; 
	
	//** Some calculation, keeping new argv processing compatable with existing code until attribute generation is updated for any number of array definitions.
        nRowsInChunk = arrayDimensionInfo[0].nDataItemsPerChunck;
        nColsInChunk = arrayDimensionInfo[1].nDataItemsPerChunck;
		nCellMax = nRowChunks * nRowsInChunk * nColChunks * nColsInChunk;
        //printf("chunkinfo is: %d,%d, %d, %d\n", nRowChunks , nRowsInChunk , nColChunks , nColsInChunk);
      
	//New code to be used when attribute and array generation are updated to allow any number of arrays. 
	//Establish base by first multiply the maxinum number of elements in each array together.
 	//Now factor in the chunking factor for each array except the first. 
#ifdef __UNDEFINED__ 
	nCellMax = arrayDimensionInfo[0].nFinish + 1;
    	for (int i = 1; i < nDimensionCnt; i++) {
		nCellMax = (arrayDimensionInfo[i].nFinish +  1) * nCellMax;
	}
	for (int i = 1; i < nDimensionCnt; i++) {
                nCellMax = (arrayDimensionInfo[i].nDataItemsPerChunck) * nCellMax;
        }
        //printf("base + itemsperchunk=%d\n",nCellMax); 
#endif 
	srandom(time(0));
	if (0 == nIsDense) { 
		/*
		** This is the SPARSE representation.
		*/
		for(int i = 0;i < nRowChunks; i++ ) { 
			for(int j = 0;j < nColChunks; j++ ) { 
			
				if (i+j) { 
					printf("\n;\n{ %d, %d } [[", i, j);
				} else { 
					printf("{ %d, %d } [[", i, j );
				}

				nCount = 0;
	
				for (int n = 0; n < nRowsInChunk; n++ ) { 
					for (int m = 0; m < nColsInChunk; m++ ) { 

						nCellNum = (((i * nRowsInChunk) + n) * 
										(nRowsInChunk * nRowChunks )) + 
									 ((j * nColsInChunk) + m) ;

						if(dbProb > ((double)random() / (double)INT_MAX))
						{
							printf(" {%d, %d} ",
								i * nRowsInChunk + n, 
								j * nColsInChunk + m
							);
							
							if (nIsRandom) 
								print_random_attr( nTypeCnt, szTypesListStr );
							else {
								print_det_attr( nTypeCnt, szTypesListStr, 
												nCellNum, nCellMax);
							}
							nCount++;
						}
					}
				}
				printf(" ]]");
   			}
		}
		printf("\n");

	} else { 
		/*
		** Dense data.
		*/
		for(int i = 0;i < nRowChunks; i++ ) { 
			for(int j = 0;j < nColChunks; j++ ) { 
				nCount = 0;

				if (i+j)
					printf(";\n[\n");
				else 
					printf("[\n");

				for (int n = 0; n < nRowsInChunk; n++ ) { 

					if (n)
						printf(",\n[ ");
					else 
						printf("[ ");

					for (int m = 0; m < nColsInChunk; m++ ) { 

						if (m) printf(", "); 

#ifdef  __UNDEFINED__

printf("\n+=============================+\n");
printf("||  i (RowChunks)	=   %3d ||\n", i);
printf("||	  RowsInChunk  =   %3d ||\n", nRowsInChunk);
printf("||  n (RowsInChunk)  =   %3d ||\n", n);
printf("||	  nRowChunks   =   %3d ||\n", nRowChunks );
printf("||  j (ColChunks)	=   %3d ||\n", j); 
printf("||	  nColsInChunk =   %3d ||\n", nColsInChunk); 
printf("||  m (nColsInChunk) =   %3d ||\n", m);
printf("||	  nColChunks   =   %3d ||\n", nColChunks);
printf("+=============================+\n");

#endif 
						//
						// Tricky bit here .... 
						// 
						nCellNum = (((i * nRowsInChunk) + n) * 
						//				(nRowsInChunk * nRowChunks )) + 
						  				(nColsInChunk * nColChunks )) + 
									 ((j * nColsInChunk) + m) ;

						if ((1.0 == dbProb) || 
							(dbProb > ((double)random() / (double)INT_MAX))) 
						{

							if (nIsRandom) 
								print_random_attr( nTypeCnt, szTypesListStr );
							else {
								print_det_attr( nTypeCnt, szTypesListStr, 
												nCellNum, nCellMax);
							}
						} else { 
							print_empty_attr();
						}
						nCount++;
					}
					printf("]");
				}
				printf("\n]");
			}
		}
		printf("\n");
	}
}
