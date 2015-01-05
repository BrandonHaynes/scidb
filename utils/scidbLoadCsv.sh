#!/bin/bash
#
#
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
#

#set -x # to trace execution

PROG_NAME="scidbLoadCsv"


function debug { local level="$1" ; local msg="$2" ;
    local DEBUG_LIMIT=0 # 0 on checkin: raise this number to get more debugging
    # TODO: add switch to enable debugging (conflict with -d / dimension switch?
    if  (( level \<= DEBUG_LIMIT )) ; then
        echo "${PROG_NAME}: ${2}" >&2
    fi
}

function fail { local exitCode="$1" ; local msg="$2" ;
    debug -1 "terminating with exit ${exitCode}, ${msg}"
    exit "${exitCode}"
    debug -1 "function fail: never reached"
}

function assert { local val="$1" ; local failureMsg="$2" ; local lineNo="$3" ;
    if (( ! val )) ; then
        debug 0 "${failureMsg} at "$0" line ${lineNo}"  
        exit 2 ;
    fi
}

function aflQuery { local qflags="$1" ; local timed="$2" ; local query="$3"
    debug 0 "$query"
    local TIME_AFL=0            # TODO: add switch to force query timing
    if (( timed || TIME_AFL )) ; then
        /usr/bin/time -f "%E real" iquery "$qflags" -p "$SCIDB_PORT" --format=csv+ -a -q "$query"
    else
                                   iquery "$qflags" -p "$SCIDB_PORT" --format=csv+ -a -q "$query"
    fi
}

function aflQueryChecked { local qflags="$1" ; local timed="$2" local query="$3"
    aflQuery "$qflags" "$timed" "$query"
    if [ $? -ne 0 ] ; then
        fail 1 "iquery $qflags failure for query:  \'$query\'"
    fi
}

# wait for an array to be gone from the namespace
# (because remove can return before that actually happens)
function waitUntilArrayGone { local name="$1" ; local maxWaitSecs="$2"

    # so while the show succeeds and we haven't slept 10 times ...
        for (( I=0 ; I < maxWaitSecs ; I++)) ; do
            aflQuery "" "" "show ( $name )" > /dev/null 2>&1
            if [ $? -ne 0 ] ; then break; fi # can't show it, it must be gone
            debug -1 "sleeping until the remove of array $name completes"
            sleep 1 ;
        done

        #even that doesn't work.  still coming up with
        # "duplicate key value violates unique constraint "array_name_key"
        echo "sleeping for 5s to avoid the 'duplicate key value violates unique constraint' error"
        sleep 5 ;
}
        

    # the the target is truly gone before starting the rename

    function typeNameToNSC { local typeName="$1" ;
        # csv2scidb needs a string of {N,S,C} to drive its output formatting
        # we have the scidb types, so we'll translate down from those
        # TODO: make csv2scidb take the type names
        case "$typeName" in
        char)   TYPENAMETONSC="C" ;;
        int8)   TYPENAMETONSC="N" ;;
        uint8)  TYPENAMETONSC="N" ;;
        int16)  TYPENAMETONSC="N" ;;
        uint16) TYPENAMETONSC="N" ;;
        int32)  TYPENAMETONSC="N" ;;
        uint32) TYPENAMETONSC="N" ;;
        int64)  TYPENAMETONSC="N" ;;
        uint64) TYPENAMETONSC="N" ;;
        float)  TYPENAMETONSC="N" ;;
        double) TYPENAMETONSC="N" ;;
        string) TYPENAMETONSC="S" ;;
        datetime) TYPENAMETONSC="S" ;;
        *)  fail 2 "bad string '$typeName' to function typeNameToNSC()" ;;
        esac
    }

    function typeNameToByteEstimate { local typeName="$1" ;
        # to estimate good chunk sizes in each dimension, we need to know how much
        # space each (non-dimensional) attribute will consume in each cell.
        case "$typeName" in
        char)   let TYPE_NAME_TO_BYTE_ESTIMATE=1 ;;
        int8)   let TYPE_NAME_TO_BYTE_ESTIMATE=1 ;;
        uint8)  let TYPE_NAME_TO_BYTE_ESTIMATE=1 ;;
        int16)  let TYPE_NAME_TO_BYTE_ESTIMATE=2 ;;
        uint16) let TYPE_NAME_TO_BYTE_ESTIMATE=2 ;;
        int32)  let TYPE_NAME_TO_BYTE_ESTIMATE=4 ;;
        uint32) let TYPE_NAME_TO_BYTE_ESTIMATE=4 ;;
        int64)  let TYPE_NAME_TO_BYTE_ESTIMATE=8 ;;
        uint64) let TYPE_NAME_TO_BYTE_ESTIMATE=8 ;;
        float)  let TYPE_NAME_TO_BYTE_ESTIMATE=4 ;;
        double) let TYPE_NAME_TO_BYTE_ESTIMATE=8 ;;
        string) let TYPE_NAME_TO_BYTE_ESTIMATE=12 ;;   # TODO: how can we estimate this better?
        datetime) let TYPE_NAME_TO_BYTE_ESTIMATE=8 ;;  # TODO: 64-bit int?
        *)  fail 2 "bad string '$typeName' to function typeNameToNSC()"
            let TYPE_NAME_TO_BYTE_ESTIMATE=0 ;;
        esac
    }

    debug 3 "add sanity check that -h was able to read the attribute names"
    debug 3 "add TRAPs to clean up all tmpfiles and tmp arrays"

    function usage { local progName=$1
        echo 'usage:'
        echo '    scidbLoadCsv [-ChtT] [-p port] [-a attribute-list] [-c chunksize-list] [-d dimension-list]'
        echo '                 [-m min-max-list] [-F <char>] <type>-list arrayName'
        echo ' argument lists are comma-separated'
        echo ' <types> are string,double,[u]int{8,16,32,64}'
        echo
        echo options:
        echo '    -C, --check-arrays      (for debug) after loading, check that the expected data got loaded by counting them'
        echo '    -h, --header-attributes the first line of file has column names. (FILE only, does not work for piped inputs)'
        echo '    -p,                     port number (on which to communicate to scidb)'
        echo '                            this option is exclusive with -a'
        echo '    --help                  print this guide to usage on stdout'
        echo '    -t, --tee               pass the cvs file through to stdout in addition to loading it'
        echo '    -T, --timing            (for debug) query timing (real time only) is written on standard error'
        echo '    -a, --attribute-list    name the attributes(columns) of the csv file.  exclusive with -h'
        echo '    -c, --chunksize-list    give explicit chunksizes of attributes that become dimensions'
        echo '    -d, --dimension-list    list (by 0-based column numbers) of attributes that will become dimensions (vs cell attributes)'
        echo '    -D, --dimension list    list (by name ) of attributes that will become dimensions (vs cell attributes)'
        echo '    -m, --min-max-list      give the min and max of dimensions.  default is \"*:*\"'
        echo '    -F, --field-separator   specify a character other than \",\" that is the column separator in the csv file'
        echo example:
        echo "    echo banannas,0.49 > /tmp/foo"
        echo "    echo apples,2.18 >> /tmp/foo"
        echo "    cat /tmp/foo | scidbLoadCsv.sh -a fruit,price -D price string,double fruitPrices"
        echo 
        echo '        will create an array using the AFL statement:'
        echo '            CREATE ARRAY <fruit:string, price:double> fruitPrices[]'
        echo '        and the array will be filled with data imported from the csv data'

        exit 1
        #NOTREACHED
    }

    function pow { local X=$1 ; local Y=$2
        POW=`awk "BEGIN { print exp(log($X)*$Y); }"`
    }

    function root { local X=$1 ; local Y=$2
        ROOT=`awk "BEGIN { print exp(log($X)/$Y); }"`
    }

    function intRoot { local X=$1 ; local Y=$2
        INTROOT=`awk "BEGIN { print int(exp(log($X)/$Y)); }"`
    }

    function isInteger { local STRING=$1 ;
        if echo "$STRING" | egrep -v '[0-9]*' > /dev/null 2>&1 ; then
            false
        else
            true
        fi
    }

    #
    # I. parse the command line
    #
    COMMAND="$0 $*"
    SCIDB_PORT=1239
    TARGET_NAME=""
    ARG_ATTRIBUTES_TYPES=""
    #OPTION_CHECK=0
    OPTION_ATTRIBUTES_FROM_HEADER=0
    OPTION_ATTRIBUTES=""
    FS=","
    declare -a DIMENSION_ARRAY

    TEMP=`getopt -o CD:hp:tTa:c:d:F:m: --long check-arrays,chunksize-list,dimension-list,dimension-attributes,header-of-attributes,port,tee,attribute-list,field-separator,min-max-list -n ${PROG_NAME} -- "$@" `
    STATUS=$?
    if (( STATUS != 0 )) ; then
        echo $COMMAND
        echo "${0}: getopt failure"
        usage $0
        #NOTREACHED
    fi
    eval set -- "$TEMP"

    while true; do
        case "$1" in
        # options without arg
        -C|--check-arrays) debug 2 "got -C" ; OPTION_CHECK=1 ; shift ;;
        -h|--header-of-names)
            # TODO: this won't work as-is becaue head ends up somehow eating the remainder of the input'
            list=`head -1`  ; # steal one line from standard in
            attrNamesWithSpaces=`echo ${list} | sed -e 's/,/ /g'` ; # TODO: attrNames from file should honor $FS
            debug 2 "-h attrNames are ${attrNamesWithSpaces}" ;
            shift ;;
        -t|--tee) debug 2 "got -t" ; OPTION_TEE=1 ; shift ;; # stdin is to be copied to stdout
        -T|--timing) debug 2 "got -t" ; OPTION_TIMING=1 ; shift ;; # query timing should be written to stderr
        # options WITH an argument
        -a|--attribute-list)  # direct specification of attrNames vs -h | -header-of-names
            attrNamesWithSpaces=`echo "${2}" | sed -e 's/,/ /g'` ;
            debug 2 "-a attrnames are ${attrnamesWithSpaces}" ;
            shift 2 ;; 
        -c|--chunksize-list) # list coreesponding to -d list, giving explicit chunksizes
            chunksizesWithSpaces=`echo "${2}" | sed -e 's/,/ /g'`
            debug 2 "-a chunksizes are ${chunksizesWithSpaces}"
            shift 2 ;; 
        -d|--dimension-list) # list of which of the attrNames will become dimensions
            dimensionsWithSpaces=`echo "${2}" | sed -e 's/,/ /g'`
            debug 2 "-a dimensions are ${dimensionsWithSpaces}"
            shift 2 ;; 
        -D|--dimension-attributes) # list of which of the attrNames will become dimensions
            dimensionAttributesWithSpaces=`echo "${2}" | sed -e 's/,/ /g'`
            debug 2 "-a dimensions are ${dimensionAttributesWithSpaces}"
            shift 2 ;; 
        -F|--field-separator)
            debug 2 "got -F ${2}" ;         # -F : any single character
            if [ "${#2}" -ne 1 ] ; then
                echo $COMMAND
                echo "${0}: -F is '${2}' but it may only be a single character"
                usage $0
            else
                FS="$2";
            fi
            shift 2 ;;
        -m|--min-max-list) # instead of the script using *:* or * for min and max, user may specify all of them
            minMaxWithSpaces=`echo "${2}" | sed -e 's/,/ /g'`
            debug 2 "-a min-max are ${minMaxWithSpaces}"
            shift 2 ;; 
        -p|--port)
            SCIDB_PORT="$2"
            shift 2 ;;
        --) debug 3 "end of options" ; shift ; break ;;
        *) fail 1 "${0}: Unknown command-line switch: a1=${1}" ;;
        esac
    done

    #
    # validate the arguments and the options
    #

    #
    # arguments (should probably move these last)
    #
    NUM_ARG=${#}
    if (( NUM_ARG != 2 )) ; then
        echo $COMMAND
        echo "${0}: exactly two arguments are required: typeList and arrayName, but got ${NUM_ARG}: ${*}"
        usage $0
        #NOTREACHED
    fi

    # $1
    typesWithSpaces=`echo "${1}" | sed -e 's/,/ /g'`
    declare -a TYPE_ARRAY
    TYPE_ARRAY=(${typesWithSpaces})  # also array from space-delimited list

    # $2
    TARGET_NAME=$2

    #
    # options: convert attrNames and their types to arrays and check them
    # 
    declare -a NAME_ARRAY=(${attrNamesWithSpaces})
    NAME_LEN="${#NAME_ARRAY[@]}"
    declare -a CHUNKSIZE_ARRAY=(${chunksizesWithSpaces})
    CHUNKSIZE_LEN="${#CHUNKSIZE_ARRAY[@]}"
    echo "chunksizesWithSpaces ${chunksizesWithSpaces} CHUNKSIZE_ARRAY is $CHUNKSIZE_ARRAY"
    echo "CHUNKSIZE_ARRAY[0] is ${CHUNKSIZE_ARRAY[0]}"
    declare -a DIMENSION_ARRAY=(${dimensionsWithSpaces})
    DIMENSION_LEN="${#DIMENSION_ARRAY[@]}"
    declare -a DIMENSION_ATTRS_ARRAY=(${dimensionAttributesWithSpaces})
    DIMENSION_ATTRS_LEN="${#DIMENSION_ATTRS_ARRAY[@]}"
    declare -a MINMAX_ARRAY=(${minMaxWithSpaces})
    MINMAX_LEN="${#MINMAX_ARRAY[@]}"


    (( NUM_DIMENSIONS= DIMENSION_LEN + DIMENSION_ATTRS_LEN ))
    (( NUM_CELL_ATTRS= NAME_LEN - NUM_DIMENSIONS ))
    debug 2 "@@@@@@@ NAME_LEN is $NAME_LEN ***********"
    debug 2 "@@@@@@@ NUM_DIMENSIONS is $NUM_DIMENSIONS ***********"
    debug 2 "@@@@@@@ NUM_CELL_ATTRS is $NUM_CELL_ATTRS ***********"

    #
    # ARGUMENT CHECKING: the length and form of some arguments depends on others
    #    diagnose what the user has done wrong to help him out
    #

    # -a / -h : attrNames
    if (( NAME_LEN < 2 )) ; then
        echo $COMMAND
        echo "${0}: -a or -h must define 2 or more attribute names, you specified ${NAME_LEN}"
        usage $0
    fi

    # -c : chunksize list, length should be |-a| - |-d|
    if (( CHUNKSIZE_LEN != 0 && CHUNKSIZE_LEN != NUM_DIMENSIONS )) ; then
        echo $COMMAND
        echo "${0}: -d/-D specifies $NUM_DIMENSIONS dimension(s), but -c has $CHUNKSIZE_LEN entries"
        usage $0
    fi
    # -c : chunksize list, make sure they are are numeric
    for (( I=0 ; I < CHUNKSIZE_LEN ; I++ )) ; do
        CHUNKSIZE=${CHUNKSIZE_ARRAY[$I]}
        if isInteger $CHUNKSIZE
        then
            echo "CHUNKSIZE $CHUNKSIZE is integer-valued"
            true # noop
        else
            echo $COMMAND
            echo "${0}: chunksize $CHUNKSIZE is not an integer"
            usage $0
        fi
    done
    # -d / -D : dimensions check that they are numeric, named
    # isIntegerArray $DIMENSION_ARRAY

    # -m :  min,max indices
    #       should be [min:max] [,...]
    if (( MINMAX_LEN != 0 && MINMAX_LEN != NUM_DIMENSIONS )) ; then
        echo $COMMAND
        echo "${0}: -d/-D specifies $NUM_DIMENSIONS dimension(s), but -m has $MINMAX_LEN entries"
        usage $0
    fi

    # -p : port number, check that its numeric
    if  isInteger $SCIDB_PORT 
    then
        true # noop
    else
        echo $COMMAND
        echo "${0}: $STRING is not an integer"
        usage $0
    fi

    #
    # reprocess the -D option, by turning each name into an attribute index number
    # so that it can be processed just like the -d option.
    #
    # alternative way to specfify which columns become dimensions
    # instead of giving a list of column numbers, one may give a list
    # of column names.  This has to be done *after* column names are defined
    # so this feature will not work if the column names are named
    # by the csv file itself (-h flag)
    # append dimensions from DIMENSION_ATTRS_ARRAY, which are names,
    # to DIMENSION_ARRAY, which are column indices
    #
    
    # first we will need a function to map names to column index
    function attributeIndex { local Name="$1" ;
        local I 
        for (( I=0 ; I < NAME_LEN ; I++ )) ; do
            if [[ "${NAME_ARRAY[$I]}" == "$Name" ]] ; then
               let ATTRIBUTE_INDEX=$I ; 
               return
            fi
        done
        let ATTRIBUTE_INDEX=-1
    }
    
    # now iterate over the DIMENSION_ATTRS_ARRAY (-d / -D)
    # and for each name, append its valid index
    # to DIMENSION_ARRAY, or output an error message
    # and quit when done going through the list
    DIMENSION_ATTRS_ERRORS=0 
    DIMENSION_ATTRS_LEN="${#DIMENSION_ATTRS_ARRAY[@]}"
    for (( I=0 ; I < DIMENSION_ATTRS_LEN ; I++ )) ; do
        attributeIndex ${DIMENSION_ATTRS_ARRAY[$I]}
        if (( ATTRIBUTE_INDEX >= 0 )) ; then
            DIMENSION_ARRAY[${#DIMENSION_ARRAY[@]}]=${ATTRIBUTE_INDEX} ; # append another column to DIMENSION_ARRAY
        else
            debug 0 "-D: ${DIMENSION_ATTRS_ARRAY[$I]} is not the name of a column/attribute"
            DIMENSION_ATTRS_ERRORS=1 # true
        fi
    done
    if (( DIMENSION_ATTRS_ERRORS != 0)) ; then
        debug 0 "-D: exiting due to bad dimension name(s)"
        exit 2
    fi

    debug 2 "NAME_ARRAY is ${NAME_ARRAY[*]}"
    debug 2 "TYPE_ARRAY is ${TYPE_ARRAY[*]}"
    debug 2 "TARGET_NAME is ${TARGET_NAME}"


    #
    # sanity checks
    #
    NAME_LEN="${#NAME_ARRAY[@]}"
    TYPE_LEN="${#TYPE_ARRAY[@]}"
    assert $(( TYPE_LEN >= 1 ))  "You must have at least two columns of data in the type list" "$LINENO"
    assert $(( DIMENSION_ATTRS_LEN >= 0 ))  "You must specify at least one dimension using the -d or -D switches" "$LINENO"
    NONDIM_LEN=$((NAME_LEN - DIMENSION_ATTRS_LEN))
    debug 2 NONDIM_LEN is $NONDIM_LEN
    assert $(( NONDIM_LEN >= 0 ))  "You must not specify all of your columns as dimensions because there will be nothing left to store in the cell" "$LINENO"

# set M to be 128K < M < fraction of cache size of processor...        
# e.g         128K < M < 512K = 2048K/4        
# then determine the chunk size in cells by dividing by the estimate of the cell size
# TODO: add switch to get CHUNK_SIZE_BYTES from the proper place
#       if /proc/cpuinfo is present, we can get cache size from there.  For Windows/Mac, ???
let CHUNK_SIZE_BYTES=`expr 512 '*' 1024` # 128K <= Target <= machine cache size (~2M on 2011 pc's)

#
# this is somewhat of a hack to take a list of attribute names and a string
# of types and conver them to a string of "< name:type, name:type ...>"
# so perhaps this whole thing should be re-written in python
#
DIMENSION_LEN="${#DIMENSION_ARRAY[@]}"
function isDimension { local idx="$1" ;
    local I 
    for (( I=0 ; I < DIMENSION_LEN ; I++ )) ; do
        if (( DIMENSION_ARRAY[I] == idx )) ; then
           let IS_DIMENSION=1 ; 
           return
        fi
    done
    let IS_DIMENSION=0
}


#
# get CELL_SIZE_ESTIMATE_LOAD ... sum over all non-dimension attrNames
#
let CELL_SIZE_ESTIMATE_LOAD=0
for (( I=0 ; I < NAME_LEN ; I++ )) ; do
    TYPE_NAME="${TYPE_ARRAY[${I}]}"
    typeNameToByteEstimate "$TYPE_NAME" ;
    (( CELL_SIZE_ESTIMATE_LOAD += TYPE_NAME_TO_BYTE_ESTIMATE ))
    debug 2 "CELL_SIZE_ESTIMATE_LOAD $CELL_SIZE_ESTIMATE_LOAD"

    debug 2 isDimension "$I"
    if (( ! IS_DIMENSION )) ; then
        (( CELL_SIZE_ESTIMATE_POST_REDIM += TYPE_NAME_TO_BYTE_ESTIMATE ))
        debug 2 "CELL_SIZE_ESTIMATE_POST_REDIM $CELL_SIZE_ESTIMATE_POST_REDIM"
    fi
done

CSV_TO_SCIDB_NSC=""
LOAD_CHUNK_SIZE=$(( CHUNK_SIZE_BYTES / CELL_SIZE_ESTIMATE_LOAD ))
LOAD_CHUNK_SIZE=10000   # TBD: should use the above line, but there may be an issue where count() on unbounded arrays depends on the chunksize
                        #      that needs to be resolved before using the automatic chunk-size determination above
debug 2 "LOAD_CHUNK_SIZE ********************************************************"
debug 1 "LOAD_CHUNK_SIZE is $LOAD_CHUNK_SIZE"
POST_REDIM_CHUNK_SIZE=$(( CHUNK_SIZE_BYTES / CELL_SIZE_ESTIMATE_POST_REDIM ))
debug 2 "POST_REDIM_CHUNK_SIZE is $POST_REDIM_CHUNK_SIZE"


#
# calculate POST_REDIM_ATTRIBUTES -- the non-dimenson attrNames
#
LOAD_ATTRIBUTES="<"
POST_REDIM_ATTRIBUTES="<"
let CELL_SIZE_ESTIMATE_POST_REDIM=0
for (( I=0 ; I < NAME_LEN ; I++ )) ; do
    TYPE_NAME="${TYPE_ARRAY[${I}]}"
    typeNameToNSC "$TYPE_NAME" ; CSV_TO_SCIDB_NSC+="${TYPENAMETONSC}"

    # append it to the list of attrNames for the linear load array
    if (( I > 0)) ; then
        LOAD_ATTRIBUTES+=", "
    fi
    LOAD_ATTRIBUTES+="${NAME_ARRAY[${I}]}"
    LOAD_ATTRIBUTES+=" : "
    LOAD_ATTRIBUTES+="$TYPE_NAME"

    # of not a dimension, append it to the list of (non-dimension) attrNames in the redimensioned version
    isDimension "$I"
    debug 1 "isDimension $I result is $IS_DIMENSION"
    if (( ! IS_DIMENSION )) ; then                     # if I is not in the list of dimensions
        if (( ${#POST_REDIM_ATTRIBUTES} > 1 )) ; then
            POST_REDIM_ATTRIBUTES+=", "
        fi
        POST_REDIM_ATTRIBUTES+="${NAME_ARRAY[${I}]}"
        POST_REDIM_ATTRIBUTES+=" : "
        POST_REDIM_ATTRIBUTES+="$TYPE_NAME"
    fi
done
LOAD_ATTRIBUTES+=">"
POST_REDIM_ATTRIBUTES+=">"

#
# now the tricky one -- the dimensions.  Note that in this case
# we iterate over the dimension specification order (not the column naming order)
# so that the user can re-order the original attrNames into the order they desire
#
POST_REDIM_DIMENSIONS="["
let CELL_SIZE_ESTIMATE_POST_REDIM=0
let chunksizeRemainder=$POST_REDIM_CHUNK_SIZE
for (( I=0 ; I < DIMENSION_LEN ; I++ )) ; do
    DIM_NUM=${DIMENSION_ARRAY[${I}]} ;
    DIM_NAME=${NAME_ARRAY[${DIM_NUM}]} ;
    TYPE_NAME="${TYPE_ARRAY[${DIM_NUM}]}"
    debug 1 "@@@@ building dimensions: I:$I"
    debug 1 "@@@@ building dimensions: DIM_NUM:$DIM_NUM "
    debug 1 "@@@@ building dimensions: DIM_NAME:$DIM_NAME "
    debug 1 "@@@@ building dimensions: TYPE_NAME:$TYPE_NAME "

    typeNameToNSC "$TYPE_NAME" ;

    isDimension "$DIM_NUM"
    assert "$IS_DIMENSION"1 "in iterating over dimensions, found a supposed non-dimension"

    if (( I > 0 )) ; then
        POST_REDIM_DIMENSIONS+=", "
    fi
    POST_REDIM_DIMENSIONS+="${DIM_NAME}"
    # check for non-integer dimension
    debug 2 "TYPE_NAME is $TYPE_NAME"
    FIRST3=`expr substr "$TYPE_NAME" 1 3`
    FIRST4=`expr substr "$TYPE_NAME" 1 4`
    if [ "$FIRST3" == "int" -o "$FIRST4" == "uint" ] ; then
        # any integer dimension is turned into an int64 dimension
        # since there is no advantage to using a shorter type and
        # even if we do, it will make the parser think we are using a non-integer
        # type and then it will not permit a lower bound to be set
        if (( DIM_NUM < ${#MINMAX_ARRAY[@]} )) ; then #the user specified the minmax
            POST_REDIM_DIMENSIONS+="=${MINMAX_ARRAY[${DIM}]},"
        else
            POST_REDIM_DIMENSIONS+="=*:*," # requires *:* when no specifics are given
        fi
    else
        # non-integer dimension REQUIRES "(type)" specifier and one *
        if (( DIM_NUM < ${#MINMAX_ARRAY[@]} )) ; then #the user specified the minmax
            POST_REDIM_DIMENSIONS+="(${TYPE_NAME})=${MINMAX_ARRAY[${DIM_NUM}]},"
        else
            POST_REDIM_DIMENSIONS+="(${TYPE_NAME})=*,"
        fi
    fi

    # TODO: dynamic chunksize computation can move into the ELSE clause below
    # given N dimensions, let CSRemainder = totalChunkSize
    # chunksize[0] = floor( CSRemainder ^ (1/N), but not less than 1
    # chunksize[1] = floor( (CSRemainder/chunksize[0]) ^ (1/(N-1))  , but not less than 1
    # chunksize[2] = floor( (CSRemainder/chunksize[0]/chunksize[1]) ^ (1/(N-2)) , but not < 1 
    # etc
    ((DIVISOR = DIMENSION_LEN - I))
    intRoot ${chunksizeRemainder} ${DIVISOR} ; let local rootOfRemainder=$INTROOT
    if (( rootOfRemainder < 1 )); then (( rootOfRemainder = 1 )); fi
    debug 2 "CHUNKSIZE----------------------------------------------------------------------"
    debug 2 "I $I chunksizeRemainder $chunksizeRemainder DIVISOR $DIVISOR ROOT $ROOT rootOfRemainder $rootOfRemainder"
    (( chunksizeRemainder = chunksizeRemainder/rootOfRemainder ))

    debug 2 "rootOfRemainder= ${rootOfRemainder}"
    debug 2 "@@@@ I is ${I}"
    debug 2 "@@@@ DIM_NUM is ${DIM_NUM}"
    debug 2 "@@@@ #CHUNKSIZE_ARRAY[@] is ${#CHUNKSIZE_ARRAY[@]}"
    debug 2 "@@@@ CHUNKSIZE_ARRAY[${DIM_NUM}] is ${CHUNKSIZE_ARRAY[${DIM_NUM}]}"
    if (( ${#CHUNKSIZE_ARRAY[@]} > 0 )) ; then
        debug 2 "CHUNKSIZE if "
        POST_REDIM_DIMENSIONS+="${CHUNKSIZE_ARRAY[${I}]},0"
    else
        # TODO: dynamic chunksize computation can move here
        debug 2 "CHUNKSIZE else "
        POST_REDIM_DIMENSIONS+="${rootOfRemainder},0"
    fi
done
POST_REDIM_DIMENSIONS+="]"

if [ "$POST_REDIM_DIMENSIONS" = "[]" ]; then
    # no dimensions
    debug 0 "Error: you must specify at least one dimension using the -d or -D switches"
    exit 2
fi
if [ "$POST_REDIM_ATTRIBUTES" = "<>" ]; then
    debug 0 "Error: you must have specified at least one column that does not become a dimension: "
    exit 2
fi

debug 1 "CSV_TO_SCIDB_NSC is ${CSV_TO_SCIDB_NSC}    333333333333333333333333333333333"
debug 1 "LOAD_ATTRIBUTES is ${LOAD_ATTRIBUTES}    333333333333333333333333333333333"
debug 1 "POST_REDIM_ATTRIBUTES is ${POST_REDIM_ATTRIBUTES}    333333333333333333333333333333333"
debug 0 "POST_REDIM_DIMENSIONS is ${POST_REDIM_DIMENSIONS}    333333333333333333333333333333333"

#
# II. Make the fifos (named pipes) used to allow the data to stream into the database
#     load command, versus requiring an entire file to exist on a filesystem somewhere
#
# II.A - a fifo to tee the csv data into so it can be passed on to stdout:
CSV_FIFO=""
CSV_FIFO_REDIRECT=""
if (( OPTION_TEE )) ; then
    CSV_FIFO=/tmp/${PROG_NAME}.csv.fifo.${$}
    rm -rf ${CSV_FIFO}
    mkfifo ${CSV_FIFO}
    tee ${CSV_FIFO} &   # this copies stdin to stdout and CSV_FIFO
    debug 1 "tee enabled"
    debug 2 "CSV_FIFO is ${CSV_FIFO}, so we will take input from that instead of stdin"
    CSV_FIFO_REDIRECT=" cat ${CSV_FIFO} | "
else
   debug 2 "csv2scidb is taking its data from stdin"
fi
debug 3 "CSV_FIFO is \'${CSV_FIFO}\'"
debug 3 "CSV_FIFO_REDIRECT is \'${CSV_FIFO_REDIRECT}\'"

#
# II. Make the fifo (named pipe)  and filter csv data from standard in, into it
#
LOAD_FIFO=/tmp/${PROG_NAME}.load.fifo.${$}
rm -rf $LOAD_FIFO
mkfifo $LOAD_FIFO
debug 2 "LOAD_FIFO is $LOAD_FIFO"
#ls -l "$LOAD_FIFO"
#debug 2 "proves we have the load data"



#
# III. create the initial (linear) array and load it
#
TMP_LOAD="${PROG_NAME}_load_${$}"

aflQuery   "" 0 "remove( ${TMP_LOAD} )" 2> /dev/null
aflQueryChecked "" 0 "CREATE ARRAY ${TMP_LOAD}${LOAD_ATTRIBUTES}[INDEX=0:*,$LOAD_CHUNK_SIZE,0]"
debug 2 "temporary array created";

# we can't use aflQueryChecked, because the command is backgrounded -- status is only available after the wait
LOAD_STDOUT=${PROG_NAME}_load_stdout
LOAD_STDERR=${PROG_NAME}_load_stderr
aflQuery "-n" 1 "load(${TMP_LOAD}, '${LOAD_FIFO}', 0 )" > $LOAD_STDOUT 2> $LOAD_STDERR &
LOAD_PID="${!}"


let TEE_PID=0
if (( OPTION_TEE )) ; then
    fail 1 "TEE option is not supported yet"
    debug -1 "390 should not be reached"
    csv2scidb -c "$LOAD_CHUNK_SIZE" -p "$CSV_TO_SCIDB_NSC" > "$LOAD_FIFO" < "$CSV_FIFO" &
    let TEE_PID=$!
else
    debug 0 "csv2scidb -c $LOAD_CHUNK_SIZE -p $CSV_TO_SCIDB_NSC > $LOAD_FIFO"
    csv2scidb -c "$LOAD_CHUNK_SIZE" -p "$CSV_TO_SCIDB_NSC" > "$LOAD_FIFO"
    let STATUS=$?
    if [ "$STATUS" -ne 0 ] ; then
        fail $STATUS "csv2scidb failed with status $STATUS"
        debug -1 "398 should not be reached"
    fi
fi

wait "$LOAD_PID"   # now get the status of the backgrounded load command
let STATUS=$?
if [ $STATUS -ne 0 ] ; then
    cat $LOAD_STDOUT      # now send it to our stdout
    cat $LOAD_STDERR >&2  # and send errs to our stderr
    fail $STATUS "load(${TMP_LOAD}, '${LOAD_FIFO}', 0 )"
    debug -1 "403 should not be reached"
fi

if (( TEE_PID )) ; then
    wait $TEE_PID   # now get the status of the backgrounded load command
    let STATUS=$?
    if (( $STATUS )) ; then
        fail $STATUS "background csv2scidb failed"
        debug -1 "403 should not be reached"
    fi
fi

debug 3 "temporary array ${TMP_LOAD} loaded --------------------------------------------------------";
debug 3 "LOAD_FIFO was $LOAD_FIFO ------------------- listing it--------------------"
#ls -l "$LOAD_FIFO"

if (( OPTION_CHECK )) ; then
    debug -1 "Checks of the size and contents of the linear load array."
    aflQueryChecked "" 1 "scan ( ${TMP_LOAD} )"   # NOCHECKIN - can only do this for a test array
    aflQueryChecked "" 1 "aggregate ( ${TMP_LOAD} ), count(*) )"
    # aflQueryChecked "" 1 "aggregate ( ${TMP_LOAD} ), avg ( ${ATTR_NAME} ) )"
    for (( I=0 ; I < NAME_LEN ; I++ )) ; do
        ATTR_NAME="${NAME_ARRAY[${I}]}"
        aflQueryChecked "" 1 "aggregate ( ${TMP_LOAD}, min ( ${ATTR_NAME} ) )"
        aflQueryChecked "" 1 "aggregate ( ${TMP_LOAD}, max ( ${ATTR_NAME} ) )"
    done
fi



#
# IV. convert the linear array into the requested array
#
TMP_REDIM_STORE_ARRAY=${PROG_NAME}_redim_${$}
aflQuery "" 0 "remove(${TMP_REDIM_STORE_ARRAY})" 2> /dev/null

debug 1              "CREATE ARRAY ${TMP_REDIM_STORE_ARRAY} ${POST_REDIM_ATTRIBUTES} ${POST_REDIM_DIMENSIONS}"
aflQueryChecked "" 0 "CREATE ARRAY ${TMP_REDIM_STORE_ARRAY} ${POST_REDIM_ATTRIBUTES} ${POST_REDIM_DIMENSIONS}"
#aflQueryChecked "" 0 "show($TMP_LOAD)"
#aflQueryChecked "" 0 "show($TMP_REDIM_STORE_ARRAY)"
aflQueryChecked "-n" 1 "store ( redimension ( ${TMP_LOAD}, ${TMP_REDIM_STORE_ARRAY} ), ${TMP_REDIM_STORE_ARRAY} )"

aflQuery   "" 1 "remove(${TARGET_NAME})" > /dev/null 2>&1
# getting the "duplicate key" error here, so have to wait more sometimes
# waitUntilArrayGone ${TARGET_NAME} "20"

aflQueryChecked "" 1 "rename(${TMP_REDIM_STORE_ARRAY}, ${TARGET_NAME})"
if (( OPTION_CHECK )) ; then
    debug -1 "Checks of the size and contents of the linear load array."
    aflQueryChecked "" 0 "aggregate ( ${TARGET_NAME} ), count(*) )"
fi

#
# IV. clean up
#
aflQuery   "" 0 "remove(${TMP_LOAD})" 2> /dev/null
aflQuery   "" 0 "remove(${TMP_REDIM_STORE_ARRAY})" 2> /dev/null

rm -rf "$LOAD_FIFO"
rm -rf "$CSV_FIFO" 

#
# exit status -- if we made it here, we succeeded
#
echo "${0}: success" >&2
exit 0 # success

