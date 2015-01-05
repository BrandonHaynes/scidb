:
# nyse_loader - invoke loadpipe/loadcsv to get our trading data into SciDB

set -x

# Based on attribute types in nyse_flat, see make_nyse_arrays.sh.
TYPE_PATTERN=NNNNNNNSNNNSSSSSSSN

nc -d -k -l 8080 | loadpipe.py --delim=\| --verbosity=2 \
    -- -t $TYPE_PATTERN -a nyse_flat -A nyse_day
