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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "bsdiff.h"

#define BUF_SIZE 32768

unsigned char buf1[BUF_SIZE];
unsigned char buf2[BUF_SIZE];
unsigned char buf3[BUF_SIZE];
unsigned char buf4[BUF_SIZE];

/**
 * Unit tests for the bsdiff/bspatch library routines
 */

void bufToFile(void *buf, off_t len, const char *filename) {
	FILE * f = fopen(filename, "wb");
	fwrite(buf, 1, len, f);
	fclose(f);
}

int TestPatch(unsigned char *first, off_t firstLen, unsigned char *second, off_t secondLen, int verbose)
{
	const unsigned int resultsBufSize = (firstLen + secondLen) * 2 + 32 + 256;		/* Diff's can be larger than the original arrays, just more compressible */
	unsigned char *resultsBuf = malloc(resultsBufSize);
	unsigned char *results = NULL;
	off_t resultsLen = 0;
	int returnVal = 0;
	unsigned int i;

	if (verbose != 0) printf("Creating the patch\n");
	if ((returnVal = bsdiff_nocompress(first, firstLen, second, secondLen, resultsBuf, resultsBufSize, &resultsLen)) != 0) {
		if (verbose != 0) printf("Could not diff arrays!  Error was %d\n", returnVal);
		goto err;
	}

	printf("Applying the patch\n");
	if ((returnVal = bspatch_nocompress(first, firstLen, resultsBuf, resultsLen, &results, &resultsLen)) != 0) {
		if (verbose != 0) printf("Could not apply patch for arrays!  Error was %d\n", returnVal);
		goto err;
	} else {
		if (verbose != 0) printf("Validating the resulting data\n");
		if (resultsLen != secondLen) {
			if (verbose != 0) printf("Final buffer (%ld) not the same length as input buffer (%ld)!\n", resultsLen, firstLen);
			goto err;
		}

		if ((returnVal = memcmp(second, results, secondLen)) != 0) {
			if (verbose != 0) printf("Reconstructed array not equal to original!  memcmp returned %d\n", returnVal);
			for (i = 0; i < secondLen; i++) {
				if (second[i] != results[i]) {
					printf("%d\t%d\t%d\n", i, second[i], results[i]);
				}
			}
			goto err;
		}
		free(results);
	}

	return 0;

err:
	if (results != NULL) free(results);
	free(resultsBuf);
	return -1;
}


int main(int argc, char** argv)
{
	unsigned char val;
	unsigned int i;
	int returnVal;

	assert(BUF_SIZE > 2000);

	/**
	 * Make up a bunch of data to put into each buffer.
	 * buf1[] -- Random garbage, but with 1,000 zeroed entries in the middle
	 * buf2[] -- The exact same random garbage, but with different values set every 1,000
	 * buf3[] -- Incrementing integers
	 * buf4[] -- All zeroes
	 */
	printf("Generating sample buffers for differencing\n");
	val = 97;
	for (i = 0; i < BUF_SIZE; i++) {
		buf1[i] = val;
		buf2[i] = ((i % 1000 == 0) ? (unsigned char)i : val);
		buf3[i] = (unsigned char)i;
		buf4[i] = 0;	/* Should be unnecessary; oh well. */
		val += 97;
	}
	for (i = 1000; i < 2000; i++) {
		buf1[i] = 0;
	}

	/* And one single-point error */
	buf1[800] = 10;

	/* Now, try differencing some buffers */

	/* Trivial case first:  All the same, all zero */
	printf("Generating patch for all-zero buffer against another all-zero buffer of the same size\n");
	if ((returnVal = TestPatch(&buf4[0], BUF_SIZE, &buf4[0], BUF_SIZE, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Generating patch for two small similar random buffers\n");
	if ((returnVal = TestPatch(&buf1[0], 20, &buf2[0], 20, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Generating patch for two similar random buffers\n");
	if ((returnVal = TestPatch(&buf1[0], BUF_SIZE, &buf2[0], BUF_SIZE, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Generating patch from a zero buffer to a random buffer\n");
	if ((returnVal = TestPatch(&buf4[0], BUF_SIZE, &buf1[0], BUF_SIZE, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Generating patch from a random buffer to a zero buffer\n");
	if ((returnVal = TestPatch(&buf1[0], BUF_SIZE, &buf4[0], BUF_SIZE, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Generating patch from a random buffer to a monotonically-increasing buffer\n");
	if ((returnVal = TestPatch(&buf1[0], BUF_SIZE, &buf3[0], BUF_SIZE, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Generating patch from a monotonically-increasing buffer to a random buffer\n");
	if ((returnVal = TestPatch(&buf3[0], BUF_SIZE, &buf1[0], BUF_SIZE, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Generating patch from a random small buffer to a monotonically-increasing buffer\n");
	if ((returnVal = TestPatch(&buf1[0], 1000, &buf3[0], BUF_SIZE, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Generating patch from a monotonically-increasing buffer to a random small buffer\n");
	if ((returnVal = TestPatch(&buf1[0], 1000, &buf3[0], BUF_SIZE, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Creating an empty patch\n");
	if ((returnVal = TestPatch(&buf1[0], 0, &buf3[0], 0, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Going from an empty patch to a large buffer\n");
	if ((returnVal = TestPatch(&buf1[0], 0, &buf3[0], BUF_SIZE, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Going from a large buffer to an empty patch\n");
	if ((returnVal = TestPatch(&buf3[0], BUF_SIZE, &buf1[0], 0, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Creating a unit patch\n");
	if ((returnVal = TestPatch(&buf1[0], 1, &buf3[0], 1, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Going from a unit patch to a large buffer\n");
	if ((returnVal = TestPatch(&buf1[0], 1, &buf3[0], BUF_SIZE, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	printf("Going from a large buffer to a unit patch\n");
	if ((returnVal = TestPatch(&buf3[0], BUF_SIZE, &buf1[0], 1, 1)) != 0) {
		printf("Failed!  Error code %d\n", returnVal);
		return returnVal;
	}

	return 0;
}
