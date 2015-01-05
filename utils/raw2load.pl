#!/usr/bin/perl
#
#  This script takes the raw 'short read' data, and reformats it
#  in a manner suitable for bulk loading into SciDB.
# 
#  The format for the raw 'short read' data is as follows: 
# 
# [1] @PHUSCA-W21172_91027:1:1:0:1521#0/1
# [2] NAGCCCTGCCCCTTCTGAGAGTCCCTTGTTAAGCAA
# [3] +PHUSCA-W21172_91027:1:1:0:1521#0/1
# [4] DNWWWWVVUWWWWWWVVUWUUWUWWWWWWVUTSTSB
# [5] @PHUSCA-W21172_91027:1:1:0:1784#0/1
# [6] NACCAGGACTATTTGCACTCTTTGGGGAAGGCTCGA
#
#  The lines correspond to the following: 
#  
#  [1], and [3] are (the same) SEQ_ID, differentiated only by the first 
#  character. SEQ_ID's have the following structure. For each short read, the 
#  SEQ_ID contains: 
# 
#   PHUSCA-W21172_91027:1:1:0:1521#0/1
#   | Instrument ID   | | | |   |   |
#                       | | |   |   |
#        Flowcell Lane__/ | |   |   | 
#                         | |   |   | 
#               Tile Num__/ |   |   |
#                           |   |   |
#       X Position on Tile__/   |   |
#                               |   |
#           Y Position on Tile__/   |
#                                   |
#                          Ignore __/
#
#  For this SEQ_ID, the line [2] corresponds to the sequence of 
#  short reads (nucleotides: G,C,T,A or N), and the line [4] corresponds to 
#  a 'confidence value', than can be computed as follows: 
#
#  If the character is 'X', then Q::FLOAT = -10 . log10 ( ascii(X) - 33 );
#
#  The schema that is our target looks like this: 
#
#  CREATE ARRAY Reads ( Read::CHAR(1), Confidence::FLOAT) 
#                     [ SEQ_ID=0:*, POSITION=0:50 ]; 
# 
#  CREATE ARRAY Seq-Info ( FlowCell::INTEGER, Lane::INTEGER, 
#                          X::INTEGER, Y::INTEGER ) 
#                     [ SEQ_ID=0:*]; 
# 
#  The idea is to take the collection of the format above, and turn it into 
#  two files of the following form. 
#
#   [ [ ('%c',%f) {, ('%c',%f) }*35 ] 
# 		{, [ ('%c',%f) {, ('%c',%f) }*35 ] }*84999999 ]
#
#      and
#
#   [ (%d,%d,%d,%d) {, (%d,%d,%d,%d,%d)}*84999999) ]
#
#   Note that our data set is all taken from a single machine (is this the 
#   case?)
#
#  NOTE: For the purposes of getting a handle on what this data 'looks like', 
#        as part of the transformation process I'll track numbers to get us 
#        a handle on range. 
#
$min_FC = 1000000;
$max_FC = 0;
$min_TN = 1000000;
$max_TN = 0;
$min_X  = 1000000;
$max_X  = 0;
$min_Y  = 1000000;
$max_Y  = 0;
#
sub MIN { 
	$_[0] = $_[1] if $_[1] < $_[0];
}
#
sub MAX { 
	$_[0] = $_[1] if $_[1] > $_[0];
}
#
sub F_FROM_C { 
	#
	# 10*log10(p/(1-p))
	#
	$c=ord($_[0]);
	return (-10.0 * (log($c)/log(10)));
}
#
#  Parse it as a state machine. 
$state=0;
$mdcd="";
$nccd="";
# 
$in_file = shift(@ARGV);
open ( IN_FILE, "<".$in_file) || die "Failed to open input file\n";
#
$out_file_one = $in_file . "_reads";
$out_file_two = $in_file . "_meta";
#
open ( OF1, ">".$out_file_one);
open ( OF2, ">".$out_file_two);
#
printf( OF1 "[");
printf( OF2 "[");
#
while(<IN_FILE>) {
	if ($state == 0) { 
		#
		# SEQ_ID
		#
        # printf("state = 0 >%s<\n", $_);
		#
		$state = 1;
		($machine_id, $fc_lane, $tile_num, $x_pos, $y_pos, $ignore) = 
				split(/[:#]/, $_ );
#
# printf("\tmachine_id = >%s<\n\tfc_lane = >%d<\n\ttile_num=%d\n\tx_pos=%d\n\ty_pos=%d\n\tignore=%s\n", 
#  $machine_id, $fc_lane, $tile_num, $x_pos, $y_pos, $ignore);
#

		&MIN($min_FC,$fc_lane);
		&MAX($max_FC,$fc_lane);

		&MIN($min_TN,$tile_num);
		&MAX($max_TN,$tile_num);

		&MIN($min_X,$x_pos);
		&MAX($max_X,$x_pos);

		&MIN($min_Y,$Y_pos);
		&MAX($max_Y,$Y_pos);

		printf(OF2 "%s(%d,%d,%d,%d)",$mdcd, $fc_lane, $tile_num, $x_pos, $y_pos);
		$mdcd=",";
		
	} elsif (1 == $state) { 
		#
		# Nucleotides. 
		#
		# printf("state = 1 >%s<\n", $_);
		#
		$state = 2;
		@BPS = unpack('C*', $_);
		
	} elsif (2 == $state) { 
		#
		# We can ignore this, as we aready have seen the SEQ_ID
		# 
		# printf("state = 2 >%s<\n", $_);
		#
		$state = 3;

	} elsif (3 == $state) { 
		#
		# Confidence values. 
		#
		# printf("state = 3 >%s<\n", $_);
		@CIN = unpack('C*', $_);
		#
		# Now, print 'em out. 
		#
		printf(OF1 "%s[('%c',%f)", $nccd,@BPS[0], &F_FROM_C(@CIN[0]));
		$nccd=",";
		for($i=1;$i<36;$i++) { 
			printf(OF1 ",('%c',%f)", @BPS[$i], &F_FROM_C(@CIN[0]));
		}
		printf(OF1 "]");
		$state = 0;
	}
}
#
# Cleanup and close. 
#
close IN_FILE;
#
printf( OF1 "]");
printf( OF2 "]");
close OF1;
close OF2;
#
printf("\n--== DONE ==--\n");
printf("Min Flowcell Lane = %d, Max Flowcell Lane = %d\n", $min_FC, $max_FC);
printf("Min Tile Num = %d, Max Tile Num= %d\n", $min_TN, $max_TN);
printf("Min Pos X = %d, Max Pos X = %d\n", $min_X, $max_X);
printf("Min Pos Y = %d, Max Pos Y = %d\n", $min_Y, $max_Y);
