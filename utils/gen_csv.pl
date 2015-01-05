#!/usr/bin/perl
#Problems with this? On ubuntu install this way:
#$ sudo apt-get install libmath-round-perl
use Math::Round;

$example = "Ex:\n  gen_csv.pl str20u30n10 int5n0 40\nWill generate 40 rows, two columns. First: string 20-30 characters long, 10% null; second: int between 0 and 5, never null.";

sub random_int
{
  my $lower_bound=shift;
  my $upper_bound=shift;
  if ( $lower_bound == $upper_bound )
  { 
    return $lower_bound;
  }
  return round ( (rand ( $upper_bound - $lower_bound )) + $lower_bound );
}

sub throw_error
{
  print "ERROR: ";
  while ( my $arg = shift ) 
  {
    print $arg;
  }
  print "\n";
  exit 1;
}

$OPCODE_TYPE_STRING=1;
$OPCODE_TYPE_INT=2;
@ops = ();

sub print_ops
{
  for $i ( 0 .. $#ops ) 
  {
     print "OP: ";

     if ($ops[$i][0] == $OPCODE_TYPE_STRING)
     {
        print "str", $ops[$i][1], "u", $ops[$i][2], "n", ($ops[$i][3] * 100), "\n";
     }
     elsif ($ops[$i][0] == $OPCODE_TYPE_INT)
     {
        print "int", $ops[$i][1], "n", ($ops[$i][2] * 100), "\n";
     }
  
     else
     {
        throw_error "Unknown opcode ", $ops[$i][0];
     }
  }
}

sub execute_ops
{
  my @chars=('a'..'z','A'..'Z','0'..'9','_');
  my $nChars = $#chars+1;

  foreach (1..$numRows)
  {
    $comma=0;
    for $i ( 0 .. $#ops )
    {
       if($comma==0)
       {
          $comma = 1;
       }
       else
       {
          print ",";
       }

       if ($ops[$i][0] == $OPCODE_TYPE_STRING)
       {
          if ( $ops[$i][3] == 0 || rand >= $ops[$i][3] )
          {
             my $size = random_int $ops[$i][1], $ops[$i][2];
             foreach (1..$size)
             {
               print $chars[rand $nChars];
             }
          }
       }
       elsif ($ops[$i][0] == $OPCODE_TYPE_INT)
       {     
          if ( $ops[$i][2] == 0 || rand >= $ops[$i][2] )
          {
             print random_int 0, $ops[$i][1];
          }
       } 
       else
       {
          throw_error "Unknown opcode ", $ops[$i][0];
       }
    }
    print "\n";
  }
}

$nArgs = @ARGV;

$numRows=-1;
@csv_items=();
foreach (1..$nArgs)
{
  my $arg = shift;
  
  if ( $arg =~ /^\d+$/ )
  {
    if ( $numRows != -1 )
    {
      throw_error "extraneous argument ", $arg, "; the number of rows is already specified as $numRows";  
    }
    $numRows = $arg;
  }
  elsif ( $arg =~ /^(\w\w\w)/ )
  {
    my $opcode = $1;
    my @op_entry = ();

    if ( $opcode eq "str" )
    {
      if ($arg =~ /^str(\d+)u(\d+)n(\d+)$/ )
      {
         push @op_entry, $OPCODE_TYPE_STRING;
         push @op_entry, $1;
         push @op_entry, $2;
         push @op_entry, (($3) / 100);
         push @ops, [ @op_entry ] ;
      }
      else
      {
	 throw_error "cannot parse opcode ", $arg, "; expecting strxxuyynzz where xx,yy,zz are integers for lower bound, upper bound and null percentage"
      } 
    }
    elsif ( $opcode eq "int" )
    {
      if ($arg =~ /^int(\d+)n(\d+)$/ )
      {
          push @op_entry, $OPCODE_TYPE_INT;
          push @op_entry, $1;
          push @op_entry, (($2) / 100);
          push @ops, [ @op_entry ] ;
      }
      else
      {
         throw_error "cannot parse opcode ", $arg, "; expecting intxxuyynzz where xx,yy,zz are integers for upper bound and null percentage"
      }
    }
    else
    {
      throw_error "unknown opcode ", $opcode;
    }    
  }
}

if ($numRows == -1 )
{ 
  throw_error "You did not set the desired number of rows. Please provide an integer.\n", $example ;
}

if (scalar(@ops) == 0)
{
  throw_error "You did not provide any column definitions. Need at least one.\n", $example ;
}

#print_ops;

execute_ops;
