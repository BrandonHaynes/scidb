#!/usr/bin/perl
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
# This script takes as input a directory name (e.g. root of either SciDB trunk or P4 trunk),
# and modifies the copyright blocks in them.
#
# @note:
#   - A typical use case is to extend the copyright by another year.
#   - A file's begin-copyright year will be preservd.
#   - A totally different copyright block will be replaced in full.
#
# @prerequisites:
#   - The current directory includes files license.txt and license.py.txt,
#     which includes copyright blocks for C-style and Python-style, respectively.
#
# @note The following directories and files are omitted:
#   - all directories in @SKIP_DIRS
#   - all files in @SKIP_FILES
#   - all files ending with ~
#
# @note The files in which missing copyright blocks are added are
#   - py, h, c, cpp, hpp, sh, java, yy, ll
#   - their "in" version, e.g. py.in.
#   To expand the script to handle more file types, follow the example of 'java'
#   (search all occurences of 'java' in this file).
#
# @def A "copyright block" is a block of lines starting with a line that includes BEGIN_COPYRIGHT
#      and ends with a line that includes END_COPYRIGHT.
#
# @author dzhang
#
use strict;

# Some constants.
my $BEGIN_COPYRIGHT = "BEGIN_COPYRIGHT";
my $END_COPYRIGHT = "END_COPYRIGHT";
my $NEW_COPYRIGHT_FILE_STAR = "license.txt";
my $NEW_COPYRIGHT_FILE_SHARP = "license.py.txt";

my $TRUE = 1;
my $FALSE = 0;

my $STATE_BEFORE = 0;  # The state before seeing the copyright block (when scanning a file)
my $STATE_DURING = 1;  # The state during processing the copyright block.
my $STATE_AFTER  = 2;  # The state after processing the copyright block.

# Variables that are used to gather some statistics of how many files are found with/without copyright blocks.
my %hashSuffixToCount;         # has copyright
my %hashSuffixToCountMissing;  # no copyright

# The following variables will be set by getNewCopyright(), which is called by main().
my @newCopyrightStar;
my @newCopyrightSharp;
my $newCopyrightYear = "2000";

# The following dir/file names will be skipped.
my @SKIP_DIRS = (
    ".",
    "..",
    "stage",
    ".svn",
    "3rdparty",
    "lib_json"
);

my @SKIP_FILES = (
    $NEW_COPYRIGHT_FILE_STAR,
    $NEW_COPYRIGHT_FILE_SHARP,
    "bsdiff.c",
    "bspatch.c",
    "FindProtobuf.cmake", # See ticket:3215
    "MurmurHash3.h",
    "MurmurHash3.cpp",
    "statistics.py",      # The statistics.py package was introduced in Python 3. But we have Python 2.
    "counter.py",         # The Counter class was introduced in Python 2.7. But we have 2.6.6.
    "PSF_license.txt",    # The file that contains the PSF license agreement
    "scidb_psf.py"        # Code we borrowed, that have PSF license
);

# As the name says.
#
sub printUsageAndExit
{
    print "Usage: ", __FILE__, " rootDir [replace]\n";
    print "In the read mode, the script shows how many files contains the copyright block, breaking down by suffix.\n";
    print "In the replace mode, the script replaces the copyright blocks with new content, or add if not exist.\n";
    exit;
}

# Return the first char of the line containing $BEGIN_COPYRIGHT, if found; or 0, if not found.
# @param file
# @return the first char of the copyright line, if copyright block is found; or 0.
# @throw if the first chars of the lines containing $BEGIN_COPYRIGHT and $END_COPYRIGHT do not match.
# @throw if the first char of the line containing $BEGIN_COPYRIGHT is not "*" or "#".
# @throw if there exists a line containing $BEGIN_COPYRIGHT but not a line containing $END_COPYRIGHT.
#
sub returnFirstCharIfMatch
{
    my($file) = @_;
    local(*HAND);
    my $firstChar;  # first char of the beginCopyright line; or false if not exist

    open(HAND, $file) or die "Cannot open $file.\n";
    while (<HAND>) {
        if ( /^\s*(.)\s+$BEGIN_COPYRIGHT/ ) {
            $firstChar = $1;
            die "First char not * or #!\n" unless ($firstChar eq "*" or $firstChar eq "#");
            last;
        }
    }

    unless ($firstChar) {
        close HAND;
        return 0;
    }

    while (<HAND>) {
        if ( /^\s*(.)\s+$END_COPYRIGHT/ ) {
            unless ($firstChar eq $1) {
                die "ERROR! In $file, the lines of $BEGIN_COPYRIGHT and $END_COPYRIGHT have different first char.\n";
            }
            close HAND;
            return $firstChar;
        }
    }
    die "ERROR! $file has $BEGIN_COPYRIGHT but not $END_COPYRIGHT.\n";
}

# Get the suffix of a file name.
# @param[in] filename
# @return the suffix
#
# @note: certain types of files with suffix "in" have more detailed suffixs returned.
#   E.g. "py.in" is a different category from "in".
#   These are the types given at the top of the file, for which missing copyright blocks will be added,
#
sub getSuffix
{
    my($file) = @_;
    if ($file =~ /(.*)\.((py|h|c|cpp|hpp|sh|java|yy|ll)\.in)$/) {
        return $2;
    }
    elsif ($file =~ /(.*)\.(.+)/) {
        return $2;
    }
    return $file;
}

# Replace the copyright block in a file with a new copyright block, either @newCopyrightStar or @newCopyrightSharp.
# @param file
# @param isStar  whether using $NEW_COPYRIGHT_FILE_STAR
# @return whether replaced.
#
# Case 1: old copyright block and the new one are the same.
# Action: skip the file.
#
# Case 2: old copyright block and the new one differ only in copyright duration.
#     Case 2.1: old copyright has a duration.
#     Action: if the ending year = current year, skip.
#             else change the ending year to current year, but keep the rest of the old copyright block.
#
#     Case 2.2: old copyright has a single year.
#     Action: if the year = current year, skip.
#             elif the year < current year, change to a duration, from that year to the current year.
#             else error out.
#
# Case 3: old copyright and the new one are drastically different.
# Action: replace the whole copyright block with the new copyright block.
#
sub replace
{
    # sanity check
    die "calling replace(), but new copyright not set" unless ($#newCopyrightStar>0 and $#newCopyrightSharp>0);

    my $tmpFile = "/tmp/replace_copyright_tmp.txt";
    my($file, $isStar) = @_;
    my $state = $STATE_BEFORE;
    my $line;

    # Divide the file into three pieces: beforeCopyright, oldCopyright, afterCopyright.
    local(*HAND);
    my @beforeCopyright = ();
    my @oldCopyright = ();
    my @afterCopyright = ();
    open(HAND, $file) or die "Can't open $file for read.\n";
    $state = $STATE_BEFORE;
    while ($line = <HAND>) {
        if ($state == 0) {
            if ( $line =~ /^\s*(.)\s+$BEGIN_COPYRIGHT/ ) {
                if ($isStar) {
                    die "$_, $file, $isStar, $1" unless ($1 eq "*");
                }
                else {
                    die unless ($1 eq "#");
                }
                push @oldCopyright, $line;
                $state = $STATE_DURING;
            }
            else {
                push @beforeCopyright, $line;
            }
        }
        elsif ($state == $STATE_DURING) {
            push @oldCopyright, $line;
            if ( $line =~ /^\s*(.)\s+$END_COPYRIGHT/ ) {
                $state = $STATE_AFTER;
            }
        }
        else {
            push @afterCopyright, $line;
        }
    }
    close HAND;

    # Determine the case (see comment at the top of the method).
    our @newCopyright;
    *newCopyright = $isStar ? \@newCopyrightStar : \@newCopyrightSharp;

    my $needToReplace = $FALSE;
    my $usingNewCopyright = $FALSE;  # if need to replace, whether to use the new one or the old one.

    if ($#newCopyright != $#oldCopyright) {
        # the number of lines in the copyright headers are different ==> regard totally different
        $needToReplace = $TRUE;
        $usingNewCopyright = $TRUE;
    }
    else {
        my $differ = $FALSE;  # whether some line in the two copyright blocks differ
        for (my $i=0; $i<=$#oldCopyright; $i++) {
            if ( $oldCopyright[$i] eq $newCopyright[$i] ) {
                next;
            }
            if ( $differ ) {
                # multiple lines differ ==> regard totally different
                $needToReplace = $TRUE;
                $usingNewCopyright = $TRUE;
                last;
            }

            $differ = $TRUE;
            unless ( $newCopyright[$i] =~ /Copyright.*\d\d\d\d.*\d\d\d\d/ ) {
                # the line that differ is not the line containing copyright duration ==> regard totally different
                $needToReplace = $TRUE;
                $usingNewCopyright = $TRUE;
                last;
            }
            if ( $oldCopyright[$i] =~ /^(.*Copyright.*\d\d\d\d.*)(\d\d\d\d)(.*)$/ ) {
                # The old copyright block has a duration...

                if ( $2 eq $newCopyrightYear ) {
                    # the ending year in the old copyright note is equal to the current year ==> should skip
                    next;  # but make sure the remaining lines match.
                }
                else {
                    # change the ending year, but keep the old copyright block,
                    $oldCopyright[$i] = $1 . $newCopyrightYear . $3 . "\n";
                    $needToReplace = $TRUE;
                    $usingNewCopyright = $FALSE;
                    next; # make sure the remaining lines match.
                }
            }
            elsif ( $oldCopyright[$i] =~ /^(.*Copyright.*)(\d\d\d\d)(.*)$/ ) {
                # The old copyright block has a single year..

                if ( $2 eq $newCopyrightYear ) {
                    # the ending year in the old copyright note is equal to the current year ==> should skip
                    next;  # but make sure the remaining lines match.
                }
                elsif ( (0 + $2) < (0 + $newCopyrightYear) ) {
                    # change to a duration, but keep the old copyright block,
                    $oldCopyright[$i] = $1 . $2 . " - " . $newCopyrightYear . $3 . "\n";
                    $needToReplace = $TRUE;
                    $usingNewCopyright = $FALSE;
                    next; # make sure the remaining lines match.
                }
                else {
                    die "In file $file, the ending copyright year is later than what's in the new copyright block!"
                }
            }
            else {
                # This line in the old copyright block does not have duration or year ==> regard totally different.
                $needToReplace = $TRUE;
                $usingNewCopyright = $TRUE;
                last;
            }
        } # end for ($i=0; $i<=$#oldCopyright; $i++) {
    }

    # if no need to replace: return.
    unless ( $needToReplace ) {
        return $FALSE;  # not replaced
    }

    # copy to preserve the permission
    system("cp", "-p", "$file", "$tmpFile");

    local(*HAND2);
    open(HAND2, ">$tmpFile") or die "Can't write to $tmpFile.";

    foreach $line (@beforeCopyright) {
        print HAND2 $line;
    }
    our @tmpCopyright;
    *tmpCopyright = ($usingNewCopyright ? \@newCopyright : \@oldCopyright);
    foreach my $line (@tmpCopyright) {
        print HAND2 $line;
    }
    foreach $line (@afterCopyright) {
        print HAND2 $line;
    }
    close HAND2;

    system("mv", "$tmpFile", "$file");

    return $TRUE;  # replaced
}

# Add the copyright block in a file.
# @param file
# @param isStar  whether using $NEW_COPYRIGHT_FILE_STAR
#
sub addCopyright
{
    # sanity check
    die "calling addCopyright(), but new copyright not set" unless ($#newCopyrightStar>0 and $#newCopyrightSharp>0);

    my $tmpFile = "/tmp/replace_copyright_tmp.txt";
    my($file, $isStar) = @_;

    # copy to preserve the permission
    system("cp", "-p", "$file", "$tmpFile");

    local(*HAND2);
    open(HAND2, ">$tmpFile") or die "Can't write to $tmpFile.";

    local(*HAND);
    open(HAND, $file) or die "Can't open $file for read.\n";

    if ($isStar) {
        print HAND2 "/*\n";
        print HAND2 "**\n";
        foreach my $line (@newCopyrightStar) {
            print HAND2 $line;
        }
        print HAND2 "*/\n";
    } else {
        # For shells, typically the first line starts with "#!" or ":". Keep it that way.
        my $firstLine = <HAND>;
        my $firstLinePrinted = 0;
        if ( $firstLine and ($firstLine =~ /^#!/ or $firstLine =~ /^:/) ) {
            print HAND2 $firstLine;
            $firstLinePrinted = 1;
        }

        print HAND2 "#\n";
        foreach my $line (@newCopyrightSharp) {
            print HAND2 $line;
        }
        print HAND2 "#\n";

        unless ($firstLinePrinted) {
            print HAND2 $firstLine;
        }
    }

    while (<HAND>) {
        print HAND2;
    }
    close HAND2;
    close HAND;

    system("mv", "$tmpFile", "$file");
}

# For each file in the directory containing $BEGIN_COPYRIGHT, increase the count for entry in %hashSuffixToCount.
# Also, in replace mode, replace the copyright block.
# @param dir
# @param isReplaceMode
#
sub loopDir
{
    my($dir, $isReplaceMode) = @_;
    chdir($dir) || die "Cannot chdir to $dir.\n";
    local(*DIR);
    opendir(DIR, ".");
    while (my $f = readdir(DIR)) {
        next if ( -l $f || $f =~ /\~$/ );  # skip symbolic links and names ending with tilde.
        if (-d $f) {
            next if (grep {$_ eq $f} @SKIP_DIRS);
            loopDir($f, $isReplaceMode);
        }
        elsif (-f $f) {
            next if (grep {$_ eq $f} @SKIP_FILES);
            my $firstChar = &returnFirstCharIfMatch($f);
            my $suffix = &getSuffix($f);
            if ($firstChar) {
                if ($isReplaceMode) {
                    &replace($f, $firstChar eq "*");
                }
                if (exists $hashSuffixToCount{$suffix}) {
                    $hashSuffixToCount{$suffix} ++;
                }
                else {
                    $hashSuffixToCount{$suffix} = 1;
                }
            }
            else {
                my $missing = 0;

                # h, c, cpp, hpp, java, yy, ll
                if ($suffix eq "h" or $suffix eq "h.in" or
                    $suffix eq "c" or $suffix eq "c.in" or
                    $suffix eq "cpp" or $suffix eq "cpp.in" or
                    $suffix eq "hpp" or $suffix eq "hpp.in" or
                    $suffix eq "java" or $suffix eq "java.in" or
                    $suffix eq "yy" or $suffix eq "yy.in" or
                    $suffix eq "ll" or $suffix eq "ll.in"
                ) {
                    if ($isReplaceMode) {
                        &addCopyright($f, $TRUE);  # isStar
                    }
                    $missing = 1;
                }
                # py, sh
                elsif ($suffix eq "py"    or $suffix eq "sh" or
                       $suffix eq "py.in" or $suffix eq "sh.in") {
                    if ($isReplaceMode) {
                        &addCopyright($f, $FALSE);  # not isStar
                    }
                    $missing = 1;
                }

                if ($missing) {
                    if (exists $hashSuffixToCountMissing{$suffix}) {
                        $hashSuffixToCountMissing{$suffix} ++;
                    }
                    else {
                        $hashSuffixToCountMissing{$suffix} = 1;
                    }
                }
            }
        }
    }
    closedir(DIR);
    chdir("..");
}

# Report result from %hashSuffixToCount.
#
sub report
{
    my $count = 0;
    my $countMissing = 0;
    print "******** Has copyright *********\n";
    foreach my $key (keys %hashSuffixToCount) {
        print $key, "\t", $hashSuffixToCount{$key}, "\n";
        $count += $hashSuffixToCount{$key};
    }
    print "Subtotal: $count.\n";

    print "******** No copyright *********\n";
    foreach my $key (keys %hashSuffixToCountMissing) {
        print $key, "\t", $hashSuffixToCountMissing{$key}, "\n";
        $countMissing += $hashSuffixToCountMissing{$key};
    }
    print "Subtotal: $countMissing.\n";

    print "******** Total ****************\n";
    print "Total match: " . ($count + $countMissing) . ".\n";
}

# Get the new copyright.
# @param isStar
# @note assign values to
#    - @newCopyrightStar (if $isStar == true)
#    - @newCopyrightSharp (if $isStar == false)
#    - $newCopyrightYear
#
sub getNewCopyright
{
    my($isStar) = @_;
    my $firstChar = $isStar ? "*" : "#";
    my $file = $isStar ? $NEW_COPYRIGHT_FILE_STAR : $NEW_COPYRIGHT_FILE_SHARP;
    our @newCopyright;
    *newCopyright = ($isStar ? \@newCopyrightStar : \@newCopyrightSharp);

    local(*HAND);
    my $state = $STATE_BEFORE;

    open(HAND, $file) or die "Can't open $file for read.\n";
    while (my $line = <HAND>) {
        if ($state == $STATE_BEFORE) {
            if ( $line =~ /^\s*(.)\s+$BEGIN_COPYRIGHT/ ) {
                push @newCopyright, $line;
                $state = $STATE_DURING;
            }
        }
        elsif ($state == $STATE_DURING) {
            push @newCopyright, $line;
            if ( $line =~ /\s*Copyright\s*\(C\)\s*\d\d\d\d.*(\d\d\d\d)/ ) {
                if ( (0 + $newCopyrightYear) < (0 + $1) ) {
                    $newCopyrightYear = $1;
                }
            }
            if ( $line =~ /^\s*(.)\s+$END_COPYRIGHT/ ) {
                $state = $STATE_AFTER;
            }
        }
    }
    close HAND;

    # sanity check
    my $c = &returnFirstCharIfMatch($file);
    die "$file does not start with $firstChar" unless ($c and ($c eq $firstChar));
    die unless (0 + $newCopyrightYear) > 2000;
}

# The main function.
#
sub main
{
    if ($#ARGV < 0 || $#ARGV > 1 || ($#ARGV==1 && not $ARGV[1] eq "replace")) {
        printUsageAndExit;
    }
    &getNewCopyright($TRUE);  # $isStar
    &getNewCopyright($FALSE); # not $isStar;

    my $root = $ARGV[0];
    my $isReplaceMode = ($#ARGV==1);
    if ($isReplaceMode) {
        print "Replacing copyright blocks in '$root' ...\n";
    }
    else {
        print "Examining (read-only) copyright blocks in '$root' ...\n";
    }
    &loopDir($root, $isReplaceMode);
    &report;
}

&main;

