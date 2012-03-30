#!/usr/bin/env perl
use POSIX qw(strftime);

open OLD, "<$ARGV[0]" or die $!;
open NEW, ">$ARGV[0]".".new" or die $!;

while($line = <OLD>)
{
#   $dt = DateTime->now;
#   $date = $dt->ymd;
#   $time = $dt->hms;
#   $formatT = "$date $time";
   $str = strftime "%Y-%m-%d %H:%M:%S", localtime;
   $line =~ s/current_timestamp/\'$str\'/g;
   print NEW $line;
}

close OLD;
close NEW;

