#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $TOPCNT = 10;
my $n50contigthreshold = 100;
my @n50sizes;

my %stats;

my @cutoffs = qw/1
                 50
                 100 
                 250 
                 500 
                 1000 
                 5000 
                 10000
                 15000
                 20000
                 25000
                 30000
                 35000
                 40000
                 50000
                 100000
                 125000
                 150000
                 200000
                 250000
                 500000
                 1000000/;

while (<>)
{
  chomp;

  if (/^#/)
  {
    print "$_\n";
    next;
  }
  elsif (/^SHORT/)
  {
    my ($tag, $cutoff, $cnt, $sum, $degree, $cov) = split /\t/, $_;

    $stats{$cutoff}->{cnt}    += $cnt;
    $stats{$cutoff}->{sum}    += $sum;
    $stats{$cutoff}->{degree} += $degree;
    $stats{$cutoff}->{cov}    += $cov;
  }
  else
  {
    my ($len, $fdegree, $rdegree, $cov) = split /\t/, $_;

    if ($len >= $n50contigthreshold)
    {
      push @n50sizes, $len;
    }

    foreach my $c (@cutoffs)
    {
      if ($len >= $c) 
      { 
        $stats{$c}->{cnt}++; 
        $stats{$c}->{sum}    += $len; 
        $stats{$c}->{degree} += ($fdegree + $rdegree) * $len;
        $stats{$c}->{cov}    += $cov * $len;
      }
    }
  }
}


printf "%-11s% 10s% 10s% 13s% 10s% 10s% 10s% 10s\n", 
       "Threshold", "Cnt", "Sum", "Mean", "N50", "N50Cnt", "Deg", "Cov";

@cutoffs = sort {$b <=> $a} keys %stats;

if (scalar @cutoffs > 0)
{
  @n50sizes = sort {$b <=> $a} @n50sizes;
  
  my $n50sum = 0;
  my $n50candidates = scalar @n50sizes;
  
  my $curcutoff = 0;
  my $cursize   = $cutoffs[$curcutoff];
  my $n50cutoff = $stats{$cursize}->{sum} / 2;

  for (my $i = 0; $i < $n50candidates; $i++)
  {
    $n50sum += $n50sizes[$i];

    if ($n50sum >= $n50cutoff)
    {
      $stats{$cursize}->{n50}    = $n50sizes[$i];
      $stats{$cursize}->{n50cnt} = $i+1;

      $curcutoff++;

      while ($curcutoff < scalar @cutoffs)
      {
        $cursize   = $cutoffs[$curcutoff];
        $n50cutoff = $stats{$cursize}->{sum} / 2;

        if ($n50sum >= $n50cutoff)
        {
          $stats{$cursize}->{n50}    = $n50sizes[$i];
          $stats{$cursize}->{n50cnt} = $i+1;

          $curcutoff++;
        }
        else
        {
          last;
        }
      }

      if ($curcutoff >= scalar @cutoffs)
      {
        last;
      }
    }
  }

  while ($curcutoff < scalar @cutoffs)
  {
    $cursize = $cutoffs[$curcutoff];
    $stats{$cursize}->{n50} = 0;
    $stats{$cursize}->{n50cnt} = 0;

    $curcutoff++;
  }

  
  foreach my $t (@cutoffs)
  {
    my $c      = $stats{$t}->{cnt};
    my $s      = $stats{$t}->{sum};
    my $n50    = $stats{$t}->{n50};
    my $n50cnt = $stats{$t}->{n50cnt};

    my $degree = $stats{$t}->{degree} / $s;
    my $cov    = $stats{$t}->{cov} / $s;
  
    printf ">%-10s% 10d% 10d%13.02f%10d%10d%10.02f%10.02f\n", 
            $t, $c, $s, ($c?$s/$c:0.0), $n50, $n50cnt, $degree, $cov;
  }
}

my $n50candidates = scalar @n50sizes;
my $topsum = 0;

for (my $i = 0; ($i < $TOPCNT) && ($i < $n50candidates); $i++)
{
  $topsum += $n50sizes[$i];
  my $j = $i+1;
  print "max_$j:\t$n50sizes[$i]\t$topsum\n";
}

if ($N50_TARGET)
{
  my $n50sum = 0;
  my $n50cutoff = $N50_TARGET/2;
  my $n50found = 0;

  print "global_n50target: $N50_TARGET\n";

  for (my $i = 0; $i < $n50candidates; $i++)
  {
    $n50sum += $n50sizes[$i];

    if ($n50sum >= $n50cutoff)
    {
      my $n50size = $n50sizes[$i];
      my $n50cnt = $i + 1;
      $n50found = 1;
      print "global_n50: $n50size\n";
      print "global_n50cnt: $n50cnt\n";

      last;
    }
  }

  if (!$n50found)
  {
    print "global_n50: <$n50contigthreshold\n";
    print "global_n50cnt: >$n50candidates\n";
  }
}
