#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $skippedreads = 0;
my $goodreads = 0;
my $shortreads = 0;
my $goodbp = 0;

my $suffix = "";

if (exists $ENV{map_input_file})
{
  my $filename = $ENV{map_input_file};

  if    ($filename =~ /_1\.fastq/) { $suffix = "_1"; }
  elsif ($filename =~ /_2\.fastq/) { $suffix = "_2"; }

  print STDERR "Processing $filename suffix: $suffix\n";
}

my $tag = undef;
my $seq = undef;

while(<>)
{
  next if /^#/;
  next if /^\s*$/;
  next if /^\+/;

  if (/^@(\S+)/)
  {
    ## Parse fastq
    $tag = $1;
    $seq = <>;

    my $h2 = <>;
    my $qual = <>;
  }
  elsif (/^>(\S+)/)
  {
    ## Parse (1-line) fasta
    $tag = $1;
    $seq = <>;
  }
  elsif (/^(\S+)\t(\S+)/)
  {
    $tag = $1;
    $seq = $2;
  }
  else
  {
    die "Invalid file format expected > or @ saw $_\n";
  }

  chomp($seq);
  $seq = uc($seq);

  $tag =~ s/\s+/_/g;
  $tag =~ s/[:#-\.]/_/g;
  #print STDERR "tag: $tag seq: $seq\n";

  $tag .= $suffix;

  ## Hard chop a few bases off of each end of the read
  if ($TRIM5 || $TRIM3)
  {
    #$print STDERR "$seq => ";
    $seq = substr($seq, $TRIM5, length($seq)-$TRIM5-$TRIM3);
    #print STDERR "$seq\n";
  }

  ## Automatically trim Ns off the very ends of reads
  $seq =~ s/^N+//;
  $seq =~ s/N+$//;

  ## Check for non-dna characters
  if ($seq =~ /[^ACTG]/)
  {
    #print STDERR "WARNING: non-DNA characters found in $tag: $seq\n";
    $skippedreads++;
    $tag = undef;
    next;
  }


  ## check for short reads
  my $l = length $seq;
  my $end = $l - $K;

  if ($l <= $K)
  {
    #print STDERR "WARNING: read $tag is too short $l < $K\n";
    $shortreads++;
    next;
  }

  ## Now emit the edges of the de Bruijn Graph

  $goodreads++;
  $goodbp += $l;

  my $ustate = "5";
  my $vstate = "i";

  my %seen;

  my $chunk = "";

  for (my $i = 0; $i < $end; $i++)
  {
    my $u = substr($seq, $i, $K);
    my $v = substr($seq, $i+1, $K);

    my ($uc, $ud) = canonical($u);
    my ($vc, $vd) = canonical($v);

    if (($i == 0) && ($ud eq 'r')) 
    { $ustate = 6; }

    my $t = "$ud$vd";
    my $tr = flip_link($t);

    $uc = str2dna($uc);
    $vc = str2dna($vc);

    if ($i+1 == $end)
    {
      $vstate = "3";
    }

    my $f = substr($seq, $i, 1);
    my $l = substr($seq, $i+$K, 1);

    $f = rc($f);

    my $seen = (exists $seen{$u}) || (exists $seen{$v}) || ($u eq $v);
    $seen{$u} = $i;

    if ($seen)
    {
      $chunk++;
      #print STDERR "repeat internal to $tag: $uc u$i $chunk\n";
    }

    print "$uc\t$t\t$l\t$tag$chunk\t$ustate\n";

    if ($seen)
    {
      $chunk++;
      #print STDERR "repeat internal to $tag: $vc v$i $chunk\n";
    }

    print "$vc\t$tr\t$f\t$tag$chunk\t$vstate\n";

    $ustate = "m";
  }

  $tag = undef;
  %seen = ();
}

hadoop_counter("reads_skipped", $skippedreads);
hadoop_counter("reads_short",   $shortreads);
hadoop_counter("reads_good",    $goodreads);
hadoop_counter("reads_goodbp",  $goodbp);
hadoop_counter("reads_files",   1);
