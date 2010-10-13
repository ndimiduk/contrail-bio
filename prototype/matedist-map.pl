#!/usr/bin/perl -w
use strict;

use lib ".";
use PAsm;

my $linking_reads    = 0;
my $internal_mates   = 0;
my $internal_dist    = 0;
my $internal_distsq  = 0;
my $internal_invalid = 0;
my $internal_mean    = 0;
my $internal_variance = 0;

my $unique_contigs = 0;
my $all_contigs = 0;

while (<>)
{
  chomp;

  my $node = {};

  my @vals = split /\t/, $_;

  my $nodeid = shift @vals;

  my $msgtype = shift @vals; ## nodemsg

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  else
  {
    die "Unknown msg: $_\n";
  }

  my $len = node_len($node);
  my $isunique = node_isunique($node);

  $all_contigs++;
  if ($isunique) { $unique_contigs++; }

  ## Only consider mates from unique contigs
  if ($isunique && defined $node->{$R5})
  {
    my %contiginfo;

    foreach my $readstr (@{$node->{$R5}})
    {
      my ($read,$offset) = split /:/, $readstr;

      my $rc = 0;
      if ($read =~ /^~/)
      {
        $rc = 1;
        $read =~ s/^~//;
      }

      my $basename = mate_basename($read);

      if (defined $basename)
      {
        if (exists $contiginfo{$basename})
        {
          $contiginfo{$basename}->{internal} = 1;

          my $idist;

          if ($contiginfo{$basename}->{rc} && !$rc)
          {
            $idist = $contiginfo{$basename}->{dist} - $offset + 1;
          }
          elsif ($rc && !$contiginfo{$basename}->{rc})
          {
            $idist = $offset - ($len - $contiginfo{$basename}->{dist}) + 1;
          }

          if (defined $idist)
          {
            $internal_mates++;
            $internal_dist += $idist;
            $internal_distsq += ($idist * $idist);

            $internal_variance += ($idist-$internal_mean) *
                                  ($idist-$internal_mean) *
                                  ($internal_mates-1) /
                                  ($internal_mates);

            $internal_mean     += ($idist - $internal_mean) / 
                                  $internal_mates;
          }
          else
          {
            $internal_invalid++;
          }
        }
        else
        {
          my $dist = $offset;

          if (!$rc) { $dist = $len - $offset; }

          $contiginfo{$basename}->{rc}   = $rc;
          $contiginfo{$basename}->{dist} = $dist;
          $contiginfo{$basename}->{read} = $read;
        }
      }
    }

    foreach my $basename (keys %contiginfo)
    {
      my $info = $contiginfo{$basename};

      next if defined $info->{internal};

      my $rc   = $info->{rc};
      my $dist = $info->{dist};
      my $read = $info->{read};

      print "$basename\t$MATEDIST\t$nodeid\t$read\t$rc\t$dist\t$isunique\n";
      $linking_reads++;
    }
  }
}

hadoop_counter("unique_contigs",   $unique_contigs);
hadoop_counter("all_contigs",      $all_contigs);

hadoop_counter("internal_mates",   $internal_mates);
hadoop_counter("internal_dist",    $internal_dist);
hadoop_counter("internal_distsq",  $internal_distsq);
hadoop_counter("internal_invalid", $internal_invalid);
hadoop_counter("linking_reads",    $linking_reads);

my $stdev = sqrt($internal_variance);
printf STDERR "internal_mean: %0.2f\n", $internal_mean;
printf STDERR "internal_stdev: %0.2f\n", $stdev;



