#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $USAGE = "confirm-starts.pl reads.fa graph.txt\n";

my $readfasta = shift @ARGV or die $USAGE;

open READS, "< $readfasta" or die "Can't open $readfasta ($!)\n";

my %readseq;

my $id;
my $seq;

## Load the reads
while (<READS>)
{
  chomp;

  ($id, $seq) = split /\t/, $_;
  $readseq{$id} = $seq; 
}

my $numreads = scalar keys %readseq;
print STDERR "Loaded $numreads reads\n";

## Now scan the graph
my $nodeschecked = 0;
my $readschecked = 0;
my $fwd_good = 0;
my $fwd_err = 0;
my $rc_good = 0;
my $rc_err = 0;

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

  $nodeschecked++;
  
  my $nodestr = node_str($node);

  if (exists $node->{$R5})
  {
    foreach my $readstr (@{$node->{$R5}})
    {
      $readschecked++;

      my ($read, $offset) = split /:/, $readstr;

      my $rc = 0;

      my $readstr;
      my $nodestr_s;
      my $readstr_s;

      if ($read =~ /^~/)
      {
        $rc = 1;
        $read =~ s/^~//;

        $readstr = $readseq{$read};

        $nodestr_s = substr($nodestr, 0, $offset+1);
        $readstr_s = substr($readstr, 0, length($nodestr_s));

        $nodestr_s = substr(rc($nodestr_s), 0, length($readstr_s));
      }
      else
      {
        $readstr = $readseq{$read};
        $nodestr_s = substr($nodestr, $offset, length($readstr));
        $readstr_s = substr($readstr, 0, length($nodestr_s));
      }

      if (uc($nodestr_s) ne uc($readstr_s))
      {
        print ">$read $nodeid $rc $offset fail\n";
        print "n: $nodestr_s $nodestr\n";
        print "r: $readstr_s $readstr\n";

        if ($rc) { $rc_err++; } else { $fwd_err++; }
      }
      else
      {
        #print ">$read $nodeid $rc $offset ok\n";
        #print "n: $nodestr_s $nodestr\n";
        #print "r: $readstr_s $readstr\n";

        if ($rc) { $rc_good++; } else { $fwd_good++; }
      }
    }
  }
}

print STDERR "Checked $readschecked reads in $nodeschecked nodes\n";
print STDERR "fwd: $fwd_good good $fwd_err err\n";
print STDERR "rev: $rc_good good $rc_err err\n";


