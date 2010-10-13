#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $linksremoved = 0;
my $threadsremoved = 0;
my $tipskept = 0;

sub	trimNodes
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  ## if (exists $node->{trim})
  ## {
  ##   print STDERR "trimNodes: $nodeid\n";
  ##   print STDERR Dumper($node);
  ##   print STDERR "\n\n";
  ## }

  if (exists $node->{trim})
  {
    foreach my $dir (qw/f r/)
    {
      if (exists $node->{trim}->{$dir})
      {
        my $degree = node_degree($node, $dir);
        my $numtrim = scalar @{$node->{trim}->{$dir}};

        # print STDERR "TrimNodes $nodeid removing $numtrim of $degree in $dir dir\n";

        my $besttip = -1;

        if ($numtrim == $degree)
        {
          ## All edges in this direction are tips, keep the longest one

          $besttip = 0;
          my $bestlen = node_len($node->{trim}->{$dir}->[0]);

          for (my $i = 1; $i < $numtrim; $i++)
          {
            my $l = node_len($node->{trim}->{$dir}->[$i]);
            if ($l > $bestlen)
            {
              $bestlen = $l;
              $besttip = $i;
            }
          }

        }

        for (my $i = 0; $i < $numtrim; $i++)
        {
          my $tip = $node->{trim}->{$dir}->[$i]->{nodeid};

          if ($i == $besttip)
          {
            ## keep this one
            print_node($tip, $node->{trim}->{$dir}->[$i]);
            $tipskept++;

            #print STDERR "Keeping $besttip $tip\n";
          }
          else
          {
            ## remove it
            my $adj = $node->{trim}->{$dir}->[$i]->{adj};
            node_removelink($node, $tip, $adj);

            $linksremoved++;
          }
        }
      }
    }
  }

  $threadsremoved += node_cleanthreads($node);

  print_node($nodeid, $node);
}

my $node = {};
my $nodeid = undef;

while (<>)
{
  #print "==> $_";
  chomp;
  my @vals = split /\t/, $_;

  my $curnodeid = shift @vals;
  if (defined $nodeid && $curnodeid ne $nodeid)
  {
    trimNodes($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $TRIMMSG)
  {
    my $adj  = shift @vals;

    my $dir = substr($adj, 0, 1);

    my $x = shift @vals;
    shift @vals; ## nodemsg

    my $nn;
    $nn->{nodeid} = $x;
    $nn->{adj} = $adj;

    parse_node($nn, \@vals);

    push @{$node->{trim}->{$dir}}, $nn;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

trimNodes($nodeid, $node);


hadoop_counter("removed_threads", $threadsremoved);
hadoop_counter("removed_links", $linksremoved);
hadoop_counter("tips_kept", $tipskept);
