#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my %nodes;

my $nodecnt = 0;
my $markcnt = 0;

sub markNodes
{
  $markcnt++;
  print STDERR "Marking the graph: $markcnt nodecnt: $nodecnt\n";
  hadoop_counter("nodecnt", $nodecnt);
  hadoop_counter("markcnt", 1);

  my $nodeid;
  my $node;

  my $locallink = 0;
  my $localcheck = 0;
  my $remotemark = 0;

  while (($nodeid, $node) = each %nodes)
  {
    ## Check the forward and reverse adjacencies

    foreach my $adj (qw/f r/)
    {
      $node->{"$CANCOMPRESS$adj"}->[0] = 0;

      my ($vid, $vdir) = node_gettail($node, $adj);

      if (defined $vid)
      {
        if ($vid eq $nodeid) { next; }

        if (exists $nodes{$vid})
        {
          $localcheck++;

          my $vnode = $nodes{$vid};
          my ($ttid, $ttdir) = node_gettail($vnode, flip_dir($vdir));

          if (defined $ttid)
          {
            die if ($ttid ne $nodeid);

            ## sweet, we have a shared link
            $locallink++;
            $node->{"$CANCOMPRESS$adj"}->[0] = 1;
          }
        }
        else
        {
          $remotemark++;

          ## tell the sole neighbor it is safe to compress
          print "$vid\t$HASUNIQUEP\t$nodeid\t$adj\n";
        }
      }
    }

    print_node($nodeid, $node);
  }

  hadoop_counter("locallink",  $locallink);
  hadoop_counter("localcheck", $localcheck);
  hadoop_counter("remotemark", $remotemark);

  print STDERR "locallink: $locallink\n";
  print STDERR "localcheck: $localcheck\n";
  print STDERR "remotemark: $remotemark\n";

  undef %nodes;
  $nodecnt = 0;
}


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

  $nodes{$nodeid} = $node;
  $nodecnt++;

  if ($nodecnt >= $LOCALNODES)
  {
    markNodes();
  }
}

markNodes();


