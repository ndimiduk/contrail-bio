#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my %nodes;
my $nodecnt = 0;
my $updatecnt = 0;

sub isMale
{
  my $nodeid = shift;
  my $rand = getrand($nodeid);
  my $male = ($rand >= .5) ? 1 : 0;

  #print STDERR "$nodeid $rand $male\n";

  return $male;
}

sub getBuddy
{
  my $node = shift;
  my $dir = shift;

  my ($buddy, $buddydir);

  if ($node->{"$CANCOMPRESS$dir"}->[0])
  {
    ($buddy, $buddydir) = node_gettail($node, $dir);
    die if !$buddy;
  }

  return ($buddy, $buddydir);
}

sub updateGraph
{
  $updatecnt++;

  print STDERR "Updating the graph: $updatecnt nodecnt: $nodecnt\n";
  hadoop_counter("nodecnt", $nodecnt);
  hadoop_counter("updatecnt", 1);

  my $nodeid;
  my $node;

  my $localupdate = 0;
  my $remoteupdate = 0;
  my $mergestomake = 0;

  while (($nodeid, $node) = each %nodes)
  {
    my $compress     = undef;
    my $compressdir  = undef;
    my $compressbdir = undef;

    my ($fbuddy, $fbuddydir) = getBuddy($node, $fwd);
    my ($rbuddy, $rbuddydir) = getBuddy($node, $rev);

    ## If there is not at least 1 buddy, nothing to do
    next if !defined $fbuddy && !defined $rbuddy;

    my $male = isMale($nodeid);
    
    if ($male)
    {
      ## Prefer Merging forward
      if (defined $fbuddy)
      {
        my $fmale = isMale($fbuddy);

        if (!$fmale)
        {
          $compress     = $fbuddy;
          $compressdir  = $fwd;
          $compressbdir = $fbuddydir;
        }
      }

      if ((!defined $compress) && (defined $rbuddy))
      {
        my $rmale = isMale($rbuddy);

        if (!$rmale)
        {
          $compress     = $rbuddy;
          $compressdir  = $rev;
          $compressbdir = $rbuddydir;
        }
      }
    }
    else
    {
      if (defined $rbuddy && defined $fbuddy)
      {
        my $fmale = isMale($fbuddy);
        my $rmale = isMale($rbuddy);

        if (!$fmale && !$rmale)
        {
          if (($nodeid lt $fbuddy) && 
              ($nodeid lt $rbuddy))
          {
            ## FFF and I'm the local minimum, go ahead and compress
            $compress     = $fbuddy;
            $compressdir  = $fwd;
            $compressbdir = $fbuddydir;
          }
        }
      }
      elsif (!defined $rbuddy)
      {
        my $fmale = isMale($fbuddy);

        if (($nodeid lt $fbuddy) && !$fmale)
        {
          ## Its X*=>FF and I'm the local minimum
          $compress     = $fbuddy;
          $compressdir  = $fwd;
          $compressbdir = $fbuddydir;
        }
      }
      elsif (!defined $fbuddy)
      {
        my $rmale = isMale($rbuddy);

        if (($nodeid lt $rbuddy) && !$rmale)
        {
          ## Its FF=>X* and I'm the local minimum
          $compress     = $rbuddy;
          $compressdir  = $rev;
          $compressbdir = $rbuddydir;
        }
      }
    }

    if (defined $compress)
    {
      #print STDERR "compress $nodeid $compress $compressdir $compressbdir\n";
      $mergestomake++;

      ## Save that I'm supposed to merge
      $node->{$MERGE}->[0] = $compressdir;

      ## Now tell my ~CD neighbors about my new nodeid
      my $toupdate = flip_dir($compressdir);

      foreach my $adj (qw/f r/)
      {
        my $key = "$toupdate$adj";

        my $origadj = flip_dir($adj) . $compressdir;
        my $newadj  = flip_dir($adj) . $compressbdir;

        if (exists $node->{$key})
        {
          foreach my $p (@{$node->{$key}})
          {
            if (exists $nodes{$p})
            {
              $localupdate++;

              #print STDERR "Local update: $nodeid $p $origadj $compress $newadj\n";

              my $update;
              $update->{oid}  = $nodeid;
              $update->{odir} = $origadj;
              $update->{nid}  = $compress;
              $update->{ndir} = $newadj;

              push @{$nodes{$p}->{update}}, $update;
            }
            else
            {
              $remoteupdate++;
              print "$p\t$UPDATEMSG\t$nodeid\t$origadj\t$compress\t$newadj\n";
            }
          }
        }
      }
    }
  }

  print STDERR "update local: $localupdate remote: $remoteupdate mergestomake: $mergestomake\n";
  hadoop_counter("localupdate",  $localupdate);
  hadoop_counter("remoteupdate", $remoteupdate);
  hadoop_counter("mergestomake", $mergestomake);

  my $updatednodes = 0;

  while (($nodeid, $node) = each %nodes)
  {
    if (defined $node->{update})
    {
      $updatednodes++;
      foreach my $up (@{$node->{update}})
      {
        node_replacelink($nodeid, $node,
                         $up->{oid}, $up->{odir},
                         $up->{nid}, $up->{ndir});
      }
    }

    print_node($nodeid, $node);
  }

  print STDERR "updatednodes: $updatednodes\n";
  hadoop_counter("updatednodes", $updatednodes);

  undef %nodes;
  $nodecnt = 0;
}



while (<>)
{
  #print STDERR $_;

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
    updateGraph();
  }
}

updateGraph();
