#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $needstocompress = 0;

my $V = 0;

sub	compressNodes
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  if (exists $node->{compress})
  {
    foreach my $badj (qw/f r/)
    {
      next if (!exists $node->{compress}->{$badj});

      my $cid   = $node->{compress}->{$badj}->{id};
      my $cdir  = $node->{compress}->{$badj}->{dir};
      my $cbdir = $node->{compress}->{$badj}->{bdir};
      my $cnode = $node->{compress}->{$badj}->{node};

      if ($V)
      {
        select STDERR;

        print "[==\n";
        print "Merging $nodeid $cid $cdir $cbdir\n";

        print_node($nodeid, $node);
        print_node($cid, $cnode);
      }

      ## update the node string

      my $astr = node_str($cnode);
      if ($cdir eq "r") 
      { 
        $astr = rc($astr); 
        node_revreads($cnode);
      }

      my $bstr = node_str($node);
      if ($cbdir eq "r") 
      { 
        $bstr = rc($bstr); 
        node_revreads($node);
      }

      my $shift = length($astr) - $K + 1;
      node_addreads($cnode, $node, $shift);
      $node->{$R5} = $cnode->{$R5};

      my $str = str_concat($astr, $bstr);
      if ($cbdir eq "r") { $str = rc($str); }

      node_setstr($node, $str);

      if ($cbdir eq "r")
      {
        node_revreads($node);
      }

      my $amerlen = length($astr) - $K + 1;
      my $bmerlen = length($bstr) - $K + 1;

      $node->{$COVERAGE}->[0] = (($node->{$COVERAGE}->[0] * $amerlen) + 
                                 ($cnode->{$COVERAGE}->[0] * $bmerlen)) 
                                / ($amerlen + $bmerlen);
      if ($THREADREADS)
      {
        if (defined $cnode->{$THREAD})
        {
          if ($cbdir ne $cdir)
          {
            ## Flip the direction of the threads
            foreach my $thread (@{$cnode->{$THREAD}})
            {
              my ($t,$link,$r) = split /:/, $thread;

              my ($ta, $tb) = split //, $t;
              $ta = flip_dir($ta);

              push @{$node->{$THREAD}}, "$ta$tb:$link:$r";
            }
          }
          else
          {
            foreach my $thread (@{$cnode->{$THREAD}})
            {
              push @{$node->{$THREAD}}, $thread;
            }
          }
        }
      }

      ## update the appropriate neighbors with $cnode's pointers

      #print " orig: $cdir $cbdir\n";

      $cdir = flip_dir($cdir);
      $cbdir = flip_dir($cbdir);
     
      foreach my $adj (qw/f r/)
      {
        my $key = "$cdir$adj";
        my $fkey = "$cbdir$adj";

        #print "  Updating my $fkey with cnode $key\n";

        if (exists $cnode->{$key})
        {
          $node->{$fkey} = $cnode->{$key};
        }
        else
        {
          delete $node->{$fkey};
        }
      }

      ## Now update the can compress flag
      my $ccdir = $cnode->{"$CANCOMPRESS$cdir"}->[0];
      $node->{"${CANCOMPRESS}$cbdir"}->[0] = $ccdir;

      if ($V) { print_node($nodeid, $node); }
    }  

    node_cleanthreads($node);
  }

  ## Update the tail pointers, and cancompress flag
  foreach my $adj (qw/f r/)
  {
    my ($tail, $taildir) = node_gettail($node, $adj);

    ## check for a cycle, and break link
    if (!defined $tail || $tail eq $nodeid)
    {
      $node->{"${CANCOMPRESS}$adj"}->[0] = 0;
    }
  }

  if (($node->{"$CANCOMPRESS$fwd"}->[0]) ||
      ($node->{"$CANCOMPRESS$rev"}->[0]))
  {
    $needstocompress++;
  }

  if ($V) { select STDOUT; }

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
    compressNodes($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $COMPRESSPAIR)
  {
    my $xd = shift @vals;
    my $bd = shift @vals;

    my $x  = shift @vals;
    shift @vals; ## nodemsg

    my $nn;
    $nn->{nodeid} = $x;

    parse_node($nn, \@vals);

    if (exists $node->{compress}->{$bd})
    {
      my $other = $node->{compress}->{$bd}->{id};
      die "Multiple compress $bd messages sent to $nodeid: $x $other\n";
    }

    $node->{compress}->{$bd}->{id}   = $x;
    $node->{compress}->{$bd}->{dir}  = $xd;
    $node->{compress}->{$bd}->{bdir} = $bd;
    $node->{compress}->{$bd}->{node} = $nn;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

compressNodes($nodeid, $node);


hadoop_counter("needcompress", $needstocompress);

