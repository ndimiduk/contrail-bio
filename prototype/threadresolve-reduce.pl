#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

sub	resolveNode
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  if (exists $node->{resolve})
  {
    my %resolved;
    $node->{nodeid} = $nodeid;

    ## Remove the links to the split node, and add new link(s)
    foreach my $dir (keys %{$node->{resolve}})
    {
      foreach my $oid (keys %{$node->{resolve}->{$dir}})
      {
        #print STDERR "Removing $nodeid $oid $dir\n";
        #print STDERR Dumper($node);
        node_removelink($node, $oid, $dir);

        my $k = "$dir:$oid";
        $resolved{$k} = 1;

        foreach my $nid (@{$node->{resolve}->{$dir}->{$oid}})
        {
          #print STDERR "Adding $nodeid $dir $nid\n";
          push @{$node->{$dir}}, $nid;
        }
      }
    }

    ## Update the threading reads now that the link is resolved
    if (defined $node->{$THREAD})
    {
      my $threads = scalar @{$node->{$THREAD}};

      for (my $i = 0; $i < $threads; $i++)
      {
        my $thread = $node->{$THREAD}->[$i];
      
        my ($tdir, $tn, $read) = split /:/, $thread;

        if (exists $node->{resolve}->{$tdir} &&
            exists $node->{resolve}->{$tdir}->{$tn})
        {
          ## It is possible the old link was split into multiple
          ## We can only resolve unambiguous cases
          if (scalar @{$node->{resolve}->{$tdir}->{$tn}} == 1)
          {
            #print STDERR "Update thread: $thread";
            my $nid = $node->{resolve}->{$tdir}->{$tn}->[0];
            $thread = "$tdir:$nid:$read";
            #print STDERR "new: $thread\n";

            $node->{$THREAD}->[$i] = $thread;
          }
        }
      }

      node_cleanthreads($node);
    }

    ## Cleanup the threadible msgs
    if (defined $node->{$THREADIBLEMSG})
    {
      my @threadmsgs = @{$node->{$THREADIBLEMSG}};
      $node->{$THREADIBLEMSG} = undef;

      foreach my $k (@threadmsgs)
      {
        if (!exists $resolved{$k})
        {
          push @{$node->{$THREADIBLEMSG}}, $k;
        }
      }
    }
  }

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
    resolveNode($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $RESOLVETHREADMSG)
  {
    my $dir = shift @vals;
    my $oid = shift @vals;
    my $nid = shift @vals;

    push @{$node->{resolve}->{$dir}->{$oid}}, $nid;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

resolveNode($nodeid, $node);

