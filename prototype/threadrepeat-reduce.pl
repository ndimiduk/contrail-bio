#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $xcut         = 0;
my $deadend      = 0;
my $halfdecision = 0;
my $invalidhalf  = 0;

sub	updateNode
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  if (exists $node->{update})
  {
    my %threads;
    my %edges;

    ## Index the edges that exist
    foreach my $t (qw/ff fr rf rr/)
    {
      if (exists $node->{$t})
      {
        foreach my $v (@{$node->{$t}})
        {
          $edges{$t}->{$v} = 1;
        }
      }
    }

    if (exists $node->{$THREAD})
    {
      ## Index the threading reads
      foreach my $thread (@{$node->{$THREAD}})
      {
        my ($t,$link,$read) = split /:/, $thread;

        if (exists $edges{$t}->{$link})
        {
          $threads{$t}->{$link}->{$read} = 1;
        }
      }

      delete $node->{$THREAD};
    }

    ## Index the new threads
    foreach my $tdir (keys %{$node->{update}})
    {
      foreach my $link (keys %{$node->{update}->{$tdir}})
      {
        die "Unknown edge $nodeid $tdir:$link", Dumper($node)
          if !exists $edges{$tdir}->{$link};

        foreach my $read (@{$node->{update}->{$tdir}->{$link}})
        {
          $threads{$tdir}->{$link}->{$read} = 1;
        }
      }
    }

    ## Record the threads
    foreach my $tdir (keys %threads)
    {
      foreach my $link (keys %{$threads{$tdir}})
      {
        foreach my $read (keys %{$threads{$tdir}->{$link}})
        {
          push @{$node->{$THREAD}}, "$tdir:$link:$read";
        }
      }
    }
  }

  ## Clean old threadible tags
  delete $node->{$THREADPATH};
  delete $node->{$THREADIBLEMSG};
  
  my %edges;

  my $tandem = 0;

  my %degree;
  $degree{f} = 0;
  $degree{r} = 0;

  ## Index the edges that exist
  foreach my $x (qw/f r/)
  {
    foreach my $y (qw/f r/)
    {
      my $t = "$x$y";
      if (exists $node->{$t})
      {
        foreach my $v (@{$node->{$t}})
        {
          $edges{$x}->{$t}->{$v} = 1;
          $degree{$x}++;

          if ($v eq $nodeid)
          {
            $tandem = 1;
          }
        }
      }
    }
  }

  my $fd = $degree{f};
  my $rd = $degree{r};

  if (($fd <= 1) && ($rd <= 1))
  {
     ## This is NOT a branching node, nothing to do
     delete $node->{$THREAD};
  }
  elsif ($tandem)
  {
    ## Don't attempt to thread through tandems

  }
  elsif (($fd == 0) || ($rd == 0))
  {
    ## Deadend node, could be a palidrome though
    $deadend++;
    #$node->{$THREADPATH}->[0] = "D";
  }
  elsif (0 && (($fd == 1) || ($rd == 1)))
  {
    ## Half decision node, split

    my %reads;

    foreach my $thread (@{$node->{$THREAD}})
    {
      my ($t,$link,$read) = split /:/, $thread;
      my $dir = substr($t, 0, 1);

      if (exists $edges{$dir}->{$t}->{$link})
      {
        push @{$reads{$read}}, "$t:$link";
      }
    }

    my $valid = 1;

    foreach my $read (keys %reads)
    {
      if ((scalar @{$reads{$read}} > 2) ||
         ((scalar @{$reads{$read}} == 2) &&
           (substr($reads{$read}->[0], 0, 1) eq 
            substr($reads{$read}->[1], 0, 1))))
      {
        $valid = 0;
      }
    }

    if (!$valid)
    {
      $invalidhalf++;
      print STDERR "WARNING $nodeid invalid-half: ", join(" ", @{$node->{$THREAD}}), "\n";
    }

    $halfdecision++;
    $node->{$THREADPATH}->[0] = "H";
  }
  else
  {
    ## I'm an X-node. See if there are read threads for all pairs

    if (($fd < 2) || ($rd < 2)) { $halfdecision++; }

    ## Index the threading reads
    my %reads;

    foreach my $thread (@{$node->{$THREAD}})
    {
      #print STDERR "$nodeid $thread\n";
      my ($t,$link,$read) = split /:/, $thread;
      my $dir = substr($t, 0, 1);

      if (exists $edges{$dir}->{$t}->{$link})
      {
        push @{$reads{$read}}, "$t:$link";
      }
      else
      {
        print STDERR "WARNING: thread $thread no longer valid\n";
      }
    }

    ## Index the pairs
    my %pairs;

    foreach my $read (keys %reads)
    {
      ## If there are more than 2, then there was a name collision
      ## Also make sure we have a f and r link

      if (scalar @{$reads{$read}} == 2)
      {
        my ($ta, $linka) = split /:/, $reads{$read}->[0];
        my ($tb, $linkb) = split /:/, $reads{$read}->[1];
        
        my $dira = substr($ta, 0, 1);
        my $dirb = substr($tb, 0, 1);

        ## Ignore non an f-r thread
        if ($dira ne $dirb)
        {
          $pairs{$ta}->{$linka}->{$tb}->{$linkb}->{cnt}++;
          $pairs{$tb}->{$linkb}->{$ta}->{$linka}->{cnt}++;

          push @{$pairs{$ta}->{$linka}->{$tb}->{$linkb}->{read}}, $read;
          push @{$pairs{$tb}->{$linkb}->{$ta}->{$linka}->{read}}, $read;
        }
      }
    }

    ## See if there is a thread from every edge to some other edge
    my $haveall = 1;

    OUTER:
    foreach my $xa (qw/f r/)
    {
      foreach my $ya (qw/f r/)
      {
        my $ta = "$xa$ya";

        if (exists $node->{$ta})
        {
          if (!exists $pairs{$ta})
          {
            $haveall = 0;
            last OUTER;
          }

          foreach my $va (@{$node->{$ta}})
          {
            if (!exists $pairs{$ta}->{$va})
            {
              $haveall = 0;
              last OUTER;
            }

            ## Make sure there is a link from $va to at least 1 other node
            my $xb = flip_dir($xa);

            my $links = 0;

            foreach my $yb (qw/f r/)
            {
              my $tb = "$xb$yb";

              if (exists $node->{$tb} &&
                  exists $pairs{$ta}->{$va}->{$tb})
              {
                foreach my $vb (keys %{$pairs{$ta}->{$va}->{$tb}})
                {
                  my $weight = $pairs{$ta}->{$va}->{$tb}->{$vb}->{cnt};

                  if ($weight >= $MINTHREADWEIGHT)
                  {
                    $links++;
                  }
                }
              }
            }

            if ($links == 0)
            {
              $haveall = 0;
              last OUTER;
            }
          }
        }
      }
    }

    if ($haveall == 1)
    {
      $xcut++;
      $node->{$THREADPATH}->[0] = "X";
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
    updateNode($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $UPDATEMSG)
  {
    my $tdir = shift @vals;
    my $link = shift @vals;
    push @{$node->{update}->{$tdir}->{$link}}, @vals;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

updateNode($nodeid, $node);

hadoop_counter("threadible",   $xcut+$halfdecision+$deadend);
hadoop_counter("xcut",         $xcut);
hadoop_counter("deadend",      $deadend);
hadoop_counter("halfdecision", $halfdecision);
hadoop_counter("invalidhalf",  $invalidhalf);
