#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $allneedsplit = 0;
my $resolved = 0;    
my $totalthreadedbp = 0;
my $uniquethreadedbp = 0;

my $V = 0;

my %nodes;

sub resolveThreads
{
  my $nodeid = shift;
  my $node = shift;
 
  my $print_node = 1;

  if (defined $node->{$THREADPATH})
  {
    $allneedsplit++;

    print STDERR "Selecting master for $nodeid ", join(" ", @{$node->{$THREADPATH}}), "\n";

    my $masterid = $nodeid;

    if (defined $node->{$THREADIBLEMSG})
    {
      print STDERR " threaded neighbors: ", join(" ", @{$node->{$THREADIBLEMSG}}), "\n";

      foreach my $port (@{$node->{$THREADIBLEMSG}})
      {
        my ($t, $v) = split /:/, $port;

        if ($v le $masterid)
        {
          $masterid = $v;
        }
      }
    }

    if ($masterid ne $nodeid)
    {
      print STDERR "Skipping $nodeid, waiting for $masterid\n";
    }
    else
    {
      ## I'm the master of my local neighborhood
      print STDERR "Resolving $nodeid\n";

      $resolved++;

      $print_node = 0;

      my $tandem = 0;

      my %degree;
      $degree{f} = 0;
      $degree{r} = 0;

      my %pairs;

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
              $degree{$x}++;

              if ($v eq $nodeid) { $tandem = 1; }
            }
          }
        }
      }

      my $fd = $degree{f};
      my $rd = $degree{r};

      if (($fd <= 1) && ($rd <= 1))
      {
        ## I'm not a branching node
        print STDERR "WARNING: supposed to split a non-branching node $_\n";

        delete $node->{$THREADPATH};

        print_node($nodeid, $node);

        foreach my $et (qw/ff fr rf rr/)
        {
          if (exists $node->{$et})
          {
            my $ret = flip_link($et);

            foreach my $v (@{$node->{$et}})
            {
              print "$v\t$RESOLVETHREADMSG\t$ret\t$nodeid\t$nodeid\n";
            }
          }
        }

        return;
      }

      if ($tandem)
      {
        print STDERR "WARNING: splitting a tandem\n";
      }

      if ((scalar @{$node->{$THREADPATH}} > 1) ||
          (substr($node->{$THREADPATH}->[0],0,1) eq "P") ||
          (substr($node->{$THREADPATH}->[0],0,1) eq "T"))
      {
        ## Mates were threaded through this node
        foreach my $path (@{$node->{$THREADPATH}})
        {
          my @fields = split /-/, $path;

          my $t = shift @fields;
          my $msg = pop @fields;

          if (($t eq "T") || ($t eq "P"))
          {
            if ($fields[-1] le $fields[0])
            {
              @fields = reverse @fields;

              for (my $i = 1; $i < scalar @fields - 1; $i++)
              {
                $fields[$i] = flip_link($fields[$i]);
              }
            }

            my $l = join("-", @fields);
            push @{$pairs{$l}}, "*$t$msg";

            print STDERR "$l *$t$msg\n";
          }
          else
          {
            die "Unknown path type: $path\n";
          }
        }
      }
      else
      {
        ## X-cut or half decision node

        if (($fd <= 1) || ($rd <= 1))
        {
          ## I'm a half-decision, do full split
          my $unique = undef;

          if    ($fd == 1) { $unique = "f"; }
          elsif ($rd == 1) { $unique = "r"; }

          if (defined $unique)
          {
            my ($uniqueid, $uniquedir) = node_gettail($node, $unique);
            die Dumper($node) if !defined $uniqueid;

            my $uniquelink = "$unique$uniquedir";

            my $nonunique = flip_dir($unique);
            foreach my $x (qw/f r/)
            {
              my $t = "$nonunique$x";

              if (exists $node->{$t})
              {
                foreach my $v (@{$node->{$t}})
                {
                  if ($uniquelink le $t)
                  {
                    # f-r
                    my $l = "$uniquelink:$uniqueid-$t:$v";
                    push @{$pairs{$l}}, "*half";
                  }
                  else
                  {
                    # r-f
                    my $l = "$t:$v-$uniquelink:$uniqueid";
                    push @{$pairs{$l}}, "*half";
                  }
                }
              }
            }
          }
          else
          {
            ## I'm a deadend

          }
        }

        ## If I'm an X-cut or a half decision, keep track of the spanning reads
        my %reads;

        ## Index the threading reads
        foreach my $thread (@{$node->{$THREAD}})
        {
          my ($t,$link,$read) = split /:/, $thread;
          $reads{$read}->{"$t:$link"}++;
        }

        ## Index the pairs
        foreach my $read (keys %reads)
        {
          if (scalar keys %{$reads{$read}} == 2)
          {
            my @links = sort keys %{$reads{$read}};

            if ((substr($links[0],0,1) eq "f") &&
                (substr($links[1],0,1) eq "r"))
            {
              my $l = join("-", @links);
              push @{$pairs{$l}}, $read;
            }
          }
        }
      }

      my %portstatus;

      my $str = node_str($node);

      my $threadedbp = length($str) - $K + 1;
      $uniquethreadedbp += $threadedbp;

      my $copies = scalar keys %pairs;

      ## Now unzip for reads that span the node
      my $copy = 0;
      foreach my $pt (keys %pairs)
      {
        if ($pairs{$pt}->[0] =~ /^\*T/)
        {
          my @path = split /-/, $pt;

          ##   0  -  1  -  2  -      3     -  4
          ##  Foo - me0 - me1 - (implicit) - Bar

          ##  selfcopies = 5 - 2
          ##  first copy = 0
          ##  mid copy   = 1
          ##  last  copy = 2

          my $selfcopies = scalar @path - 2 + 1;

          print STDERR "Resolving tandem $nodeid into $selfcopies copies $pt : ", join(",", @{$pairs{$pt}}), "\n";

          for (my $i = 0; $i < $selfcopies; $i++)
          {
            $copy++;
            $totalthreadedbp += $threadedbp;

            my $p = $copy - 1;
            my $n = $copy + 1;

            my $previd = "${nodeid}_${p}";
            my $nextid = "${nodeid}_${n}";
            my $newnodeid = "${nodeid}_${copy}";

            my ($aport, $bport);

            if ($i == 0)
            {
              $aport = $path[0];
              $portstatus{$aport} = 1;
            }
            else
            {
              $aport = flip_link($path[$i]);
              $aport .= ":$previd";

              $portstatus{flip_link($path[$i]).":$nodeid"} = 1;
            }

            if ($i == $selfcopies - 1)
            {
              $bport = $path[$i+1];
              $portstatus{$bport} = 1;
            }
            else
            {
              $bport = $path[$i+1];
              $bport .= ":$nextid";
              $portstatus{$path[$i+1].":$nodeid"} = 1;
            }

            my ($at, $alink) = split /:/, $aport;
            my ($bt, $blink) = split /:/, $bport;

            my $newnode;
            $newnode->{$at}->[0] = $alink;
            $newnode->{$bt}->[0] = $blink;

            node_setstr($newnode, $str);

            ## TODO: Fix coverage
            $newnode->{$COVERAGE}->[0] = 1;

            print_node($newnodeid, $newnode);

            print STDERR "  $alink $at $newnodeid $bt $blink\n";

            if ($i == 0)
            {
              $at = flip_link($at);
              print "$alink\t$RESOLVETHREADMSG\t$at\t$nodeid\t$newnodeid\n";
            }

            if ($i == $selfcopies - 1)
            {
              $bt = flip_link($bt);
              print "$blink\t$RESOLVETHREADMSG\t$bt\t$nodeid\t$newnodeid\n";
            }
          }
        }
        else
        {
          ## simple non tandem
          
          ## Foo - (implicit copy) - Bar

          $copy++;
          $totalthreadedbp += $threadedbp;

          my $newnodeid = "${nodeid}_${copy}";

          print STDERR "$newnodeid $pt : ", join(",", @{$pairs{$pt}}), "\n";

          my ($aport, $bport) = split /-/, $pt;

          my ($at, $alink) = split /:/, $aport;
          my ($bt, $blink) = split /:/, $bport;

          my $newnode;
          $newnode->{$at}->[0] = $alink;
          $newnode->{$bt}->[0] = $blink;

          node_setstr($newnode, $str);

          $newnode->{$COVERAGE}->[0] = $node->{$COVERAGE}->[0] / $copies;

          foreach my $tread (@{$pairs{$pt}})
          {
            next if ($tread =~ /^\*/);

            push @{$newnode->{$THREAD}}, "$at:$alink:$tread";
            push @{$newnode->{$THREAD}}, "$bt:$blink:$tread";
          }

          print_node($newnodeid, $newnode);

          $at = flip_link($at);
          $bt = flip_link($bt);

          print "$alink\t$RESOLVETHREADMSG\t$at\t$nodeid\t$newnodeid\n";
          print "$blink\t$RESOLVETHREADMSG\t$bt\t$nodeid\t$newnodeid\n";

          $portstatus{$aport} = 1;
          $portstatus{$bport} = 1;
        }
      }

      ## Check for dangling (non-spanned) ports
      ## copy this node, but separate from graph
      foreach my $t (qw/ff fr rf rr/)
      {
        if (defined $node->{$t})
        {
          foreach my $nn (@{$node->{$t}})
          {
            if (!exists $portstatus{"$t:$nn"})
            {
              if ($nn eq $nodeid)
              {
                print STDERR " WARNING: tandem repeat is not fully resolved\n";
                next;
              }

              $copy++;
              $totalthreadedbp += $threadedbp;

              my $newnodeid = "${nodeid}_${copy}";

              print STDERR "  $newnodeid half-split - $t:$nn\n";

              my $newnode;
              $newnode->{$t}->[0] = $nn;

              node_setstr($newnode, $str);

              $newnode->{$COVERAGE}->[0] = 1;

              print_node($newnodeid, $newnode);

              my $dir = flip_link($t);
              print "$nn\t$RESOLVETHREADMSG\t$dir\t$nodeid\t$newnodeid\n";
            }
          }
        }
      }
    }
  }

  if ($print_node)
  {
    print_node($nodeid, $node);
  }
}



while (<>)
{
  chomp;

  my @vals = split /\t/, $_;

  my $nodeid  = shift @vals;
  my $msgtype = shift @vals;

  my $node = {};

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  else
  {
    die "Unknown msg: $_\n";
  }

  resolveThreads($nodeid, $node);
}

hadoop_counter("allneedsplit", $allneedsplit);
hadoop_counter("resolved", $resolved);
hadoop_counter("needsplit", $allneedsplit - $resolved);
hadoop_counter("uniquethreadedbp", $uniquethreadedbp);
hadoop_counter("totalthreadedbp", $totalthreadedbp);
