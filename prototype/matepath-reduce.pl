#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $totaleval      = 0;
my $totalvalid     = 0;
my $totalabort     = 0;
my $totalambiguous = 0;

my $V = 1;

my $resolved_edges = 0;
my $resolved_bundles = 0;

my %graph;

sub find_path
{
  my $ctg1   = shift;
  my $ctg2   = shift;
  my $edist  = shift;
  my $et     = shift;
  my $weight = shift;
  my $wiggle = mate_insertstdev()/sqrt($weight);

  if ($wiggle < 30) { $wiggle = 30; }

  print STDERR "Searching from $ctg1 to $ctg2 $et $edist +/- $wiggle $weight\n";
  $totaleval++;

  my $valid = undef;
  my $validcnt = 0;

  my $toolong = 0;
  my $invalid = 0;
  my $deadend = 0;
  my $outside = 0;

  my $MAXDEADEND = 5000;

  my $abort = 0;

  my @S;

  ## Initialize state
  {
    my $s;
    $s->{node} = $ctg1;
    $s->{dir}  = substr($et, 0, 1);
    $s->{dist} = -$K + 1;
    $s->{depth} = 0;

    push @S, $s;
  }

  while (scalar @S > 0)
  {
    my $s = pop @S;

    my $n = $s->{node};
    my $d = $s->{dist};
    my $x = $s->{dir};

    if (($n eq $ctg2) && ($d >= $edist - $wiggle))
    {
      if ($x ne substr($et, 1, 1))
      {
        $invalid++;
      }
      elsif ($d > $edist + $wiggle)
      {
        $toolong++;
      }
      else
      {
        ## found a valid path!
        $valid = $s;
        $validcnt++;

        print STDERR "Valid path:";

        my $cur = $s;
        while (defined $cur)
        {
          my $id    = $cur->{node};
          my $dir   = $cur->{dir};
          my $dist  = $cur->{dist};
          my $depth = $cur->{depth};
          print STDERR " $depth:$id:$dir:$dist";
          $cur = $cur->{prev};
        }

        print STDERR "\n";

        if ($validcnt >= 5)
        {
          last;
        }
      }
    }
    elsif ($d < $edist + $wiggle)
    {
      if (exists $graph{$n})
      {
        ## keep looking
        my $l = node_len($graph{$n});

        foreach my $y (qw/f r/)
        {
          my $t = "$x$y";
          if (exists $graph{$n}->{$t})
          {
            my $yy = $y; #flip_dir($y);

            foreach my $v (@{$graph{$n}->{$t}})
            {
              my $vcov = node_cov($graph{$v});
              my $skip = 0;

              ## Can't visit a unique node more than once
              if ($vcov < $MAX_SCAFF_UNIQUE_COV)
              {
                my $p = $s;
                while (defined $p)
                {
                  if ($p->{node} eq $v)
                  {
                    $skip = 1;
                    last;
                  }

                  $p = $p->{prev};
                }
              }

              if (!$skip)
              {
                my $ss;
                $ss->{node} = $v;
                $ss->{dir} = $yy;
                $ss->{depth} = $s->{depth} + 1;

                if ($s->{depth} == 0)
                {
                  $ss->{dist} = $s->{dist};
                }
                else
                {
                  $ss->{dist} = $s->{dist} + $l - $K + 1;
                }

                $ss->{prev} = $s;

                push @S, $ss;
              }
            }
          }
        }
      }
      else
      {
        $outside++;
      }
    }
    else
    {
      $deadend++;

      if ($deadend >= $MAXDEADEND)
      {
        print STDERR "Too many deadends... aborting\n";
        $abort = 1;
        $totalabort++;
        last;
      }
    }
  }

  print STDERR "Found $validcnt valid paths, $abort abort, $invalid invalid, $toolong toolong, $deadend deadends, $outside outside\n";

  if ($validcnt > 1) { $totalambiguous++; }

  if (!$abort && $validcnt == 1)
  {
    $totalvalid++;

    ## reverse the list
    while (defined $valid->{prev})
    {
      $valid->{prev}->{next} = $valid;
      $valid = $valid->{prev};
    }

    my $s = $valid;

    while (defined $s)
    {
      my $depth = $s->{depth};
      my $id    = $s->{node};
      my $dir   = $s->{dir};
      my $dist  = $s->{dist};

      $s = $s->{next};

     # print STDERR "$depth $id $dir $dist\n";
    }

    return $valid;
  }

  return undef;
}

sub	threadMates
{
  my $nodes = scalar keys %graph;

  print STDERR "Threading graph with $nodes nodes\n";

  foreach my $nodeid (sort keys %graph)
  {
    my $node = $graph{$nodeid};
    my $isunique = node_isunique($node);

    if (defined $node->{$BUNDLE} && $isunique)
    {
      my %bundles;

      ## Organize the bundles
      foreach my $bstr (@{$node->{$BUNDLE}})
      {
        my ($ctg,$et,$dist,$weight) = split /:/, $bstr;

        my $b;
        $b->{ctg}    = $ctg;
        $b->{et}     = $et;
        $b->{dist}   = $dist;
        $b->{weight} = $weight;

        my $dir = substr($et, 0, 1);

        push @{$bundles{$dir}}, $b;
      }

      ## Examine F and R bundles separately
      foreach my $ut (keys %bundles)
      {
        my @bundles = sort {$b->{weight} <=> $a->{weight}} @{$bundles{$ut}};
        my $bundlecnt = scalar @bundles;

        if ($V)
        {
          my $deg = node_degree($node, $ut);
          my $cov = node_cov($node);
          my $len = node_len($node);

          print STDERR ">> examining $nodeid $ut bun=$bundlecnt deg=$deg cov=$cov len=$len\n";

          for (my $eidx = 0; $eidx < $bundlecnt; $eidx++)
          {
            my $b = $bundles[$eidx];
            my $et    = $b->{et};
            my $ctg2  = $b->{ctg};
            my $edist = $b->{dist};
            my $w     = $b->{weight};

            my $cov2  = node_cov($graph{$ctg2});

            print STDERR "++ $eidx $et $ctg2 d=$edist w=$w cov2=$cov2\n"; 
          }
        }

        ## See if there is a sequence compatible with the bundle distances
        for (my $eidx = 0; $eidx < $bundlecnt; $eidx++)
        {
          my $bundle = $bundles[$eidx];

          my $ctg1  = $nodeid;
          my $ctg2  = $bundle->{ctg};
          my $edist = $bundle->{dist};
          my $et    = $bundle->{et};
          my $w     = $bundle->{weight};

          my $cov2  = node_cov($graph{$ctg2});

          print STDERR "== $eidx $et $ctg2 d=$edist w=$w cov2=$cov2\n"; 

          ## Don't try to thread to repeat nodes
          if ($cov2 > $MAX_SCAFF_UNIQUE_COV)
          {
            print STDERR "== repeat bundle\n";
            next;
          }

          ## Don't thread mates to junk
          if ($cov2 < $MIN_SCAFF_UNIQUE_COV)
          {
            print STDERR "== junk bundle\n";
            next;
          }

          ## Only consider strong bundles
          if ($w < $MIN_SCAFF_MATE_WEIGHT)
          {
            print STDERR "== thin bundle\n";
            next;
          }

          my $path = find_path($ctg1, $ctg2, $edist, $et, $w);

          if (defined $path)
          {
            $resolved_bundles++;

            my $pd  = $path->{dir};
            my $cur = $path->{next};

            while (defined $cur)
            {
              my $cn = $cur->{node};
              my $cd = $cur->{dir};

              print STDERR "  $cn $pd$cd\n";

              my $tstr = "$ut:$pd$cd:$cn";
              push @{$node->{$MATETHREAD}}, $tstr;

              $pd = $cd;
              $cur = $cur->{next};

              $resolved_edges++;
            }

            last;
          }
        }
      }
    }

    print_node($nodeid, $node);
  }
}


while (<>)
{
  #print "==> $_";
  chomp;
  my @vals = split /\t/, $_;

  my $nodeid = shift @vals;
  my $msgtype = shift @vals;

  if ($msgtype ne $NODEMSG)
  {
    die "Unknown msg: $_\n";
  }

  my $node = {};

  parse_node($node, \@vals);

  $graph{$nodeid} = $node;
}

threadMates();


hadoop_counter("resolved_edges",   $resolved_edges);
hadoop_counter("resolved_bundles", $resolved_bundles);

hadoop_counter("total_eval",       $totaleval);
hadoop_counter("total_valid",      $totalvalid);
hadoop_counter("total_abort",      $totalabort);
hadoop_counter("total_ambiguous",  $totalambiguous);
