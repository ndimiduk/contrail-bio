#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $V = 0;

my %nodes;

my $tchains   = 0;
my $tcchains  = 0;
my $ttotallen = 0;
my $tsaved    = 0;

my $chains   = 0;
my $cchains  = 0;
my $totallen = 0;
my $saved    = 0;

my $needcompress = 0;


sub find_tails
{
  my $startid = shift;

  my $ftail = $startid;
  my $fdir  = $rev;
  my $fdist = 0;

  my $rtail = $startid;
  my $rdir  = $fwd;
  my $rdist = 0;

  my %seen;
  $seen{$startid} = 1;

  foreach my $startdir (qw/r f/)
  {
    my $curnode = $nodes{$startid};

    my $curid = $startid;
    my $curdir = $startdir;

    my $canCompress;
    my $dist = 0;

    do
    {
      $canCompress = 0;

      my ($nextid, $nextdir) = node_gettail($curnode, $curdir);

      if ((defined $nextid) && 
          (exists $nodes{$nextid}) &&
          (!exists $seen{$nextid}))
      {
        $seen{$nextid} = 1;

        $curnode = $nodes{$nextid};
        my ($nt, $nd) = node_gettail($curnode, flip_dir($nextdir));

        #print STDERR "$curid ($curdir) -> $nextid ($nextdir): "; 
        
        if ((defined $nt) && ($nt eq $curid))
        {
          $dist++;
          $canCompress = 1;

          $curid = $nextid;
          $curdir = $nextdir;
        }
      }
    } while ($canCompress);

    if ($startdir eq "r")
    {
      $rtail = $curid;
      $rdir  = flip_dir($curdir);
      $rdist = $dist;
    }
    else
    {
      $ftail = $curid;
      $fdir  = flip_dir($curdir);
      $fdist = $dist;
    }
  }

  return ($rtail, $rdir, $rdist,
          $ftail, $fdir, $fdist);
}


sub localmerge
{
  my $nodeid;
  my $node;

  if ($V) { select STDERR; }

  while (($nodeid, $node) = each %nodes)
  {
    next if exists $node->{DONE};

    my ($rtail, $rdir, $rdist, 
        $ftail, $fdir, $fdist) = find_tails($nodeid);

    $node->{DONE} = 1;

    $nodes{$rtail}->{DONE} = 1;
    $nodes{$ftail}->{DONE} = 1;

    my $chainlen = 1 + $fdist + $rdist;

    $chains++;
    $tchains++;

    $totallen += $chainlen;
    $ttotallen += $chainlen;

    if ($V) { print STDERR "$nodeid $chainlen $rtail $rdir $rdist $ftail $fdir $fdist"; }

    my $domerge = 0;

    if ($chainlen > 1)
    {
      my $rtnode = $nodes{$rtail};

      my $allinmemory = 1;
      foreach my $t (qw/ff fr rf rr/)
      {
        if (defined $rtnode->{$t})
        {
          foreach my $v (@{$rtnode->{$t}})
          {
            if (!exists $nodes{$v})
            {
              $allinmemory = 0;
              last;
            }
          }
        }
      }

      if ($allinmemory)     { $domerge = 2; }
      elsif ($chainlen > 2) { $domerge = 1; }
    }

    if ($V) { print STDERR " domerge=$domerge\n"; }

    ## merge the internal nodes of the chain together in memory
    ## If domerge == 1 result will be rtail -> ftail
    ## If domerge == 2 result will be X* -> rtail...ftail

    if ($domerge)
    {
      $chainlen--; ## Replace the chain with 1 ftail
      if ($domerge == 1) { $chainlen--; } ## Need rtail too

      ## start at the rtail, and merge until the ftail
      my $rtnode = $nodes{$rtail};
      my $ftnode = $nodes{$ftail};

      if ($V) 
      { 
        select STDERR;
        
        print "[==\n"; 
        print_node($rtail, $rtnode);
      }

      ## mergedir is the direction to merge relative to rtail
      my $mergedir = $rdir;
      my ($first, $firstdir) = node_gettail($rtnode, $mergedir);
      my ($nt, $nd);

      if (!defined $first || 
          !exists $nodes{$first} ||
          !(($nt,$nd) = node_gettail($nodes{$first}, flip_dir($firstdir))) ||
          !(defined $nt) ||
          !($nt eq $rtail))
      {
        die;
      }

      my $mstr = node_str($rtnode);
      if ($mergedir eq $rev) 
      { 
        $mstr = rc($mstr); 
        node_revreads($rtnode);
      }

      my ($cur, $curdir) = ($first, $firstdir);

      my ($lastid, $lastdir);

      my $mergelen = 0;

      my $curnode;

      my $merlen = length($mstr) - $K + 1;
      my $covsum = ($rtnode->{$COVERAGE}->[0] * $merlen);
      my $covlen = $merlen;

      my $shift = $merlen;

      while ($cur ne $ftail)
      {
        $curnode = $nodes{$cur};
        die if !defined $curnode;

        if ($V) { print_node($cur, $curnode); }

        ## curnode can be deleted
        $curnode->{DONE} = 2;
        $mergelen++;

        my $bstr = node_str($curnode);
        if ($curdir eq $rev) 
        { 
          $bstr = rc($bstr); 
          node_revreads($curnode);
        }

        $mstr = str_concat($mstr, $bstr);

        $merlen = length($bstr) - $K + 1;
        $covsum += ($curnode->{$COVERAGE}->[0] * $merlen);
        $covlen += $merlen;

        node_addreads($rtnode, $curnode, $shift);
        $shift += $merlen;

        ($lastid, $lastdir) = ($cur, $curdir);
        ($cur, $curdir) = node_gettail($curnode, $lastdir);
      }

      if ($V) { print_node($ftail, $ftnode); }
      if ($V) { print "==\n"; }

      ## If we made it all the way to the ftail, 
      ## see if we should do the final merge
      if (($domerge == 2) && 
          ($cur eq $ftail) && 
          ($mergelen == ($chainlen-1)))
      {
        $mergelen++;
        $rtnode->{DONE} = 2;

        my $bstr = node_str($ftnode);
        if ($curdir eq $rev) 
        { 
          $bstr = rc($bstr); 
          node_revreads($ftnode);
        }

        $mstr = str_concat($mstr, $bstr);

        $merlen = length($bstr) - $K + 1;
        $covsum += ($ftnode->{$COVERAGE}->[0] * $merlen);
        $covlen += $merlen;

        node_addreads($rtnode, $ftnode, $shift);

        ## we want the same orientation for ftail as before
        if ($curdir eq $rev) { $mstr = rc($mstr); }
        node_setstr($ftnode, $mstr);

        ## Copy reads over
        $ftnode->{$R5} = $rtnode->{$R5};
        if ($curdir eq $rev) { node_revreads($ftnode); }

        $ftnode->{$COVERAGE}->[0] = $covsum / $covlen;

        ## Update ftail's new neigbors to be rtail's old neighbors
        ## Update the rtail neighbors to point at ftail
        ## Update the can compress flags
        ## Update threads

        ## Clear the old links from ftnode in the direction of the chain
        foreach my $adj (qw/f r/)
        {
          delete $ftnode->{$fdir.$adj};
        }

        ## Now move the links from rtnode to ftnode
        foreach my $adj (qw/f r/)
        {
          my $origdir = flip_dir($rdir) . $adj;
          my $newdir  = $fdir . $adj;

          if (exists $rtnode->{$origdir})
          {
            foreach my $v (@{$rtnode->{$origdir}})
            {
              if ($v eq $rtail)
              {
                ## Cycle on rtail

                if ($V) { print STDERR "Fixing rtail cycle\n"; }

                my $cycled = $fdir;

                if ($rdir eq $adj) { $cycled .= flip_dir($fdir) }
                else               { $cycled .= $fdir; }

                push @{$ftnode->{$cycled}}, $ftail;
              }
              else
              {
                push @{$ftnode->{$newdir}}, $v;

                die if (!exists $nodes{$v});

                node_replacelink($v, $nodes{$v},
                                 $rtail, flip_link($origdir),
                                 $ftail, flip_link($newdir));
              }
            }
          }
        }


        ## Now move the can compresflag from rtnode into ftnode
        delete $ftnode->{"$CANCOMPRESS$fdir"};

        if (exists $rtnode->{$CANCOMPRESS . flip_dir($rdir)})
        {
          $ftnode->{"$CANCOMPRESS$fdir"}->[0] = 
            $rtnode->{$CANCOMPRESS . flip_dir($mergedir)}->[0];
        }

        ## Break cycles
        foreach my $dir (qw/f r/)
        {
          my ($nextid, $nextdir) = node_gettail($ftnode, $dir);

          if (defined $nextid && ($nextid eq $ftail))
          {
            if ($V) { print STDERR "Breaking tail $ftail\n"; }
            $ftnode->{"$CANCOMPRESS$fwd"}->[0] = 0;
            $ftnode->{"$CANCOMPRESS$rev"}->[0] = 0;
          }
        }

        ## Confirm there are no threads in $ftnode in $fdir
        if (defined $ftnode->{$THREAD})
        {
          my @oldthreads = @{$ftnode->{$THREAD}};
          delete $ftnode->{$THREAD};

          foreach my $thread (@oldthreads)
          {
            my ($t,$link,$read) = split /:/, $thread;
            #die $thread if substr($t,0,1) eq $fdir;

            if (substr($t,0,1) ne $fdir)
            {
              push @{$ftnode->{$THREAD}}, $thread;
            }
          }
        }

        ## Now copy over rtnodes threads in !$rdir
        if (defined $rtnode->{$THREAD})
        {
          foreach my $thread (@{$rtnode->{$THREAD}})
          {
            my ($t,$link,$read) = split /:/, $thread;
            #die $thread if substr($t,0,1) eq $rdir;
            if (substr($t,0,1) ne $rdir)
            {
              substr($t,0,1) = $fdir;
              push @{$ftnode->{$THREAD}}, "$t:$link:$read";
            }
          }
        }

        if ($V) { print_node($ftail, $ftnode); }
        if ($V) { print STDERR "==]\n"; }
      }
      else
      {
        if ($mergelen < $chainlen)
        {
            print STDERR "Hit an unexpected cycle mergelen: $mergelen chainlen: $chainlen\n";
            print STDERR Dumper ($rtnode);
            print STDERR Dumper ($ftnode);
            die;
        }

        if ($mergedir eq $rev) { $mstr = rc($mstr); }
        node_setstr($rtnode, $mstr);

        if ($mergedir eq $rev) { node_revreads($rtnode); }

        $rtnode->{$COVERAGE}->[0] = $covsum / $covlen;
        
        my $mergeftaildir = $lastdir;
        if ($lastdir ne $mergedir) { $mergeftaildir = flip_dir($mergeftaildir); }

        ## update rtail->first with rtail->ftail link
        node_replacelink($rtail, $rtnode,
                         $first, "$mergedir$firstdir",
                         $ftail, "$mergeftaildir$curdir");

        ## update ftail->last with ftail->rtail link
        node_replacelink($ftail, $ftnode,
                         $lastid, flip_link("$lastdir$curdir"),
                         $rtail,  flip_link("$mergeftaildir$curdir"));

        if ($THREAD)
        {
          if (defined $curnode->{$THREAD})
          {
            print STDERR "ERROR: curnode has threads\n";
            print STDERR Dumper($curnode);
            die;
          }
        }

        if ($V) { print_node($rtail, $rtnode); }
        if ($V) { print STDERR "==]\n"; }
      }

      $saved  += $mergelen;
      $tsaved += $mergelen;

      $cchains++;
      $tcchains++;
    }
  }

  if ($V) { select STDOUT; }

  if ($chains >= 100000)
  {
    print STDERR "chains: $chains cchains:$cchains totallen: $totallen saved: $saved\n";

    hadoop_counter("chains",        $chains);
    hadoop_counter("cchains",       $cchains);
    hadoop_counter("totalchainlen", $totallen);
    hadoop_counter("saved",         $saved);

    $chains   = 0;
    $cchains  = 0;
    $totallen = 0;
    $saved    = 0;
  }

  while (($nodeid, $node) = each %nodes)
  {
    next if ((exists $node->{DONE}) && ($node->{DONE} > 1));
    print_node($nodeid, $node);

    if ((exists $node->{"$CANCOMPRESS$fwd"} && $node->{"$CANCOMPRESS$fwd"}->[0]) ||
        (exists $node->{"$CANCOMPRESS$rev"} && $node->{"$CANCOMPRESS$rev"}->[0]))
    {
      $needcompress++;
    }
  }

  undef %nodes;
}


my $lasttag = "";

my $nodecnt = 0;

while (<>)
{
  if ($V) { print STDERR "==> $_"; }
  chomp;

  my @vals = split /\t/, $_;

  my $tag     = shift @vals;
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

  if ($tag ne $lasttag)
  {
    localmerge();
  }

  $nodes{$nodeid} = $node;

  $lasttag = $tag;
}

localmerge();


hadoop_counter("chains",        $chains);
hadoop_counter("cchains",       $cchains);
hadoop_counter("totalchainlen", $totallen);
hadoop_counter("saved",         $saved);
hadoop_counter("needcompress",  $needcompress);
