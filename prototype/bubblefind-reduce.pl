#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $poppedbubbles = 0;
my $bubbleschecked = 0;

my $V = 1;


## edit distance modified from Text::Levenshtein CPAN module
sub fastdistance
{
  my $word1 = shift;
  my $word2 = shift;
  
  my @d;
  
  my $len1 = length $word1;
  my $len2 = length $word2;
  
  $d[0][0] = 0;
  for (1 .. $len1) 
  {
     $d[$_][0] = $_;
  }
  for (1 .. $len2) 
  {
    $d[0][$_] = $_;
  }
  
  for my $i (1 .. $len1) 
  {
    my $w1 = substr($word1,$i-1,1);
    for (1 .. $len2) 
    {
      $d[$i][$_] = _min3($d[$i-1][$_]+1, 
                         $d[$i][$_-1]+1, 
                         $d[$i-1][$_-1]+($w1 eq substr($word2,$_-1,1) ? 0 : 1));
    }
  }

  return $d[$len1][$len2];
}


sub	checkBubble
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  if (exists $node->{bubblelink})
  {
    my $popped = 0;

    foreach my $minor (keys %{$node->{bubblelink}})
    {
      ## Sort potential bubble strings in order of decreasing coverage
      my @interior = sort {$node->{bubblelink}->{$minor}->{$b}->{cov} <=>
                           $node->{bubblelink}->{$minor}->{$a}->{cov}}
                     keys %{$node->{bubblelink}->{$minor}};

      my $choices = scalar @interior;

      if ($choices > 1)
      {
        ## See if there are any pairwise compatible strings
        for (my $i = 0; $i < $choices; $i++)
        {
          my $u = $interior[$i];
          next if defined $node->{bubblelink}->{$minor}->{$u}->{popped};

          my $ustr = dna2str($node->{bubblelink}->{$minor}->{$u}->{str});
          my $ud   = $node->{bubblelink}->{$minor}->{$u}->{dir};
          my $umd  = $node->{bubblelink}->{$minor}->{$u}->{minord};

          for (my $j = $i+1; $j < $choices; $j++)
          {
            my $v = $interior[$j];
            next if defined $node->{bubblelink}->{$minor}->{$v}->{popped};

            my $vstr = dna2str($node->{bubblelink}->{$minor}->{$v}->{str});
            my $vd   = $node->{bubblelink}->{$minor}->{$v}->{dir};
            my $vmd  = $node->{bubblelink}->{$minor}->{$v}->{minord};
            my $vcov = $node->{bubblelink}->{$minor}->{$v}->{cov};

            if (($ud ne $vd) && ($ud ne flip_link($vd)))
            {
              $vstr = rc($vstr);
            }

            my $distance = fastdistance($ustr, $vstr);
            my $threshold = _max2(length($ustr), length($vstr)) * $BUBBLEEDITRATE;

            $bubbleschecked++;

            if ($V)
            {
              print STDERR "Bubble comparison:\n$u\t$ustr\n$v\t$vstr\n";
              print STDERR "edit distance: $distance threshold: $threshold\n";
            }

            if ($distance <= $threshold)
            {
              ## Found a bubble!

              my $vmerlen = length($vstr) - $K + 1;
              my $extracov = sprintf("%0.02f", $vcov * $vmerlen);
              push @{$node->{$POPBUBBLE}}, "$minor|$vmd|$v|$umd|$u|$extracov";
              $node->{bubblelink}->{$minor}->{$v}->{popped} = 1;

              if ($V)
              {
                print STDERR "POP $nodeid $u $v\n";
              }

              ## remove the link to the now dead node
              node_removelink($node, $v, $vd);
              $popped++;
              $poppedbubbles++;

              ## Update the threads
              if (exists $node->{$THREAD})
              {
                foreach my $thread (@{$node->{$THREAD}})
                {
                  my ($t,$link,$read) = split /:/, $thread;

                  if (($t eq $vmd) && ($v eq $link))
                  {
                    $thread = "$ud:$u:$link";
                  }
                }
              }
            }
          }
        }
      }
    }

    if ($popped)
    {
      node_cleanthreads($node);
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
    checkBubble($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $BUBBLELINKMSG)
  {
    my $dir    = shift @vals;
    my $id     = shift @vals;
    my $minord = shift @vals;
    my $minor  = shift @vals;
    my $str    = shift @vals;
    my $cov    = shift @vals;

    $node->{'bubblelink'}->{$minor}->{$id}->{str}    = $str;
    $node->{'bubblelink'}->{$minor}->{$id}->{dir}    = $dir;
    $node->{'bubblelink'}->{$minor}->{$id}->{minord} = $minord;
    $node->{'bubblelink'}->{$minor}->{$id}->{cov}    = $cov;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

checkBubble($nodeid, $node);

hadoop_counter("bubbles_checked", $bubbleschecked);
hadoop_counter("bubble_edges_popped", $poppedbubbles);

