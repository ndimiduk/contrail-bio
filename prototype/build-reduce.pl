#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;


my $nodecnt = 0;

sub simplify_and_print
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  $nodecnt++;

  my $seq = dna2str($nodeid);
  my $rc  = rc($seq);

  node_setstr_raw($node, $nodeid);

  $seq = substr($seq, 1);
  $rc  = substr($rc,  1);

  foreach my $x (qw/f r/)
  {
    my $degree = 0;
    my @thread;

    foreach my $y (qw/f r/)
    {
      my $t = "$x$y";

      if (exists $node->{$t})
      {
        $degree += scalar keys %{$node->{$t}};
      }
    }

    foreach my $y (qw/f r/)
    {
      my $t = "$x$y";

      if (exists $node->{$t})
      {
        my @vs;

        foreach my $vc (keys %{$node->{$t}})
        {
          my $v = ($x eq $fwd) ? $seq : $rc;

          $v .= $vc;
          $v = rc($v) if ($y eq $rev);

          my $link = str2dna($v);

          push @vs, $link;

          if ($THREADREADS && ($degree > 1))
          {
            foreach my $r (@{$node->{$t}->{$vc}})
            {
              push @{$node->{$THREAD}}, "$t:$link:$r";
            }
          }
        }

        $node->{$t} = \@vs;
      }
    }
  }

  print_node($nodeid, $node);
}

my $node;
my $nodeid = undef;

while (<>)
{
  chomp;

  #my ($curnode, $type, $neighbor, $tag) = split /\t/, $_;
  my ($curnode, $type, $neighbor, $tag, $state) = split /\t/, $_;

  if ((defined $nodeid) && ($curnode ne $nodeid))
  {
    simplify_and_print($nodeid, $node);
    $node = undef;
  }

  $nodeid = $curnode;

  if ($THREADREADS)
  {
    if ((!defined $node->{$type}->{$neighbor}) ||
        (scalar @{$node->{$type}->{$neighbor}} < $MAXTHREADREADS))
    {
      push @{$node->{$type}->{$neighbor}}, $tag;
    }
  }
  else
  {
    $node->{$type}->{$neighbor} = 1;
  }

  if ((!defined $node->{$MERTAG}) || ($tag lt $node->{$MERTAG}->[0]))
  {
    $node->{$MERTAG}->[0] = $tag;
  }

  if ($state ne "i")
  {
    $node->{$COVERAGE}->[0]++;

    if ($state eq "5" || $state eq "6")
    {
      if ((!defined $node->{$R5}) ||
          (scalar @{$node->{$R5}} < $MAXR5))
      {
        if ($state eq "6")
        {
          my $pos = $K-1;
          push @{$node->{$R5}}, "~$tag:$pos";
        }
        else
        {
          push @{$node->{$R5}}, "$tag:0";
        }
      }
    }
  }
}

simplify_and_print($nodeid, $node);

hadoop_counter("nodecount", $nodecnt);
