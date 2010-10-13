#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $V = 1;

my $foundshort   = 0;
my $foundlong    = 0;
my $foundinvalid = 0;
my $foundvalid   = 0;

my $active       = 0;
my $toolong      = 0;


my $wiggle = mate_insertstdev()/sqrt(10); ## todo: sqrt(weight)
if ($wiggle < 30) { $wiggle = 30; }

sub	processHop
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  if (exists $node->{$BUNDLE})
  {
    foreach my $b (@{$node->{$BUNDLE}})
    {
      if ($b =~ /^#/)
      {
        ## There is a valid path saved away
        $foundvalid++;
      }
    }
  }

  if (exists $node->{hop})
  {
    foreach my $h (@{$node->{hop}})
    {
      if ($V)
      {
        print STDERR "Checking exp: ",
                     $h->{dest},    " ",
                     $h->{expdist},
                     $h->{expdir},  " | cur: ",
                     $h->{curdist},
                     $h->{curdir},  " ",
                     $h->{path}, "\n";
      }

      if ($h->{dest} eq $nodeid)
      { 
        if ($h->{curdist} < $h->{expdist} - $wiggle)
        {
          if ($V) { print STDERR "Found too short\n"; }
          $foundshort++;
          next;
        }

        if ($h->{curdist} > $h->{expdist} + $wiggle)
        {
          if ($V) { print STDERR "Found too long\n"; }
          $foundlong++;
          next;
        }

        if ($h->{curdir} ne $h->{expdir})
        {
          if ($V) { print STDERR "Found invalid\n"; }
          $foundinvalid++;
          next;
        }

        ## Success!
        $foundvalid++;

        my $p = $h->{path};
        my $pp = "#$p:$nodeid";
        push @{$node->{$BUNDLE}}, $pp;

        if ($V) { print STDERR "Found Valid path:$pp\n"; }
      }

      if ($h->{curdist} > $h->{expdist} + $wiggle)
      {
        $toolong++;
        next;
      }

      ## The current path is still active, save away for next hop
      my $curdist = $h->{curdist} + node_len($node) - $K + 1;
      my $path    = $h->{path} . ":$nodeid";

      my $msg = $path        . "%".
                $curdist     . "%".
                $h->{curdir} . "%".
                $h->{expdist}. "%".
                $h->{expdir} . "%".
                $h->{dest};

      if ($V) { print STDERR "Saving: $msg\n"; }

      push @{$node->{$MATETHREAD}}, $msg;
      $active++;
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
    processHop($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $MATETHREAD)
  {
    my $h;
    $h->{path}     = shift @vals;
    $h->{curdist}  = shift @vals;
    $h->{curdir}   = shift @vals;
    $h->{expdist}  = shift @vals;
    $h->{expdir}   = shift @vals;
    $h->{dest}     = shift @vals;

    push @{$node->{hop}}, $h;
  }
  else
  {
    die "Unknown msg: $msgtype $_\n";
  }
}

processHop($nodeid, $node);

hadoop_counter("foundshort",   $foundshort);
hadoop_counter("foundlong",    $foundlong);
hadoop_counter("foundinvalid", $foundinvalid);
hadoop_counter("foundvalid",   $foundvalid);
hadoop_counter("active",       $active);
hadoop_counter("toolong",      $toolong);
