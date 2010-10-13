#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

while (<>)
{
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

  if (defined $node->{$MATETHREAD})
  {
    ## Broadcast the thread messages
    my $dir;
    my $u;
    my $ut;

    my $incoming;
    my $cur;
    my $idx;
    my $tandem;

    #print STDERR ">$nodeid\n";
    #foreach my $tstr (@{$node->{$MATETHREAD}})
    #{
    #  print STDERR "$tstr\n";
    #}

    foreach my $tstr (@{$node->{$MATETHREAD}})
    {
      my ($td, $vt, $v) = split /:/, $tstr;

      if (!defined $dir || $td ne $dir)
      {
        $dir = $td;
        $idx = 1;
        $tandem = "P";

        my $deg = node_degree($node, $td);

        if ($deg > 1)
        {
          ## find bogus links in this direction
          foreach my $bdd (qw/f r/)
          {
            my $bd = "$td$bdd";
            if (exists $node->{$bd})
            {
              foreach my $bv (@{$node->{$bd}})
              {
                if (($bd ne $vt) || ($bv ne $v))
                {
                  ## bogus link to $bv via bd
                  ## Don't delete the link right away, in case we need it for
                  ## other mates (shouldn't happen though)
                  print "$nodeid\t$KILLLINKMSG\t$bd\t$bv\n";
                  print STDERR "Clean dead link from $nodeid $bd $bv : $tstr\n";
                }
              }
            }
          }
        }

        $incoming = flip_link($vt).":$nodeid";
        $cur = $v;
      }
      elsif ($cur eq $v)
      {
        ## Went through a tandem
        $incoming .= "-".flip_link($vt);
        $tandem = "T";
      }
      else
      {
        my $outgoing = "$vt:$v";
        my $label = "b$nodeid\_$idx$dir";

        print "$cur\t$UPDATEMSG\t$tandem-$incoming-$outgoing-$label\n";

        $idx++;
        $incoming = flip_link($vt).":$cur";
        $cur = $v;
        $tandem = "P";
      }
    }

    delete $node->{$MATETHREAD};
  }

  delete $node->{$THREAD};
  delete $node->{$BUNDLE};
  delete $node->{$THREADPATH};
  
  print_node($nodeid, $node);
}
