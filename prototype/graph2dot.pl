#!/usr/bin/perl -w
use strict;
use Getopt::Long;
use File::Basename;
use lib ".";
use lib dirname($0);
use PAsm;


my $MAXLEN = 8;
my $SHOW_THREADS = 0;
my $HELPFLAG = 0;
my $SHOW_BUNDLES = 0;
my $TINY = 0;
my $LOW_COV = 0;
my $COLOR_THREADS = 0;

my %graph;
my @tag;

my $result = GetOptions(
"h"         => \$HELPFLAG,
"threads"   => \$SHOW_THREADS,
"l=s"       => \$MAXLEN,
"tag=s"     => \@tag,
"bundle"    => \$SHOW_BUNDLES,
"tiny"      => \$TINY,
"cov=s"     => \$LOW_COV,
"color_threads" => \$COLOR_THREADS,
);

if ($HELPFLAG)
{
  print "USAGE: graph2dot.pl [-color_threads] [-cov lowcov] [-threads] [-bundles] [-l maxseqlen] [-tag id] graph > graph.dot\n";
  exit 0;
}

my $threadidx = 0;

my @pallete = qw/red green orange blue yellow indigo violet/;

my %tagid;
foreach my $t (@tag){$tagid{$t}=1;}

my %libcolor;
$libcolor{"mom"}  = "red";
$libcolor{"mom+"} = "pink";

$libcolor{"dad"}     = "blue";
$libcolor{"dad+"}    = "cyan3";
$libcolor{"parents"} = "violet";
$libcolor{"mix"}     = "black";

$libcolor{"prb"}    = "green";
$libcolor{"sib"}    = "darkorange";
$libcolor{"child+"} = "yellow";

print STDERR "Coloring by Threads\n" if ($COLOR_THREADS);


print "digraph structs{\n";
print "  node [shape=record];\n";
print "  rankdir=LR\n";

while (<>)
{
  chomp;

  my $node = {};

  my @vals = split /\t/, $_;

  my $nodeid = shift @vals;

  $graph{$nodeid}->{node} = 1;

  my $msgtype = shift @vals; ## nodemsg

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  else
  {
    die "Unknown msg: $_\n";
  }

  my $seq  = node_str($node);
  my $info = "len=" . length($seq) . " cov=" . $node->{$COVERAGE}->[0];

  if (length($seq) > $MAXLEN)
  {
    $seq = substr($seq, 0, $MAXLEN) . "...";
  }

  my $label = $nodeid;

  if ($TINY)
  {
    $label = ".";
    $seq = ".";
    $info = ".";
  }

  my $color = "black";
  my $style;

  if (exists $tagid{$nodeid})
  {
    $color = "goldenrod3";
    $style = "filled";
  }
  elsif (exists $node->{$THREADPATH})
  {
    $color = "blue";
    $style = "filled";
  }
  elsif ($COLOR_THREADS)
  {
    my %libs;

    if (exists $node->{$THREAD})
    {
      foreach my $thread (@{$node->{$THREAD}})
      {
        my ($et, $v, $red) = split /:/, $thread;
        my $lib = (split /_/, $red)[0];

        $libs{$lib}++;
      }

      if (scalar keys %libs == 1)
      {
        my $l = (keys %libs)[0];
        $color = $libcolor{$l};
      }
      else
      {
        if (exists $libs{mom} && exists $libs{dad})
        {
          if (exists $libs{prb} || exists $libs{sib})
          {
            $color = $libcolor{mix};
          }
          else
          {
            $color = $libcolor{"parents"};
          }
        }
        elsif (exists $libs{mom})
        {
          $color = $libcolor{"mom+"};
        }
        elsif (exists $libs{dad})
        {
          $color = $libcolor{"dad+"};
        }
        else
        {
          $color = $libcolor{"child+"};
        }
      }
    }
  }

  if ($node->{$COVERAGE}->[0] < $LOW_COV)
  {
    $color = "grey";
  }


  print "  $nodeid [label=\"$label | <f> $seq | <r> $info\" color=\"$color\"";
  print " style=\"$style\"" if (defined $style);
  print "]\n";

  foreach my $ut (qw/f r/)
  {
    foreach my $vt (qw/f r/)
    {
      my $ot = "$ut$vt";

      next if $ot eq "rr";

      if (exists $node->{$ot} && scalar @{$node->{$ot}})
      {
        foreach my $b (@{$node->{$ot}})
        {
          next if (($ut ne $vt) && ($nodeid lt $b));

          print "  $nodeid:$ut -> $b:$vt [weight=100 color=\"$color\"]\n";
          $graph{$b}->{edge}++;
        }
      }
    }
  }

  if ($SHOW_THREADS && defined $node->{$THREAD})
  {
    my %threadingreads;

    foreach my $thread (@{$node->{$THREAD}})
    {
      my ($t,$link,$read) = split /:/, $thread;

      my $c;
      $c->{t} = $t;
      $c->{link} = $link;

      push @{$threadingreads{$read}}, $c;
    }

    my %threads;
    my $col = 0;


    my %seen;

    foreach my $read (keys %threadingreads)
    {
      if (scalar @{$threadingreads{$read}} == 2)
      {
        my @threads = sort {$a->{link} cmp $b->{link}}
                      @{$threadingreads{$read}};

        my $an = $threads[0]->{link};
        my $at = substr($threads[0]->{t}, 1, 1);

        my $bn = $threads[1]->{link};
        my $bt = substr($threads[1]->{t}, 1, 1);

        if (!defined $seen{"$an:$at:$bn:$bt"})
        {
          my $cc = $pallete[$col];

          print " $an:$at -> $bn:$bt [color=\"$cc\" weight=1 arrowhead=\"normal\" arrowtail=\"normal\"]\n";

          $col = ($col+1) % (scalar @pallete);
          $seen{"$an:$at:$bn:$bt"} = 1;
        }
      }
    }
  }

  if ($SHOW_BUNDLES && defined $node->{$BUNDLE})
  {
    foreach my $b (@{$node->{$BUNDLE}})
    {
      my ($ctg,$edgetype,$dist,$weight,$unique) = split /:/, $b;

      if ($nodeid le $ctg)
      {
        print "  $nodeid -> $ctg [label=\"dist=$dist weight=$weight type=$edgetype\" arrowhead=\"normal\" arrowtail=\"normal\" style=\"dashed\" len=4]\n";
      }
    }
  }
}

foreach my $nodeid (keys %graph)
{
  if (!exists $graph{$nodeid}->{node})
  {
    print "  $nodeid [label=\"$nodeid | <f> | <r> \"]\n";
  }
}

print "}\n";
