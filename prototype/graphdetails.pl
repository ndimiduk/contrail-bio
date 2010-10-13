#!/usr/bin/perl -w
use strict;
use Getopt::Long;
use File::Basename;
use lib ".";
use lib dirname($0);
use PAsm;

my @defaultfields = qw/edge bundle/;

my %validfields;
$validfields{"str"}         =  $STR;
$validfields{"cov"}         =  $COVERAGE;
$validfields{"r5"}          =  $R5;
$validfields{"compress"}    =  $CANCOMPRESS;
$validfields{"bubble"}      =  $POPBUBBLE;
$validfields{"merge"}       =  $MERGE;
$validfields{"mertag"}      =  $MERTAG;
$validfields{"thread"}      =  $THREAD;
$validfields{"path"}        =  $THREADPATH;
$validfields{"matethread"}  =  $MATETHREAD;
$validfields{"bundle"}      =  $BUNDLE;

$validfields{"edge"}        = "*edge";
$validfields{"threadcnt"}   = "*threadcnt";
$validfields{"threadedge"}  = "*threadedge";


my $filtertype;
my $showall = 0;
my $HELPFLAG = 0;
my @tag;
my @field;

my $result = GetOptions(
"h"         => \$HELPFLAG,
"tag=s"     => \@tag,
"field=s"   => \@field,
"all"       => \$showall,
"type=s"    => \$filtertype,
);

if ($HELPFLAG)
{
  print "USAGE: graphdetails.pl [-field <field>] [-all] [-type <node_type>] [-tag id] graph > graph.fields\n";
  print "  fields:\n";
  foreach my $v (sort keys %validfields)
  {
    my $code = $validfields{$v};
    print "     $v ($code)\n";
  }

  print "\n";
  print "default: @defaultfields\n";

  exit 0;
}

my %tagid;
foreach my $t (@tag){$tagid{$t}=1;}

my %fields;
if (scalar @field)
{
  foreach my $f (@field)
  {
    if (exists $validfields{$f})
    {
      $fields{$validfields{$f}} = 1;
    }
    else
    {
      die "Unknown field: $f\n";
    }
  }
}
elsif ($showall)
{
  foreach my $vf (keys %validfields)
  {
    $fields{$validfields{$vf}} = 1;
  }
}
else
{
  # set defaults
  foreach my $df (@defaultfields)
  {
    $fields{$validfields{$df}} = 1;
  }
}

sub print_all
{
  my $node = shift;
  my $code = shift;

  if (defined $node->{$code})
  {
    foreach my $t (@{$node->{$code}})
    {
      print "$code\t$t\n";
    }
  }
}

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

  if ((scalar keys %tagid == 0) || exists $tagid{$nodeid})
  {
    my $type = node_branchtype($nodeid, $node);

    if (defined $filtertype && ($filtertype ne $type)) { next; }

    my $len = node_len($node);
    my $cov = node_cov($node);

    print ">$nodeid len=$len cov=$cov type=$type\n";

    foreach my $vf (sort keys %validfields)
    {
      my $nf = $validfields{$vf};
      if (exists $fields{$nf})
      {
        print_all($node, $nf);
      }
    }

    if (exists $fields{$validfields{"edge"}})
    {
      foreach my $et (qw/ff fr rf rr/)
      {
        print_all($node, $et);
      }
    }

    if (exists $fields{$validfields{"threadcnt"}})
    {
      my %threads;

      if (defined $node->{$THREAD})
      {
        foreach my $thread (@{$node->{$THREAD}})
        {
          my ($t,$link,$read) = split /:/, $thread;

          $threads{$t}->{$link}++;
        }
      }

      foreach my $et (qw/ff fr rf rr/)
      {
        if (exists $node->{$et})
        {
          foreach my $v (@{$node->{$et}})
          {
            my $cnt = 0;
            if (exists $threads{$et}->{$v})
            {
              $cnt = $threads{$et}->{$v};
            }

            print "$THREAD\t$et:$v\t$cnt\n";
          }
        }
      }
    }

    if (exists $fields{$validfields{"threadedge"}})
    {
      my %edges;

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
            }
          }
        }
      }

      ## Index the threading reads
      my %reads;
      my %links;

      foreach my $thread (@{$node->{$THREAD}})
      {
        #print STDERR "$nodeid $thread\n";
        my ($t,$link,$read) = split /:/, $thread;
        my $dir = substr($t, 0, 1);

        $links{$link}++;

        if (exists $edges{$dir}->{$t}->{$link})
        {
          push @{$reads{$read}}, "$t:$link";
        }
        else
        {
          print STDERR "WARNING: node $nodeid thread $thread no longer valid\n";
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

      foreach my $ta (keys %pairs)
      {
        foreach my $linka (keys %{$pairs{$ta}})
        {
          foreach my $tb (keys %{$pairs{$ta}->{$linka}})
          {
            foreach my $linkb (keys %{$pairs{$ta}->{$linka}->{$tb}})
            {
              my $weight = $pairs{$ta}->{$linka}->{$tb}->{$linkb}->{cnt};

              print  "$ta:$linka-$tb:$linkb\t$weight\t",
                           join(" ", @{$pairs{$ta}->{$linka}->{$tb}->{$linkb}->{read}}), "\n";
            }
          }
        }
      }

      foreach my $et (qw/ff fr rf rr/)
      {
        if (exists $node->{$et})
        {
          foreach my $v (@{$node->{$et}})
          {
            if (!exists $pairs{$et}->{$v})
            {
              print "$et:$v\t0\n";
            }
          }
        }
      }
    }
  }
}
