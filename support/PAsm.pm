#!/usr/bin/perl

package PAsm;

use strict;
use warnings;

use base 'Exporter';
use Data::Dumper;

my %ADVANCED;

sub getENV
{
  my $tag = shift;
  my $retval = shift;

  $ADVANCED{$tag} = $retval;

  $retval = $ENV{$tag}
    if defined $ENV{$tag};

  return $retval;
}

sub show_advanced
{
  print "Advanced Options\n";
  print "=================================================================\n";
  foreach my $key (sort keys %ADVANCED)
  {
    my $default = $ADVANCED{$key};
    print "$key: $default\n";
  }
}

## Environment settings
###############################################################################

## Target genome size for n50 statistics
our $N50_TARGET = getENV("N50_TARGET", 0);

## RANDSEED for randomized pair merging
my $RANDSEED = getENV("RANDSEED", 0);

## Safe choice for testing
our $LOCALNODES = getENV("LOCALNODES", 10000);

## Contiging and Error Correction
###############################################################################

## K is the original substring length
our $K = getENV("K", 3);
die "K ($K) must be odd\n" if ($K % 2 != 1);
die "K ($K) must be at least 3" if ($K < 3);

## TIPLENGTH is the maximum node length to trim away
our $TIPLENGTH = getENV("TIPLENGTH", 2*$K);

## Number of 5' and 3' bases to initially trim
our $TRIM5 = getENV("TRIM5", 0);
our $TRIM3 = getENV("TRIM3", 0);

## Max acceptable edit distance rate for bubble popping
our $BUBBLEEDITRATE = getENV("BUBBLEEDITRATE", 0.05);
our $MAXBUBBLELEN   = getENV("MAXBUBBLELEN", 5*$K);

## Thresholds for removing low coverage nodes
our $MAX_LOW_COV_LEN = getENV("MAX_LOW_COV_LEN", 2*$K);
our $MAX_LOW_COV_THRESH = getENV("MAX_LOW_COV_THRESH", 5);

## Try to resolve short repeats
our $THREADREADS = getENV("THREADREADS", 1);

## Number of reads that pass through a link to resolve
our $MINTHREADWEIGHT = getENV("MINTHREADWEIGHT", 5);

## Don't record threading reads at branches with this many reads
our $MAXTHREADREADS = getENV("MAXTHREADREADS", 10000);

## Maximum number of read starts to store
our $MAXR5          = getENV("MAXR5", 250);


## Scaffolding
###############################################################################

## Require at least this many mates in a bundle
our $MIN_SCAFF_MATE_WEIGHT = getENV("MIN_SCAFF_MATE_WEIGHT", 5);

## Only attempt to scaffold contig at least this long
our $MIN_SCAFF_CTG_LEN     = getENV("MIN_SCAFF_CTG_LEN", $K);

## Only attempt to scaffold contigs with at most this coverage
our $MIN_SCAFF_UNIQUE_COV  = getENV("MIN_SCAFF_UNIQUE_COV", 10);
our $MAX_SCAFF_UNIQUE_COV  = getENV("MAX_SCAFF_UNIQUE_COV", 45);

## Insert Length
our $INSERT_LEN = getENV("INSERT_LEN", 0);

## Start or extend hop messages
our $FIRST_HOP = getENV("FIRST_HOP", 0);


## Constants and Data Structures
###############################################################################

## constants
our $fwd         = "f";
our $rev         = "r";

# message types
our $NODEMSG           = "N";
our $HASUNIQUEP        = "P";
our $UPDATEMSG         = "U";
our $TRIMMSG           = "T";
our $KILLMSG           = "K";
our $EXTRACOV          = "V";
our $KILLLINKMSG       = "L";
our $COMPRESSPAIR      = "C";
our $BUBBLELINKMSG     = "B";
our $THREADIBLEMSG     = "I";
our $RESOLVETHREADMSG  = "R";
our $MATEDIST          = "D";
our $MATEEDGE          = "E";

# nodemsg fields
our $STR         = "s";
our $COVERAGE    = "v";
our $R5          = "5";

our $CANCOMPRESS = "c";
our $POPBUBBLE   = "p";
our $MERGE       = "m";
our $MERTAG      = "t";
our $THREAD      = "d";
our $THREADPATH  = "e";
our $MATETHREAD  = "a";
our $BUNDLE      = "b";

our @EXPORT = qw/
                 $K 
                 $LOCALNODES
                 $THREADREADS
                 $MINTHREADWEIGHT
                 $MAXTHREADREADS
                 $MAXR5
                 $TIPLENGTH
                 $TRIM5
                 $TRIM3
                 $BUBBLEEDITRATE
                 $MAXBUBBLELEN
                 $MAX_LOW_COV_LEN
                 $MAX_LOW_COV_THRESH
                 $N50_TARGET
                 $MIN_SCAFF_MATE_WEIGHT
                 $MIN_SCAFF_CTG_LEN
                 $MIN_SCAFF_UNIQUE_COV
                 $MAX_SCAFF_UNIQUE_COV
                 $FIRST_HOP
                 $NODEMSG 
                 $TRIMMSG
                 $STR
                 $BUBBLELINKMSG
                 $THREADIBLEMSG
                 $RESOLVETHREADMSG
                 $MATEDIST
                 $MATEEDGE
                 $HASUNIQUEP
                 $KILLMSG
                 $EXTRACOV
                 $KILLLINKMSG
                 $COMPRESSPAIR
                 $UPDATEMSG
                 $CANCOMPRESS
                 $POPBUBBLE
                 $MERGE
                 $MERTAG
                 $THREAD
                 $THREADPATH
                 $MATETHREAD
                 $COVERAGE
                 $R5
                 $BUNDLE
                 $fwd
                 $rev
                 show_advanced
                 canonical
                 str2dna
                 dna2str
                 getrand
                 node_degree
                 node_str
                 node_setstr
                 node_str_raw
                 node_setstr_raw
                 node_len
                 node_cov
                 node_isunique
                 node_gettail
                 node_printlinks
                 node_replacelink
                 node_removelink
                 node_haslink
                 node_cleanthreads
                 node_addreads
                 node_revreads
                 node_branchtype
                 mate_basename
                 mate_insertlen
                 mate_insertstdev
                 mate_mateid
                 flip_dir
                 flip_link
                 str_concat
                 rc 
                 parse_node 
                 parse_node_lite
                 print_node 
                 hadoop_counter
                 _min2
                 _max2
                 _min3
                 _max3
                 /;

## Helpers
###############################################################################

sub _min2
{
  return $_[0] < $_[1] ? $_[0] : $_[1];
}

sub _max2
{
  return $_[0] > $_[1] ? $_[0] : $_[1];
}

sub _min3
{
  return $_[0] < $_[1]
         ? $_[0] < $_[2] ? $_[0] : $_[2]
         : $_[1] < $_[2] ? $_[1] : $_[2];
}

sub _max3
{
  return $_[0] > $_[1]
         ? $_[0] > $_[2] ? $_[0] : $_[2]
         : $_[1] > $_[2] ? $_[1] : $_[2];
}

sub hadoop_counter
{
  my ($tag, $value) = @_;

  print STDERR "reporter:counter:asm,$tag,$value\n";
}

## Sequence Processing
###############################################################################

sub rc
{
  my $sequence = reverse $_[0];
  $sequence =~ tr/GATC/CTAG/;
  return $sequence;
}

sub canonical
{
  my $seq = $_[0];
  my $rc = rc($seq);

  if ($seq lt $rc)
  { return ($seq, $fwd); }

  return ($rc, $rev);
}

{
  my $num = 0;
  my $asciibase = ord('A');

  my %str2dna_;
  my %dna2str_;

  foreach my $x (qw/A C G T/)
  {
    my $p = $x;

    my $c = chr($num+$asciibase);

    $str2dna_{$p} = $c;
    $dna2str_{$c} = $p;

    $num++;

    foreach my $y (qw/A C G T/)
    {
      my $p = "$x$y";

      my $c = chr($num+$asciibase);
      $str2dna_{$p} = $c;
      $dna2str_{$c} = $p;

      $num++;
    }
  }

  foreach my $x (qw/A C G T/)
  {
    foreach my $y (qw/A C G T/)
    {
      my $m = $str2dna_{"$x$y"};

      foreach my $z (qw/A C G T/)
      {
        my $p = "$x$y$z";
        $str2dna_{$p} = $m.$str2dna_{$z};

        foreach my $w (qw/A C G T/)
        {
          my $p = "$x$y$z$w";
          my $n = $m.$str2dna_{"$z$w"};

          $str2dna_{$p} = $n;
        }
      }
    }
  }

  sub str2dna
  {
    my $seq = $_[0];

    my $retval;
    my $l = length($seq);
    for (my $i = 0; $i < $l; $i+=4)
    {
      $retval .= $str2dna_{substr($seq, $i, 4)};
    }

    return $retval;
  }

  sub dna2str
  {
    my $dna = $_[0];
    my $retval = "";

    for (my $i = 0; $i < length($dna); $i++)
    {
      $retval .= $dna2str_{substr($dna, $i, 1)};
    }

    return $retval;
  }
}

sub flip_dir
{
  my $retval = $_[0];
  $retval =~ tr/fr/rf/;
  return $retval;
}

sub flip_link
{
  my $retval = reverse $_[0];
  $retval =~ tr/fr/rf/;
  return $retval;
}

my %DNA2BIN;
$DNA2BIN{A} = 0; ## "00";
$DNA2BIN{C} = 1; ## "01";
$DNA2BIN{G} = 2; ## "10";
$DNA2BIN{T} = 3; ## "11";

sub getrand
{
  my $nodeid = $_[0];

  my $extra = undef;

  if ($nodeid =~ /_/)
  {
    my @p = split /_/, $nodeid;
    $nodeid = shift @p;
    $extra = join ("0", @p);
  }

  my $str = dna2str($nodeid);

  my $seed = $RANDSEED;
  my $len = length($str);

  for (my $i = 0; $i < $len; $i+=16)
  {
    my $last = $i+16;
    if ($last > $len) { $last = $len; }

    my $cval = 0;
    for (my $j = $i; $j < $last; $j++)
    {
      $cval <<= 2;
      $cval += $DNA2BIN{substr($str, $j, 1)};
    }

    #$seed ^= $cval;
    $seed = ($seed<<4)^($seed>>28)^$cval;
  }

  srand($seed);
  my $rand = rand;

  if (defined $extra)
  {
    srand($rand*$extra);
    $rand = rand;
  }

  #print STDERR " $str $seed $rand\n";

  return $rand;
}

## Node methods
###############################################################################

sub print_node
{
  my $nodeid = shift;
  my $node = shift;
  my $tagfirst = shift;

  return if !defined $nodeid;

  if ($tagfirst)
  {
    my $tag = $node->{$MERTAG}->[0];
    $node->{$MERTAG} = undef;

    print "$tag\t";
  }

  print "$nodeid\t$NODEMSG";
  
  if (defined $node->{$STR})
  {
    my $str = $node->{$STR}->[0];
    print "\t*$STR\t$str";
  }

  if (defined $node->{$COVERAGE})
  {
    my $cov = sprintf("%0.02f", $node->{$COVERAGE}->[0]);
    print "\t*$COVERAGE\t$cov";
  }

  foreach my $t (qw/ff fr rf rr/)
  {
    if (defined $node->{$t} && scalar @{$node->{$t}})
    {
      print "\t*$t";
      foreach my $i (@{$node->{$t}}) { print "\t$i"; }
    }
  }

  foreach my $adj (qw/f r/)
  {
    if (defined $node->{"$CANCOMPRESS$adj"})
    {
      my $canCompress = $node->{"$CANCOMPRESS$adj"}->[0];
      print "\t*$CANCOMPRESS$adj\t$canCompress";
    }
  }

  if (defined $node->{$MERGE})
  {
    my $mergedir = $node->{$MERGE}->[0];
    print "\t*$MERGE\t$mergedir";
  }

  if (defined $node->{$MERTAG})
  {
    my $mertag = $node->{$MERTAG}->[0];
    print "\t*$MERTAG\t$mertag";
  }

  if (defined $node->{$THREAD})
  {
    print "\t*$THREAD"; 
    foreach my $t (@{$node->{$THREAD}}) { print "\t$t"; } 
  }

  if (defined $node->{$THREADPATH})
  {
    print "\t*$THREADPATH";
    foreach my $t (@{$node->{$THREADPATH}}) { print "\t$t"; }
  }

  if (defined $node->{$THREADIBLEMSG})
  {
    print "\t*$THREADIBLEMSG";
    foreach my $t (@{$node->{$THREADIBLEMSG}}) { print "\t$t"; }
  }

  if (defined $node->{$POPBUBBLE})
  {
    print "\t*$POPBUBBLE"; 
    foreach my $u (@{$node->{$POPBUBBLE}}) { print "\t$u"; } 
  }

  if (defined $node->{$R5})
  {
    print "\t*$R5";
    foreach my $r (@{$node->{$R5}}) { print "\t$r"; }
  }

  if (defined $node->{$BUNDLE})
  {
    print "\t*$BUNDLE";
    foreach my $b (@{$node->{$BUNDLE}}) { print "\t$b"; }
  }

  if (defined $node->{$MATETHREAD})
  {
    print "\t*$MATETHREAD";
    foreach my $m (@{$node->{$MATETHREAD}}) { print "\t$m"; }
  }

  print "\n";
}


sub parse_node
{
  my $node = shift;
  my $vals = shift;

  my $type;
  foreach my $v (@{$vals})
  {
    if ($v =~ /^\*(\w+)$/)
    {
      $type = $1;
    }
    else
    {
      push @{$node->{$type}}, $v;
    }
  }
}

my $fkept = 0;
my $fskip = 0;

sub parse_node_lite
{
  my $strref  = shift;
  my $desired = shift;

  my $node = {};

  my ($preamble, @fields) = split /\t\*/, $$strref;
  my ($nodeid, $msgtype) = split /\t/, $preamble;

  die "ERROR: UNKNOWN MSG TYPE $msgtype\n" if ($msgtype ne $NODEMSG);

  foreach my $fstr (@fields)
  {
    my ($type, @values) = split /\t/, $fstr;

    if (exists $desired->{$type})
    {
      $node->{$type} = \@values;
    }
  }

  #my $getfield = 0;
  #my $type;

  #my $idx = 0;

  #foreach (split /\t/, $$strref)
  #{
  #  if ($idx == 0)
  #  {
  #    $nodeid = $_;
  #    $idx++;
  #  }
  #  elsif ($idx == 1)
  #  {
  #    die "ERROR: UNKNOWN MSG TYPE $_\n" if ($_ ne $NODEMSG);
  #    $idx++;
  #  }
  #  elsif (/^\*/)
  #  {
  #    $type = substr($_,1);
  #    $getfield = exists $fields->{$type};
  #  }
  #  elsif ($getfield)
  #  {
  #    push @{$node->{$type}}, $_;
  #  }
  #}
  
  return ($nodeid, $node);
}


sub str_concat
{
  my $astr = shift;
  my $bstr = shift;

  my $as = substr($astr, -($K-1));
  my $bs = substr($bstr, 0, $K-1);
  die "$as $bs" if $as ne $bs;

  return $astr.substr($bstr, $K-1);
}

sub node_degree
{
  my $node = shift;
  my $type = shift;

  my $cnt = 0;

  foreach my $adj (qw/f r/)
  {
    my $key = "$type$adj";
    if (defined $node->{$key})
    {
      $cnt += scalar @{$node->{$key}};
    }
  }

  return $cnt;
}

sub node_gettail
{
  my ($node, $tailadj) = @_;

  my $tail = undef;
  my $taildir = undef;
  my $count = 0;

  foreach my $td (qw/f r/)
  {
    my $key = "$tailadj$td";
    if (defined $node->{$key})
    {
      $count += scalar @{$node->{$key}};
      $tail = $node->{$key}->[0];
      $taildir = $td;
    }
  }

  if ($count != 1)
  {
    $tail = undef;
    $taildir = undef;
  }

  return ($tail, $taildir);
}


sub node_printlinks
{
  my $node = shift;
  
  foreach my $t (qw/ff fr rf rr/)
  {
    if (exists $node->{$t})
    {
      foreach my $b (@{$node->{$t}})
      {
        print STDERR "$t $b\n";
      }
    }
  }
}

sub node_removelink
{
  my ($node, $id, $dir) = @_;

  die if !defined $node;
  die if !defined $id;
  die if !defined $dir;

  my $found = 0;

  if (defined $node->{$dir})
  {
    for (my $i = 0; $i < scalar @{$node->{$dir}}; $i++)
    {
      if ($node->{$dir}->[$i] eq $id)
      {
        $found = 1;

        splice @{$node->{$dir}}, $i, 1;

        delete $node->{$dir}
          if (scalar @{$node->{$dir}} eq 0);

        last;
      }
    }
  }

  if (!$found)
  {
    print STDERR "Can't remove $id $dir\n";
    print STDERR Dumper($node);
    die;
  }
}

sub node_cleanthreads
{
  my $node = shift;

  my $threadsremoved = 0;

  if (defined $node->{$THREAD})
  {
    ## after tip removal, there may be dead threads
    ## Only keep threads associated with current edges

    my %edges;
    
    foreach my $et (qw/ff fr rf rr/)
    {
      if (exists $node->{$et})
      {
        foreach my $v (@{$node->{$et}})
        {
          $edges{$et}->{$v} = 1;
        }
      }
    }
    
    my @oldthread = @{$node->{$THREAD}};
    $node->{$THREAD} = undef;

    foreach my $thread (@oldthread)
    {
      my ($tdir,$tn,$read) = split /:/, $thread;

      if (exists $edges{$tdir}->{$tn})
      {
        push @{$node->{$THREAD}}, $thread;
      }
      else
      {
        $threadsremoved++;
      }
    }
  }

  return $threadsremoved;
}

sub node_haslink
{
  my ($node, $t, $v) = @_;

  if (exists $node->{$t})
  {
    foreach my $b (@{$node->{$t}})
    {
      if ($b eq $v)
      {
        return 1;
      }
    }
  }

  return 0;
}


sub node_replacelink
{
  my ($nodeid, $node, $origid, $origdir, $newid, $newdir) = @_;

  die if !defined $origid;
  die if !defined $origdir;
  die if !defined $newid;
  die if !defined $newdir;

  my $found = 0;

  if (defined $node->{$origdir})
  {
    for (my $i = 0; $i < scalar @{$node->{$origdir}}; $i++)
    {
      if ($node->{$origdir}->[$i] eq $origid)
      {
        $found = 1;
        if ($origdir eq $newdir)
        {
          $node->{$newdir}->[$i] = $newid;
        }
        else
        {
          splice @{$node->{$origdir}}, $i, 1;

          delete $node->{$origdir}
            if (scalar @{$node->{$origdir}} eq 0);

          push @{$node->{$newdir}}, $newid;
        }

        last;
      }
    }
  }

  if (!$found)
  {
    print STDERR "cant replace $nodeid $origid $origdir $newid $newdir\n";
    print STDERR Dumper($node);
    node_printlinks($node);
    die;
  }

  if (defined $node->{$THREAD})
  {
    foreach my $thread (@{$node->{$THREAD}})
    {
      my ($t,$link,$r) = split /:/, $thread;
      if (($t eq $origdir) && ($link eq $origid))
      {
        $thread = "$newdir:$newid:$r";
      }
    }
  }
}


sub node_cov
{
  my $node = shift;

  if (defined $node->{$COVERAGE})
  {
    return $node->{$COVERAGE}->[0];
  }

  return 0;
}


sub node_len
{
  my $node = shift;
  return length dna2str($node->{$STR}->[0]);
}

sub node_isunique
{
  my $node = shift;

  my $cov = node_cov($node);
  my $len = node_len($node);

  return (($cov > $MIN_SCAFF_UNIQUE_COV) && 
          ($cov < $MAX_SCAFF_UNIQUE_COV) &&
          ($len >= $MIN_SCAFF_CTG_LEN)) ? 1 : 0;
}

sub node_str
{
  my $node = shift;
  return dna2str($node->{$STR}->[0]);
}

sub node_str_raw
{
  my $node = shift;
  return $node->{$STR}->[0];
}

sub node_setstr
{
  my ($node, $str) = @_;
  $node->{$STR}->[0] = str2dna($str);
}

sub node_setstr_raw
{
  my ($node, $str) = @_;
  $node->{$STR}->[0] = $str;
}

sub node_addreads
{
  my ($node, $othernode, $shift) = @_;

  if (defined $othernode->{$R5})
  {
    foreach my $rstr (@{$othernode->{$R5}})
    {
      my ($read, $offset) = split /:/, $rstr;
      $offset += $shift;

      push @{$node->{$R5}}, "$read:$offset";
    }
  }
}

sub node_revreads
{
  my $node = shift;

  my $len = node_len($node);

  if (defined $node->{$R5})
  {
    foreach my $rstr (@{$node->{$R5}})
    {
      my ($read, $pos) = split /:/, $rstr;

      if ($read =~ /^~/)
      {
        $read =~ s/^~//;
      }
      else
      {
        $read = "~$read";
      }
      
      $pos = $len -1 - $pos;

      $rstr = "$read:$pos";
    }
  }
}

sub node_branchtype
{
  my $nodeid = shift;
  my $node = shift;

  my $fdegree = node_degree($node, "f");
  my $rdegree = node_degree($node, "r");

  my $type = "full";

  if    ($fdegree + $rdegree == 0)           { $type = "single"; }
  elsif ($fdegree + $rdegree == 1)           { $type = "tip"; }
  elsif (($fdegree == 0) || ($rdegree == 0)) { $type = "dead"; }
  elsif (($fdegree == 1) && ($rdegree == 1)) { $type = "non"; }
  elsif (($fdegree == 1) || ($rdegree == 1)) { $type = "half"; }

  my $tandem = 0;
  foreach my $tt (qw/ff fr rf rr/)
  {
    if (defined $node->{$tt})
    {
      foreach my $v (@{$node->{$tt}})
      {
        if ($v eq $nodeid) { $tandem++; }
      }
    }
  }

  if ($tandem) { $type .= "_tandem"; }

  my $len = node_len($node);

  if (($len % 2) == 0)
  {
    my $seq = node_str($node);
    my $rc  = rc($seq);

    my $palindrome = 1;

    for(my $i = 0; $i < $len/2; $i++)
    {
      if (substr($seq, $i, 1) ne substr($rc, $i, 1))
      {
        $palindrome = 0;
        last;
      }
    }

    if ($palindrome) { $type .= "_palindrome"; }
  }

  return $type;
}




## Helpers for processing mates
###############################################################################

sub mate_insertlen
{
  my $read1 = shift;
  my $read2 = shift;

  return $INSERT_LEN;
}

sub mate_insertstdev
{
  return .1*$INSERT_LEN;
}

sub mate_basename
{
  my $readid = shift;

  if (($readid =~ /_1$/) || ($readid =~ /_2$/))
  {
    return substr($readid, 0, -2);
  }

  return undef;
}

sub mate_mateid
{
  my $read1 = shift;

  if ($read1 =~ /\_/)
  {
    my ($name, $mate) = split /\_/, $read1;

    my $read2 = "$name\_1";
    if ($mate eq "1") { $read2 = "$name\_2"; }

    return $read2;
  }

  return undef;
}



1;
