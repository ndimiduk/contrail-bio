#!/usr/bin/perl -w
use strict;

use Getopt::Long;
use File::Basename;
use lib ".";
use lib dirname($0);
use PAsm;

my $RESTART = 0;


## Set up base configuration
##############################################################################

## Path to assembler source code directory
my $SRCDIR = dirname($0);

## Directory for output files when not running on hadoop
my $WORKDIR = ".";

## Path to Hadoop binary
my $HADOOP    = "/opt/UMhadoop/bin/hadoop";

## Path to Hadoop streaming jar
my $STREAMING = "/opt/UMhadoop/contrib/streaming/hadoop-0.20.0-streaming.jar";

## Path to base HDFS location
my $HDFSBASE  = "/users/mschatz/";

## Number of reducers
my $TASKS = 80;

## Enable compression
my $HADOOP_COMPRESS = 1;

## Remove intermediate files
my $DOCLEANUP = 1;

## Host specific configuration
my $hostname = `hostname`; chomp($hostname);

## Local scratch dir and memory
my $SCRATCH_DIR = "/tmp";
my $SORT_MEM    = "1G";
my $LOCALNODES  = 1000;

if (($hostname eq "ginkgo.umiacs.umd.edu") ||
    ($hostname eq "walnut.umiacs.umd.edu"))
{
  $SCRATCH_DIR = "/scratch1/mschatz";
  $SORT_MEM    = "50G";
  $LOCALNODES  = 1000000;
}


## IBM Settings
my $USEIBM = 1;
if ($USEIBM)
{
  #$HADOOP    = "/Users/mschatz/build/hadoop-0.20.1+133/bin/hadoop";
  #$STREAMING = "/Users/mschatz/build/hadoop-0.20.1+133/contrib/streaming/hadoop-0.20.1+133-streaming.jar";

  $HADOOP    = "/nfshomes/mschatz/build/packages/hadoop-0.20.1+133/bin/hadoop";
  $STREAMING = "/nfshomes/mschatz/build/packages/hadoop-0.20.1+133/contrib/streaming/hadoop-0.20.1+133-streaming.jar";

  $TASKS = 500;
  $HDFSBASE  = "/umdnsf/shared/SRA000271/";
  $HADOOP_COMPRESS = 1;

  $LOCALNODES = 1000;
}


## Logging
##############################################################################

my $LOGFILENAME;

sub initLog
{
  if (!defined $LOGFILENAME)
  {
    $LOGFILENAME = "$WORKDIR/contrail.$$.log";
  }

  open LOG, "> $LOGFILENAME";
  print "Logging to $LOGFILENAME\n";

  open DETAILS, "> $WORKDIR/contrail.$$.details";

  my $oldfh = select LOG; $| = 1;
  select DETAILS; $| = 1;
  select $oldfh; $| = 1;
}

sub msg
{
  print @_;
  print LOG @_;
  print DETAILS @_;
}

sub logmsg
{
  print DETAILS @_;
}

my $TOTALSTEPS = 0;


## Process options
##############################################################################

my $USAGE = "contrail.pl [-reads readfile] [-start stage] [options] K\n";
my $cmdargs = join " ", @ARGV;

my $HELP = 0;
my $HADOOP_DIR;
my $HDFSDIR;
my $USE_HADOOP = 0;

my $STOP_AFTER;
my $STARTAT;

my $N50_TARGET;
my $UNIQUE_COV = 0;
my $INSERT_LEN = 0;
my $SHOWADVANCED = 0;

my $READS;

my @advanced;

my %stages;
$stages{'initial'}     = 1;
$stages{'removetips'}  = 1;
$stages{'popbubbles'}  = 1;
$stages{'lowcov'}      = 1;
$stages{'repeats'}     = 1;
$stages{'scaffolding'} = 1;
$stages{'status'}      = 1;
$stages{'fasta'}       = 1;

my $result = GetOptions(
  "h"            => \$HELP,
  "reads=s"      => \$READS,
  "start=s"      => \$STARTAT,
  "stop=s"       => \$STOP_AFTER,
  "hadoop=s"     => \$HADOOP_DIR,
  "scratch=s"    => \$SCRATCH_DIR,
  "mem=s"        => \$SORT_MEM,
  "work=s"       => \$WORKDIR,
  "n50=s"        => \$N50_TARGET,
  "unique=s"     => \$UNIQUE_COV,
  "insert=s"     => \$INSERT_LEN,
  "log=s"        => \$LOGFILENAME,
  "adv=s"        => \@advanced,
  "showadv"      => \$SHOWADVANCED,
);

if ($SHOWADVANCED)
{
  show_advanced();
  exit 0;
}



if ($HELP)
{
  print $USAGE;
  print "\n";
  print "Run options\n";
  print "=================================================================\n";
  print " -work <dir>        : Specify the output work directory\n";
  print " -log <filename>    : Log to <filename> instead of contrail.pid.log\n";
  print " -reads <path>      : Path to reads: $HDFSBASE/<path>\n";
  print " -start <stage>     : Start at a given stage\n";
  print " -stop <stage>      : Stop after a given stage\n";
  print " -hadoop <asmdir>   : Run on Hadoop in: $HDFSBASE/<asmdir>\n";
  print " -mem <limit>       : Available RAM (default: $SORT_MEM)\n";
  print " -scratch <dir>     : Offline sort for local run (default: $SCRATCH_DIR)\n";
  print "\n";
  print "Assembly Options\n";
  print "=================================================================\n";
  print " -n50 <target>      : Target for N50 computation\n";
  print " -unique <cov>      : Unique Cov threshold (requires -insert)\n";
  print " -insert <len>      : Insert length (requires -unique) \n";
  print " -adv key=value     : Set an advanced attribute\n";
  print "\n";
  print "Stages\n";
  print "=================================================================\n";
  print " initial     : Construct initial unedited graph\n";
  print " removetips  : Remove Tips\n";
  print " popbubbles  : Pop Bubbles\n";
  print " lowcov      : Remove low coverage nodes\n";
  print " repeats     : Resolve Simple Repeats\n";
  print " scaffolding : Bundle Mates and resolve repeats\n";
  
  exit 0;
}

my $K = shift @ARGV or die $USAGE;

if (defined $HADOOP_DIR)
{
  if (! -x $HADOOP)
  {
    print STDERR "ERROR: Can't execute $HADOOP\n";
    exit 1;
  }

  if (! -r $STREAMING)
  {
    print STDERR "ERROR: Can't access $STREAMING\n";
    exit 1;
  }

  $HDFSDIR = "$HDFSBASE/$HADOOP_DIR";
  $USE_HADOOP = 1;
}
else
{
  if ((-e $WORKDIR) && (!defined $STARTAT))
  {
    print STDERR "ERROR: work directory already exists. but not restarting\n";
    print STDERR "       Use -start initial to force overwrite\n";
    exit 1;
  }

  if (!-e $SCRATCH_DIR)
  {
    print STDERR "ERROR: Can't write to scratch directory\n";
    exit 1;
  }

  if (defined $READS)
  {
    if (! -r $READS)
    {
      print STDERR "Can't access reads file: $READS\n";
      exit 1;
    }
  }
}

if (defined $STARTAT)
{
  if (defined $STARTAT && !defined $stages{$STARTAT})
  {
    print STDERR "WARNING: Unknown stage $STARTAT\n";
    exit 1;
  }

  if (defined $STOP_AFTER && !defined $stages{$STOP_AFTER})
  {
    print STDERR "WARNING: Unknown stage $STOP_AFTER\n";
    exit 1;
  }
}
else
{
  if (!defined $READS)
  {
    print STDERR "ERROR: You must specify -reads or -start\n";
    exit 1;
  }
}


if (($INSERT_LEN == 0) && ($UNIQUE_COV == 0))
{
  print STDERR "WARNING: INSERT_LEN is 0, assuming unpaired reads\n";
}
elsif (($INSERT_LEN == 0) && ($UNIQUE_COV > 0))
{
  print STDERR "ERROR: INSERT_LEN is 0, but UNIQUE_COV is $UNIQUE_COV\n";
  exit 1;
}
elsif (($INSERT_LEN > 0) && ($UNIQUE_COV == 0))
{
  print STDERR "ERROR: UNIQUE_COV is 0, but INSERT_LEN is $INSERT_LEN\n";
  exit 1;
}



mkdir $WORKDIR if (!-e $WORKDIR);

initLog();
msg "Running: $0 $cmdargs\n";

my $uname = `uname -a`; chomp($uname);
logmsg "Hostname: $hostname\n";
logmsg "System: $uname\n\n";

$ENV{K} = $K;
$ENV{MAX_SCAFF_UNIQUE_COV} = $UNIQUE_COV;
$ENV{INSERT_LEN} = $INSERT_LEN;
$ENV{LOCALNODES} = $LOCALNODES;
$ENV{LC_ALL} = "C";  ## Make local sort sane

my $envstr  = " -cmdenv K=$K"
            . " -cmdenv MAX_SCAFF_UNIQUE_COV=$UNIQUE_COV"
            . " -cmdenv INSERT_LEN=$INSERT_LEN"
            . " -cmdenv LOCALNODES=$LOCALNODES";


my $confstr = " -jobconf mapred.map.tasks=$TASKS"
            . " -jobconf mapred.reduce.tasks=$TASKS"
            . " -jobconf mapred.child.java.opts=-Xmx1024m";


if ($HADOOP_COMPRESS)
{
  $confstr .= " -jobconf mapred.compress.map.output=true" 
            . " -jobconf mapred.output.compress=true";

            #" -jobconf mapred.map.output.compression.codec=org.apache.hadoop.io.compress.GzipCode" .
            #" -jobconf mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCode";
}

if (scalar @advanced > 0)
{
  foreach (@advanced)
  {
    $_ =~ s/^\s*//;
    $_ =~ s/\s*$//g;

    die "Malformed option $_\n" if ($_ !~ /=/);

    my ($key,$value) = split /=/, $_;

    $ENV{$key} = $value;
    $envstr .= " -cmdenv $key=$value";
  }

  msg "Environment: $envstr\n";
}







## Time management
##############################################################################

sub printTimestamp
{
  my $stamp = shift;
  my $time = shift;
  my $logonly = shift;

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($time);

  $mon++;
  $year+=1900;

  my $tstamp = sprintf "%04d-%02d-%02d %02d:%02d:%02d",
              $year, $mon, $mday, $hour, $min, $sec;

  if ($logonly)
  {
    logmsg "== $stamp $tstamp\n";
  }
  else
  {
    msg "== $stamp $tstamp\n";
  }
}

sub printTotalTime
{
  my $msg = shift;
  my $diff = shift;

  msg "== $msg $diff s $TOTALSTEPS steps\n";
  $TOTALSTEPS = 0;
}


my $STARTTIME = time;
msg "\n";
printTimestamp("Starting time", $STARTTIME);

sub finish
{
  my $ENDTIME = time;
  printTimestamp("Stopping time", $ENDTIME);
  printTotalTime("Total time", $ENDTIME-$STARTTIME);
}



### HDFS Helpers
#################################################################

sub hdfs_exists
{
  my $path = shift;
  my $rc = system("($HADOOP fs -stat $path) >& /dev/null");

  return !$rc;
}

sub hdfs_remove
{
  my $path = shift;
  if (hdfs_exists($path))
  {
    logmsg "Deleting $path\n";
    my $rc = system("($HADOOP fs -rmr $path) >& /dev/null");
    if ($rc)
    {
      die "ERROR: Couldn't delete old file $path\n";
    }
  }
}

sub hdfs_require
{
  my $path = shift;
  my $exists = hdfs_exists($path);

  if (!$exists)
  {
    die "ERROR: Can't find required file $path\n";
  }
}

sub hdfs_mkdir
{
  my $path = shift;

  logmsg "Making $path\n";

  my $rc = system("($HADOOP fs -mkdir $path) >& /dev/null");
  if ($rc)
  {
    die "ERROR: Couldn't make dir $path\n";
  }
}

sub hdfs_fetch
{
  my $path = shift;
  my $file = shift;
  my $merge = shift;

  my @vals = split /\//, $path;

  my $dir = pop @vals;

  if (-e $file)
  {
    msg "WARNING: removing local $file\n";
    system("rm -rf $file"); 
  }
  
  msg "Fetching $path to $file\n";

  if ($merge)
  {
    system("$HADOOP fs -getmerge $path $file");
  }
  else
  {
    system("$HADOOP fs -get $path $file");
  }
}

sub fetch
{
  my $remote = shift;
  my $local  = shift;
  my $merge  = shift;

  if ($USE_HADOOP)
  {
    hdfs_fetch("$HDFSDIR/$remote", $local, $merge);
  }
}



sub hdfs_put
{
  my $src = shift;
  my $dst = shift;

  if (hdfs_exists($dst))
  {
    logmsg "WARNING: replacing old $dst from hdfs\n";
    system("$HADOOP fs -rmr $dst");
  }

  my $rc = system("$HADOOP fs -put $src $dst");
  die "Can't load $src to $dst ($rc)\n" if $rc;
}

sub hdfs_rename
{
  my $src = shift;
  my $dst = shift;

  hdfs_require($src);

  if (hdfs_exists($dst))
  {
    logmsg "WARNING: replacing old $dst from hdfs\n";
    system("$HADOOP fs -rmr $dst");
  }

  my $rc = system("$HADOOP fs -mv $src $dst");
  die "Can't rename $src to $dst ($rc)\n" if $rc;
}

## Helpers
##############################################################################

my %starttimes;

sub hasStarted
{
  my $stage = shift;

  if (!defined $STARTAT || $STARTAT eq $stage)
  {
    $STARTAT = undef;
    $starttimes{$stage} = time;
    return 1;
  }

  return 0;
}

sub checkDone
{
  my $stage = shift;
  
  printTotalTime("Total time: $stage", time-$starttimes{$stage});

  if (defined $STOP_AFTER && $STOP_AFTER eq $stage)
  {
    finish();
    exit 0;
  }
}

sub save_result
{
  my $orig = shift;
  my $new  = shift;

  msg "  Save results to $new\n";

  if ($USE_HADOOP)
  {
    hdfs_rename("$HDFSDIR/$orig", "$HDFSDIR/$new");
  }
  else
  {
    system("mv $WORKDIR/$orig $WORKDIR/$new");
  }
}


sub runLocal
{
  my $desc    = shift;
  my $mapper  = shift;
  my $reducer = shift;
  my $input   = shift;
  my $output  = shift;
  my $extra   = shift;
  my $conf    = shift;

  my $cmd;
  my $sa = "sort -S $SORT_MEM -T $SCRATCH_DIR";


  if (defined $extra && $extra == 20)
  {
    $extra = 2;
  }

  if (defined $extra && $extra == 23)
  {
    $extra = 2;
  }

  if (defined $extra && $extra == 21)
  {
    $cmd = "($SRCDIR/$mapper $input > $WORKDIR/$output) >& "
          . "$WORKDIR/$output.err";
  }
  elsif (defined $extra && $extra == 22)
  {
    $cmd = "($mapper $WORKDIR/$input | $sa | "
          . "$SRCDIR/$reducer > $WORKDIR/$output) >& "
          . "$WORKDIR/$output.err";
  }
  elsif (defined $extra && $extra == 3)
  {
    $cmd = "($SRCDIR/$mapper $WORKDIR/$input $WORKDIR/$conf | $sa | "
          . "$SRCDIR/$reducer > $WORKDIR/$output) >& "
          . "$WORKDIR/$output.err";
  }
  elsif (defined $extra && $extra == 2)
  {
    $cmd = "($SRCDIR/$mapper $input | $sa | "
          . "$SRCDIR/$reducer > $WORKDIR/$output) >& "
          . "$WORKDIR/$output.err";
  }
  else
  {
    $cmd = "($SRCDIR/$mapper $WORKDIR/$input | $sa | "
          . "$SRCDIR/$reducer > $WORKDIR/$output) >& "
          . "$WORKDIR/$output.err";
  }

  msg "$desc:\t";
  logmsg "\n";
  logmsg "  $cmd\n";

  my $start = time;

  my $rc = system($cmd);

  my $end = time;
  my $diff = $end - $start;

  msg " $diff s";

  die "ERROR: $cmd failed ($rc)" if $rc;

  return 0;
}


sub runStreaming
{
  my $desc    = shift;
  my $mapper  = shift;
  my $reducer = shift;
  my $input   = shift;
  my $output  = shift;
  my $extra   = shift;
  my $conf    = shift;

  my $indir = "$HDFSDIR/$input";

  if (defined $extra && 
      (($extra == 21) || ($extra == 23))) 
  {
    $indir = "$HDFSBASE/$input";
  }

  hdfs_exists($indir);
  hdfs_remove("$HDFSDIR/$output");

  my $cmd = "$HADOOP jar $STREAMING "
          . " -input $indir";

  if (defined $extra && $extra == 21)
  {
    $cmd   .= " -output $HDFSDIR/$output"
            . " -mapper ./$mapper"
            . " -file $SRCDIR/PAsm.pm"
            . " -file $SRCDIR/$mapper";

    $conf = " -jobconf mapred.reduce.tasks=0";
  }
  elsif (defined $extra && $extra == 22)
  {
    $cmd   .= " -output $HDFSDIR/$output"
            . " -mapper $mapper"
            . " -reducer ./$reducer"
            . " -file $SRCDIR/PAsm.pm"
            . " -file $SRCDIR/$reducer";
  }
  else
  {
    if (defined $extra && $extra == 3)
    {
      $cmd .= " -input $HDFSDIR/$conf";
    }
  
    $cmd   .= " -output $HDFSDIR/$output"
            . " -mapper ./$mapper"
            . " -reducer ./$reducer"
            . " -file $SRCDIR/PAsm.pm"
            . " -file $SRCDIR/$mapper"
            . " -file $SRCDIR/$reducer";
  }

  $cmd .= $envstr;
  
  if (defined $conf && $extra != 3) { $cmd .= $conf; }
  else                              { $cmd .= $confstr; }
   

  $cmd .= " -jobconf mapred.job.name=\"$desc $input\"";

  msg "$desc:\t";
  logmsg "\n$cmd\n";

  my $start = time;

  my $jobid;

  open(STATUS, "$cmd 2>&1 |")
   or die "Can't fork $cmd $!\n";

  my $err = 0;

  while (<STATUS>)
  {
    logmsg $_;

    if (/INFO streaming.StreamJob: Running job:/)
    {
      chomp;
      my @vals = split /\s+/, $_;
      $jobid = pop @vals;
      msg "$jobid\t";
    }
    elsif (/ERROR/)
    {
      msg $_;
      $err++;
    }
  }

  ## Detect unknown errors
  if (!defined $jobid) { $err = -1; }

  my $end = time;
  my $diff = $end - $start;

  msg " $diff s";

  die "ERROR: $cmd failed ($err)\n" if ($err);

  return $jobid;
}

sub get_hadoop_counter
{
  my $jobid   = shift;
  my $counter = shift;
  my $dir     = shift;

  my $group   = "asm";

  my $val = `$HADOOP job -counter $jobid \"$group\" \"$counter\"`;
  chomp $val;

  return $val;
}


sub get_local_counter
{
  my $jobid   = shift;
  my $counter = shift;
  my $file    = shift;

  my $group   = "asm";

  my @retval = `grep "^reporter:counter:$group,$counter," $WORKDIR/$file.err | cut -f3 -d','`;

  my $sum = 0;

  foreach (@retval)
  {
    chomp;
    $sum += $_;
  }

  return $sum;
}



sub runStep
{
  my $desc    = shift;
  my $mapper  = shift;
  my $reducer = shift;
  my $input   = shift;
  my $output  = shift;
  my $extra   = shift;
  my $conf    = shift;

  printTimestamp($desc, time, 1);
  $TOTALSTEPS++;

  logmsg "RunStep:\n";
  logmsg " desc: $desc\n";
  logmsg " mapper: $mapper\n";
  logmsg " reducer: $reducer\n";
  logmsg " input: $input\n";
  logmsg " output: $output\n";
  logmsg " extra: $extra\n" if defined $extra;
  logmsg " conf: $conf\n"   if defined $conf;

  if ($USE_HADOOP)
  {
    return runStreaming($desc, $mapper, $reducer, $input, $output, $extra, $conf);
  }
  else
  {
    return runLocal($desc, $mapper, $reducer, $input, $output, $extra, $conf);
  }
}

sub get_counter
{
  my $jobid = shift;
  my $counter = shift;
  my $dir = shift;

  if ($USE_HADOOP)
  {
    return get_hadoop_counter($jobid, $counter, $dir);
  }
  else
  {
    return get_local_counter($jobid, $counter, $dir);
  }
}

sub cleanup
{
  my $dir = shift;

  if ($DOCLEANUP)
  {
    logmsg "Removing old $dir\n";

    if ($USE_HADOOP)
    {
      hdfs_remove("$HDFSDIR/$dir");
    }
    else
    {
      system("rm -rf $WORKDIR/$dir");
      # system("rm -rf $WORKDIR/$dir.err");
    }
  }
}









## Compress simple chains
##############################################################################

sub compressChains
{
  my $start = shift;
  my $final = shift;

  my $stage = 0;
  my $lastremaining = 0;

  if (0) ## ($RESTART)
  {
    $stage = 5;
    $lastremaining = 35941;
    msg "Restarting after Merge $stage, $lastremaining remaining\n";
  }
  else
  {
    my $jobid = runStep("  Compressible", 
                        "compressible-map.pl", 
                        "compressible-reduce.pl", 
                        "$start", "$start.0");

    my $compressible = get_counter($jobid, "compressible", "$start.0");
    msg "  $compressible compressible\n";

    $lastremaining = $compressible;
  }

  while ($lastremaining > 0)
  {
    my $prev = $stage;
    $stage++;

    my $input  = "$start.$prev";
    my $output = "$start.$stage";

    my $remaining = 0;

    if ($lastremaining < $LOCALNODES)
    {
      ## Send all the compressible nodes to the same machine for serial processing

      my $jobid = runStep("  QMark  $stage", 
                          "quickmark-map.pl", 
                          "quickmark-reduce.pl", 
                          "$input", "$input.0");

      my $tomerge = get_counter($jobid, "compressible_neighborhood", "$input.0");
      msg "  $tomerge marked\n";

      $jobid = runStep("  QMerge $stage", 
                       "quickmerge-map.pl", 
                       "quickmerge-reduce.pl", 
                       "$input.0", "$output");

      $remaining = get_counter($jobid, "needcompress", $output);

      cleanup($input);
      cleanup("$input.0");
    }
    else
    {
      ## Use the randomized algorithm

      srand($stage);
      my $RANDSEED = rand(1000000000);
      $ENV{RANDSEED} = $RANDSEED;
      logmsg "  RANDSEED=$RANDSEED ";

      my $jobid = runStep("  Mark  $stage", 
                          "pairmark-map.pl", "pairmark-reduce.pl", 
                          "$input", "$input.0", 1, 
                          " -cmdenv RANDSEED=$RANDSEED $confstr");

      my $tomerge = get_counter($jobid, "mergestomake", "$input.0");
      msg "  $tomerge marked\n";
      cleanup($input);

      $jobid = runStep("  Merge $stage", 
                       "pairmerge-map.pl", "pairmerge-reduce.pl", 
                       "$input.0", "$output", 1, " -cmdenv RANDSEED=$RANDSEED $confstr");

      $remaining = get_counter($jobid, "needcompress", $output);
      cleanup("$input.0");
    }

    my $percchange = sprintf "%0.2f", $lastremaining ? 100*($remaining - $lastremaining) / $lastremaining : 0;

    msg "  $remaining remaining ($percchange%)\n";

    $lastremaining = $remaining;
  }

  save_result("$start.$stage", $final);
  msg "\n";
}



## Recursively remove tips
##############################################################################
sub removetips
{
  my $current = shift;
  my $prefix = shift;
  my $final = shift;

  my $round = 0;
  my $remaining = 1;

  if ($RESTART)
  {
    ## Restart after completing round 0
    $round = 1;
    msg "Restarting after completing Remove Tips $round\n";
  }

  while ($remaining)
  {
    $round++;
    my $output = "$prefix.$round";
    my $removed = 1;

    if (!$RESTART)
    {
      my $jobid = runStep("Remove Tips $round", 
                          "tipremove-map.pl", "tipremove-reduce.pl",
                          $current, $output);

      $removed = get_counter($jobid, "tipsremoved", $output);
      $remaining = get_counter($jobid, "tips_kept", $output);
      msg "  $removed tips removed, $remaining remaining\n";
    }

    if ($removed)
    {
      if (!$RESTART && $round > 1)
      {
        cleanup($current);
      }

      $current = "$output.cmp";
      compressChains($output, $current);
      $remaining = 1;

      $RESTART = 0;
    }

    cleanup($output);
  }

  save_result($current, $final);

  msg "\n";
}


## Recursively pop bubbles
##############################################################################

sub popallbubbles
{
  my $basename     = shift;
  my $intermediate = shift;
  my $final        = shift;

  my $allpopped = 0;
  my $popped    = 1;
  my $round     = 1;

  while ($popped)
  {
    my $findname = "$intermediate.$round.f";
    my $popname  = "$intermediate.$round";
    my $cmpname  = "$intermediate.$round.cmp";

    my $jobid = runStep("Find Bubbles $round", 
                        "bubblefind-map.pl", "bubblefind-reduce.pl",
                        $basename, $findname);

    my $potential = get_counter($jobid, "potentialbubbles", $findname);

    msg "  $potential potential bubbles\n";

    $jobid = runStep("  Pop $round", 
                     "bubblepop-map.pl", "bubblepop-reduce.pl",
                     $findname, $popname);

    $popped = get_counter($jobid, "bubblespopped", $popname);

    msg "  $popped bubbles popped\n";

    cleanup($findname);

    if ($popped > 0)
    {
      if ($round > 1)
      {
        cleanup($basename);
      }

      compressChains($popname, $cmpname);

      $basename = $cmpname;
      $allpopped += $popped;
      $round++;
    }

    cleanup($popname);
  }

  ## Copy the basename to the final name
  save_result($basename, $final);
  msg "\n";

  return $allpopped;
}


## resolve repeats
##############################################################################

sub resolveRepeats
{
  my $current  = shift;
  my $prefix   = shift;
  my $final    = shift;
  my $scaffold = shift;

  my $threadiblecnt = 1;
  my $phase = 1;

  while ($threadiblecnt)
  {
    if ($scaffold)
    {
      msg "Scaffolding phase $phase\n";

      my $jobid;

      if (1) ##$phase > 1)
      {
      ## Find Mates
      my $jobid = runStep("  edges", "matedist-map.pl", "matedist-reduce.pl",
                          $current, "$prefix.$phase.edges");

      my $allctg          = get_counter($jobid, "all_contigs",      "$prefix.$phase.edges");
      my $uniquectg       = get_counter($jobid, "unique_contigs",   "$prefix.$phase.edges");

      my $linking         = get_counter($jobid, "linking_edges",    "$prefix.$phase.edges");
      my $internaldist    = get_counter($jobid, "internal_dist",    "$prefix.$phase.edges");
      my $internaldistsq  = get_counter($jobid, "internal_distsq",  "$prefix.$phase.edges");
      my $internalcnt     = get_counter($jobid, "internal_mates",   "$prefix.$phase.edges");
      my $internalinvalid = get_counter($jobid, "internal_invalid", "$prefix.$phase.edges");

      my $internalavg = sprintf("%0.2f", ($internalcnt ? $internaldist/$internalcnt : 0));
      my $variance    = $internalcnt ?  ($internaldistsq - ($internaldist*$internaldist)/$internalcnt) : 0;
      my $internalstd = sprintf("%0.2f", sqrt(abs($variance)));

      msg "  $linking linking edges, $internalcnt internal $internalavg +/- $internalstd avg, $internalinvalid invalid\n";

      ## Bundle mates
      $jobid = runStep("  bundles", "matebundle-map.pl", "matebundle-reduce.pl",
                       "$prefix.$phase.edges", "$prefix.$phase.bundles", 3, $current);

      my $ubundles   = get_counter($jobid, "unique_bundles", "$prefix.$phase.bundles");

      my $uniqctgperc    = sprintf("%0.2f", ($allctg > 0) ? 
                                   (100 * $uniquectg / $allctg) : 0);

      msg "  $ubundles U-bundles $uniquectg U-contigs ($uniqctgperc%)\n";
      }

      my $USE_FRONTIER = 1;
      my $MAXSTAGE = 10;

      if ($USE_HADOOP || $USE_FRONTIER)
      {
        ## Perform frontier search for mate-consistent paths
        my $active = 1;
        my $stage  = 0;

        my $curgraph = "$prefix.$phase.bundles";

        while (($active > 0) && ($stage < $MAXSTAGE))
        {
          $stage++;

          my $prevgraph = $curgraph;
          $curgraph = "$prefix.$phase.search$stage";

          if ($stage == 1)
          {
            $ENV{FIRST_HOP} = 1;
            logmsg "  FIRST_HOP=1\n";

            $jobid = runStep("  search $stage", 
                             "matehop-map.pl", "matehop-reduce.pl", 
                             $prevgraph, $curgraph, 1,
                             " -cmdenv FIRST_HOP=1 $confstr");
          }
          else
          {
            $ENV{FIRST_HOP} = 0;
            logmsg "  FIRST_HOP=0\n";

            $jobid = runStep("  search $stage", 
                             "matehop-map.pl", "matehop-reduce.pl", 
                             $prevgraph, $curgraph);
          }

          my $short   = get_counter($jobid, "foundshort",   $curgraph);
          my $long    = get_counter($jobid, "foundlong",    $curgraph);
          my $invalid = get_counter($jobid, "foundinvalid", $curgraph);
          my $valid   = get_counter($jobid, "foundvalid",   $curgraph);
          my $toolong = get_counter($jobid, "toolong",      $curgraph);
          $active     = get_counter($jobid, "active",       $curgraph);

          msg " active: $active toolong: $toolong | valid: $valid short: $short long: $long invalid: $invalid\n";
        }

        $jobid = runStep("  update",
                         "matehopfinalize-map.pl", "matehopfinalize-reduce.pl",
                         $curgraph, "$prefix.$phase.matepath");

        my $bresolved = get_counter($jobid, "resolved_bundles", "$prefix.$phase.matepath");
        my $eresolved = get_counter($jobid, "resolved_edges",   "$prefix.$phase.matepath");
        my $ambig     = get_counter($jobid, "total_ambiguous",  "$prefix.$phase.matepath");

        msg " $bresolved bundles resolved, $eresolved edges, $ambig ambiguous \n";
      }
      else
      {
        ## find mate-consistent path in memory
        $jobid = runStep("  paths", "matepath-map.pl", "matepath-reduce.pl", 
                         "$prefix.$phase.bundles", "$prefix.$phase.matepath");
        my $bresolved = get_counter($jobid, "resolved_bundles", "$prefix.$phase.matepath");
        my $eresolved = get_counter($jobid, "resolved_edges",   "$prefix.$phase.matepath");
        my $eval      = get_counter($jobid, "total_eval",       "$prefix.$phase.matepath");
        my $valid     = get_counter($jobid, "total_valid",      "$prefix.$phase.matepath");
        my $abort     = get_counter($jobid, "total_abort",      "$prefix.$phase.matepath");
        my $ambig     = get_counter($jobid, "total_ambiguous",  "$prefix.$phase.matepath");

        msg "  $bresolved bundles resolved, $eresolved edges, paths: $eval eval $valid valid $abort abort $ambig ambig\n";
      }

      ## Record path
      $jobid = runStep("  finalize", "matefinalize-map.pl", "matefinalize-reduce.pl", 
                       "$prefix.$phase.matepath", "$prefix.$phase.final");
      my $updates = get_counter($jobid, "updates", "$prefix.$phase.final");
      msg "  $updates nodes resolved\n";

      ## Clean bogus links from unique nodes
      $jobid = runStep("  clean", "matecleanlink-map.pl", "matecleanlink-reduce.pl", 
                       "$prefix.$phase.final", "$prefix.$phase.scaff");
      my $deadedges = get_counter($jobid, "removed_edges", "$prefix.$phase.scaff");
      msg "  $deadedges edges removed\n";

      $threadiblecnt = $updates + $deadedges;

      $current = "$prefix.$phase.scaff";
    }
    else
    {
      my $output = "$prefix.$phase.threads";

      ## Find threadible nodes
      my $jobid = runStep("Thread Repeats $phase", 
                          "threadrepeat-map.pl", "threadrepeat-reduce.pl", 
                          $current, $output);

      $threadiblecnt = get_counter($jobid, "threadible",   $output);
      my $xcut       = get_counter($jobid, "xcut",         $output);
      my $half       = get_counter($jobid, "halfdecision", $output);
      my $deadend    = get_counter($jobid, "deadend",      $output);
      msg "  $threadiblecnt threadible ($xcut xcut, $half half, $deadend deadend)\n";

      $current = $output;
    }

    if ($threadiblecnt > 0)
    {
      ## Mark threadible neighbors
      my $threadible = "$prefix.$phase.threadible";
      my $jobid = runStep("  Threadible $phase", 
                          "threadible-map.pl", "threadible-reduce.pl", 
                          $current, $threadible);

      $threadiblecnt = get_counter($jobid, "threadible", $threadible);
      msg "  $threadiblecnt threaded nodes\n";


      ## Resolve a subset of threadible nodes
      my $resolved = "$prefix.$phase.resolved";
      $jobid = runStep("  Resolve $phase", 
                       "threadresolve-map.pl", "threadresolve-reduce.pl",
                        $threadible, $resolved);

      my $remaining = get_counter($jobid, "needsplit", $resolved);
      msg "  $remaining remaining\n";

      ## Cleanup
      compressChains($resolved, "$prefix.$phase.cmp");

      removetips("$prefix.$phase.cmp", 
                 "$prefix.$phase.tips",
                 "$prefix.$phase.tipsfin");

      popallbubbles("$prefix.$phase.tipsfin", 
                    "$prefix.$phase.pop", 
                    "$prefix.$phase.popfin");

      $current = "$prefix.$phase.popfin";

      computestats($current);
      $phase++;
      msg "\n";
    }
  }
  
  my $UNROLL_TANDEM = 1;

  if ($scaffold && $UNROLL_TANDEM)
  {
    my $output = "$prefix.unroll";

    msg "\n\n";

    ## Unroll simple tandem repeats
    my $jobid = runStep("Unroll tandems", 
                        "unrolltandem-map.pl", "unrolltandem-reduce.pl",
                        $current, $output);

    my $unrolled = get_counter($jobid, "simpletandem", $output);
    my $tandem   = get_counter($jobid, "tandem",       $output);
    msg "  $unrolled unrolled ($tandem total)\n";

    compressChains($output, "$output.cmp");

    $current = "$output.cmp";
  }

  ## The current phase did nothing, so save away current
  save_result($current, $final);
  msg "\n";

  if (!$USE_HADOOP)
  {
    msg "Create fasta\n";
    system("$SRCDIR/graph2fa.pl $WORKDIR/$final > $WORKDIR/$final.fa");
  }
}


## Compute Assembly Statistics
##############################################################################

sub computestats
{
  my $dir = shift;

  my $n50str = "";
  $ENV{N50_TARGET} = $N50_TARGET if (defined $N50_TARGET);
  $n50str = " -cmdenv N50_TARGET=$N50_TARGET" if (defined $N50_TARGET);
  $n50str .= " -jobconf mapred.map.tasks=$TASKS -jobconf mapred.reduce.tasks=1";

  runStep("Compute Stats", "stats-map.pl", "stats-reduce.pl", "$dir", "$dir.stats", 0, $n50str);

  msg "\n";
  
  fetch("$dir.stats", "$WORKDIR/$dir.stats", 1);

  msg "\n";
  msg "Stats $dir\n";
  msg "====================================================================================\n";
  my $stats = `cat $WORKDIR/$dir.stats`;
  msg $stats;
  msg "\n";
}





## Assembly Pipeline

my $basic         = "00-basic.txt";
my $initial       = "01-initial.txt";
my $initialcmp    = "02-initialcmp.txt";
my $notips        = "03-notips.txt";
my $notipscmp     = "04-notipscmp.txt";
my $nobubbles     = "05-nobubbles.txt";
my $nobubblescmp  = "06-nobubblescmp.txt";
my $lowcov        = "07-lowcov.txt";
my $lowcovcmp     = "08-lowcovcmp.txt";
my $repeats       = "09-repeats.txt";
my $repeatscmp    = "10-repeatscmp.txt";
my $scaff         = "11-scaffold.txt";
my $final         = "99-final.txt";


## Build Initial Graph
##############################################################################
if (hasStarted("initial"))
{
  my $SPLIT = 1;

  my $reads_goodbp = 0;
  my $reads_good   = 0;
  my $reads_short  = 0;
  my $reads_skip   = 0;
  my $nodecnt      = 0;

  if($USE_HADOOP || $SPLIT)
  {
    my $prep = "$basic-prep";
    my $jobid = runStep("Preprocess", 
                        "build-map.pl", "", 
                        $READS, $prep, 21);

    $reads_goodbp = get_counter($jobid, "reads_goodbp",  $prep);
    $reads_good   = get_counter($jobid, "reads_good",    $prep);
    $reads_short  = get_counter($jobid, "reads_short",   $prep);
    $reads_skip   = get_counter($jobid, "reads_skipped", $prep);

    my $reads_all = $reads_good + $reads_short + $reads_skip;
    die "No good reads available" if !$reads_good;

    my $frac_reads = sprintf("%.02f", 100*$reads_good/$reads_all);

    msg "  $reads_good ($frac_reads%) good reads, $reads_goodbp bp\n";

    $jobid = runStep("Build Initial", 
                     "cat", "build-reduce.pl", 
                     $prep, $basic, 22);

    $nodecnt = get_counter($jobid, "nodecount",     $basic);
    msg "  $nodecnt nodes\n";
  }
  else
  {
    my $jobid = runStep("Preprocess", 
                        "build-map.pl", "build-reduce.pl", 
                        $READS, $basic, 23);

    $nodecnt      = get_counter($jobid, "nodecount",     $basic);
    $reads_goodbp = get_counter($jobid, "reads_goodbp",  $basic);
    $reads_good   = get_counter($jobid, "reads_good",    $basic);
    $reads_short  = get_counter($jobid, "reads_short",   $basic);
    $reads_skip   = get_counter($jobid, "reads_skipped", $basic);

    my $reads_all = $reads_good + $reads_short + $reads_skip;
    die "No good reads available" if !$reads_good;

    my $frac_reads = sprintf("%.02f", 100*$reads_good/$reads_all);

    msg "  $nodecnt nodes [$reads_good ($frac_reads%) good reads, $reads_goodbp bp]\n";
  }

  logmsg "reads_good: $reads_good\n";
  logmsg "reads_short: $reads_short\n";
  logmsg "reads_skip: $reads_skip\n";
  logmsg "reads_goodbp: $reads_goodbp\n";
  logmsg "nodecnt: $nodecnt\n";

  my $jobid = runStep("  Quick Merge", 
                      "quickmerge-map.pl", "quickmerge-reduce.pl",
                      $basic, $initial);

  my $savings = get_counter($jobid, "saved", $initial);
  msg "  $savings saved\n";

  compressChains($initial, $initialcmp);

  computestats($initialcmp);

  checkDone("initial");
}


## Remove Tips
##############################################################################
if (hasStarted("removetips"))
{
  removetips($initialcmp, $notips, $notipscmp);
  computestats($notipscmp);
  checkDone("removetips");
}


## Pop Bubbles
##############################################################################
if (hasStarted("popbubbles"))
{
  popallbubbles($notipscmp, $nobubbles, $nobubblescmp);
  computestats($nobubblescmp);
  checkDone("popbubbles");
}


## Remove Low Coverage
##############################################################################
if (hasStarted("lowcov"))
{
  my $jobid = runStep("Remove Low Coverage", 
                      "cliplowcoverage-map.pl", "cliplowcoverage-reduce.pl", 
                      $nobubblescmp, $lowcov);

  my $lcremoved = get_counter($jobid, "lowcovremoved", $lowcov);
  msg "  $lcremoved low coverage nodes removed\n";

  compressChains($lowcov, "$lowcov.c");

  removetips("$lowcov.c", "$lowcov.t", "$lowcov.tc");
  popallbubbles("$lowcov.tc", "$lowcov.b", $lowcovcmp);
  computestats($lowcovcmp);
  checkDone("lowcov");
}




## Resolve simple repeats
##############################################################################
if (hasStarted("repeats"))
{
  resolveRepeats($lowcovcmp, $repeats, $repeatscmp, 0);
  computestats($repeatscmp);
  checkDone("repeats");
}


## Bundle mates to resolve repeats
##############################################################################
if (hasStarted("scaffolding"))
{
  if ($INSERT_LEN > 0)
  {
    resolveRepeats($repeatscmp, $scaff, $final, 1);
    computestats($final);
  }
  else
  {
    save_result($repeatscmp, $final);
    save_result("$repeatscmp.stats", "$final.stats");
  }

  checkDone("scaffolding");
}


## Convert to fasta format
##############################################################################
if (hasStarted("fasta"))
{
  if (!$USE_HADOOP)
  {
    msg "Create Fasta\n";
    system("$SRCDIR/graph2fa.pl $WORKDIR/$final > $WORKDIR/$final.fa");
  }
  checkDone("fasta");
}


if (!$USE_HADOOP)
{
  ## Compute graph status
  ##############################################################################

  if (hasStarted("status"))
  {
    msg "Checking graph\n";

    system("$SRCDIR/check-graph.pl $WORKDIR/$final > $WORKDIR/$final.status");

    my $stats = `tail -2 $WORKDIR/$final.status`;
    msg $stats;

    checkDone("status");
  }
}


finish();
