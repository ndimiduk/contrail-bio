#!/usr/bin/perl -w
use strict;

die "Must set K" if !exists $ENV{K};
my $K = $ENV{K};
my $HADOOP = "/opt/UMhadoop/bin/hadoop";
my $BATCH = 2500000;
  
my $short = "reads";

system("chmod +x ./count-kmers");
#system("chmod +x ./emit-mers.pl");

print STDERR "Counting $K-mers\n";

while (<>)
{
  chomp;
  next if (/^\s*$/);

  my @fields = split /\s+/, $_;

  ## skip $fields[0], it is the line number
  my $filename = $fields[1];
  my $local = $filename;

  print STDERR "reporter:status:Processing $filename\n";
  print STDERR "Processing \"$filename\"\n";

  my $fetch = 0;

  if ($filename =~ /^hdfs:/)
  {
    $fetch = 1;
    my $fetchcmd = "$HADOOP fs -get $filename $short";

    print STDERR "$fetchcmd\n";
    system($fetchcmd);

    $local = $short;
  }

  if (! -r $local)
  {
    die "Can't read localfile $local ($filename) ($!)\n";
  }

  my $cmd;

  #if ($filename =~ /\.gz$/)
  #{
  #  $cmd = "zcat $local | ./emit-mers.pl";
  #}
  #else
  #{
  #  $cmd = "./emit-mers.pl $local";
  #}

  #print STDERR "$cmd\n";
  #system($cmd);

  if ($filename =~ /\.gz$/)
  {
    $cmd = "(zcat $local | ./count-kmers -l $BATCH -k $K -S) 2> count-kmers.stderr";
  }
  else
  {
    $cmd =  "(cat $local | ./count-kmers -l $BATCH -k $K -S) 2> count-kmers.stderr";
  }

  print STDERR "$cmd\n";
  system($cmd);

  open ERR, "count-kmers.stderr" 
    or die "Can't open count-kmers.stderr ($!)\n";

  while (<ERR>)
  {
    print STDERR $_;

    if (/(\d+) sequences processed, (\d+) bp scanned/)
    {
      print STDERR "reporter:counter:asm,reads,$1\n";
      print STDERR "reporter:counter:asm,readlen,$2\n";
    }
    elsif (/(\d+) total distinct mers/)
    {
      print STDERR "reporter:counter:asm,distinct mers,$1\n";
    }
  }

  close ERR;

  print STDERR "reporter:counter:asm,files,1\n";

  system("rm -f count-kmers.stderr");
  system("rm -f $short") if ($fetch);
}
