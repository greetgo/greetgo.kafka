use warnings FATAL => 'all';
use strict;

use File::Basename;
use File::Path qw(make_path);

my $me = $0;
my $myDir = dirname($me);
my $outDir = "$myDir/out";
my $baseName = basename($me);
$baseName =~ s/\.pl$//;

&runPerl("$myDir/zoo/volumes_zoo_1.yaml.pl");
&runPerl("$myDir/zoo/volumes_kafka_1.yaml.pl");

my $volumesContent = '';

$volumesContent .= &assemble("volumes", "volumes_");

&writeFile("$outDir/volumes.yaml", $volumesContent);

exit(1);

sub assemble {
  my $subDir = shift;
  my $namePrefix = shift;

  my $dir = "$outDir/$subDir";

  my $ret = "";

  my @list = do {
    opendir my $dh, $dir or die "Cannot open dir $dir: $!";
    map {"$dir/$_"} grep {/^$namePrefix/} grep {!/^\.\.?$/} readdir $dh;
  };

  for my $file (@list) {
    $ret .= &readFile($file);
  }

  return $ret;
}

sub writeFile {
  my $fileName = shift;
  my $fileContent = shift;

  make_path dirname($fileName);

  open my $DATA, '>', $fileName or die "Cannot write to file $fileName: $!";
  print $DATA $fileContent;
}

sub readFile {
  my $fileName = shift;
  open my $DATA, '<', $fileName or die "Cannot read file $fileName: $!";
  local $/;
  <$DATA>
}

sub runPerl {
  my $scriptFile = shift;
  system('perl', $scriptFile) == 0 or die "Cannot execute script $scriptFile : $?";
}
