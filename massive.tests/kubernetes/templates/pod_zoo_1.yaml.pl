use strict;
use warnings FATAL => 'all';
use File::Basename;
use File::Path qw(make_path);

my $me = $0;
my $myDir = dirname($me);
my $outDir = "$myDir/../out";
my $baseName = basename($me);
$baseName =~ s/\.pl$//;

my $templateFile = "$myDir/$baseName";

my $template = &readFile($templateFile);

&make(1);
&make(2);
&make(3);

exit(0);

sub make {
  my $N = shift;

  my $content = $template;
  $content =~ s/zoo-1-/zoo-$N-/g;
  $content =~ s/zookeeper-1\n/zookeeper-$N\n/g;
  $content =~ s/value:\s+"1"\s*#\s*ZOOKEEPER_SERVER_ID/value: "$N" # ZOOKEEPER_SERVER_ID/g;

  my $newName = $baseName;
  $newName =~ s/1/$N/;

  &writeFile("$outDir/pod/$newName", $content);
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
