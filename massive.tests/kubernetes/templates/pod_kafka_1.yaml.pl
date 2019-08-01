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
&make(4);

exit(0);

sub make {
  my $N = shift;

  my $content = $template;
  $content =~ s/kafka-1\n/kafka-$N\n/g;
  $content =~ s/kafka-1-data/kafka-$N-data/g;
  $content =~ s/kafka-1-0\.kafka-1\.default\.svc\.cluster\.local/kafka-$N-0.kafka-$N.default.svc.cluster.local/g;
  $content =~ s/value:\s*"1"\s*#\s*KAFKA_BROKER_ID/value: "$N" # KAFKA_BROKER_ID/g;

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
