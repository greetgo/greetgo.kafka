use strict;
use warnings FATAL => 'all';
use File::Basename;

my $me = $0;
my $myDir = dirname($me);
my $outDir = "$myDir/out";

my @nodes = (
  "dock1",
  "dock2",
  "dock3"
);

my @letLabels =

  map {{ node => $_->[0], label => $_->[1], value => $_->[2], }}
    (

      [ qw/dock3 mass-volume-kafka-1 true/ ],
      [ qw/dock3 mass-volume-kafka-2 true/ ],
      [ qw/dock3 mass-volume-kafka-3 true/ ],
      [ qw/dock3 mass-volume-kafka-4 true/ ],

      [ qw/dock2 mass-volume-zoo-1-data true/ ],
      [ qw/dock2 mass-volume-zoo-2-data true/ ],
      [ qw/dock2 mass-volume-zoo-3-data true/ ],

      [ qw/dock2 mass-volume-zoo-1-log true/ ],
      [ qw/dock2 mass-volume-zoo-2-log true/ ],
      [ qw/dock2 mass-volume-zoo-3-log true/ ],

      [ qw/dock1 mass-server true/ ],

      [ qw/dock1 kafka-manager true/ ],
      [ qw/dock1 prometheus true/ ],
      [ qw/dock1 kafka-exporter true/ ],
      [ qw/dock1 grafana true/ ],
      [ qw/dock1 postgres true/ ],

    );

my $setNodeLabelsShFile = "$outDir/set_node_labels.sh";

do {

  open my $out, '>', $setNodeLabelsShFile or die "Cannot write to file $setNodeLabelsShFile: $!";

  &removeAllLabels($out);
  &setAllLabels($out);

};

system qq{ ssh dock1 rm -rf wrk };
system qq{ ssh dock1 mkdir wrk };
system qq{ scp $setNodeLabelsShFile dock1:wrk/set_node_labels.sh };
system qq{ ssh dock1 sh wrk/set_node_labels.sh };

exit(0);

sub removeAllLabels {
  my $out = shift;

  for my $node (@nodes) {
    for my $label (keys %{{ map {$_->{label} => 1} @letLabels }}) {
      &removeLabel($out, $node, $label);
      print "removeLabel $node $label\n";
    }
  }

}

sub removeLabel {
  my $out = shift;
  my $node = shift;
  my $labelName = shift;

  print $out "kubectl label --overwrite node/$node ${labelName}-\n";
}

sub setLabel {
  my $out = shift;
  my $node = shift;
  my $labelName = shift;
  my $value = shift;

  print $out "kubectl label --overwrite node/$node ${labelName}=${value}\n";
}

sub setAllLabels {
  my $out = shift;

  for my $let (@letLabels) {
    &setLabel($out, $let->{node}, $let->{label}, $let->{value});
    print "setLabel $let->{node} $let->{label} $let->{value}\n";
  }

}
