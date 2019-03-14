package kz.greetgo.kafka_old.probes;

public class ProbeOldConsumer {
  public static void main(String[] args) {
    kafka.tools.ConsoleConsumer.main(new String[]{
        //"--zookeeper", "localhost:2181", "--topic", "pompei_gcory_dt_exe_in", "--from-beginning"
        "--zookeeper", "localhost:2181", "--topic", "pompei_gcory_dt_exe_in"
        //"--help"
    });
  }
}
