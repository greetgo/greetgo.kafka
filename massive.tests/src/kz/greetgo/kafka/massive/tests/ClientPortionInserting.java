package kz.greetgo.kafka.massive.tests;

import kz.greetgo.kafka.massive.tests.model.Client;
import kz.greetgo.kafka.producer.KafkaFuture;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.util.RND;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static kz.greetgo.kafka.massive.tests.MassiveTestServer.TOPIC_CLIENT;
import static kz.greetgo.kafka.massive.tests.TimeUtil.nanosRead;

public class ClientPortionInserting {
  private final LongParameter portion;
  private final LongParameter portionCount;
  private final ProducerFacade mainProducer;
  private final BoolParameter parallel;
  private final Path workingFile;
  private final Command insertClientPortion;
  private final File stopFile;

  public ClientPortionInserting(LongParameter portion,
                                LongParameter portionCount,
                                ProducerFacade mainProducer,
                                BoolParameter parallel,
                                Path workingFile,
                                Command insertClientPortion, File stopFile) {
    this.portion = portion;
    this.portionCount = portionCount;
    this.mainProducer = mainProducer;
    this.parallel = parallel;
    this.workingFile = workingFile;
    this.insertClientPortion = insertClientPortion;
    this.stopFile = stopFile;
  }

  public void execute() throws IOException {
    stopFile.getParentFile().mkdirs();
    stopFile.createNewFile();

    boolean parallel = this.parallel.value();

    int portionCount = this.portionCount.getAsInt();

    long startedAt = System.nanoTime();
    int clientTotalCount = 0;

    int insertedPortions = 0;

    for (int u = 0; u < portionCount && workingFile.toFile().exists() && stopFile.exists(); u++) {

      if (parallel) {

        List<KafkaFuture> futures = new ArrayList<>();

        String id = RND.str(3);

        int count = portion.getAsInt();
        long started = System.nanoTime();
        for (int i = 0; i < count; i++) {
          Client client = new Client();
          client.id = id + "-" + i;
          client.surname = RND.str(10);
          client.name = ((i == 10) || (i % 2000 == 0)) ? "err" : "ok";

          futures.add(mainProducer
            .sending(client)
            .setAuthor("manager-" + RND.plusInt(10))
            .kafkaId("k-id-" + RND.str(5))
            .toTopic(TOPIC_CLIENT)
            .go());

          clientTotalCount++;

        }

        long middle = System.nanoTime();

        futures.forEach(KafkaFuture::awaitAndGet);

        long end = System.nanoTime();

        System.out.println("5hb4326gv :: №" + (u + 1) + " Inserted " + count
          + " clients for " + nanosRead(end - started) + " : middle for " + nanosRead(middle - started)
          + " in thread " + Thread.currentThread().getName());

      } else {

        String id = RND.str(3);

        long started = System.nanoTime();
        int count = portion.getAsInt();
        for (int i = 0; i < count; i++) {
          Client client = new Client();
          client.id = id + "-" + i;
          client.surname = RND.str(10);
          client.name = ((i == 10) || (i % 2000 == 0)) ? "err" : "ok";

          mainProducer
            .sending(client)
            .toTopic(TOPIC_CLIENT)
            .setAuthor("manager-" + RND.plusInt(10))
            .kafkaId("k-id-" + RND.str(5))
            .go()
            .awaitAndGet();

          clientTotalCount++;

        }

        System.out.println("g5v43gh2v5 :: Inserted " + count
          + " clients for " + nanosRead(System.nanoTime() - started)
          + " in thread " + Thread.currentThread().getName());
      }

      insertedPortions++;

      if (insertClientPortion.run()) {
        break;
      }


    }

    long finishedAt = System.nanoTime();

    System.out.println("5jb426hb :: Inserted " + insertedPortions +
      " portions and " + clientTotalCount + " clients for " + nanosRead(finishedAt - startedAt));

  }

  public void ping() throws IOException {
    if (insertClientPortion.run()) {
      execute();
    }
  }
}
