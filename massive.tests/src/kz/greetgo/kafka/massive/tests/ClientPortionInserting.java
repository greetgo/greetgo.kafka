package kz.greetgo.kafka.massive.tests;

import kz.greetgo.kafka.massive.tests.model.Client;
import kz.greetgo.kafka.producer.KafkaFuture;
import kz.greetgo.kafka.producer.ProducerFacade;
import kz.greetgo.util.RND;

import java.util.ArrayList;
import java.util.List;

import static kz.greetgo.kafka.massive.tests.TimeUtil.nanosRead;

public class ClientPortionInserting {
  private final LongParameter portion;
  private final LongParameter portionCount;
  private final ProducerFacade mainProducer;
  private final BoolParameter parallel;

  public ClientPortionInserting(LongParameter portion,
                                LongParameter portionCount,
                                ProducerFacade mainProducer,
                                BoolParameter parallel) {
    this.portion = portion;
    this.portionCount = portionCount;
    this.mainProducer = mainProducer;
    this.parallel = parallel;
  }

  public void execute() {
    boolean parallel = this.parallel.value();

    int portionCount = this.portionCount.getAsInt();

    long startedAt = System.nanoTime();
    int clientTotalCount = 0;

    for (int u = 0; u < portionCount; u++) {

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
            .toTopic("CLIENT")
            .go());

          clientTotalCount++;

        }

        long middle = System.nanoTime();

        futures.forEach(KafkaFuture::awaitAndGet);

        long end = System.nanoTime();

        System.out.println("5hb4326gv :: Inserted " + count + " clients for " + nanosRead(end - started)
          + " : middle for " + nanosRead(middle - started));

      } else {

        String id = RND.str(3);

        long started = System.nanoTime();
        int count = portion.getAsInt();
        for (int i = 0; i < count; i++) {
          Client client = new Client();
          client.id = id + "-" + i;
          client.surname = RND.str(10);
          client.name = i == 10 ? "err" : "ok";

          mainProducer
            .sending(client)
            .toTopic("CLIENT")
            .go()
            .awaitAndGet();
        }

        System.out.println("g5v43gh2v5 :: Inserted " + count
          + " clients for " + nanosRead(System.nanoTime() - started));
      }

    }

    long finishedAt = System.nanoTime();

    System.out.println("5jb426hb :: Inserted " + portionCount +
      " portions and " + clientTotalCount + " clients for " + nanosRead(finishedAt - startedAt));

  }
}
