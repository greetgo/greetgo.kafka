package kz.greetgo.kafka.consumer;

public interface InnerProducer<Model> {

  void send(Model model);

}
