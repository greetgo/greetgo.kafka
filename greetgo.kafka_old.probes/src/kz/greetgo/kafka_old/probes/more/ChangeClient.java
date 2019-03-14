package kz.greetgo.kafka_old.probes.more;

import kz.greetgo.kafka_old.core.HasId;

public abstract class ChangeClient  implements HasId {
  public String id;

  @Override
  public String getId() {
    return id;
  }
  
}
