package kz.greetgo.kafka.probes.more;

import kz.greetgo.kafka.core.HasId;

public abstract class ChangeClient  implements HasId {
  public String id;

  @Override
  public String getId() {
    return id;
  }
  
}
