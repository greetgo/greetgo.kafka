package kz.greetgo.kafka.core;

import com.esotericsoftware.kryo.Kryo;

public interface KryoPreparation {

  void prepareKryo(Kryo kryo);

}
