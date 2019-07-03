package kz.greetgo.kafka.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class ConsumerConfigDefaults {
  private final List<ParameterDefinition> parameterDefinitionList = new ArrayList<>();
  private final Map<String, ParameterDefinition> parameterDefinitionMap = new HashMap<>();
  private static final Map<String, ParameterValueValidator> validatorMap;

  static {
    Map<String, ParameterValueValidator> vMap = new HashMap<>();
    vMap.put("Long", new ParameterValueValidatorLong());
    vMap.put("Int", new ParameterValueValidatorInt());
    vMap.put("Str", new ParameterValueValidatorStr());
    validatorMap = unmodifiableMap(vMap);
  }

  public static ConsumerConfigDefaults withDefaults() {
    ConsumerConfigDefaults consumerConfigDefaults = new ConsumerConfigDefaults();
    consumerConfigDefaults.addDefaults();
    return consumerConfigDefaults;
  }

  public void addDefaults() {
    addDefinition(" Long   con.auto.commit.interval.ms           1000  ");
    addDefinition(" Long   con.session.timeout.ms               30000  ");
    addDefinition(" Long   con.heartbeat.interval.ms            10000  ");
    addDefinition(" Long   con.fetch.min.bytes                      1  ");
    addDefinition(" Long   con.max.partition.fetch.bytes      1048576  ");
    addDefinition(" Long   con.connections.max.idle.ms         540000  ");
    addDefinition(" Long   con.default.api.timeout.ms           60000  ");
    addDefinition(" Long   con.fetch.max.bytes               52428800  ");
    addDefinition(" Long   con.max.poll.interval.ms            300000  ");
    addDefinition(" Long   con.max.poll.records                   500  ");
    addDefinition(" Long   con.receive.buffer.bytes             65536  ");
    addDefinition(" Long   con.request.timeout.ms               30000  ");
    addDefinition(" Long   con.send.buffer.bytes               131072  ");
    addDefinition(" Long   con.fetch.max.wait.ms                  500  ");

    addDefinition(" Int out.worker.count        1  ");
    addDefinition(" Int out.poll.duration.ms  800  ");
  }

  public void addDefinition(String definitionStr) {
    String[] split = definitionStr.trim().split("\\s+");
    ParameterValueValidator validator = requireNonNull(validatorMap.get(split[0]));
    String parameterName = split[1];
    String defaultValue = split[2];

    ParameterDefinition definition = new ParameterDefinition(parameterName, defaultValue, validator);
    parameterDefinitionList.add(definition);
    parameterDefinitionMap.put(definition.parameterName, definition);
  }

  public List<ParameterDefinition> parameterDefinitionList() {
    return parameterDefinitionList;
  }

  private static final
  ParameterDefinition DEFAULT_PARAMETER_DEFINITION = new ParameterDefinition(null, "", new ParameterValueValidatorStr());

  public ParameterDefinition getDefinition(String parameterName) {
    requireNonNull(parameterName);

    {
      ParameterDefinition pd = parameterDefinitionMap.get(parameterName);
      if (pd != null) {
        return pd;
      }
    }

    return DEFAULT_PARAMETER_DEFINITION;
  }
}
