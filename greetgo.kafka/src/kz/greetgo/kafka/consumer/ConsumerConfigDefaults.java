package kz.greetgo.kafka.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class ConsumerConfigDefaults {
  private static final List<ParameterDefinition> parameterDefinitionList;
  private static final Map<String, ParameterDefinition> parameterDefinitionMap;
  private static final Map<String, ParameterValueValidator> validatorMap;

  static {
    Map<String, ParameterValueValidator> vMap = new HashMap<>();
    vMap.put("Long", new ParameterValueValidatorLong());
    vMap.put("Int", new ParameterValueValidatorInt());
    vMap.put("Str", new ParameterValueValidatorStr());
    validatorMap = unmodifiableMap(vMap);

    List<ParameterDefinition> list = new ArrayList<>();

    addDefinition(list, " Long   con.auto.commit.interval.ms           1000  ");
    addDefinition(list, " Long   con.session.timeout.ms               30000  ");
    addDefinition(list, " Long   con.heartbeat.interval.ms            10000  ");
    addDefinition(list, " Long   con.fetch.min.bytes                      1  ");
    addDefinition(list, " Long   con.max.partition.fetch.bytes      1048576  ");
    addDefinition(list, " Long   con.connections.max.idle.ms         540000  ");
    addDefinition(list, " Long   con.default.api.timeout.ms           60000  ");
    addDefinition(list, " Long   con.fetch.max.bytes               52428800  ");
    addDefinition(list, " Long   con.max.poll.interval.ms            300000  ");
    addDefinition(list, " Long   con.max.poll.records                   500  ");
    addDefinition(list, " Long   con.receive.buffer.bytes             65536  ");
    addDefinition(list, " Long   con.request.timeout.ms               30000  ");
    addDefinition(list, " Long   con.send.buffer.bytes               131072  ");
    addDefinition(list, " Long   con.fetch.max.wait.ms                  500  ");

    addDefinition(list, " Int out.worker.count        1  ");
    addDefinition(list, " Int out.poll.duration.ms  800  ");

    parameterDefinitionList = unmodifiableList(list);
    parameterDefinitionMap = unmodifiableMap(list.stream().collect(toMap(x -> x.parameterName, x -> x)));
  }

  private static void addDefinition(List<ParameterDefinition> list, String definitionStr) {
    String[] split = definitionStr.trim().split("\\s+");
    ParameterValueValidator validator = requireNonNull(validatorMap.get(split[0]));
    String parameterName = split[1];
    String defaultValue = split[2];

    list.add(new ParameterDefinition(parameterName, defaultValue, validator));
  }

  public static List<ParameterDefinition> parameterDefinitionList() {
    return parameterDefinitionList;
  }

  private static final
  ParameterDefinition DEFAULT_PARAMETER_DEFINITION = new ParameterDefinition(null, "", new ParameterValueValidatorStr());

  public static ParameterDefinition getDefinition(String parameterName) {
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
