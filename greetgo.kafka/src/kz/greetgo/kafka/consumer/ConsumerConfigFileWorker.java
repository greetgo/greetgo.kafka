package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.config.ConfigContent;
import kz.greetgo.kafka.core.config.ConfigEventType;
import kz.greetgo.kafka.core.config.EventConfigFile;
import kz.greetgo.kafka.core.config.EventRegistration;
import kz.greetgo.kafka.util.Handler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class ConsumerConfigFileWorker {

  private static final List<ParameterDefinition> parameterDefinitionList;
  private static final Map<String, ParameterValueValidator> validatorMap;

  static {
    Map<String, ParameterValueValidator> vMap = new HashMap<>();
    vMap.put("Long", new ParameterValueValidatorLong());
    vMap.put("Int", new ParameterValueValidatorInt());
    vMap.put("Str", new ParameterValueValidatorStr());
    validatorMap = Collections.unmodifiableMap(vMap);

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

    parameterDefinitionList = Collections.unmodifiableList(list);
  }

  private static void addDefinition(List<ParameterDefinition> list, String definitionStr) {
    String[] split = definitionStr.trim().split("\\s+");
    ParameterValueValidator validator = requireNonNull(validatorMap.get(split[0]));
    String parameterName = split[1];
    String defaultValue = split[2];

    list.add(new ParameterDefinition(parameterName, defaultValue, validator));
  }

  private final Handler configDataChanged;
  private final EventConfigFile parentConfig;
  private final EventConfigFile parentConfigError;
  private final EventConfigFile hostConfig;
  private final EventConfigFile hostConfigError;
  private final EventConfigFile hostConfigActualValues;
  private final EventRegistration parentConfigEventRegistration;
  private final EventRegistration hostConfigEventRegistration;

  public ConsumerConfigFileWorker(Handler configDataChanged,
                                  EventConfigFile parentConfig, EventConfigFile parentConfigError,
                                  EventConfigFile hostConfig, EventConfigFile hostConfigError,
                                  EventConfigFile hostConfigActualValues) {

    this.configDataChanged = configDataChanged;
    this.parentConfig = parentConfig;
    this.parentConfigError = parentConfigError;
    this.hostConfig = hostConfig;
    this.hostConfigError = hostConfigError;
    this.hostConfigActualValues = hostConfigActualValues;

    parentConfigEventRegistration = parentConfig.addEventHandler(type -> {
      if (type == ConfigEventType.UPDATE) {
        parentConfigUpdated();
      }
    });
    hostConfigEventRegistration = hostConfig.addEventHandler(type -> {
      if (type == ConfigEventType.UPDATE) {
        hostConfigUpdated();
      }
    });
  }

  private final AtomicReference<ConfigContent> parentContent = new AtomicReference<>(null);
  private final AtomicReference<ConfigContent> hostContent = new AtomicReference<>(null);


  public int getWorkerCount() {
    return hostContent.get().getLongValue("out.worker.count");
  }

  public Map<String, Object> getConfigMap() {
    return hostContent.get().getConfigMap("con.");
  }

  public Duration pollDuration() {
    return Duration.ofMillis(hostContent.get().getLongValue("out.poll.duration.ms"));
  }

  public void close() {
    hostConfigEventRegistration.unregister();
    parentConfigEventRegistration.unregister();
  }


  private ConfigContent createParentConfigContent() {

    List<String> lines = new ArrayList<>();

    for (ParameterDefinition pd : parameterDefinitionList) {
      lines.add(pd.parameterName + "=" + pd.defaultValue);
    }

    return new ConfigContent(String.join("\n", lines).getBytes(UTF_8));

  }

  private ConfigContent createHostConfigContent() {

    ConfigContent parent = this.parentContent.get();

    List<String> lines = new ArrayList<>();
    for (ParameterDefinition pd : parameterDefinitionList) {
      if (parent.parameterExists(pd.parameterName)) {
        lines.add(pd.parameterName + " : inherits");
      } else {
        lines.add(pd.parameterName + "=" + pd.defaultValue);
      }
    }

    return new ConfigContent(String.join("\n", lines).getBytes(UTF_8));

  }

  public void start() {
    {
      byte[] contentInBytes = parentConfig.readContent();
      if (contentInBytes == null) {
        ConfigContent content = createParentConfigContent();
        parentContent.set(content);
        parentConfigError.delete();
      } else {
        ConfigContent content = new ConfigContent(contentInBytes);
        parentContent.set(content);
        parentConfigError.writeContent(content.generateErrorsInBytes());
      }
    }

    parentConfig.ensureLookingFor();

    {
      byte[] contentInBytes = hostConfig.readContent();
      if (contentInBytes != null) {
        ConfigContent content = new ConfigContent(contentInBytes, parentContent::get);
        hostContent.set(content);
        hostConfigError.writeContent(content.generateErrorsInBytes());
        hostConfigActualValues.writeContent(content.generateActualValuesInBytes());
      } else {
        ConfigContent content = createHostConfigContent();
        hostConfigError.delete();
        hostContent.set(content);
        hostConfigActualValues.writeContent(content.generateActualValuesInBytes());
      }
    }

    hostConfig.ensureLookingFor();

  }

  private void parentConfigUpdated() {
    byte[] contentInBytes = parentConfig.readContent();
    if (contentInBytes == null) {
      return;
    }

    ConfigContent content = new ConfigContent(contentInBytes);
    parentContent.set(content);
    parentConfigError.writeContent(content.generateErrorsInBytes());

    configDataChanged.handler();
  }

  private void hostConfigUpdated() {
    byte[] contentInBytes = hostConfig.readContent();
    if (contentInBytes == null) {
      return;
    }

    ConfigContent content = new ConfigContent(contentInBytes, parentContent::get);
    hostContent.set(content);
    hostConfigError.writeContent(content.generateErrorsInBytes());
    hostConfigActualValues.writeContent(content.generateActualValuesInBytes());

    configDataChanged.handler();
  }

}
