package kz.greetgo.kafka.consumer;

import kz.greetgo.kafka.core.config.ConfigContent;
import kz.greetgo.kafka.core.config.ConfigEventType;
import kz.greetgo.kafka.core.config.EventConfigFile;
import kz.greetgo.kafka.core.config.EventRegistration;
import kz.greetgo.kafka.util.Handler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static kz.greetgo.kafka.consumer.ConsumerConfigDefaults.parameterDefinitionList;

public class ConsumerConfigFileWorker {


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
    return hostContent.get().getIntValue("out.worker.count").orElse(0);
  }

  public Map<String, Object> getConfigMap() {
    return hostContent.get().getConfigMap("con.");
  }

  public Duration pollDuration() {
    return Duration.ofMillis(hostContent.get().getLongValue("out.poll.duration.ms").orElse(800L));
  }

  public void close() {
    hostConfigEventRegistration.unregister();
    parentConfigEventRegistration.unregister();
  }


  private ConfigContent createParentConfigContent() {

    List<String> lines = new ArrayList<>();

    for (ParameterDefinition pd : parameterDefinitionList()) {
      lines.add(pd.parameterName + " = " + pd.defaultValue);
    }

    return new ConfigContent(String.join("\n", lines).getBytes(UTF_8));

  }

  private ConfigContent createHostConfigContent() {

    ConfigContent parent = this.parentContent.get();

    List<String> lines = new ArrayList<>();
    for (ParameterDefinition pd : parameterDefinitionList()) {
      if (parent.parameterExists(pd.parameterName)) {
        lines.add(pd.parameterName + " : inherits");
      } else {
        lines.add(pd.parameterName + "=" + pd.defaultValue);
      }
    }

    return new ConfigContent(String.join("\n", lines).getBytes(UTF_8), this.parentContent::get);

  }

  public void start() {
    {
      byte[] contentInBytes = parentConfig.readContent();
      if (contentInBytes != null) {
        ConfigContent content = new ConfigContent(contentInBytes);
        parentContent.set(content);
        parentConfigError.writeContent(content.generateErrorsInBytes());
      } else {
        ConfigContent content = createParentConfigContent();
        parentContent.set(content);
        parentConfig.writeContent(content.contentInBytes);
        parentConfigError.delete();
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
        hostConfig.writeContent(content.contentInBytes);
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
    parentConfig.writeContent(content.contentInBytes);
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
    hostConfig.writeContent(content.contentInBytes);
    hostConfigError.writeContent(content.generateErrorsInBytes());
    hostConfigActualValues.writeContent(content.generateActualValuesInBytes());

    configDataChanged.handler();
  }

}
