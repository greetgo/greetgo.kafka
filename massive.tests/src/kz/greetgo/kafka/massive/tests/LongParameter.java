package kz.greetgo.kafka.massive.tests;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

public class LongParameter {
  private final Path valueFile, setFile;
  private final String name;
  private final AtomicLong valueCatcher;

  public LongParameter(Path workingDir, String name, long initialValue) throws IOException {
    this(workingDir, name, new AtomicLong(initialValue));
  }

  public LongParameter(Path workingDir, String name, AtomicLong valueCatcher) throws IOException {
    this.name = name;
    this.valueCatcher = valueCatcher;
    valueFile = workingDir.resolve(name).resolve("value.txt");
    setFile = workingDir.resolve(name).resolve("set");

    setFile.toFile().getParentFile().mkdirs();

    if (!valueFile.toFile().exists()) {
      writeCurrentValue();
    } else {
      readCurrentValue();
    }

    setFile.toFile().createNewFile();
  }

  private void readCurrentValue() throws IOException {
    valueCatcher.set(Long.parseLong(new String(Files.readAllBytes(valueFile), StandardCharsets.UTF_8).trim()));
    System.out.println("SetValue: " + name + " = " + valueCatcher.get());
  }

  private void writeCurrentValue() throws IOException {
    Files.write(valueFile, ("" + valueCatcher.get()).getBytes(StandardCharsets.UTF_8));
    System.out.println("ReadValue: " + name + " = " + valueCatcher.get());
  }

  public void ping() throws IOException {

    if (!valueFile.toFile().exists()) {
      writeCurrentValue();
    } else if (!setFile.toFile().exists()) {
      setFile.toFile().createNewFile();
      readCurrentValue();
    }

  }

  public int getAsInt() {
    return (int) valueCatcher.get();
  }

  public long get() {
    return valueCatcher.get();
  }
}
