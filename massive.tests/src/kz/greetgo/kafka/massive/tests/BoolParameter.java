package kz.greetgo.kafka.massive.tests;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class BoolParameter {

  private final String name;
  private final AtomicBoolean valueCatcher;
  private final Path trueFile, falseFile;

  public BoolParameter(Path workingDir, String name, AtomicBoolean valueCatcher) throws IOException {
    this.name = name;
    this.valueCatcher = valueCatcher;
    trueFile = workingDir.resolve(name + "-true");
    falseFile = workingDir.resolve(name + "-false");

    workingDir.toFile().getParentFile().mkdirs();

    initFiles();


  }

  private void initFiles() throws IOException {
    boolean trueExists = trueFile.toFile().exists();
    boolean falseExists = falseFile.toFile().exists();

    if (trueExists && falseExists || !trueExists && !falseExists) {
      saveValue();
    } else {
      valueCatcher.set(trueExists);
    }

  }

  public BoolParameter(Path workingDir, String name, boolean initialValue) throws IOException {
    this(workingDir, name, new AtomicBoolean(initialValue));
  }

  public void ping() throws IOException {

    boolean trueExists = trueFile.toFile().exists();
    boolean falseExists = falseFile.toFile().exists();

    if (trueExists && falseExists) {
      saveValue();
      return;
    }

    if (!trueExists && !falseExists) {
      valueCatcher.set(!valueCatcher.get());
      System.out.println("SetValue: " + name + " = " + valueCatcher.get());
      saveValue();
      return;
    }

  }

  private void saveValue() throws IOException {
    boolean value = valueCatcher.get();

    if (value) {
      falseFile.toFile().delete();
      trueFile.toFile().createNewFile();
    } else {
      trueFile.toFile().delete();
      falseFile.toFile().createNewFile();
    }

  }

  public boolean value() {
    return valueCatcher.get();
  }

}
