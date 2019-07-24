package kz.greetgo.kafka.massive.tests;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class Command {

  private final String name;
  private final Path commandFile;

  public Command(Path workingDir, String name) throws IOException {
    this.name = name;
    commandFile = workingDir.resolve(name);
    commandFile.toFile().getParentFile().mkdirs();
    commandFile.toFile().createNewFile();
  }

  public boolean run() throws IOException {
    File file = commandFile.toFile();

    if (file.exists()) {
      return false;
    }

    file.createNewFile();
    System.out.println("Run command " + name);
    return true;
  }

}
