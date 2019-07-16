package kz.greetgo.kafka.massive.tests;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MassiveTestServer {
  public static void main(String[] args) throws IOException {

    Path pwd = new File(".").getAbsoluteFile().toPath().normalize();

    Path workingDir = pwd.resolve("build").resolve("MassiveTestServer");

    Files.createDirectories(workingDir);

    Path workingFile = workingDir.resolve("working.file");

    Files.createFile(workingFile);



    System.out.println(pwd);

  }
}
