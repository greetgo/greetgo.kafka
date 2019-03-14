package kz.greetgo.kafka2.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NetUtil {

  public static boolean canConnectToAnyBootstrapServer(String bootstrapServers) {

    for (String hostPort : bootstrapServers.split(",")) {

      if (canConnectTo(hostPort)) {
        return true;
      }

    }

    return false;
  }

  public static boolean canConnectTo(String hostPort) {
    String[] split = hostPort.split(":");
    if (split.length != 2) {
      throw new IllegalArgumentException("hostPort must contain only one comma");
    }

    String host = split[0].trim();
    int port = Integer.parseInt(split[1].trim());

    try (Socket socket = new Socket(host, port)) {

      OutputStream outputStream = socket.getOutputStream();
      outputStream.write("1\r\n\r\n".getBytes(UTF_8));
      outputStream.flush();

      InputStream inputStream = socket.getInputStream();
      //noinspection ResultOfMethodCallIgnored
      inputStream.read();

      return true;

    } catch (IOException e) {

      if (e instanceof ConnectException) {
        return false;
      }

      if (e instanceof UnknownHostException) {
        return false;
      }

      throw new RuntimeException(e);
    }
  }

}
