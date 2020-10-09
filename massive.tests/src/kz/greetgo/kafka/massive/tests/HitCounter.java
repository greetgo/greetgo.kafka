package kz.greetgo.kafka.massive.tests;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class HitCounter {


  private static class HitKey {
    final String graphName;
    final String timestamp;

    private HitKey(String graphName) {
      this.graphName = requireNonNull(graphName);
      SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
      this.timestamp = sdf.format(new Date());
    }

    String sortKey() {
      return graphName + '-' + timestamp;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + '{' + sortKey() + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      HitKey hitKey = (HitKey) o;

      if (!graphName.equals(hitKey.graphName)) return false;
      if (!timestamp.equals(hitKey.timestamp)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = graphName.hashCode();
      result = 31 * result + timestamp.hashCode();
      return result;
    }
  }

  private final ConcurrentHashMap<HitKey, Long> hits = new ConcurrentHashMap<>();

  public void hit(String graphName) {
    hits.compute(new HitKey(graphName), (x, value) -> value == null || value == 0 ? 1 : value + 1);
  }

  public void clear() {
    hits.clear();
  }

  public void show(Path hitCounterDir) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss");
    File file = hitCounterDir.resolve("report-hitCounter-" + sdf.format(new Date()) + ".txt").toFile();
    try (PrintStream out = new PrintStream(file, "UTF-8")) {

      hits.entrySet()
        .stream()
        .sorted(Comparator.comparing(x -> x.getKey().sortKey()))
        .forEachOrdered(e -> {
          out.println(e.getKey() + " - " + e.getValue());
        });

    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}
