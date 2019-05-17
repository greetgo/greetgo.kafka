package kz.greetgo.strconverter.simple.acceptors;

import kz.greetgo.strconverter.simple.core.NameValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NameValueList {
  private final List<String> names = new ArrayList<>();
  private final Map<String, Object> values = new HashMap<>();

  public void add(String name, Object value) {
    names.add(name);
    values.put(name, value);
  }

  public List<NameValue> list() {
    return names.stream()
      .map(name -> new NameValue(name, values.get(name)))
      .collect(Collectors.toList());
  }

  public Object getValue(String name) {
    return values.get(name);
  }

}
