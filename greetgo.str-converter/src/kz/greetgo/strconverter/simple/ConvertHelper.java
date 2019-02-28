package kz.greetgo.strconverter.simple;

import java.util.HashMap;
import java.util.Map;

public class ConvertHelper {
  public final Map<String, Class<?>> aliasClassMap = new HashMap<>();
  public final Map<Class<?>, String> classAliasMap = new HashMap<>();

  private final Map<Class<?>, AcceptorManager> classAcceptorManagerMap = new HashMap<>();

  public synchronized AcceptorManager getAcceptorManager(Class<?> aClass) {

    AcceptorManager acceptorManager = classAcceptorManagerMap.get(aClass);

    if (acceptorManager != null) return acceptorManager;

    classAcceptorManagerMap.put(aClass, acceptorManager = new AcceptorManager(aClass));

    return acceptorManager;
  }

  public AcceptorManager getAcceptorManager(String alias) {
    Class<?> aClass = aliasClassMap.get(alias);
    if (aClass == null) throw new RuntimeException("No alias " + alias);
    return getAcceptorManager(aClass);
  }


  public synchronized void useClass(Class<?> aClass, String alias) {
    checkAvailableChars(alias);

    Class<?> alreadyClass = aliasClassMap.get(alias);
    String alreadyAlias = classAliasMap.get(aClass);

    if (alreadyClass != null && alreadyAlias != null) {
      if (alreadyClass.equals(aClass) && alreadyAlias.equals(alias)) return;

      throw new IllegalArgumentException("Alias " + alias + " already registered for " + alreadyClass
          + ", and " + aClass + " already registered for " + alreadyClass);
    }

    if (alreadyClass != null) {
      throw new IllegalArgumentException("Alias " + alias + " already registered for " + alreadyClass);
    }

    if (alreadyAlias != null) {
      throw new IllegalArgumentException(aClass + " already registered for alias " + alreadyAlias);
    }

    aliasClassMap.put(alias, aClass);
    classAliasMap.put(aClass, alias);
  }

  private void checkAvailableChars(String alias) {
    if (alias.length() == 0) throw new RuntimeException("Alias cannot be empty");

    if (!isJavaVariableFirstChar(alias.charAt(0))) {
      throw new RuntimeException("First character of alias must be letter");
    }

    for (int i = 1, n = alias.length(); i < n; i++) {
      if (!isJavaVariableChar(alias.charAt(i))) {
        throw new RuntimeException("Characters of alias must be letters or digits: alias = " + alias);
      }
    }
  }

  public static boolean isJavaVariableChar(char c) {
    if (isJavaVariableFirstChar(c)) return true;
    if (c == '$') return true;
    if (c == '_') return true;
    return Character.isDigit(c);
  }

  public static boolean isJavaVariableFirstChar(char c) {
    if (c == '$') return true;
    if (c == '_') return true;
    return Character.isLetter(c);
  }

  public Object createObjectWithAlias(String alias) {
    Class<?> aClass = aliasClassMap.get(alias);
    if (aClass == null) throw new RuntimeException("No alias " + alias);
    try {
      return aClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public String getAliasForOrThrowError(Class<?> aClass) {
    String alias = classAliasMap.get(aClass);
    if (alias == null) throw new RuntimeException("No alias for " + aClass);
    return alias;
  }
}
