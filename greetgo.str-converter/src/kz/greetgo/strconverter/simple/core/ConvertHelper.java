package kz.greetgo.strconverter.simple.core;

import kz.greetgo.strconverter.simple.acceptors.AcceptorManager;
import kz.greetgo.strconverter.simple.errors.AliasAlreadyRegistered;
import kz.greetgo.strconverter.simple.errors.ClassAlreadyRegistered;
import kz.greetgo.strconverter.simple.errors.NoRegisteredClassForAlias;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConvertHelper {
  public final Map<String, Class<?>> aliasClassMap = new ConcurrentHashMap<>();
  public final Map<Class<?>, String> classAliasMap = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<Class<?>, AcceptorManager> classAcceptorManagerMap = new ConcurrentHashMap<>();

  public AcceptorManager getAcceptorManager(Class<?> aClass) {
    return classAcceptorManagerMap.computeIfAbsent(aClass, AcceptorManager::new);
  }

  public AcceptorManager getAcceptorManager(String alias) {
    Class<?> aClass = aliasClassMap.get(alias);
    if (aClass == null) {
      throw new NoRegisteredClassForAlias(alias);
    }
    return getAcceptorManager(aClass);
  }


  public synchronized void useClass(Class<?> aClass, String alias) {
    checkAvailableChars(alias);

    Class<?> alreadyClass = aliasClassMap.get(alias);

    if (alreadyClass != null) {

      if (alreadyClass.equals(aClass)) {
        return;
      }

      throw new AliasAlreadyRegistered(alias, aClass, alreadyClass);
    }

    String alreadyAlias = classAliasMap.get(aClass);

    if (alreadyAlias != null) {

      if (alreadyAlias.equals(alias)) {
        return;
      }


      throw new ClassAlreadyRegistered(aClass, alias, alreadyAlias);
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
