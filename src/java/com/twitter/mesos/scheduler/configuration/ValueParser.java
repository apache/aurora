package com.twitter.mesos.scheduler.configuration;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.StringUtils;

/**
 * A simple parser interface to handle value parsing and type casting.
 *
 * @param <T> The value type.
 *
 * @author William Farner
 */
public interface ValueParser<T> {

  /**
   * Parses a string as a value of the target type.
   *
   * @param s The string value to parse.
   * @return The parsed value.
   * @throws ParseException If the value could not be parsed.
   */
  T parse(String s) throws ParseException;

  /**
   * Similar to {@link #parse(String)}, but with a default value if the string is empty.
   *
   * @param s The string value to parse.
   * @param defaultValue Default value to apply when {@code s} is empty.
   * @return The parsed value.
   * @throws ParseException If the value could not be parsed.
   */
  T parseWithDefault(String s, T defaultValue) throws ParseException;

  /**
   * Thrown when a value cannot be parsed to the target type.
   */
  public class ParseException extends Exception {
    public ParseException(String msg) {
      super(msg);
    }
  }

  /**
   * String parser.
   */
  public static class StringParser implements ValueParser<String> {
    @Override public String parse(String s) throws ParseException {
      if (StringUtils.isEmpty(s)) {
        throw new ParseException("value must not be empty.");
      }
      return s;
    }

    @Override public String parseWithDefault(String s, String defaultValue) {
      if (StringUtils.isEmpty(s)) {
        return defaultValue;
      }
      return s;
    }
  }

  /**
   * Integer parser.
   */
  public static class IntegerParser implements ValueParser<Integer> {
    @Override public Integer parse(String s) throws ParseException {
      try {
        return Integer.parseInt(s);
      } catch (NumberFormatException e) {
        throw new ParseException("Invalid integer value: " + e.getMessage());
      }
    }

    @Override public Integer parseWithDefault(String s, Integer defaultValue)
        throws ParseException {

      if (StringUtils.isEmpty(s)) {
        return defaultValue;
      }
      return parse(s);
    }
  }

  /**
   * Long parser.
   */
  public static class LongParser implements ValueParser<Long> {
    @Override public Long parse(String s) throws ParseException {
      try {
        return Long.parseLong(s);
      } catch (NumberFormatException e) {
        throw new ParseException("Invalid long value: " + e.getMessage());
      }
    }

    @Override public Long parseWithDefault(String s, Long defaultValue) throws ParseException {
      return StringUtils.isEmpty(s) ? defaultValue : parse(s);
    }
  }

  /**
   * Double parser.
   */
  public static class DoubleParser implements ValueParser<Double> {
    @Override public Double parse(String s) throws ParseException {
      try {
        return Double.parseDouble(s);
      } catch (NumberFormatException e) {
        throw new ParseException("Invalid double value: " + e.getMessage());
      }
    }

    @Override public Double parseWithDefault(String s, Double defaultValue) throws ParseException {
      return StringUtils.isEmpty(s) ? defaultValue : parse(s);
    }
  }

  /**
   * Boolean parser.
   */
  public static class BooleanParser implements ValueParser<Boolean> {
    @Override public Boolean parse(String s) throws ParseException {
      try {
        return Boolean.parseBoolean(s);
      } catch (NumberFormatException e) {
        throw new ParseException("Invalid boolean value: " + e.getMessage());
      }
    }

    @Override
    public Boolean parseWithDefault(String s, Boolean defaultValue) throws ParseException {
      return StringUtils.isEmpty(s) ? defaultValue : parse(s);
    }
  }

  /**
   * Utility class containing a registry of parsers by type class.
   */
  public static final class Registry {
    private static final ImmutableMap<Class, ValueParser> REGISTRY =
        ImmutableMap.<Class, ValueParser>of(
          String.class, new StringParser(),
          Integer.class, new IntegerParser(),
          Long.class, new LongParser(),
          Double.class, new DoubleParser(),
          Boolean.class, new BooleanParser()
    );

    private Registry() {
      // Utility class.
    }

    /**
     * Gets a parser of the target type.
     *
     * @param type Type to get a parser for.
     * @param <T> Value parser type.
     * @return An optional value parser for the requested type.
     */
    @SuppressWarnings("unchecked")
    public static <T> Optional<ValueParser<T>> getParser(Class<T> type) {
      return Optional.<ValueParser<T>>fromNullable(REGISTRY.get(type));
    }
  }
}
