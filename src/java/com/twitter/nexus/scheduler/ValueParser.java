package com.twitter.nexus.scheduler;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;

/**
 * A simple parser interface to handle value parsing and type casting.
 *
 * @author wfarner
 */
public interface ValueParser<T> {
  public T parse(String s) throws ParseException;
  public T parseWithDefault(String s, T defaultValue) throws ParseException;

  public class ParseException extends Exception {
    public ParseException(String msg) {
      super(msg);
    }
  }

  public static class StringParser implements ValueParser<String> {
    @Override
    public String parse(String s) throws ParseException {
      if (StringUtils.isEmpty(s)) throw new ParseException("value must not be empty.");
      return s;
    }

    @Override
    public String parseWithDefault(String s, String defaultValue) {
      if (StringUtils.isEmpty(s)) return defaultValue;
      return s;
    }
  }

  public static class IntegerParser implements ValueParser<Integer> {
    @Override
    public Integer parse(String s) throws ParseException {
      try {
        return Integer.parseInt(s);
      } catch (NumberFormatException e) {
        throw new ParseException("Invalid integer value: " + e.getMessage());
      }
    }

    @Override
    public Integer parseWithDefault(String s, Integer defaultValue) throws ParseException {
      if (StringUtils.isEmpty(s)) return defaultValue;
      return parse(s);
    }
  }

  public static class LongParser implements ValueParser<Long> {
    @Override
    public Long parse(String s) throws ParseException {
      try {
        return Long.parseLong(s);
      } catch (NumberFormatException e) {
        throw new ParseException("Invalid long value: " + e.getMessage());
      }
    }

    @Override
    public Long parseWithDefault(String s, Long defaultValue) throws ParseException {
      if (StringUtils.isEmpty(s)) return defaultValue;
      return parse(s);
    }
  }

  public static class DoubleParser implements ValueParser<Double> {
    @Override
    public Double parse(String s) throws ParseException {
      try {
        return Double.parseDouble(s);
      } catch (NumberFormatException e) {
        throw new ParseException("Invalid double value: " + e.getMessage());
      }
    }

    @Override
    public Double parseWithDefault(String s, Double defaultValue) throws ParseException {
      if (StringUtils.isEmpty(s)) return defaultValue;
      return parse(s);
    }
  }

  public static class BooleanParser implements ValueParser<Boolean> {
    @Override
    public Boolean parse(String s) throws ParseException {
      try {
        return Boolean.parseBoolean(s);
      } catch (NumberFormatException e) {
        throw new ParseException("Invalid double value: " + e.getMessage());
      }
    }

    @Override
    public Boolean parseWithDefault(String s, Boolean defaultValue) throws ParseException {
      if (StringUtils.isEmpty(s)) return defaultValue;
      return parse(s);
    }
  }

  public static final ImmutableMap<Class, ValueParser> REGISTRY =
      ImmutableMap.<Class, ValueParser>of(
        String.class, new StringParser(),
        Integer.class, new IntegerParser(),
        Long.class, new LongParser(),
        Double.class, new DoubleParser(),
        Boolean.class, new BooleanParser()
  );
}
