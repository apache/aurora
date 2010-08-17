package com.twitter.mesos.codec;

/**
 * Interface definition for a codec.
 *
 * @author wfarner
 */
public interface Codec<A, B> {

  public B encode(A b) throws CodingException;

  public A decode(B a) throws CodingException;

  public static class CodingException extends Exception {
    public CodingException(String msg, Throwable t) {
      super(msg, t);
    }

    public CodingException(String msg) {
      super(msg);
    }
  }
}
