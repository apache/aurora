package com.twitter.nexus.util;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 * SampleTask
 *
 * @author Florian Leibert
 */
public class SampleTask implements Runnable {

  public static void main(String[] args) {
    new SampleTask().run();
  }

  @Override
  public void run() {
    InetSocketAddress address = new InetSocketAddress(0);
    try {
      HttpServer server = HttpServer.create(address, 10);
      server.createContext("/", new MyHandler());
      server.setExecutor(Executors.newCachedThreadPool());
      server.start();
      System.out.println("Server is listening on port:"+server.getAddress().getPort());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  class MyHandler implements HttpHandler {
    public void handle(HttpExchange exchange) throws IOException {
      String requestMethod = exchange.getRequestMethod();
      if (requestMethod.equalsIgnoreCase("GET")) {
        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set("Content-Type", "text/plain");
        exchange.sendResponseHeaders(200, 0);

        OutputStream responseBody = exchange.getResponseBody();
        Headers requestHeaders = exchange.getRequestHeaders();
        Set<String> keySet = requestHeaders.keySet();
        Iterator<String> iter = keySet.iterator();
        while (iter.hasNext()) {
          String key = iter.next();
          List values = requestHeaders.get(key);
          String s = key + " = " + values.toString() + "\n";
          responseBody.write(s.getBytes());
        }
        responseBody.close();
      }
    }
  }
}
