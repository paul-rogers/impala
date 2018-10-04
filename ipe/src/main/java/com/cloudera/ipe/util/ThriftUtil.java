package com.cloudera.ipe.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Helper methods for thrift
 */
public class ThriftUtil {
  /**
   * Utility method to create a Thrift client.
   * @throws TTransportException
   */
  public static <T extends TServiceClient> T createClient(
      TServiceClientFactory<T> factory,
      InetSocketAddress addr) throws TTransportException {
    TTransport t = new TSocket(addr.getHostName(), addr.getPort());
    t.open();
    TProtocol p = new TBinaryProtocol(t);
    T client = factory.getClient(p);
    return client;
  }

  /**
   * Converts a Thrift object to a JSON string.
   *
   * Disclaimer from Thrift source: TSimpleJSONProtocol is write-only and
   * produces a simple output format suitable for parsing by scripting languages.
   */
  public static String thriftToJson(TBase<?,?> data) throws TException {
    TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
    String json = serializer.toString(data);
    return json;
  }

  /**
   * Converts a byte array into a thrift object.
   * @param input
   * @param target
   * @throws IOException
   */
  public static <T extends TBase<?, ?>> void read(
      byte[] input,
      T target,
      TProtocolFactory factory) throws IOException {
    try {
      new TDeserializer(factory).deserialize(target, input);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  /**
   * Converts a thrift object into a byte array
   * @param input
   * @return
   * @throws IOException
   */
  public static <T extends TBase<?, ?>> byte[] write(
      T input,
      TProtocolFactory factory) throws IOException {
    try {
      return new TSerializer(factory).serialize(input);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public static <T extends TBase<?, ?>> void read(
      InputStream input,
      T target,
      TProtocolFactory factory) throws IOException {
    TProtocol protocol = null;
    TTransport trans = null;
    try {
      trans = new TIOStreamTransport(input);
      trans.open();
      protocol = factory.getProtocol(trans);
      target.read(protocol);
    } catch (TException e) {
      throw new IOException(e);
    } finally {
      if (protocol != null) { protocol.reset(); }
      if (trans != null) { trans.close(); }
    }
  }
}
