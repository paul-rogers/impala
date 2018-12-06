package org.apache.impala.common.serialize;

/**
 * Marks a class as able to participate in the object helper methods
 * in {@link ObjectSerializer}. An object need not implement this interface
 * if the object is explicitly serialized, only if the object is passed
 * into a helper method.
 */
public interface JsonSerializable {
  void serialize(ObjectSerializer os);
}
