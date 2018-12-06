package org.apache.impala.common.serialize;

import java.util.List;

/**
 * Interface to serialize the contents of an object.
 */
public interface ObjectSerializer {
  /**
   * Serialization options to be honored by the serializer: both here
   * and the client which drives serialization.
   */
  ToJsonOptions options();
  /**
   * Emit a quoted string field, or null.
   */
  void field(String name, String value);
  void field(String name, long value);
  void field(String name, double value);
  void field(String name, boolean value);
  /**
   * Emit the boolean field but only if non-null when the
   * elide option is off. (Allows more concise JSON by assuming
   * the default false value.)
   */
  void elidable(String name, boolean value);
  /**
   * Emit a value, unquoed, using its toString() value.
   * Use this for scalar values to be converted to non-quoted
   * strings for display.
   */
  void scalar(String name, Object value);
  /**
   * Emit a multi-line text value, which will be wrapped for
   * display.
   */
  void text(String sourceField, String sql);
  /**
   * Serializer for a child object.
   */
  ObjectSerializer object(String name);
  /**
   * Serialize a child object. Omits the field if ellision is enabled
   * and the object is null.
   */
  void object(String name, JsonSerializable obj);
  /**
   * Serializer for a child array.
   */
  ArraySerializer array(String name);
  /**
   * Serialize a list of objects. Omits the field if ellision is
   * enabled and the list is null or empty. Else, if ellision is
   * disabled, and the list is null, displays "null".
   */
  void objList(String name, List<? extends JsonSerializable> objs);
  /**
   * Serialize a list of quoted string values. Ellision as for
   * {@link #objList(String, List)}.
   */
  void strList(String name, List<String> values);
  /**
   * Serialize an array of quoted strings.Ellision as for
   * {@link #objList(String, List)}.
   */
  void strList(String name, String values[]);
  /**
   * Serialize a list of unquoted scalar values. Ellision as for
   * {@link #objList(String, List)}.
   * @param name
   * @param values
   */
  void scalarList(String name, List<?> values);
}