package org.apache.impala.common.serialize;

import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Reference implementation to demonstrate how the serialization API can
 * be implemented to build a JSON object tree. Uses JSON Simple because that
 * is already an Impala dependency.
 *
 * Note that JSON Simple uses a Map to represent an object, which loses
 * field order. Therefore, this is not a good implementation to use for
 * testing frameworks which require a deterministic field order.
 */
public abstract class JsonSerializer extends AbstractTreeSerializer {

  public static class JsonObjectSerializer extends AbstractObjectSerializer {
    private final JsonSerializer serializer_;
    private final JSONObject obj_;

    public JsonObjectSerializer(JsonSerializer serializer) {
      serializer_ = serializer;
      obj_ = new JSONObject();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void field(String name, String value) {
      obj_.put(name, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ObjectSerializer object(String name) {
      JsonObjectSerializer child = new JsonObjectSerializer(serializer_);
      obj_.put(name, child.obj_);
      return child;
    }

    @Override
    public ToJsonOptions options() {
      return serializer_.options();
    }

    @Override
    public void text(String sourceField, String sql) {
      // TODO Auto-generated method stub

    }

    @SuppressWarnings("unchecked")
    @Override
    public ArraySerializer array(String name) {
      JsonArraySerializer array = new JsonArraySerializer(serializer_);
      obj_.put(name,  array.array_);
      return array;
    }

    @Override
    public void objList(String name, List<? extends JsonSerializable> objs) {
      if (objs == null) return;
      ArraySerializer as = array(name);
      for (JsonSerializable obj : objs) {
        obj.serialize(as.object());
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void scalar(String name, Object value) {
      obj_.put(name, value);
    }
  }

  public static class JsonArraySerializer implements ArraySerializer {
    private final JsonSerializer serializer_;
    private final JSONArray array_;

    public JsonArraySerializer(JsonSerializer serializer) {
      serializer_ = serializer;
      array_ = new JSONArray();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void value(String value) {
      array_.add(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void value(long value) {
      array_.add(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void scalar(Object value) {
      array_.add(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ObjectSerializer object() {
      JsonObjectSerializer child = new JsonObjectSerializer(serializer_);
      array_.add(child.obj_);
      return child;
    }

    @Override
    public ToJsonOptions options() {
      return serializer_.options();
    }

    @Override
    public void object(JsonSerializable obj) {
      if (obj == null) {
        if (!options().elide()) scalar(null);
      } else {
        obj.serialize(object());
      }
    }
  }

  public static class JsonStringSerializer extends JsonSerializer {

    public JsonStringSerializer(ToJsonOptions options) {
      super(options);
    }

    @Override
    public void close() { }

    @Override
    public String toString() { return root_.obj_.toJSONString(); }
  }

  protected final JsonObjectSerializer root_;

  public JsonSerializer(ToJsonOptions options) {
    super(options);
    root_ = new JsonObjectSerializer(this);
  }

  @Override
  public ObjectSerializer root() {
    return root_;
  }
}
