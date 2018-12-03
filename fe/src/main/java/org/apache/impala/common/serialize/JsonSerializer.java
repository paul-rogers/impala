package org.apache.impala.common.serialize;

import org.json.simple.JSONObject;

public abstract class JsonSerializer implements TreeSerializer {

  public static class JsonObjectSerializer implements ObjectSerializer {
    private final JsonSerializer serializer_;
    private final JSONObject obj_;

    public JsonObjectSerializer(JsonSerializer serializer) {
      serializer_ = serializer;
      obj_ = new JSONObject();
    }

    @Override
    public void field(String name, String value) {
      obj_.put(name, value);
    }

    @Override
    public void field(String name, long value) {
      obj_.put(name, value);
    }

    @Override
    public void field(String name, double value) {
      obj_.put(name, value);
    }

    @Override
    public ObjectSerializer childObject(String name) {
      JsonObjectSerializer child = new JsonObjectSerializer(serializer_);
      obj_.put(name, child.obj_);
      return child;
    }

    @Override
    public ToJsonOptions options() {
      // TODO Auto-generated method stub
      return null;
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

  private final ToJsonOptions options_;
  protected final JsonObjectSerializer root_;

  public JsonSerializer(ToJsonOptions options) {
    options_ = options;
    root_ = new JsonObjectSerializer(this);
  }

  @Override
  public ObjectSerializer root() {
    return root_;
  }

  @Override
  public ToJsonOptions options() {
     return options_;
  }
}
