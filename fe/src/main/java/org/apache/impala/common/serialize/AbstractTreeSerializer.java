package org.apache.impala.common.serialize;

import java.util.List;

public abstract class AbstractTreeSerializer implements TreeSerializer {

  public static abstract class AbstractObjectSerializer implements ObjectSerializer {

    @Override
    public void field(String name, long value) {
      unquoted(name, value);
    }

    @Override
    public void field(String name, double value) {
      unquoted(name, value);
    }

    @Override
    public void field(String name, boolean value) {
      unquoted(name, value);
    }

    @Override
    public void objList(String name, List<? extends JsonSerializable> objs) {
      if (objs == null || objs.isEmpty()) return;
      ArraySerializer as = array(name);
      for (JsonSerializable obj : objs) {
        obj.serialize(as.object());
      }
    }

    @Override
    public void strList(String name, List<String> values) {
      if (values == null || values.isEmpty()) return;
      ArraySerializer as = array(name);
      for (String str : values) {
        as.value(str);
      }
    }

    @Override
    public void strList(String name, String values[]) {
      if (values == null || values.length == 0) return;
      ArraySerializer as = array(name);
      for (String str : values) {
        as.value(str);
      }
    }

    protected abstract void unquoted(String name, Object value);
  }

  protected final ToJsonOptions options_;

  public AbstractTreeSerializer(ToJsonOptions options) {
    options_ = options;
  }

  @Override
  public ToJsonOptions options() {
     return options_;
  }
}
