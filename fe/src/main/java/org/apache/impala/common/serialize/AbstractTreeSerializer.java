// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.common.serialize;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Base tree serializer implementation
 */
public abstract class AbstractTreeSerializer implements TreeSerializer {
  /**
   * Base object serializer implementation with common shortcuts.
   */
  public static abstract class AbstractObjectSerializer implements ObjectSerializer {

    @Override
    public void elidable(String name, boolean value) {
      if (value || !options().elide()) scalar(name, value);
    }

    @Override
    public void object(String name, JsonSerializable obj) {
      // If the object is null, emit: field_name: null
      if (obj == null) {
        if (!options().elide()) scalar(name, null);
        return;
      }
      // If dedup is not enabled, emit the object as a full JSON object:
      // field_name: { ... }
      if (!options().dedup()) {
        obj.serialize(object(name));
        return;
      }
      // Dedup is enabled and we've seen the object. Emit a reference
      // to the object in the form: field_name: "<n>", where n is the
      // object ID. Note that this is non-standard JSON, but it helps
      // to reduce the size of test files.
      int id = serializer().visit(obj);
      if (id >= 0) {
        field(name, "<" + id + ">");
        return;
      }
      // If dedup is enabled, and we've not seen the object before,
      // emit the object as a full JSON object with object id:
      // field_name: { object_id: n, ... }
      ObjectSerializer os = object(name);
      os.field("object_id", -id);
      obj.serialize(os);
    }

    @Override
    public void objectList(String name, List<? extends JsonSerializable> objs) {
      if (objs == null) {
        if (!options().elide()) scalar(name, null);
        return;
      }
      if (options().elide() && objs.isEmpty()) return;
      ArraySerializer as = array(name);
      for (JsonSerializable obj : objs) {
        as.object(obj);
      }
    }

    @Override
    public void stringList(String name, List<String> values) {
      if (values == null || values.isEmpty()) return;
      ArraySerializer as = array(name);
      for (String str : values) {
        as.value(str);
      }
    }

    @Override
    public void stringList(String name, String values[]) {
      if (values == null || values.length == 0) return;
      ArraySerializer as = array(name);
      for (String str : values) {
        as.value(str);
      }
    }

    @Override
    public void scalarList(String name, List<?> values) {
      if (values == null) {
        if (!options().elide()) scalar(name, null);
        return;
      }
      ArraySerializer as = array(name);
      for (Object value : values) {
        as.scalar(value);
      }
    }

    protected abstract AbstractTreeSerializer serializer();

    @Override
    public ToJsonOptions options() {
      return serializer().options();
    }
  }

  public abstract static class AbstractArraySerializer implements ArraySerializer {

    @Override
    public void object(JsonSerializable obj) {
      // If the object is null, emit "null"
      if (obj == null) {
        if (!options().elide()) scalar(null);
        return;
      }
      // If dedup is not enabled, serialize the object per standard JSON.
      if (!options().dedup()) {
        obj.serialize(object());
        return;
      }
      // If dedup is enabled, and we've seen the object before, emit "<n>"
      // which is non-standard JSON.
      int id = serializer().visit(obj);
      if (id >= 0) {
        value("<" + id + ">");
        return;
      }
      // Dedup enabled, have not seen the object before. Emit the object with an
      // object ID.
      ObjectSerializer os = object();
      os.field("object_id", -id);
      obj.serialize(os);
    }

    protected abstract AbstractTreeSerializer serializer();

    @Override
    public ToJsonOptions options() {
      return serializer().options();
    }
  }

  protected final ToJsonOptions options_;
  private final Map<Object,Integer> refsMap_ = new IdentityHashMap<>();

  public AbstractTreeSerializer(ToJsonOptions options) {
    options_ = options;
  }

  @Override
  public ToJsonOptions options() {
     return options_;
  }

  protected int visit(Object obj) {
    Integer id = refsMap_.get(obj);
    if (id != null) return id;
    id = refsMap_.size() + 1;
    refsMap_.put(obj, id);
    return -id;
  }
}
