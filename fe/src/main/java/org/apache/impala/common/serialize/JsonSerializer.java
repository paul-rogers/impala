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
  /**
   * JSON-Simple based object serializer.
   */
  public static class JsonObjectSerializer extends AbstractObjectSerializer {
    private final JsonSerializer serializer_;
    private final JSONObject obj_;

    public JsonObjectSerializer(JsonSerializer serializer) {
      serializer_ = serializer;
      obj_ = new JSONObject();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void field(String name, long value) {
      obj_.put(name, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void field(String name, double value) {
      obj_.put(name, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void field(String name, boolean value) {
      obj_.put(name, value);
    }

    // JSON-Simple uses un-parameterized Map objects as its JSON objects.
    // These trigger unchecked warnings when used. Since we can't change
    // JSON-Simple to use parametrized maps, we suppress the warnings.
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
    public void text(String name, String value) {
      // JSON objects don't differentiate between regular and
      // multi-line strings.
      field(name, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ArraySerializer array(String name) {
      JsonArraySerializer array = new JsonArraySerializer(serializer_);
      obj_.put(name, array.array_);
      return array;
    }

    @Override
    public void objectList(String name, List<? extends JsonSerializable> objs) {
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

    @Override
    protected AbstractTreeSerializer serializer() {
      return serializer_;
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

  /**
   * Form of the JSON-Simple serializer that converts the JSON tree
   * to a string. The resulting string is correct JSON, but not to helpful
   * for testing: the JSON is all on one line and the order of fields is
   * non-deterministic. Still, this form is useful when returning JSON
   * to an external system that expects standard, compact JSON.
   */
  public static class JsonStringSerializer extends JsonSerializer {
    public JsonStringSerializer(ToJsonOptions options) {
      super(options);
    }

    @Override
    public void close() {}

    @Override
    public String toString() { return root_.obj_.toJSONString(); }
  }

  protected final JsonObjectSerializer root_;

  public JsonSerializer(ToJsonOptions options) {
    super(options);
    root_ = new JsonObjectSerializer(this);
  }

  @Override
  public ObjectSerializer root() { return root_; }
}
