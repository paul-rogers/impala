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

public abstract class AbstractTreeSerializer implements TreeSerializer {

  public static abstract class AbstractObjectSerializer implements ObjectSerializer {

    @Override
    public void field(String name, long value) {
      scalar(name, value);
    }

    @Override
    public void field(String name, double value) {
      scalar(name, value);
    }

    @Override
    public void field(String name, boolean value) {
      scalar(name, value);
    }

    @Override
    public void elidable(String name, boolean value) {
      if (value || !options().elide()) scalar(name, value);
    }

    @Override
    public void object(String name, JsonSerializable obj) {
      if (obj != null)
        obj.serialize(this);
      else if (!options().elide())
        scalar(name, null);
     }

    @Override
    public void objList(String name, List<? extends JsonSerializable> objs) {
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
