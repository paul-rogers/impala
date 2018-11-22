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

package org.apache.impala.util.treevis;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.impala.analysis.Expr;

/**
 * Visualizes a tree structure. Walks the tree, printing most
 * fields of most objects. Excludes fields from the Object class,
 * objects that would cause a cycle, and those specifically requested
 * to ignored. Prints scalar-like values directly, else expands objects
 * and array-like objects (including collections.)
 *
 * Objects can implement a method to do their own visualization:
 *
 *   public void visualize(Visualizer vis);
 *
 * If this method exists, the visualizer calls it instead of walking
 * the object's fields.
 */
public class Visualizer {

  /**
   * Performs actual visualization of the tree based on
   * a set of events.
   */
  public interface TreeVisualizer {
    void startObj(String name, Object obj);
    void startArray(String name);
    void field(String name, Object value);
    void special(String name, String value);
    void endArray();
    void endObj();
  }

  private static final Class<?> STD_SCALARS[] = {
      Byte.class,
      Integer.class,
      Character.class,
      Long.class,
      Float.class,
      Double.class,
      String.class,
      Boolean.class,
      Enum.class,
      AtomicLong.class,
      BigDecimal.class,
      BigInteger.class
  };

  private TreeVisualizer treeVis_;
  private Set<Class<?>> scalarTypes_ = new HashSet<>();
  private Set<Class<?>> ignoreTypes_ = new HashSet<>();
  // Infinite recursion preventer.
  // Since there is no IdentityHashMap. Values ignored.
  private Map<Object, Object> parents_ = new IdentityHashMap<>();

  public Visualizer(TreeVisualizer treeVis) {
    this.treeVis_ = treeVis;
    for (Class<?> c : STD_SCALARS) {
      scalarTypes_.add(c);
    }
  }

  /**
   * Specify a class to ignore. Objects of this type are
   * skipped during visualization, with a special message
   * for that field in place of object expansion.
   *
   * @param cls the class to skip
   */
  public void ignore(Class<?> cls) {
    ignoreTypes_.add(cls);
  }

  /**
   * Specifies a scalar-like class to be rendered using a simple
   * toString() call.
   *
   * @param cls the class to render using toString()
   */
  public void scalar(Class<?> cls) {
    scalarTypes_.add(cls);
  }

  /**
   * Visualization entry point.
   *
   * @param root the object to visualize
   */
  public void visualize(Object root) {
    treeVis_.startObj("<root>", root);
    visit(root);
    treeVis_.endObj();
  }

  public void visit(Object obj) {
    Class<?> objClass = obj.getClass();
    Method visMethod;
    try {
      visMethod = objClass.getMethod("visualize", getClass());
    } catch (NoSuchMethodException e) {
      visualizeObj(obj);
      return;
    } catch (SecurityException e) {
      throw new IllegalStateException(e);
    }
    try {
      visMethod.invoke(obj, this);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new IllegalStateException(e);
    }
  }

  private void visualizeObj(Object obj) {
    if (obj instanceof Expr) {
      System.out.print("");
    }
    Class<?> objClass = obj.getClass();
    visualizeMembers(obj, objClass);
  }

  private void visualizeMembers(Object obj, Class<?> objClass) {
    if (objClass == Object.class) return;
    Class<?> parent = objClass.getSuperclass();
    visualizeMembers(obj, parent);
    Field[] fields = objClass.getDeclaredFields();
    for (Field field : fields) {
      if (Modifier.isStatic(field.getModifiers())) { continue; }
      String name = field.getName();
      try {
        field.setAccessible(true);
        Object value = field.get(obj);
        visitValue(name, value);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        treeVis_.special(name, "<unavailable>");
      }
    }
  }

  public void visitValue(String name, Object value) {
    if (value == null) {
      treeVis_.field(name, value);
      return;
    }
    Class<?> valueClass = value.getClass();
    if (valueClass.isArray()) {
      visualizeArray(name, value);
      return;
    }
    if (scalarTypes_.contains(valueClass)) {
      treeVis_.field(name, value);
      return;
    }
    if (ignoreTypes_.contains(valueClass)) {
      treeVis_.special(name, "<Skip " + valueClass.getSimpleName() + ">");
      return;
    }
    if (parents_.containsKey(value)) {
      treeVis_.special(name, "<Back pointer to " + valueClass.getSimpleName() + ">");
      return;
    }
    parents_.put(value, value);
    if (value instanceof Collection) {
      visitCollection(name, (Collection<?>) value);
    } else {
      treeVis_.startObj(name, value);
      visit(value);
      treeVis_.endObj();
    }
    parents_.remove(value);
  }

  private void visualizeArray(String name, Object value) {
    // TODO: Specially handle scalar arrays
    int len = Array.getLength(value);
    if (len == 0) {
      treeVis_.special(name, "[]");
      return;
    }
    treeVis_.startArray(name);
    for (int i = 0; i < len; i++) {
      visitValue(Integer.toString(i), Array.get(value, i));
    }
    treeVis_.endArray();
  }

  private void visitCollection(String name, Collection<?> col) {
    if (col.isEmpty()) {
      treeVis_.special(name, "[]");
      return;
    }
    treeVis_.startArray(name);
    if (col instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) col;
      for (Entry<?, ?> entry : map.entrySet()) {
        visitValue(entry.getKey().toString(), entry.getValue());
      }
    } else {
      int i = 0;
      for (Object member : col) {
        visitValue(Integer.toString(i++), member);
      }
    }
    treeVis_.endArray();
  }
}
