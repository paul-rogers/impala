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

import static org.junit.Assert.assertEquals;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.impala.common.serialize.JsonTreeFormatter.TreeBuilder;
import org.junit.Test;

public class TreeFormatterTest {

  @Test
  public void testBuilder() {
    {
      // Trivial case
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      builder.close();
      assertEquals("", strWriter.toString());
    }
    {
      // Trivial root
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      assertEquals(1, builder.root());
      builder.close();
      assertEquals("{\n}\n", strWriter.toString());
    }
    {
      // Fields
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.root();
      builder.unquotedField(level, "f1", null);
      builder.unquotedField(level, "f2", 10);
      builder.quotedField(level, "f3", null);
      builder.quotedField(level, "f4", "abc");
      builder.close();
      assertEquals(
          "{\n  f1: null,\n  f2: 10,\n  f3: null,\n  f4: \"abc\"\n}\n",
          strWriter.toString());
    }
    {
      // Text with special characters
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.objectElement(0);
      builder.quotedField(level, "f", "abc \"foo\nbar\\\"");
      builder.close();
      assertEquals(
          "{\n  f: \"abc \\\"foo\nbar\\\\\\\"\"\n}\n",
          strWriter.toString());
    }
    {
      // Block text
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.root();
      builder.textField(level, "f", "abc \"foo\nbar\\\"");
      builder.close();
      assertEquals(
          "{\n  f:\n\"abc \\\"foo\nbar\\\\\\\"\"\n}\n",
          strWriter.toString());
    }
    {
      // Quoted fields
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.root();
      builder.unquotedField(level, "f_1", null);
      builder.unquotedField(level, "f 2", 10);
      builder.quotedField(level, "f-3", null);
      builder.quotedField(level, "f.4", "abc");
      builder.close();
      assertEquals(
          "{\n  f_1: null,\n  \"f 2\": 10,\n  \"f-3\": null,\n  \"f.4\": \"abc\"\n}\n",
          strWriter.toString());
    }
    {
      // Empty nested object at end
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.root();
      builder.unquotedField(level, "f1", 10);
      assertEquals(2, builder.objectField(level, "obj"));
      builder.close();
      assertEquals("{\n  f1: 10,\n  obj: {\n  }\n}\n", strWriter.toString());
    }
    {
      // Nested object at end
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.root();
      int level2 = builder.objectField(level, "obj");
      builder.unquotedField(level2, "g", 20);
      builder.close();
      assertEquals("{\n  obj: {\n    g: 20\n  }\n}\n", strWriter.toString());
    }
    {
      // Nested object in middle
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.root();
      int level2 = builder.objectField(level, "obj");
      builder.unquotedField(level2, "g", 20);
      builder.unquotedField(level, "h", 30);
      builder.close();
      assertEquals(
          "{\n  obj: {\n    g: 20\n  },\n  h: 30\n}\n",
          strWriter.toString());
    }
    {
      // Two roots
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.root();
      builder.unquotedField(level, "f", 10);
      assertEquals(1, builder.root());
      builder.unquotedField(level, "f", 20);
      builder.close();
      assertEquals(
          "{\n  f: 10\n},\n{\n  f: 20\n}\n",
          strWriter.toString());
    }
    {
      // Two nested objects
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.root();
      int level2 = builder.objectField(level, "obj1");
      builder.unquotedField(level2, "g", 10);
      assertEquals(level2, builder.objectField(level, "obj2"));
      builder.unquotedField(level2, "g", 20);
      builder.close();
      assertEquals(
          "{\n  obj1: {\n    g: 10\n  },\n  obj2: {\n    g: 20\n  }\n}\n",
          strWriter.toString());
    }
    {
      // Empty Array
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.root();
      assertEquals(2, builder.arrayField(level, "a"));
      builder.close();
      assertEquals("{\n  a: [\n  ]\n}\n", strWriter.toString());
    }
    {
      // Array
      StringWriter strWriter = new StringWriter();
      TreeBuilder builder = new TreeBuilder(new PrintWriter(strWriter));
      int level = builder.root();
      int level2 = builder.arrayField(level, "a");
      builder.unquotedElement(level2, 10);
      builder.unquotedElement(level2, null);
      builder.quotedElement(level2, "foo");
      builder.quotedElement(level2, null);
      builder.close();
      assertEquals("{\n  a: [\n    10,\n    null,\n    \"foo\",\n    null\n  ]\n}\n", strWriter.toString());
    }
  }

  public static class DummyObject implements JsonSerializable {
    int value_;

    public DummyObject(int value) { value_ = value; }

    @Override
    public void serialize(ObjectSerializer os) {
      os.field("d", value_);
    }
  }

  @Test
  public void testSerialize() {
    {
      JsonTreeFormatter s = new JsonTreeFormatter(ToJsonOptions.full());
      ObjectSerializer os = s.root();
      os.field("bool", true);
      assertEquals("{\n  bool: true\n}\n", s.toString());
    }
    {
      JsonTreeFormatter s = new JsonTreeFormatter(ToJsonOptions.full());
      ObjectSerializer os = s.root();
      os.field("long", 5);
      assertEquals("{\n  long: 5\n}\n", s.toString());
    }
    {
      JsonTreeFormatter s = new JsonTreeFormatter(ToJsonOptions.full());
      ObjectSerializer os = s.root();
      os.field("str", "foo");
      assertEquals("{\n  str: \"foo\"\n}\n", s.toString());
    }
    {
      JsonTreeFormatter s = new JsonTreeFormatter(ToJsonOptions.full());
      ObjectSerializer os = s.root();
      os.field("null", null);
      assertEquals("{\n  null: null\n}\n", s.toString());
    }
    {
      // Duplicate entries
      JsonTreeFormatter s = new JsonTreeFormatter(ToJsonOptions.fullCompact());
      ObjectSerializer os = s.root();
      DummyObject obj = new DummyObject(10);
      os.object("first", obj);
      os.object("second", new DummyObject(20));
      os.object("third", null);
      os.object("fourth", obj);
      assertEquals(
          "{\n  first: {\n    object_id: 1,\n    d: 10\n  },\n  second: {\n" +
          "    object_id: 2,\n    d: 20\n  },\n  fourth: \"<1>\"\n}\n",
          s.toString());
    }
    {
      // Duplicate entries
      JsonTreeFormatter s = new JsonTreeFormatter(
          ToJsonOptions.fullCompact().elide(false));
      ObjectSerializer os = s.root();
      DummyObject obj = new DummyObject(10);
      os.object("first", obj);
      os.object("second", new DummyObject(20));
      os.object("third", null);
      os.object("fourth", obj);
      assertEquals(
          "{\n  first: {\n    object_id: 1,\n    d: 10\n  },\n  second: {\n" +
          "    object_id: 2,\n    d: 20\n  },\n  third: null,\n" +
          "  fourth: \"<1>\"\n}\n",
          s.toString());
    }
    {
      // Elision
      JsonTreeFormatter s = new JsonTreeFormatter(
          ToJsonOptions.fullCompact().elide(true));
      ObjectSerializer os = s.root();
      os.field("f1", true);
      os.field("f2", false);
      os.elidable("f3", true);
      os.elidable("f4", false);
      os.field("f5", "foo");
      os.field("f6", null);
      assertEquals(
          "{\n  f1: true,\n  f2: false,\n  f3: true,\n  f5: \"foo\"\n}\n",
          s.toString());
    }
    {
      // No elision
      JsonTreeFormatter s = new JsonTreeFormatter(
          ToJsonOptions.fullCompact().elide(false));
      ObjectSerializer os = s.root();
      os.field("f1", true);
      os.field("f2", false);
      os.elidable("f3", true);
      os.elidable("f4", false);
      os.field("f5", "foo");
      os.field("f6", null);
      assertEquals(
          "{\n  f1: true,\n  f2: false,\n  f3: true,\n  f4: false,\n" +
          "  f5: \"foo\",\n  f6: null\n}\n",
          s.toString());
    }
  }
}
