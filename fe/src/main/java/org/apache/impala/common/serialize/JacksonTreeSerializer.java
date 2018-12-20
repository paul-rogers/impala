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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.impala.analysis.Expr;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

/**
 * JSON serializer which produces a streaming, formatted JSON output using the
 * <a href="https://github.com/FasterXML/jackson-docs/wiki/JacksonStreamingApi">
 * Jackson Streaming API</a>. Uses custom print formatting that omits quotes
 * around field names, and optionally omits values that are null or at defaults
 * and optionally omits empty lists,
 *
 * This version can be extended to expose the Jackson settings so that it can
 * produce fully standards-compliant JSON, and omit pretty-printing.
 *
 * The JacksonTreeStringSerializer version writes to a string which can be used
 * in tests.
 *
 * Supports object dedup. Each object carries an object_id which is determined
 * by the order that objects are emitted, and thus is consistent across runs. If
 * an object has already been seen, emits a reference to the object in the form:
 * field_name: "<n>", where n is the object ID. This is non-standard JSON, but
 * helps with trees (such as the analyzer AST) which has many references to the
 * same object.
 *
 * Note that dedup is not standard JSON and should be enabled if the goal is to
 * write JSON consumable by other tools.
 */
public class JacksonTreeSerializer extends AbstractTreeSerializer {
  /**
   * Object serializer that generates formatted JSON output via the
   * Jackson streaming API.
   */
  private static class ObjectFormatter extends AbstractObjectSerializer {
    private final JacksonTreeSerializer formatter_;
    private int level_;

    public ObjectFormatter(JacksonTreeSerializer formatter, int level) {
      formatter_ = formatter;
      level_ = level;
    }

    @Override
    public void field(String name, String value) {
      if (value != null || !options().elide()) {
        formatter_.builder_.stringField(level_, name, value);
      }
    }

    @Override
    public void scalar(String name, Object value) {
      if (value == null) {
        formatter_.builder_.nullField(level_, name);
      } else if (value instanceof Integer) {
        field(name, (Integer) value);
      } else if (value instanceof Long) {
        field(name, (Long) value);
      } else if (value instanceof Float) {
        field(name, (Float) value);
      } else if (value instanceof Double) {
        field(name, (Double) value);
      } else if (value instanceof Boolean) {
        field(name, (Boolean) value);
      } else {
        field(name, value.toString());
      }
    }

    @Override
    public ObjectSerializer object(String name) {
      return new ObjectFormatter(formatter_,
          formatter_.builder_.objectField(level_, name));
    }

    @Override
    protected AbstractTreeSerializer serializer() {
      return formatter_;
    }

    @Override
    public void text(String name, String value) {
      formatter_.builder_.stringField(level_, name, value);
    }

    @Override
    public ArraySerializer array(String name) {
      return new ArrayFormatter(formatter_,
          formatter_.builder_.arrayField(level_, name));
    }

    @Override
    public void field(String name, long value) {
      formatter_.builder_.longField(level_, name, value);
    }

    @Override
    public void field(String name, double value) {
      formatter_.builder_.doubleField(level_, name, value);
    }

    @Override
    public void field(String name, boolean value) {
      formatter_.builder_.booleanField(level_, name, value);
    }
  }

  /**
   * Array serializer that generates formatted JSON output via the
   * Jackson streaming API.
   */
  public static class ArrayFormatter extends AbstractArraySerializer {
    private final JacksonTreeSerializer formatter_;
    private int level_;

    public ArrayFormatter(JacksonTreeSerializer formatter, int level) {
      formatter_ = formatter;
      level_ = level;
    }

    @Override
    public void value(String value) {
      formatter_.builder_.stringElement(level_, value);
    }

    @Override
    public void value(long value) {
      formatter_.builder_.longElement(level_, value);
    }

    public void value(double value) {
      formatter_.builder_.doubleElement(level_, value);
    }

    public void value(boolean value) {
      formatter_.builder_.booleanElement(level_, value);
    }

    @Override
    public void scalar(Object value) {
      if (value == null) {
        formatter_.builder_.nullElement(level_);
      } else if (value instanceof Integer) {
        value((Integer) value);
      } else if (value instanceof Long) {
        value((Long) value);
      } else if (value instanceof Float) {
        value((Float) value);
      } else if (value instanceof Double) {
        value((Double) value);
      } else if (value instanceof Boolean) {
        value((Boolean) value);
      } else {
        value(value.toString());
      }
    }

    @Override
    public ObjectSerializer object() {
      return new ObjectFormatter(formatter_,
          formatter_.builder_.objectElement(level_));
    }

    @Override
    protected AbstractTreeSerializer serializer() {
      return formatter_;
    }
  }

  /**
   * Custom pretty printer that prints compact fields:
   * foo: value
   *
   * Rather than the defult:
   * foo : value
   */
  @SuppressWarnings("serial")
  private static class CustomPrettyPrinter extends DefaultPrettyPrinter {
    public CustomPrettyPrinter() {
      _objectFieldValueSeparatorWithSpaces = DEFAULT_SEPARATORS.getObjectFieldValueSeparator() + " ";
    }
  }

  /**
   * Internal shim class that converts from the serializer protocol to the Jackson
   * streaming protocol. Automatically tracks object and array levels, closing
   * inner objects/arrays on the next write to an outer level.
   */
  private static class TreeBuilder {
    private static final int MAX_DEPTH = Expr.EXPR_DEPTH_LIMIT + 20;

    private final JsonGenerator jg_;
    private int depth_ = -1;
    private final boolean isObject_[] = new boolean[MAX_DEPTH];

    public TreeBuilder(Writer out) throws IOException {
      JsonFactory jsonFactory = new JsonFactory();
      jg_ = jsonFactory.createGenerator(out);
      jg_.disable(Feature.QUOTE_FIELD_NAMES);
      jg_.setPrettyPrinter(new CustomPrettyPrinter());
    }

    public int root() {
      popTo(-1);
      try {
        isObject_[++depth_] = true;
        jg_.writeStartObject();
        return depth_;
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public int objectField(int level, String name) {
      popTo(level);
      try {
        jg_.writeFieldName(name);
        isObject_[++depth_] = true;
        jg_.writeStartObject();
        return depth_;
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public int arrayField(int level, String name) {
      popTo(level);
      try {
        jg_.writeFieldName(name);
        isObject_[++depth_] = false;
        jg_.writeStartArray();
        return depth_;
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void longField(int level, String name, long value) {
      popTo(level);
      try {
        jg_.writeNumberField(name, value);
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void doubleField(int level, String name, double value) {
      popTo(level);
      try {
        jg_.writeNumberField(name, value);
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void stringField(int level, String name, String value) {
      popTo(level);
      try {
        jg_.writeStringField(name, value);
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void booleanField(int level, String name, boolean value) {
      popTo(level);
      try {
        jg_.writeBooleanField(name, value);
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void nullField(int level, String name) {
      popTo(level);
      try {
        jg_.writeNullField(name);
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public int objectElement(int level) {
      popTo(level);
      try {
        isObject_[++depth_] = true;
        jg_.writeStartObject();
        return depth_;
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void longElement(int level, long value) {
      popTo(level);
      try {
        jg_.writeNumber(value);
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void doubleElement(int level, double value) {
      popTo(level);
      try {
        jg_.writeNumber(value);
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void stringElement(int level, String value) {
      popTo(level);
      try {
        jg_.writeString(value);
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void booleanElement(int level, boolean value) {
      popTo(level);
      try {
        jg_.writeBoolean(value);
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void nullElement(int level) {
      popTo(level);
      try {
        jg_.writeNull();
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }

    public void popTo(int level) {
      while (depth_ > level) {
        try {
          if (isObject_[depth_--]) jg_.writeEndObject();
          else jg_.writeEndArray();
        } catch (IOException e) {
          throw new SerializationError(e);
        }
      }
    }

    public void close() {
      popTo(-1);
      try {
        jg_.close();
      } catch (IOException e) {
        throw new SerializationError(e);
      }
    }
  }

  /**
   * Writes JSON as a string.
   */
  public static class JacksonTreeStringSerializer extends JacksonTreeSerializer {
    private final StringWriter stringWriter_;

    public JacksonTreeStringSerializer(StringWriter writer, ToJsonOptions options) {
      super(writer, options);
      this.stringWriter_ = writer;
    }

    public static JacksonTreeStringSerializer create(ToJsonOptions options) {
      return new JacksonTreeStringSerializer(new StringWriter(), options);
    }

    @Override
    public String toString() {
      close();
      return stringWriter_.getBuffer().append("\n").toString();
    }
  }

  private final TreeBuilder builder_;
  protected final ObjectFormatter root_;
  protected boolean closed_;

  public JacksonTreeSerializer(Writer out, ToJsonOptions options) {
    super(options);
    try {
      builder_ = new TreeBuilder(out);
      root_ = new ObjectFormatter(this, builder_.root());
    } catch (IOException e) {
      throw new SerializationError(e);
    }
  }

  @Override
  public ObjectSerializer root() {
    return root_;
  }

  @Override
  public void close() {
    if (!closed_) builder_.close();
    closed_ = true;
  }
}
