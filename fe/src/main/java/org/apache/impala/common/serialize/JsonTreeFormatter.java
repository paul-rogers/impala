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

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.impala.analysis.Expr;
import org.apache.impala.common.PrintUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * JSON serializer which produces a streaming, formatted JSON output. This form
 * is optimized for use in the test framework: it omits values that are null or
 * at defaults, omits empty lists, and uses using human-readable formatting.
 *
 * This format produces JSON, but the goal is not to be entirely JSON-compatible
 * (the output is not intended to be deserialized as JSON.) Rather, it is
 * intended to produce a human-readable serialization that tests can verify
 * using textual comparisons.
 *
 * Writes JSON to a Java PrintWriter. The specific implementation used here is
 * StringWriter so that the resulting JSON can be read to a string for use in
 * tests. A trivial change is to write directly to a file.
 *
 * Supports object dedup. Each object carries an object_id which is determined
 * by the order that objects are emitted, and thus is consistent across runs. If
 * an object has already been seen, emits a reference to the object in the form:
 * field_name: "<n>", where n is the object ID. This is non-standard JSON, but
 * helps with trees (such as the analyzer AST) which has many references to the
 * same object.
 *
 * Also supports writing long strings in a non-standard, but convenient format:
 * field_name: "
 * first line
 * second line"
 */
public class JsonTreeFormatter extends AbstractTreeSerializer {
  /**
   * Object serializer that generates formatted JSON output.
   */
  private static class ObjectFormatter extends AbstractObjectSerializer {
    private final JsonTreeFormatter formatter_;
    private int level_;

    public ObjectFormatter(JsonTreeFormatter formatter, int level) {
      formatter_ = formatter;
      level_ = level;
    }

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
    public void field(String name, String value) {
      if (value != null || !options().elide()) {
        formatter_.builder_.quotedField(level_, name, value);
      }
    }

    @Override
    public void scalar(String name, Object value) {
      formatter_.builder_.unquotedField(level_, name, value);
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
      formatter_.builder_.textField(level_, name, value);
    }

    @Override
    public ArraySerializer array(String name) {
      return new ArrayFormatter(formatter_,
          formatter_.builder_.arrayField(level_, name));
    }
  }

  /**
   * Array serializer that generates formatted JSON output.
   */
  public static class ArrayFormatter extends AbstractArraySerializer {
    private final JsonTreeFormatter formatter_;
    private int level_;

    public ArrayFormatter(JsonTreeFormatter formatter, int level) {
      formatter_ = formatter;
      level_ = level;
    }

    @Override
    public void value(String value) {
      formatter_.builder_.quotedElement(level_, value);
    }

    @Override
    public void value(long value) {
      formatter_.builder_.unquotedElement(level_, value);
    }

    @Override
    public void scalar(Object value) {
      formatter_.builder_.unquotedElement(level_, value);
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
   * Builds a JSON-like tree
   * - Fields are not quoted if they are normal literals.
   * - Block text is shown wrapped on multiple lines
   *
   * Emits material line-by-line, with each bit of content written without a
   * newline; the separator and newline is added once we see the next item at the
   * same or a lower level.
   *
   * Designed to be used to write directly to a file, or to use a {@link StringWriter}
   * to write to a {@link StringBuilder} and thus to obtain a String.
   *
   * Not meant to be used directly by application code as the interface is not
   * JSON-like.
   */
  @VisibleForTesting
  protected static class TreeBuilder {
    private static final int MAX_DEPTH = Expr.EXPR_DEPTH_LIMIT + 20;
    private static final int WRAP_WIDTH = 80;

    private final PrintWriter out_;
    private final String indent_;
    private final char terminators_[] = new char[MAX_DEPTH];
    private final boolean hasContent_[] = new boolean[MAX_DEPTH];
    private int depth_;

    public TreeBuilder(PrintWriter out) {
      out_ = out;
      indent_ = "  ";
    }

    public int root() {
      return objectElement(0);
    }

    public void unquotedField(int level, String name, Object value) {
      startField(level, name);
      out_.print(" ");
      out_.print(value == null ? "null" : value.toString());
    }

    public void quotedField(int level, String name, String value) {
      startField(level, name);
      out_.print(" ");
      if (value == null) {
        out_.print("null");
        return;
      }
      quoteValue(value);
    }

    public void unquotedElement(int level, Object value) {
      startEntry(level);
      out_.print(value == null ? "null" : value.toString());
    }

    public void quotedElement(int level, String value) {
      startEntry(level);
      if (value == null) {
        out_.print("null");
      } else {
        quoteValue(value);
      }
    }

    public void textField(int level, String name, String value) {
      startField(level, name);
      if (value == null) {
        out_.print(" null");
        return;
      }
      out_.println();
      quoteValue(PrintUtils.wrapString(value, WRAP_WIDTH));
    }

    private void quoteValue(String value) {
      out_.print('"');
      for (int i = 0; i < value.length(); i++) {
        char c = value.charAt(i);
        if (c == '"' || c == '\\') out_.print('\\');
        out_.print(c);
      }
      out_.print('"');
    }

    public int objectField(int level, String name) {
      return push(level, name, '{', '}');
    }

    public int arrayField(int level, String name) {
      return push(level, name, '[', ']');
    }

    private int push(int level, String name, char open, char close) {
      startField(level, name);
      out_.print(' ');
      out_.print(open);
      terminators_[++depth_] = close;
      hasContent_[depth_] = false;
      return depth_;
    }

    public int objectElement(int level) {
      return push(level, '{', '}');
    }

    private int push(int level, char open, char close) {
      startEntry(level);
      out_.print(open);
      terminators_[++depth_] = close;
      hasContent_[depth_] = false;
      return depth_;
    }

    private void startField(int level, String name) {
      startEntry(level);
      if (isIdent(name)) {
        out_.print(name);
      } else {
        out_.print('"');
        out_.print(name);
        out_.print('"');
      }
      out_.print(":");
    }

    private void startEntry(int level) {
      popTo(level);
      if (hasContent_[level]) {
        out_.print(",");
      }
      if (level > 0 || hasContent_[0]) out_.println();
      indent();
      hasContent_[level] = true;
    }

    public static boolean isIdent(String name) {
      if (name.isEmpty()) return false;
      if (!Character.isJavaIdentifierStart(name.charAt(0))) return false;
      for (int i = 1; i < name.length(); i++) {
        if (!Character.isJavaIdentifierPart(name.charAt(i)))
          return false;
      }
      return true;
    }

    public void popTo(int level) {
      while (depth_ > level) {
        out_.println();
        depth_--;
        indent();
        out_.print(terminators_[depth_+1]);
      }
    }

    private void indent() {
      for (int i = 0; i < depth_; i++) {
        out_.print(indent_);
      }
    }

    public void close() {
      popTo(0);
      if (hasContent_[0]) out_.println();
      out_.close();
    }
  }

  protected final ObjectFormatter root_;
  protected final StringWriter strWriter_;
  protected final TreeBuilder builder_;
  protected boolean closed_;

  public JsonTreeFormatter(ToJsonOptions options) {
    super(options);
    strWriter_ = new StringWriter();
    builder_ = new TreeBuilder(new PrintWriter(strWriter_));
    root_ = new ObjectFormatter(this, builder_.objectElement(0));
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

  @Override
  public String toString() {
    close();
    return strWriter_.toString();
  }
}
