package com.cloudera.cmf.scanner;

import com.cloudera.cmf.printer.AttribFormatter;

public interface RollUp {
  <T extends RollUp> void add(T detail);
}
