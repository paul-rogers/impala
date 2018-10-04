// Copyright (c) 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.cmf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.rules.ImpalaSessionDetailsAnalysisRule;
import com.google.common.collect.Lists;

import java.util.List;

import org.junit.Test;

public class IPEPropertiesTest {

  @Test
  public void testImpalaSessionTypes() {
    ImpalaSessionDetailsAnalysisRule rule = new ImpalaSessionDetailsAnalysisRule(
        IPEConstants.DEFAULT_IMPALA_SESSION_TYPES);
    List<String> sessionTypes = rule.getSessionTypes();
    List<String> expected = Lists.newArrayList(
        IPEConstants.IMPALA_SESSION_TYPE_BEESWAX,
        IPEConstants.IMPALA_SESSION_TYPE_HIVESERVER2);
    assertNotNull(sessionTypes);
    assertTrue(sessionTypes.size() == 2);
    assertEquals(sessionTypes, expected);
  }
}
