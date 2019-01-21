package org.apache.impala.common;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class AbstractFrontendTest {
  protected static FrontendFixture feFixture_ = FrontendFixture.instance();

  @BeforeClass
  public static void setUp() throws Exception {
    feFixture_.setUp();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    feFixture_.cleanUp();
  }

  @After
  public void tearDown() {
    feFixture_.tearDown();
  }
}
