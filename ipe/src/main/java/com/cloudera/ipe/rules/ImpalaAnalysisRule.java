// Copyright (c) 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;

/**
 * This defines an ImpalaAnalysisRule. An ImpalaAnalysisRule is just an analysis
 * rule that uses the generic type ImpalaRuntimeProfileTree.
 */
public interface  ImpalaAnalysisRule
    extends AnalysisRule<ImpalaRuntimeProfileTree> {

}
