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

package org.apache.impala.analysis;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

import org.apache.impala.authorization.AuthorizeableUri;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;

/**
 * Represents a Hadoop FileSystem URI in a SQL statement.
 */
public class HdfsUri {
  private final String location_;

  // Set during analysis
  private Path uriPath_;

  public HdfsUri(String location) {
    Preconditions.checkNotNull(location);
    this.location_ = location.trim();
  }

  public Path getPath() {
    Preconditions.checkNotNull(uriPath_);
    return uriPath_;
  }

  public void analyze(Analyzer analyzer, Privilege privilege)
      throws AnalysisException {
    analyze(analyzer, privilege, FsAction.NONE, true);
  }

  public void analyze(Analyzer analyzer, Privilege privilege, FsAction perm)
      throws AnalysisException {
    analyze(analyzer, privilege, perm, true);
  }

  public void analyze(Analyzer analyzer, Privilege privilege, boolean registerPrivReq)
      throws AnalysisException {
    analyze(analyzer, privilege, FsAction.NONE, registerPrivReq);
  }

  /**
   * Analyzes the URI.
   * Optionally check location path permission, issue warning if impala user doesn't
   * have sufficient access rights.
   * Optionally register a privilege request. Used by GRANT/REVOKE privilege statements.
   */
  public void analyze(Analyzer analyzer, Privilege privilege, FsAction perm,
      boolean registerPrivReq) throws AnalysisException {
    if (location_.isEmpty()) {
      throw new AnalysisException("URI path cannot be empty.");
    }

    uriPath_ = new Path(location_);
    if (!uriPath_.isUriPathAbsolute()) {
      throw new AnalysisException("URI path must be absolute: " + uriPath_);
    }

    uriPath_ = analyzer.fsProxy().validatePath(analyzer, uriPath_, privilege, perm);

    if (registerPrivReq) {
      analyzer.registerPrivReq(new PrivilegeRequest(
          new AuthorizeableUri(uriPath_.toString()), privilege));
    }
  }

  @Override
  public String toString() {
    // If uriPath is null (this HdfsURI has not been analyzed yet) just return the raw
    // location string the caller passed in.
    return uriPath_ == null ? location_ : uriPath_.toString();
  }

  public String getLocation() { return location_; }
}
