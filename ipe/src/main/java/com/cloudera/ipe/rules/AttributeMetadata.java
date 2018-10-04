// Copyright (c) 2014 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.AttributeDataType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * This class defines the metadata for a WorkItem attribute. For example
 * if defines the metadata for hdfs_bytes_read. It has slightly different
 * fields from AvroFilterMetadata, and it is converted into that object during
 * processing.
 */
public class AttributeMetadata {

  final String name;
  final String displayNameKey;
  final String descriptionKey;
  final AttributeDataType filterType;
  final List<String> validValues;
  final boolean supportsHistograms;
  final String unitHint;
  final List<String> aliases;
  final boolean yarnCounterBased;

  public AttributeMetadata(
      final String name,
      final String displayNameKey,
      final String descriptionKey,
      final AttributeDataType filterType,
      final List<String> validValues,
      final boolean supportsHistograms,
      final String unitHint,
      final List<String> aliases,
      final boolean yarnCounterBased) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(displayNameKey);
    Preconditions.checkNotNull(descriptionKey);
    Preconditions.checkNotNull(filterType);
    Preconditions.checkNotNull(validValues);
    Preconditions.checkNotNull(aliases);
    this.name = name;
    this.displayNameKey = displayNameKey;
    this.descriptionKey = descriptionKey;
    this.filterType = filterType;
    this.validValues = validValues;
    this.supportsHistograms = supportsHistograms;
    this.unitHint = unitHint;
    this.aliases = aliases;
    this.yarnCounterBased = yarnCounterBased;
  }

  public String getName() {
    return name;
  }

  public String getDisplayNameKey() {
    return displayNameKey;
  }

  public String getDescriptionKey() {
    return descriptionKey;
  }

  public AttributeDataType getFilterType() {
    return filterType;
  }

  public List<String> getValidValues() {
    return validValues;
  }

  public boolean getSupportsHistograms() {
    return supportsHistograms;
  }

  public String getUnitHint() {
    return unitHint;
  }

  public List<String> getAliases() {
    return aliases;
  }

  public boolean isYarnCounterBased() {
    return yarnCounterBased;
  }

  public boolean isNumeric() {
    return getFilterType() != AttributeDataType.STRING &&
           getFilterType() != AttributeDataType.BOOLEAN;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private String displayNameKey;
    private String descriptionKey;
    private AttributeDataType filterType;
    private List<String> validValues = ImmutableList.of();
    private boolean supportsHistograms = false;
    private String unitHint = null;
    private List<String> aliases = ImmutableList.of();
    private boolean yarnCounterBased = false;

    public Builder setName(String name) {
      Preconditions.checkNotNull(name);
      this.name = name;
      return this;
    }

    public Builder setDisplayNameKey(String displayNameKey) {
      Preconditions.checkNotNull(displayNameKey);
      this.displayNameKey = displayNameKey;
      return this;
    }

    public Builder setDescriptionKey(String descriptionKey) {
      Preconditions.checkNotNull(descriptionKey);
      this.descriptionKey = descriptionKey;
      return this;
    }

    public Builder setFilterType(AttributeDataType filterType) {
      Preconditions.checkNotNull(filterType);
      this.filterType = filterType;
      return this;
    }

    public Builder setValidValues(List<String> validValues) {
      Preconditions.checkNotNull(validValues);
      this.validValues = validValues;
      return this;
    }

    public Builder setSupportsHistograms(boolean supportsHistograms) {
      this.supportsHistograms = supportsHistograms;
      return this;
    }

    public Builder setUnitHint(String unitHint) {
      this.unitHint = unitHint;
      return this;
    }

    public Builder setAliases(List<String> aliases) {
      Preconditions.checkNotNull(aliases);
      this.aliases = aliases;
      return this;
    }

    public Builder setYarnCounterBased(boolean yarnCounterBased) {
      this.yarnCounterBased = yarnCounterBased;
      return this;
    }

    public AttributeMetadata build() {
      return new AttributeMetadata(name,
                                   displayNameKey,
                                   descriptionKey,
                                   filterType,
                                   validValues,
                                   supportsHistograms,
                                   unitHint,
                                   aliases,
                                   yarnCounterBased);
    }
  }
}
