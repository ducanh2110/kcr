package com.nordstrom.kafka.kcr.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import java.util.stream.Collectors;

public class JmxNameMapper implements HierarchicalNameMapper {
  @Override
  public String toHierarchicalName(Meter.Id id, NamingConvention convention) {
    String name = id.getName();
    // TODO handle multiple tags
    if (!id.getTags().isEmpty()) {
      String tags =
          id.getTags().stream()
              .map(tag -> tag.getKey() + "." + tag.getValue())
              .collect(Collectors.joining());
      name += "." + tags;
    }
    return name;
  }
}
