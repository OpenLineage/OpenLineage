/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public interface Vendors {

  static Vendors getVendors() {
    ServiceLoader<Vendor> serviceLoader = ServiceLoader.load(Vendor.class);
    List<Vendor> vendors =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                    serviceLoader.iterator(), Spliterator.IMMUTABLE & Spliterator.DISTINCT),
                false)
            .filter(Vendor::isVendorAvailable)
            .collect(Collectors.toList());

    return new VendorsImpl(vendors);
  }

  static Vendors empty() {
    return new Vendors() {

      @Override
      public Collection<VisitorFactory> getVisitorFactories() {
        return Collections.emptyList();
      }

      @Override
      public Collection<OpenLineageEventHandlerFactory> getEventHandlerFactories() {
        return Collections.emptyList();
      }
    };
  }

  Collection<VisitorFactory> getVisitorFactories();

  Collection<OpenLineageEventHandlerFactory> getEventHandlerFactories();
}
