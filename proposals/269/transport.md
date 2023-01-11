---
Status: IN PROGRESS  
Author: Julien Le Dem (original issue) / Maciej Obuchowski (this document)  
Created: 2021-10-18  
Issue: [269](https://github.com/OpenLineage/OpenLineage/issues/269)
---

## Purpose

The goal is to facilitate sending `OpenLineage` events over various protocols. 
(HTTP, queues, ...) independently of the logic via exposing `Transport` interface.

## Proposed interface

In pseudocode:
```
interface Transport {
  send(LineageEvent event);
}
```

The send method should be synchronous and return when underlying system confirms 
that message has been received. This can be for example HTTP response in 200 range, 
successful fsync on file descriptor, or successful Apache Kafka flush.

Potential error handling is deliberately unspecified on overall interface level, 
but should be specified and consistent for particular language implementation. 
The reason for that is difference between idiomatic error handling between different 
languages: while Python and Java prefer exceptions, Go prefers multiple return values, 
and Rust prefers monadic `Result<T,E>`. 

However, `Clients` and `Transports` utilizing exceptions should be careful to not directly expose 
integrations to exceptions thrown by underlying libraries - integrations should be 
independent of `Transports`. In any case, any error or exception thrown by client
should not cause integration target to fail it's job.

`Transports` are implemented for particular language's `Client`. There is no guarantee 
of availability of any `Transport` on instance that implementation exist for different 
language, with following exception.
Any reference implementation of `OpenLineage` `Client` should include default 
`HTTP Transport`. 

## Instantiation

Integration code instantiation is usually controlled by system for which they are 
written - for example Airflow spawns `LineageBackend` instance. Then, integration
code would spawn `Client`, which would be responsible for spawning relevant 
`Transport`. This is contrary to usual dependency injection approach, which creates an issue:
that `Client` needs to be aware of which `Transport` to spawn.

This would be solved by standardizing `TransportFactory` interface, which constructs
`Transports`.

```
interface TransportFactory {
    Transport create(Config config);
}
```

Transports can define additional configuration variables, that would be provided 
by `TransportFactory` in form of `Config` key-value map.
Same `Transport` implementations for different languages should
be careful to keep configuration consistent, however, some differences are accepted
if it improves user experience.

`Clients` provide default implementation of `TransportFactory` which creates `Transports` 
based on configuration contained in yaml files.
Yaml document should have top-level section named `transport`.
In that section, `type` property is required, that tells `TransportFactory which key to load.
Rest of the section will be provided to `Transport` constructor as config.

Example yaml configuration for `HttpTransport`:
```yaml
transport:
  type: "http"
  url: "http://backend:5000"
  api_key: "very-secure-api-key"
```

Buildin `Transports` are specified via case-insensitive aliases, like `http`, `kafka`.
Default `TransportFactory` can load non-buildin transports based on fully-qualified class names. 
The fully-qualified name can differ between languages. For example, Java implementation
of `Apache Kafka Transport` can be `io.openlineage.transport.KafkaTransport`.

Different `TransportFactories` could utilize other configuration methods.

To keep backwards compatibility, in absence of yaml file providing configuration. 
default `TransportFactory` assumes that `http` transport is used and that 
it utilizes `OPENLINEAGE_URL` or `OPENLINEAGE_API_KEY` environment variables.

## Relation to ProxyBackend

`ProxyBackend` can utilize Transports to route `LineageEvents` to one or more systems.
It's written in Java, so it can utilize only `Transports` that are written in 
that language. `ProxyBackend` should use the same `TransportFactory` mechanism to inject
particular transports. The default implementation of `TransportFactory` for it might be 
different.

More details on `ProxyBackend` should be on [it's own github issue](https://github.com/OpenLineage/OpenLineage/issues/269).

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project
