# Proposal: Producer Verification

**Issue**:   
**Status**: Draft  
**Date**: 2026-11-11


## Problem Statement

As the ecosystem of OpenLineage integrations grows and the OpenLineage specification evolves at the same time, it becomes progresively more difficult to understand the depth of support of the OpenLineage standard by individaul producers and consumers.

As a consequence, it is difficult to predict which producers and consumers are likely to integrate well. 

The depth of support can be defined on multiple levels and dimensions. The most impactful ones seem to be: 

1. _Major version_: what (major) version of OpenLineage specification is supported? Current major version of the specification is 2.x, but many producers are still on version 1.x. On the other hand, there might be OpenLineage consumers that do not yet support specification 2.x. 
2. _Supported event types_:  What are the event types that the interation supports? Do the producers/consumers support runtime oriented Run events, design-time oriented Job and Dataset events, or both? 
3. _Naming Convetions Compliance_: Does the integration support documented naming conventions? Does it use conflicting naming conventions? 
4. _Data Lineage Granularity_: Does the integration support the most detailed column-level lineage, dataset-level lineage, or is no lineage information at all captured? 
5. _Data Lineage Capture Method_: Does the integration support Explicit Lineage facet, Column Lineage Facet, or only implicit inputs/outputs relationships? 
6. _Job Granularity_: Does the integration support only single level of Jobs, or does it support more complex sub-hierarchies of Jobs using Paret Run or Parent Job facet?  
7. _Static Metadata_: Does the integration support static metadata like Documentation, Schema, and similar facets? 
8. _Runtime Data Observability Metrics_: Does the integrartion support data observability metrics collected at runtime like Input Statistics and Output Statistics?  

TBD examples of non-compatible combinations of producers and consumers 


TBD solution for producers - OpenLineage hosted validation API that the producers can use to send events to and get footprint results. 


TBD solution for consumers - consumer hosted API to which a footprint can be sent and the endpoint will respond with what is supported. 