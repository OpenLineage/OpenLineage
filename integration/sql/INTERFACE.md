# Cross-language interface

As goal of this project is provide single implementation for getting lineage from SQL jobs executed from different languages,
we need single interface that is very similar for every language.

The interface is `binary` - in contrast to web apis, we're not passing JSON strings, but real objects that have different 
composition depending on the target language. So, binary data we transmit is different for Java and Python.

The idea is simple. We need to pass SQL statement to the parser, and get two lists in response - one for `input` tables - ones we're reading data from
and one for `output` tables - the ones we're writing data to. This is `SqlMeta` object. For Java, it has two `List` members carrying this information.
For Python there is Python object with two lists.

The tables can be in different database schemas or even databases, so we return `DbTableMeta` object that contains up to three-level nested names.
This object carries three language-specific Strings carrying this information.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2022 contributors to the OpenLineage project