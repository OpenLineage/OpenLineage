## Intro 
Translating SQL queries into meaningful OpenLineage events is an essential feature within the lineage context. 
Although the problem is complex, once solved it can be applied within multiple components like Airflow integration, Marquez backend or even Spark SQL. 
That’s why it is important to find language agnostic solutions which can be successfully deployed within Python and JVM stacks. 

## Current parser
We’re using sql parser in Airflow integration in some extractors - Snowflake, Postgres - to understand what tables a particular operator is working on. Our code that extracts lineage is based on sqlparse library. Despite its name, it’s more of a sql tokenizer, as in example on github page:

```python
# Parsing a SQL statement:
parsed = sqlparse.parse('select * from foo')[0]
parsed.tokens
[<DML 'select' at 0x7f22c5e15368>, <Whitespace ' ' at 0x7f22c5e153b0>, <Wildcard '*' ... ]
```


This has some unpleasant consequences: in practice, we have to work looking at the next token, finding where in the sql structure we are, and generating lineage. Additionally, we have to keep the token offset - position from beginning of token sequence - manually. Generally, this approach sometimes makes sense - but in practice means that we have to implement the whole sql parser ourselves. For example, we detect that next token is table name by looking if current token is in list:

```python
def _is_in_table(token):
   return _match_on(token, [
       'FROM',
       'INNER JOIN',
       'JOIN',
       'FULL JOIN',
       'FULL OUTER JOIN',
       'LEFT JOIN',
       'LEFT OUTER JOIN',
       'RIGHT JOIN',
       'RIGHT OUTER JOIN'
   ])
```

Another example is how we extract (database,schema) table name, which should be simple and isn’t:
```python
   def parse_ident(ident: Identifier) -> str:
        # Extract table name from possible schema.table naming
        token_list = ident.flatten()
        table_name = next(token_list).value
        try:
            # Determine if the table contains the schema
            # separated by a dot (format: 'schema.table')
            dot = next(token_list)
            if dot.match(Punctuation, '.'):
                table_name += dot.value
                table_name += next(token_list).value

                # And again, to match bigquery's 'database.schema.table'
                try:
                    dot = next(token_list)
                    if dot.match(Punctuation, '.'):
                        table_name += dot.value
                        table_name += next(token_list).value
                except StopIteration:
                    # Do not insert database name if it's not specified
                    pass
            elif default_schema:
                table_name = f'{default_schema}.{table_name}'
        except StopIteration:
            if default_schema:
                table_name = f'{default_schema}.{table_name}'

        table_name = table_name.replace('`', '')
        return table_name
```

Each additional feature increases complexity exponentially - for example, by parsing multiple CTEs we might affect token offsets by wrongly “taking” more tokens that we should, that could affect how we parse select statement.

##What we want

It’s much easier to work in sequential phases, as in classical programming language (or any machine readable language, really). That means that the tokenizer runs first, and feeds its output to the parser, which produces abstract syntax trees. At this point, programming language implementation could execute resulting code, produce and execute bytecode, or compile it to native code. We would want to process the resulting tree - at each step, we’d know what are possible next elements of this tree. 

However, we don’t want to implement the parsing step ourselves. Sql grammar is very large and complex. It contains many gotchas, of which most aren’t of significant relevance to table-level lineage. They still need to be parsed - for example, until we have column-level lineage, we could skip parsing projection in the select statement. However, in parser, we need to process it since we don’t know when it ends - and “from” starts. So, we want to operate on an already created tree, looking at the elements that we want to work on. In this case, we’d have a data structure that would allow us to just look at the “from” part.

We also want type level features - instead of dynamically guessing how the current parsed element fits into the whole structure, we want to have all possible sql grammar types defined as concrete types, so that we can be reasonably sure that we covered all possible use cases. 

Of course, as we want to replace our Python implementation, the end result has to work with Python. It would be nice if it played well with other languages, especially Java.


##Rust implementation

There is no good library that satisfies our requirements in Python. [Fortunately, there exists one in Rust](https://github.com/sqlparser-rs/sqlparser-rs/). It's easy to modify and add features if we want them.

Rust has features that allow us to seriously use libraries built in it in other languages, like Python. Basically, it’s possible to build Rust library that it looks like C library, and C is “English” of programming languages - if one wants to have any foreign function interface, it has to implement C one - at least, to communicate with operating system APIs, which are (very generally) defined as C functions.
It’s not the only feature that makes it a good candidate to implement this in. Rust has very powerful enums combined with great, exhaustive pattern matching. This makes parsing and testing easy.

Current implementation is here: https://github.com/OpenLineage/OpenLineage/tree/sql/add-rust-sql-parser

##Usage in Python

We end up with a simple API, that’s easy to use from Python. 
```python
from openlineage_sql import parse

def test_parse_small():
    metadata = parse("SELECT * FROM test1")
    assert metadata.inputs == ["test1"]
    assert metadata.output is None
```
However, the biggest potential problem with using Rust (and other native) libraries in Python is the necessity to compile them. On the most basic side, we could just ship source wheels - as we can with pure python implementation - and each user would have to compile them on their side. This would mean each user would have to have a Rust compiler toolchain. It would also mean that the compilation process is rather slow. 

On the other hand, we could provide pre-built wheels. This means that our CI process would have to create binary packages that are compatible with our users systems. Fortunately, creating binaries that cover >95% use cases is not that hard. Two things are relevant here. There is manylinux project which define environments with set stable libraries - like glibc - so that wide Python environments are compatible with libraries built in that system. There is also Python’s abi3 - stable subset of Python’s ABI, so that we can build for some arbitrary version of Python like 3.7 and it will continue to work on later versions.

We can build our library using those two projects and be sure that reasonably new Linux systems could use it without having to deal with source compilation themselves. We just need to cover enough so that it makes sense. Data systems are usually deployed on standard Linux machines, usually running x86-64 processors, with reasonably new kernel. We don’t have to cover “wild” architectures, as for example popular backend products would have. It would make sense to cover ARM Linux, and both ARM and x86-64 Macs.

For other cases we can leave current implementation, and dynamically decide which one to use based on whether the Rust library is present.
The API will be the same, so the same (python) tests will pass.

##Usage in Java

Java has a JNI API that is used to implement foreign function interface. 
There are Rust crates that make using it simpler. 

In the future, Java aims to make interfacing with native libraries significantly easier by Project Panama. However, the earliest we could realistically use it is in next LTS release of Java in September 2023. 
