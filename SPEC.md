# IFQL Specification

The following document specifies the IFQL language and query execution.

## Language

The IFQL language is centered on querying and manipulating time series data.

### Notation

The syntax of the language is specified using Extended Backus-Naur Form (EBNF):

    Production  = production_name "=" [ Expression ] "." .
    Expression  = Alternative { "|" Alternative } .
    Alternative = Term { Term } .
    Term        = production_name | token [ "…" token ] | Group | Option | Repetition .
    Group       = "(" Expression ")" .
    Option      = "[" Expression "]" .
    Repetition  = "{" Expression "}" .

Productions are expressions constructed from terms and the following operators, in increasing precedence:

    |   alternation
    ()  grouping
    []  option (0 or 1 times)
    {}  repetition (0 to n times)

Lower-case production names are used to identify lexical tokens.
Non-terminals are in CamelCase.
Lexical tokens are enclosed in double quotes "".

### Representation

Source code is encoded in UTF-8.
The text need not be canonicalized.

#### Characters

This document will use the term _character_ to refer to a Unicode code point.

The following terms are used to denote specific Unicode character classes:

    newline        = /* the Unicode code point U+000A */ .
    unicode_char   = /* an arbitrary Unicode code point except newline */ .
    unicode_letter = /* a Unicode code point classified as "Letter" */ .
    unicode_digit  = /* a Unicode code point classified as "Number, decimal digit" */ .

In The Unicode Standard 8.0, Section 4.5 "General Category" defines a set of character categories.
IFQL treats all characters in any of the Letter categories Lu, Ll, Lt, Lm, or Lo as Unicode letters, and those in the Number category Nd as Unicode digits.

#### Letters and digits

The underscore character _ (U+005F) is considered a letter.

    letter        = unicode_letter | "_" .
    decimal_digit = "0" … "9" .

### Lexical Elements

#### Comments

Comment serve as documentation.
Comments begin with the character sequence `//` and stop at the end of the line.

Comments cannot start inside string or regexp literals.
Comments act like newlines.

#### Tokens

IFQL is built up from tokens.
There are several classes of tokens: _identifiers_, _keywords_, _operators_, and _literals_.
_White space_, formed from spaces, horizontal tabs, carriage returns, and newlines, is ignored except as it separates tokens that would otherwise combine into a single token.
While breaking the input into tokens, the next token is the longest sequence of characters that form a valid token.

#### Identifiers

Identifiers name entities within a program.
An identifier is a sequence of one or more letters and digits.
An identifier must start with a letter.

    identifier = letter { letter | unicode_digit } .

Examples:

    a
    _x
    longIdentifierName
    αβ

#### Keywords

The following keywords are reserved and may not be used as identifiers:

    and    import  not  return
    empty  in      or

TODO(nathanielc): Add support for `in` and `emprty` operators
TODO(nathanielc): Add support for `import` statements

#### Operators

The following character sequences represent operators:

    +   ==   !=   (   )
    -   <    !~   [   ]
    *   >    =~   {   }
    /   <=   =    ,   :
    %   >=   <-   .   |>

#### Integer literals

An integer literal is a sequence of digits representing an integer constant.
Only decimal integers are supported.

    int_lit     = "0" | decimal_lit .
    decimal_lit = ( "1" … "9" ) { decimal_digit } .

Examples:

    0
    42
    317316873

#### Unsigned Integer literals

TODO We need unsigned literals since we do not have explicit type declarations.
Or we need to solve for all number literal types in an unambiguous manner.

#### Floating-point literals

A floating-point literal is a decimal representation of a floating-point constant.
It has an integer part, a decimal point, and a fractional part.
The integer and fractional part comprise decimal digits.
One of the integer part or the fractional part may be elided.

    float_lit = decimals "." [ decimals ] |
        "." decimals .
    decimals  = decimal_digit { decimal_digit } .

Examples:

    0.
    72.40
    072.40  // == 72.40
    2.71828
    .26

#### Duration literals

A duration literal is a representation of a length of time.
It has an integer part and a duration unit part.
Multiple duration may be specified together and the resulting duration is the sum of each smaller part.

    duration_lit        = { int_lit duration_unit } .
    duration_unit       = "ns" | "u" | "µ" | "ms" | "s" | "m" | "h" | "d" | "w" .

| Units  | Meaning                                 |
| -----  | -------                                 |
| ns     | nanoseconds (1 billionth of a second)   |
| u or µ | microseconds (1 millionth of a second)  |
| ms     | milliseconds (1 thousandth of a second) |
| s      | second                                  |
| m      | minute (60 seconds)                     |
| h      | hour (60 minutes)                       |
| d      | day (24 hours)                          |
| w      | week (7 days)                           |

Durations represent a fixed length of time.
They do not change based on time zones or other time related events like daylight savings or leap seconds.

Examples:

    1s
    10d
    1h15m // 1 hour and 15 minutes
    5w

#### Date and time literals

A date and time literal represents a specific moment in time.
It has a date part, a time part and a time offset part.

    date_time_lit     = date "T" time .
    date              = year_lit "-" month "-" day .
    year              = decimal_digit decimal_digit decimal_digit decimal_digit .
    month             = decimal_digit decimal_digit .
    day               = decimal_digit decimal_digit .
    time              = hour ":" minute ":" second [ fractional_second ] time_offset .
    hour              = decimal_digit decimal_digit .
    minute            = decimal_digit decimal_digit .
    second            = decimal_digit decimal_digit .
    fractional_second = "."  { decimal_digit } .
    time_offset       = "Z" | ("+" | "-" ) hour ":" minute .

#### String literals

A string literal represents a sequence of characters.
String literals are enclosed in double quotes.
Any double quotes contained withing the string must be escaped using the backslash character "\".
String literals may not span multiple lines.

    string_lit             = `"` { unicode_char } `"` .

TODO(nathanielc): Do we want to support byte value strings? i.e. "\u65e5\u672c\u8a9e" for "日本語"?
Is there a way to introduce new escape sequences without breaking existing code?

TODO(nathanielc): How will we support multiline string literals?
We will likely have large string literals for formatting alert messages and the like.

### Variables

A variable holds a value.
A variable can only hold values defined by its type.

### Types

A type defines the set of values and operations on those values.
Types are never explicitly declared as part of the syntax.
Types are always inferred from the usage of the value.

TODO(nathanielc): Specify how type inference works. Currently it doesn't work ;)

#### Boolean types

A _boolean type_ represents a truth value, corresponding to the predeclared variables `true` and `false`.
The boolean type name is `bool`.

#### Numeric types

A _numeric type_ represents sets of integer or floating-point values.

The following numeric types exist:

    uint    the set of all unsigned 64-bit integers
    int     the set of all signed 64-bit integers
    float   the set of all IEEE-754 64-bit floating-point numbers

#### Time types

A _time type_ represents a single point in time with nanosecond precision.
The time type name is `time`.


#### Duration types

A _duration type_ represents a length of time with nanosecond precision.
The duration type name is `duration`.

#### String types

A _string type_ represents a possibly empty sequence of characters.
Strings are immutable: once created they cannot be modified.
The string type name is `string`.

The length of a string is its size in bytes, not the number of characters, since a single character may be multiple bytes.

#### Regular expression types

A _regular expression type_ represents the set of all patterns for regular expressions.
The regular expression type name is `regexp`.

#### Array types

An _array type_ represents a sequence of values of any other type.
All values in the array must be of the same type.
The length of an array is the number of elements in the array.

#### Object types

An _object type_ represents a set of unordered key and value pairs.
The key must always be a string.
The value may be any other type, and need not be the same as other values within the object.

#### Function types

A _function type_ represents a set of all functions with the same argument and result types.

TODO(nathanielc): We want to have polymorphic function signatures, via free type variables.
A function signature needs to be able to express its free type variables.

### Blocks

A _block_ is a possibly empty sequence of statements within matching brace brackets.

    Block = "{" StatementList "} .
    StatementList = { Statement } .

In addition to explicit blocks in the source code, there are implicit blocks:

1. The _universe block_ encompasses all IFQL source text.
2. Each package has a _package block_ containing all IFQL source text for that package.
3. Each file has a _file block_ containing all IFQL source text in that file.
4. Each function declaration has its own _function block_ even if not explicitly declared.

Blocks nest and influence scoping.

### Declarations and scope

A declaration binds an identifier to a variable or function.
Every identifier in a program must be declared.
No identifier may be declared twice in the same block, and no identifier may be declared both in the file and package block.

IFQL is lexically scoped using blocks:

1. The scope of a predeclared identifier is in the universe block.
2. The scope of an identifier denoting a variable or function at the top level (outside any function) is the package block.
3. The scope of a package name of an imported package is the file block of the file containing the import declaration.
4. The scope of an identifier denoting a function argument is the function body.
5. The scope of a variable declared inside a function is the innermost containing block.

An identifier declared in a block may be redeclared in an inner block.
While the identifier of the inner declaration is in scope, it denotes the entity declared by the inner declaration.

The package clause is not a declaration; the package name does not appear in any scope.
Its purpose is to identify the files belonging to the same package and to specify the default package name for import declarations.

#### Variable declarations

A variable declaration creates a variable bound to the identifier and gives it a type and initial value.

    VarDecl = identifier "=" Expression

Examples:

    n = 1
    f = 5.4
    r = z()

#### Function declarations

A function declaration defines the function parameters, their default values and the body of the function.
The function body may be a block or a single expression.
The function body must have a return statement if it is a block, otherwise the expression is the return value.

    FunctionDecl = FunctionParameters "=>" FunctionBody .
    FunctionParameters = "(" [ ParameterList [ "," ] ] ")" .
    ParameterList = ParameterDecl { "," ParameterDecl } .
    ParameterDecl = identifier [ "=" Expression ] .
    FunctionBody = Expression | Block .


Examples:

    () => 1 // function returns the value 1
    (a,b) => a + b // function returns the sum of a and b
    (x=1,y=1) => x * y // function with default values
    (a,b,c) => { // function with a block body
        d = a + b
        return d / c
    }

All function declarations are anonymous.
A function may be given a name using a variable declaration.

    add = (a,b) => a + b
    mul = (a,b) => a * b


All functions are pure in that they do not have side effects, with the exception of a built-in yield function.

TODO(nathanielc): Specify closures and the mutability of their scope

TODO(nathanielc): Function declarations are also function expressions.




### Expressions

An expression specifies the computation of a value by applying the operators and functions to operands.

#### Operands

Operands denote the elementary values in an expression.
An operand may be a literal, identifier denoting a variable, or a parenthesized expression.


TODO(nathanielc): Fill out expression details...
PEG parsers don't understand operators precedence so it difficult to express operators in expressions with the grammar.
We should simplify it and use the EBNF grammar.
This requires redoing the parser in something besides PEG.


#### Call expressions

A call expressions invokes a function with the provided arguments.

Examples:

    f(a:1, b:9.6)
    float(v:1)

#### Pipe expressions

A pipe expression is a call expression with an implicit piped argument.
Pipe expressions simplify creating long nested call chains.

A pipe expression passes the result of the left hand expression as the _pipe_ argument to the right hand call expression.
Function declarations specify which if any argument is the _pipe_ argument.

Examples:

    foo = (x=<-) => // function body elided
    bar = (x=<-) => // function body elided
    baz = (x=<-) => // function body elided
    foo() |> bar() |> baz() // equivalent to baz(x:bar(x:foo()))

### Statements

A statement controls execution.

    Statement = Declaration | ReturnStatement |
                ExpressionStatement | BlockStatment .
    Declaration = VarDecl .


#### Return statements

A terminating statement prevents execution of all statements that appear after it in the same block.
A return statement is a terminating statement.

    ReturnStatement = "return" Expression .

#### Expression statements

An expression statement is an expression where the computed value is discarded.

    ExpressionStatement = Expression .

Examples:

    1 + 1
    f()
    a

### Built-in functions

The following functions are predeclared in the universe block.


#### ...

TODO(nathanielc): Should these be defined here instead of at the operations section?
Should they be defined in both places?
Should only helper functions be defined here?
Should all built-in functions/operations be defined somewhere else?


## Query engine

The execution of a query is separate and distinct from the execution of IFQL the language.
The input into the query engine is a query specification.

The output of an IFQL program is a query specification, which then may be passed into the query execution engine.

### Query specification

A query specification consists of a set of operations and a set of edges between those operations.
The operations and edges must form a directed acyclic graph.

### Data model

The data model for the query engine consists of tables, records, columns and streams.

#### Record

A record is a tuple of values.

#### Column

A column has a label and a data type.

The available data types for a column are:

    bool     a boolean value, true or false.
    uint     an unsigned 64-bit integer
    int      a signed 64-bit integer
    float    an IEEE-754 64-bit floating-point number
    string   a sequence of unicode characters
    time     a nanosecond precision instant in time
    duration a nanosecond precision duration of time

TODO(nathanielc): Is a string data type really a bytes data type?
Or should we specify both types?

#### Table

A table is set of records, with a common set of columns and a partition key.

A table's partition key denotes which partition of data is assigned to the table.
The partition key contains time bounds and a set of tags.
The time bounds specify and inclusive lower bound and exclusive upper bound.
The tags are a set of key value pairs of strings.

A tables partition key is represented as columns on the table.
Each tag key is a column.
The lower time bound is the `_start` column and the upper time bound is the `_stop` column.

#### Stream

A stream represents a potentially unbounded dataset.
A stream partitioned into individual tables.
Within a stream each table's partition key is unique.

#### Missing values

A record may be missing a value for a specific column.
Missing values are represented will a special _null_ value.
The _null_ value can be of any data type.

#### Operations

An operation defines a transformation on a stream.
All operations may consume a stream and always produce a new stream.

Most operations output one table for every table they receive from the input stream.

Operations that modify the partition keys or that modify the data within the keys will need to repartition the tables in the output stream.

### Built-in operations

#### From

From produces a stream of tables from the specified bucket.
Each unique series is contained within its own table.
The tables schema will include a `_time` column, columns for each tag on the series and a `_value` column for the value of the series.
Each record in the table represents a single point in the series.


Example:

    from(bucket:"telegraf")

The from operation has the following properties:

* `bucket` string
    The name of the bucket to query.

#### Yield

TODO(nathanielc): Specify yield operations

#### Aggregate operations

Aggregate operations output a table for every input table they receive.
A list of columns to aggregate must be provided to the operation.
The aggregate function is applied to each column in isolation.

Any output table will have the following properties:

* It always contains a single record.
* It will have the same partition key as the input table.
* It will have a column `_time` which represents the time of the aggregated record.
    This can be set as the start or stop time of the input table.
    By default the stop time is used.
* It will contain a column for each provided aggregate column.
    The column label will be the same as the input table.
    The type of the column depends on the specific aggregate operation.

All aggregate operations have the following properties:

* `columns` list of string
    columns specifies a list of columns to aggregate.
* `timeValue` string
    timeValue specifies which time value to use on the resulting aggregate record.
    The value must be one of `_start`, or `_stop`.

##### Count

Count is an aggregate operation.
For each aggregated column, it outputs the number of non null records as an integer.


##### Mean

Mean is an aggregate operation.
For each aggregated column, it outputs the mean of the non null records as a float.


##### Skew

Skew is an aggregate operation.
For each aggregated column, it outputs the skew of the non null record as a float.

##### Spread

Spread is an aggregate operation.
For each aggregated column, it outputs the difference between the min and max values.
The type of the output column depends on the type of input column: for input columns with type `uint` or `int`, the output is an `int`; for `float` input columns the output is a `float`.
All other input types are invalid.

##### Stddev

Stddev is an aggregate operation.
For each aggregated column, it outputs the standard deviation of the non null record as a float.

##### Sum

Stddev is an aggregate operation.
For each aggregated column, it outputs the sum of the non null record.
The output column type is the same as the input column type.

#### Selector operations

Selector operations output a table for every input table they receive.
A single column on which to operate must be provided to the operation.

Any output table will have the following properties:

* It will have the same partition key as the input table.
* It will contain the same columns as the input table.
* It will have a column `_time` which represents the time of the selected record.
    This can be set as the value of any time column on the input table.
    By default the `_stop` time column is used.

All selector operations have the following properties:

* `column` string
    column specifies a which column to use when selecting.
* `timeValue` string
    timeValue specifies which time value to use on the selected record.
    The value must be the label of a column that exists on the input table.

##### First

First is a selector operation.
First selects the first non null record from the input table.

##### Last

Last is a selector operation.
Last selects the last non null record from the input table.

##### Max

Max is a selector operation.
Max selects the maximum record from the input table.

##### Min

Min is a selector operation.
Min selects the minimum record from the input table.

##### Sample

Sample is a selector operation.
Sample selects a subset of the records from the input table.
By default the sample operation uses `_time` as the `timeValue` for the operation.

The following properties define how the sample is selected.

* `n`
    Sample every Nth element
* `pos`
    Position offset from start of results to begin sampling.
    The `pos` must be less than `n`.
    If `pos` is less than 0, a random offset is used.
    Default is -1 (random offset).


#### Filter

Filter applies a predicate function to each input record, output tables contain only records which matched the predicate.
One output table is produced for each input table.
The output tables will have the same partition key and columns as their corresponding input table.

Filter has the following properties:

* `fn` function(record) bool
    Predicate function.
    The function must accept a single record parameter and return a boolean value.
    Each record will be passed to the function.
    Records which evaluate to true, will be included in the output tables.
    TODO(nathanielc): Do we need a syntax for expressing type signatures?

#### Limit

Limit caps the number of records in output tables to a fixed size n.
One output table is produced for each input table.
The output table will contain the first n records from the input table.
If the input table has less than n records all records will be output.

Limit has the following properties:

* `n` int
    The maximum number of records to output.

#### Map

Map applies a function to each record of the input tables.
The modified records are assigned to new tables based on the partition key of the input table.
The output tables are the result of applying the map function to each record on the input tables.

Map has the following properties:

* `fn` function
    Function to apply to each record.
    The return value of the function may be a single value or an object.
    TODO(nathanielc): Should we for the map function to always return an object, i.e. complete record object?

#### Range

Range filters records based on provided time bounds.
Each input tables records are filtered to contain only records that exist within the time bounds.
Each input table's partition key is modified to fit within the range bounds.
Tables where all records exists outside the range bounds are filtered entirely.

TODO(nathanielc): is there a way to make range default to aligned times so that you do not get incomplete windows?
Or maybe helper function for that purpose?

Range has the following properties:

* `start` duration or timestamp
    Specifies the oldest time to be included in the results
* `stop` duration or timestamp
    Specifies the exclusive newest time to be included in the results.
    Defaults to "now"


#### Set

Set assigns a static value to each record.
The key may modify and existing column or it may add a new column to the tables.
If the column that is modified is part of the partition key, then the output tables will be repartitioned as needed.


* `key` string
* `value` string


#### Sort

Sorts orders the records within each table.
One output table is produced for each input table.
The output tables have the same partition key and columns as their corresponding input tables.

Sort has the following properties:

* `columns` list of strings
    List of columns used to sort; precedence from left to right.
    Default is `["_value"]`
* `desc` bool
    Sort results in descending order.


#### Group

Group partitions records based on their values for specific columns.
It produces tables with new partition keys based on the provided columns.

Group has the following properties:

*  `by` list of strings
    Group by these specific columns.
    Cannot be used with `except`.
*  `keep` list of strings
    Keep specific columns that were not in the `by` columns.
    These columns will not be part of the table partition key, but will be present on the table.
    TODO(nathanielc): Does it make sense to keep all columns?
    If we want to remove columns should that be an explicit separate operation?
    I think it simplifies the group behavior
*  `except` list of strings
    Group by all other column except this list of columns.
    Cannot be used with `by`.

Examples:

    group(by:["host"]) // group records by their "host" value
    group(except:["_time", "region", "_value"]) // group records by all other columns except for _time, region, and _value
    group(by:[]) // group all records into a single partition
    group(except:[]) // group records into all unique partitions


#### Window

Window partitions records based on the value of their `_time` column.
It produces tables with new partition keys based on the new time bounds.
A single input record may exist in zero or more output tables.

Window has the following properties:

* `every` duration
    Duration of time between windows
    Defaults to `period`'s value
* `period` duration
    Duration of the windowed partition
    Default to `every`'s value
* `start` time
    The time of the initial window partition
* `round` duration
    Rounds a window's bounds to the nearest duration
    Defaults to `every`'s value


#### Join

Join merges two or more input streams into a single output stream.
Output tables will have a partition key based on the join columns and the input table time bounds.


Join has the following properties:

* `tables` map of tables
    Map of tables to join. Currently only two tables are allowed.
* `on` array of strings
    List of tag keys that when equal produces a result set.
* `fn`
    Defines the function that merges the values of the tables.
    The function must defined to accept a single parameter.
    The parameter is a map, which uses the same keys found in the `tables` map.
    The function is called for each joined set of records from the tables.


#### Type conversion operations

##### toBool

Convert a value to a bool.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toBool()`

The function `toBool` is defined as `toBool = (table=<-) => table |> map(fn:(r) => bool(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `bool` function.

##### toInt

Convert a value to a int.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toInt()`

The function `toInt` is defined as `toInt = (table=<-) => table |> map(fn:(r) => int(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `int` function.

##### toFloat

Convert a value to a float.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toFloat()`

The function `toFloat` is defined as `toFloat = (table=<-) => table |> map(fn:(r) => float(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `float` function.

##### toDuration

Convert a value to a duration.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toDuration()`

The function `toDuration` is defined as `toDuration = (table=<-) => table |> map(fn:(r) => duration(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `duration` function.

##### toString

Convert a value to a string.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toString()`

The function `toString` is defined as `toString = (table=<-) => table |> map(fn:(r) => string(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `string` function.

##### toTime

Convert a value to a time.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toTime()`

The function `toTime` is defined as `toTime = (table=<-) => table |> map(fn:(r) => time(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `time` function.

##### toUInt

Convert a value to a uint.

Example: `from(bucket: "telegraf") |> filter(fn:(r) => r._measurement == "mem" and r._field == "used") |> toUInt()`

The function `toUInt` is defined as `toUInt = (table=<-) => table |> map(fn:(r) => uint(v:r._value))`.
If you need to convert other columns use the `map` function directly with the `uint` function.



### Triggers

A trigger is associated with a table and contains logic for when it should fire.
When a trigger fires its table is materialized.
Materializing a table makes it available for any down stream operations to consume.
Once a table is materialized it can no longer be modified.

Triggers can fire based on these inputs:

| Input                   | Description                                                                                       |
| -----                   | -----------                                                                                       |
| Current processing time | The current processing time is the system time when the trigger is being evaluated.               |
| Watermark time          | The watermark time is a time where it is expected that no data will arrive that is older than it. |
| Time bounds             | The time bounds of the table.                                                                     |
| Record count            | The number of records currently in the table.                                                     |

Additionally triggers can be _finished_, which means that they will never fire again.
Once a trigger is finished, its associated table is deleted.

Currently all tables use an _after watermark_ trigger which fires only once the watermark has exceeded the time bounds of the table and then is immediately finished.

Data sources are responsible for updating the watermark for a given read operation.

### Execution model

A query specification defines what data and operations to perform.
The execution model reserves the right to perform those operations as efficiently as possible.
The execution model may rewrite the query in anyway it sees fit while maintaining correctness.

## Request and Response Formats

Included with the specification of the language and execution model, is a specification of how to submit queries and read their responses over HTTP.

### Request format

To submit a query for execution, make an HTTP POST request to the `/v1/query` endpoint.

The POST request may either submit IFQL query text as the `q` parameter, or the body may be a serialization of a query specification.
When submitting a query specification directly the `Content-Type` header is used to indicate the specific serialization format.

### Response format

The result of a query is any number of named streams.
As a stream consists of multiple tables each table is encoded as CSV UTF-8 textual data.

Before each table there are two header rows.
The first declares the data types of each column, the second declares the column labels.

Between each table is an empty line.

In addition to the columns on the tables themselves three additional columns are added.

* meta - the first column is always a meta column.
    The values `#datatype` and `#error` are the only permissible values in the meta column.
* result name - the second column is the user defined name of the result the record belongs to.
* table - is a unique value to identify each table within a result.


The row containing a `#datatype` meta specifies the data types of the remaining columns.
The possible data types are:

| Datatype     | IFQL type | Description                                                                          |
| --------     | --------- | -----------                                                                          |
| boolean      | bool      | a truth value, one of "true" or "false"                                              |
| unsignedlong | uint      | an unsigned 64-bit integer                                                           |
| long         | int       | a signed 64-bit integer                                                              |
| double       | float     | a IEEE-754 64-bit floating-point number                                              |
| string       | string    | a UTF-8 encoded string                                                               |
| dateTime     | time      | an instant in time, may be followed with a colon `:` and a description of the format |
| duration     | duration  | a length of time represented as an unsigned 64-bit integer number of nanoseconds     |

TODO(nathanielc): Again we probably need an explicit bytes type that is hex encoded or something.

When error occurs during execution the meta column will contain an `#error` value, followed by column labels for properties of the error.
The error properties are contained in the second row of the result.


Example encoding with two tables in the same result:

```
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
,result,table,_start,_stop,_time,region,host,_value
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,east,A,15.43
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,east,B,59.25
,mean,0,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,east,C,52.62

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
,result,table,_start,_stop,_time,region,host,_value
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:00Z,west,A,62.73
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:20Z,west,B,12.83
,mean,1,2018-05-08T20:50:00Z,2018-05-08T20:51:00Z,2018-05-08T20:50:40Z,west,C,51.62
```

Example error encoding:

    #error,message,reference
    ,Failed to parse query,897

