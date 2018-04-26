# Should tags and values be separate concepts in IFQL?


## Option 1 - Tags and Values are separate

The record object for a row of a table will have three properties:

```
{
    _time: // the timestamp for the row
    _tags: {} // an object of the `tag` columns of the row
    _values: {} // an object of the `value` columns of the row
}
```



## Options 2 - Tags and Values are treated as columns

The record object for a row of a table will be a single object.
The object properties map directly to the columns of the table.

When does the `kind` of a column matter? 

 * Most functions, pass the tag columns through and perform the operations on the `value` columns.
    * In aggregates, they functions automatically apply to all `value` columns.
    * Derivative applies to all `value` columns.
 * Blocks are identified by their `Tags`. Tags are key=value pairs.
     Currently only a `tag` columns can be part of the tagset for a block,
     we may be able to remove that restriction

What does it look like to drop the concept of a column kind?

* Functions would need to be explicit about which columns will be transformed.
    `derivative(cols:["_value"])` or `derivative(cols:["a", "b"])`
    `mean(cols:["_value"])` or `mean(cols:["a", "b"])`

* Group currently assumes that grouping is always done on tags which is not correct.
    We need to rethink how Group would work treating all columns as equal.
    We may want to explicity disallow grouping by `_time` since that would be confusing.


## How can we describe multiple aggregates on a single table.

```
var d = from(db:"telegraf")
   |> filter(...)
   |> range(...)
   |> window(...)
   |> aggregate(fns: {mean: mean, sum: sum})

aggregate = (fns:{}, table=<-table, col="_value") =>
    join(
        tables: fns |> objectMap(fn:(k,v) => [k, table |> v(cols:[col])]),
        except: [col],
        fn: (t) => fns |> objectMap(fn: (k,v) => [k, t[k][col]])
    )

join(tables:{
        mean: d |> mean(cols:["_value"]),
        sum: d |> sum(cols:["_value"]),
    },
    except:["_value"],
    fn: (t) => ({
        mean: t.mean._value,
        sum: t.sum._value,
    })
)
```
