---
toc_priority: 108
---

# groupSortedArray {#groupSortedArray}

Returns an array with the first N items in ascending order.

``` sql
groupSortedArray(N)(column)
```

**Arguments**

-   `N` – The number of elements to return.

If the parameter is omitted, default value 10 is used.

**Arguments**

-   `x` – The value.

**Example**

Gets the first 10 numbers:

``` sql
SELECT groupSortedArray(10)(-number) FROM numbers(100)
```

``` text
┌─groupSortedArray(10)(number)─┐
│ [0,1,2,3,4,5,6,7,8,9]        │
└──────────────────────────────┘
```

Or the last 10:

``` sql
SELECT groupSortedArray(10)(-number) FROM numbers(100)
```

``` text
┌─groupSortedArray(10)(negate(number))──────┐
│ [-99,-98,-97,-96,-95,-94,-93,-92,-91,-90] │
└───────────────────────────────────────────┘
```


**See Also**

-   [groupSortedArrayWeighted](../../../sql-reference/aggregate-functions/reference/groupsortedarrayweighted.md)