---
toc_priority: 109
---

# groupSortedArrayWeighted {#groupSortedArrayweighted}

Returns an array of the values ordering by a different column. The resulting array is sorted in ascending order of values of the second column.

**Syntax**

``` sql
groupSortedArrayWeighted(N)(x, weight)
```

**Arguments**

-   `N` — The number of elements to return.
-   `x` — The value.
-   `weight` — The weight. It uses this second column to sort. [Integer](../../../sql-reference/data-types/int-uint.md).

**Returned value**

Returns an array of the values ordering by a given column.

**Example**

Query:

``` sql
SELECT groupSortedArrayWeighted(10)(number, number) FROM numbers(1000)
```

Result:

``` text
┌─groupSortedArrayWeighted(10)(number, number)─┐
│ [0,1,2,3,4,5,6,7,8,9]                        │
└──────────────────────────────────────────────┘
```

**See Also**

-   [groupSortedArray](../../../sql-reference/aggregate-functions/reference/groupsortedarray.md)
