
SELECT groupSortedArray(5)(number) from numbers(100);

SELECT groupSortedArrayWeighted(number, number) from numbers(100);

SELECT groupSortedArrayWeighted(100)(number, number) from numbers(1000);

SELECT groupSortedArrayWeighted(100)(number, -number) from numbers(1000);

SELECT groupSortedArrayWeighted(5)(str, number) FROM (SELECT toString(number) as str, number FROM numbers(10));

SELECT groupSortedArray(5)(text) FROM (select toString(number) as text from numbers(10));

SELECT groupSortedArrayWeighted(5)(text, -number) FROM (select toString(number) as text, number from numbers(10));

SELECT groupSortedArray(10)(number, number) from numbers(100); -- { serverError 42 }

SELECT groupSortedArrayWeighted(10)(number) from numbers(100); -- { serverError 42 }

SELECT groupSortedArrayWeighted(5)(number, text) FROM (select toString(number) as text, number from numbers(10)); -- { serverError 43 }