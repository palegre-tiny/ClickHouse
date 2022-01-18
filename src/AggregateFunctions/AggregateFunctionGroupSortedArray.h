#pragma once

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include "AggregateFunctionGroupSortedArrayData.h"
#include <base/logger_useful.h>

namespace DB
{
template <typename TT, bool is_plain_column>
inline TT readItem(const IColumn * column, Arena * arena, size_t row)
{
    if constexpr (std::is_same_v<TT, StringRef>)
    {
        if constexpr (is_plain_column)
        {
            StringRef str = column->getDataAt(row);
            auto ptr = arena->alloc(str.size);
            std::copy(str.data, str.data + str.size, ptr);
            return StringRef(ptr, str.size);
        }
        else
        {
            const char * begin = nullptr;
            return column->serializeValueIntoArena(row, *arena, begin);
        }
    }
    else
    {
        return column->getUInt(row);
    }
}

template <typename T> struct ItemNTH
{
    uint64_t a;
    T b;
    bool operator < (const ItemNTH& other) const    {return (this->a < other.a);}
};

template <typename T> void getFirstNElements(T *data, size_t num_elements, size_t threshold, uint64_t *results)
{
    threshold = std::min(threshold, num_elements);

    ItemNTH<T> *dataIndexed = new ItemNTH<T>[num_elements];
    for (size_t i = 0; i < num_elements; i++) {
        dataIndexed[i].a = data[i];
        dataIndexed[i].b = i;
    }

    std::nth_element(dataIndexed, dataIndexed + threshold, dataIndexed + num_elements);
    std::sort(dataIndexed, dataIndexed + threshold);

    for (size_t i = 0; i < threshold; i++) {
        results[i] = dataIndexed[i].b;
    }

    delete []dataIndexed;
}

template <bool is_plain_column, typename T, bool is_weighted>
class AggregateFunctionGroupSortedArray : public IAggregateFunctionDataHelper<
                                              AggregateFunctionGroupSortedArrayData<T, is_weighted>,
                                              AggregateFunctionGroupSortedArray<is_plain_column, T, is_weighted>>
{
protected:
    using State = AggregateFunctionGroupSortedArrayData<T, is_weighted>;
    using Base = IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T, is_weighted>, AggregateFunctionGroupSortedArray>;

    UInt64 threshold;
    DataTypePtr & input_data_type;
    mutable std::mutex mutex;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionGroupSortedArray(UInt64 threshold_, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T, is_weighted>, AggregateFunctionGroupSortedArray>(
            argument_types_, params)
        , threshold(threshold_)
        , input_data_type(this->argument_types[0])
    {
    }

    void create(AggregateDataPtr place) const override
    {
        Base::create(place);
        this->data(place).threshold = threshold;
    }

    String getName() const override { return is_weighted ? "groupSortedArrayWeighted" : "groupSortedArray"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(input_data_type); }

    bool allocatesMemoryInArena() const override
    {
        if constexpr (std::is_same_v<T, StringRef>)
            return true;
        else
            return false;
    }

    void add(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const override { }

    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos) const override
    {
        State & data = this->data(place);
        if constexpr (is_weighted)
        {
            StringRef ref = columns[1]->getRawData();
            UInt64 values[batch_size];
            memcpy(values, ref.data, batch_size * sizeof(UInt64));
            UInt64 bestRows[2] = {0};
            size_t num_results;

            //First store the first n elements with the column number
            if (if_argument_pos >= 0)
            {
                UInt64 *value_w = values;
                
                const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
                for (size_t i = 0; i < batch_size; ++i)
                {
                    if (flags[i])
                        *(value_w++) = values[i];
                }

                batch_size = value_w - values;
            }

            num_results = std::min(2, int(batch_size));
            getFirstNElements(values, batch_size, num_results, bestRows);
            for ( size_t i = 0; i < num_results; i ++)
            {
                auto row = bestRows[i];
                data.add(readItem<T, is_plain_column>(columns[0], arena, row), values[row]);
            }
        }
        else
        {
            if (if_argument_pos >= 0)
            {
                const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
                for (size_t i = 0; i < batch_size; ++i)
                    if (flags[i])
                    {
                        data.add(readItem<T, is_plain_column>(columns[0], arena, i));
                    }
            }
            else
            {
                for (size_t i = 0; i < batch_size; ++i)
                    data.add(readItem<T, is_plain_column>(columns[0], arena, i));
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void
    deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * /*arena*/) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        auto & values = this->data(place).values;
        int old_size = offsets_to.back();
        offsets_to.push_back(old_size + values.size());

        if constexpr (std::is_same_v<T, StringRef>)
        {
            IColumn & data_to = arr_to.getData();
            for (auto value : values)
            {
                auto str = State::itemValue(value);
                if constexpr (is_plain_column)
                    data_to.insertData(str.data, str.size);
                else
                    data_to.deserializeAndInsertFromArena(str.data);
            }
        }
        else
        {
            typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            data_to.resize(old_size + values.size());
            size_t next = old_size;

            for (auto value : values)
                data_to[next++] = State::itemValue(value);
        }
    }
};
}
