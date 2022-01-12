#pragma once

#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include "AggregateFunctionGroupSortedArrayData.h"

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

template <bool is_plain_column, typename T, bool is_weighted>
class AggregateFunctionGroupSortedArray : public IAggregateFunctionDataHelper<
                                              AggregateFunctionGroupSortedArrayData<T, is_weighted>,
                                              AggregateFunctionGroupSortedArray<is_plain_column, T, is_weighted>>
{
protected:
    using State = AggregateFunctionGroupSortedArrayData<T, is_weighted>;

    UInt64 threshold;
    DataTypePtr & input_data_type;
    mutable std::mutex mutex;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionGroupSortedArray(UInt64 threshold_, UInt64 /*load_factor*/, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T, is_weighted>, AggregateFunctionGroupSortedArray>(
            argument_types_, params)
        , threshold(threshold_)
        , input_data_type(this->argument_types[0])
    {
    }

    void create(AggregateDataPtr place) const override
    {
        IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T, is_weighted>, AggregateFunctionGroupSortedArray>::create(
            place);
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
        State stateAux(threshold);

        if constexpr (is_weighted)
        {
            //First store the first n elements with the column number
            AggregateFunctionGroupSortedArrayData<size_t, true> mapAux(threshold);
            if (if_argument_pos >= 0)
            {
                const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
                for (size_t i = 0; i < batch_size; ++i)
                {
                    if (flags[i])
                        mapAux.add(i, readItem<size_t, is_plain_column>(columns[1], arena, i));
                }
            }
            else
            {
                for (size_t i = 0; i < batch_size; ++i)
                    mapAux.add(i, readItem<size_t, is_plain_column>(columns[1], arena, i));
            }

            //Now create a new map with the final type extracting values from selected columns
            for (auto item : mapAux.values)
                stateAux.add(readItem<T, is_plain_column>(columns[0], arena, item.second), item.first);
        }
        else
        {
            if (if_argument_pos >= 0)
            {
                const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
                for (size_t i = 0; i < batch_size; ++i)
                    if (flags[i])
                    {
                        auto val = readItem<T, is_plain_column>(columns[0], arena, i);
                        stateAux.add(val);
                    }
            }
            else
            {
                for (size_t i = 0; i < batch_size; ++i)
                    stateAux.add(readItem<T, is_plain_column>(columns[0], arena, i));
            }
        }

        State & data = this->data(place);
        mutex.lock();
        data.merge(stateAux);
        mutex.unlock();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *arena) const override
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

#undef DEFAULT_THRESHOLD
