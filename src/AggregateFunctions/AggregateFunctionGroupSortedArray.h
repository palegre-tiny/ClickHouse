#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>

#include <Common/SpaceSaving.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <base/logger_useful.h>

#include <typeinfo>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

namespace DB
{
struct Settings;

template <typename T>
class AggregateFunctionGroupSortedArrayData
{
public:
    AggregateFunctionGroupSortedArrayData(UInt64 threshold_ = -1) : threshold(threshold_) { }

    void add(T item, Int64 weight)
    {
        if (weight < last)
        {
            values.insert({weight, item});

            while (values.size() > threshold)
            {
                values.erase(--values.end());
                last = (--values.end())->first;
            }
        }
    }

    void merge(const AggregateFunctionGroupSortedArrayData & other)
    {
        values.insert(other.values.begin(), other.values.end());

        while (values.size() > threshold)
            values.erase(--values.end());
    }

    void setThreshold(int threshold_) { threshold = threshold_; }

    void serialize(WriteBuffer & buf) const
    {
        writeVarT(UInt64(values.size()), buf);
        for (auto value : values)
        {
            writeVarUInt(value.first, buf);

            if constexpr (std::is_same_v<T, std::string>)
                writeBinary(value.second, buf);
            else
                writeVarUInt(value.second, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        values.clear();
        UInt64 length;
        readVarUInt(length, buf);

        while (length--)
        {
            UInt64 first = 0;
            readVarUInt(first, buf);
            T second;

            if constexpr (std::is_same_v<T, std::string>)
                readBinary(second, buf);
            else
                readVarUInt(second, buf);
            values.insert({first, second});
        }
    }

    typedef std::map<Int64, T> Map;
    Map values;
    UInt64 threshold;
    Int64 last = std::numeric_limits<Int64>::max();
    int len;
};

template <bool is_plain_column, typename T>
class AggregateFunctionGroupSortedArray
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T>, AggregateFunctionGroupSortedArray<is_plain_column, T>>
{
protected:
    using State = AggregateFunctionGroupSortedArrayData<T>;
    UInt64 threshold;
    DataTypePtr & input_data_type;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionGroupSortedArray(UInt64 threshold_, UInt64 /*load_factor*/, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T>, AggregateFunctionGroupSortedArray>(argument_types_, params)
        , threshold(threshold_)
        , input_data_type(this->argument_types[0])
    {
    }

    void create(AggregateDataPtr place) const override
    {
        IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T>, AggregateFunctionGroupSortedArray>::create(place);
        this->data(place).setThreshold(threshold);
    }

    String getName() const override { return "groupSortedArray"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(input_data_type); }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if constexpr (std::is_same_v<T, std::string>)
        {
            if constexpr (is_plain_column)
            {
                this->data(place).add(columns[0]->getDataAt(row_num).toString(), columns[1]->getUInt(row_num));
            }
            else
            {
                const char * begin = nullptr;
                StringRef str_serialized = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
                this->data(place).add(str_serialized.toString(), columns[1]->getUInt(row_num));
                arena->rollback(str_serialized.size);
            }
        }
        else
        {
            this->data(place).add(columns[0]->getUInt(row_num), columns[1]->getUInt(row_num));
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

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        auto & values = this->data(place).values;
        int old_size = offsets_to.back();
        offsets_to.push_back(old_size + values.size());


        if constexpr (std::is_same_v<T, std::string>)
        {
            IColumn & data_to = arr_to.getData();
            for (auto it = values.begin(); it != values.end(); ++it)
            {
                auto ptr = arena->alloc(it->second.size());
                std::copy(it->second.data(), it->second.data() + it->second.length(), ptr);
                StringRef str_serialized(ptr, it->second.size());
                if constexpr (is_plain_column)
                    data_to.insertData(str_serialized.data, str_serialized.size);
                else
                    data_to.deserializeAndInsertFromArena(str_serialized.data);
            }
        }
        else
        {
            typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            data_to.resize(old_size + values.size());

            size_t i = 0;
            for (auto it = values.begin(); it != values.end(); ++it, ++i)
                data_to[old_size + i] = it->second;
        }
    }
};
}
