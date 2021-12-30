#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/SpaceSaving.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <base/logger_useful.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>


namespace DB
{
struct Settings;

template <typename T>
struct AggregateFunctionGroupSortedArrayData
{
    typedef std::map <int, T>Map;
    Map values;
};


template <typename T>
class AggregateFunctionGroupSortedArray
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T>, AggregateFunctionGroupSortedArray<T>>
{
protected:
    using State = AggregateFunctionGroupSortedArrayData<T>;
    UInt64 threshold;

public:
    AggregateFunctionGroupSortedArray(UInt64 threshold_, UInt64 /*load_factor*/, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T>, AggregateFunctionGroupSortedArray<T>>(argument_types_, params)
        , threshold(threshold_) {}

    String getName() const override { return "groupSortedArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(this->argument_types[0]);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & values = this->data(place).values;

        auto v0 = columns[0]->getUInt(row_num);
        auto v1 = columns[1]->getUInt(row_num);
        values.insert({v1, v0});

        while (values.size()>threshold)
            values.erase(--values.end());
     /*
        static Poco::Logger * log = &Poco::Logger::get("MergeTreeSequentialSource");
        std::string dbg_map;
        for ( const auto &val : values){
            dbg_map += "{ " + std::to_string(int(val.first)) + ", " + std::to_string(int(val.second)) + " } ";
        }
        LOG_DEBUG(log, "Read column {} {} size:{} Map: {}", v0, v1, values.size(), dbg_map.data());*/
    }
                                                                                                                                                                                                                                                                            
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & values = this->data(place).values;
        auto & values_external = this->data(rhs).values;
        values.insert(values_external.begin(), values_external.end());

        while (values.size()>threshold)
            values.erase(--values.end());
    }   

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & values = this->data(place).values;
        writeVarUInt(values.size(), buf);
        for (auto value : values){
            writeVarUInt(value.first, buf);
            writeVarUInt(value.second, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
    {
        auto & values = this->data(place).values;
        UInt64 length;
        readVarUInt(length, buf);

        while (length--){
            UInt64 first = 0;
            readVarUInt(first, buf);
            UInt64 second = 0;
            readVarUInt(second, buf);
            values.insert({first, second});
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        auto & values = this->data(place).values;

        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        size_t size = values.size();
        offsets_to.push_back(offsets_to.back() + size);
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = values.begin(); it != values.end(); ++it, ++i)
            data_to[old_size + i] = it->second;
    }
};

class AggregateFunctionGroupSortedArrayGeneric
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<std::string>, AggregateFunctionGroupSortedArrayGeneric>
{
protected:
    using State = AggregateFunctionGroupSortedArrayData<std::string>;
    UInt64 threshold;
    DataTypePtr & input_data_type;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionGroupSortedArrayGeneric(UInt64 threshold_, UInt64 /*load_factor*/, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<std::string>, AggregateFunctionGroupSortedArrayGeneric>(argument_types_, params)
        , threshold(threshold_), input_data_type(this->argument_types[0]) {}

    String getName() const override { return "groupSortedArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(input_data_type);
    }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *arena) const override
    {
        const char * begin = nullptr;
        StringRef str_serialized = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
        auto v1 = columns[1]->getUInt(row_num);
        std::string v0 = str_serialized.toString();
        add(place, v0, v1);
        
        arena->rollback(str_serialized.size);
    }

    inline void add(AggregateDataPtr __restrict place, const std::string &v0, const UInt64 &v1) const
    {
        auto & values = this->data(place).values;
        values.insert({v1, v0});
        while (values.size()>threshold)
            values.erase(--values.end());
     
        static Poco::Logger * log = &Poco::Logger::get("MergeTreeSequentialSource");
        std::string dbg_map;
        for ( const auto &val : values){
            dbg_map += "{ " + std::to_string(val.first) + ", " + val.second + " } ";
        }
        LOG_DEBUG(log, "Read column {} {} size:{} Map: {}", v0, v1, values.size(), dbg_map.data());
    }
                                                                                                                                                                                                                                                                            
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & values_external = this->data(rhs).values;
    
        for (auto value : values_external){
            add(place, value.second, value.first);
        }
    }   

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & values = this->data(place).values;
        writeVarUInt(values.size(), buf);
        for (auto value : values){
            writeVarUInt(value.first, buf);
            writeBinary(value.second, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
    {
        auto & values = this->data(place).values;
        UInt64 length;
        readVarUInt(length, buf);

        while (length--){
            UInt64 first = 0;
            readVarUInt(first, buf);
            std::string second;
            readBinary(second, buf);
            values.insert({first, second});
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn &to, Arena *arena) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & values = this->data(place).values;
        offsets_to.push_back(offsets_to.back() + values.size());

        for (auto it = values.begin(); it != values.end(); ++it){
            auto ptr = arena->alloc(it->second.size());
            std::copy(it->second.data(), it->second.data() + it->second.length(), ptr);
            StringRef str_serialized = StringRef{ptr, it->second.size()};
            data_to.deserializeAndInsertFromArena(it->second.data());
        }
    }
};

}
