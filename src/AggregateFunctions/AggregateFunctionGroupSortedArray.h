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
    UInt64 reserved;

public:
    AggregateFunctionGroupSortedArray(UInt64 threshold_, UInt64 /*load_factor*/, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T>, AggregateFunctionGroupSortedArray<T>>(argument_types_, params)
        , threshold(threshold_), reserved(100/*load_factor * threshold*/) {}

    String getName() const override { return "groupSortedArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(this->argument_types[0]);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & values = this->data(place).values;
//        if (set.capacity() != reserved)
//            set.resize(reserved);

        auto v0 = columns[0]->getUInt(row_num);
        auto v1 = columns[1]->getUInt(row_num);
        //assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num], columns[1]->getUInt(row_num)
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
                                                                                                                                                                                                                                                                            
    void merge(AggregateDataPtr __restrict /*place*/, ConstAggregateDataPtr /*rhs*/, Arena *) const override
    {/*
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);
        set.merge(this->data(rhs).value);*/
    }   

    void serialize(ConstAggregateDataPtr __restrict /*place*/, WriteBuffer & /*buf*/, std::optional<size_t> /* version */) const override
    {/*
        this->data(place).value.write(buf);
        */
    }

    void deserialize(AggregateDataPtr __restrict /*place*/, ReadBuffer & /*buf*/, std::optional<size_t> /* version  */, Arena *) const override
    {
        /*
        auto & set = this->data(place).value;
        set.resize(reserved);
        set.read(buf);
        */
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

}
