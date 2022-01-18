#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <base/logger_useful.h>

static inline constexpr UInt64 GROUP_SORTED_DEFAULT_THRESHOLD = 0xFFFFFF;

namespace DB
{
template <typename Storage>
struct AggregateFunctionGroupSortedArrayDataBase
{
    typedef typename Storage::value_type ValueType;
    AggregateFunctionGroupSortedArrayDataBase(UInt64 threshold_ = GROUP_SORTED_DEFAULT_THRESHOLD) : threshold(threshold_) { }

    virtual ~AggregateFunctionGroupSortedArrayDataBase() { }
    inline void narrowDown()
    {
        while (values.size() > threshold)
            values.erase(--values.end());
    }

    void merge(const AggregateFunctionGroupSortedArrayDataBase & other)
    {
        values.insert(other.values.begin(), other.values.end());
        narrowDown();
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarT(UInt64(values.size()), buf);
        for (auto value : values)
        {
            serializeItem(buf, value);
        }
    }

    virtual void serializeItem(WriteBuffer & buf, ValueType & val) const = 0;
    virtual void deserializeItem(ReadBuffer & buf, ValueType & val, Arena * arena) const = 0;

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        values.clear();
        UInt64 length;
        readVarUInt(length, buf);

        while (length--)
        {
            ValueType value;
            deserializeItem(buf, value, arena);
            values.insert(value);
        }

        narrowDown();
    }

    UInt64 threshold;
    Storage values;
};

template <typename T>
static void writeOneItem(WriteBuffer & buf, T item)
{
    if constexpr (std::is_same_v<T, StringRef>)
        writeBinary(item, buf);
    else
        writeBinary(item, buf);
}

template <typename T>
static void readOneItem(ReadBuffer & buf, Arena * arena, T item)
{
    if constexpr (std::is_same_v<T, StringRef>)
        item = readStringBinaryInto(*arena, buf);
    else
        readBinary(item, buf);
}

template <typename T, bool is_weighted>
struct AggregateFunctionGroupSortedArrayData
{
};

template <typename T>
struct AggregateFunctionGroupSortedArrayData<T, true> : public AggregateFunctionGroupSortedArrayDataBase<std::multimap<Int64, T>>
{
    using Base = AggregateFunctionGroupSortedArrayDataBase<std::multimap<Int64, T>>;
    using Base::Base;

    void add(T item, Int64 weight)
    {
        Base::values.insert({weight, item});
        Base::narrowDown();
    }

    void serializeItem(WriteBuffer & buf, typename Base::ValueType & value) const override
    {
        writeOneItem(buf, value.first);
        writeOneItem(buf, value.second);
    }

    virtual void deserializeItem(ReadBuffer & buf, typename Base::ValueType & value, Arena * arena) const override
    {
        readOneItem(buf, arena, value.first);
        readOneItem(buf, arena, value.second);
    }

    static T itemValue(typename Base::ValueType & value) { return value.second; }
};

template <typename T>
struct AggregateFunctionGroupSortedArrayData<T, false> : public AggregateFunctionGroupSortedArrayDataBase<std::multiset<T>>
{
    using Base = AggregateFunctionGroupSortedArrayDataBase<std::multiset<T>>;
    using Base::Base;

    void add(T item)
    {
        Base::values.insert(item);
        Base::narrowDown();
    }

    void serializeItem(WriteBuffer & buf, typename Base::ValueType & value) const override { writeOneItem(buf, value); }

    void deserializeItem(ReadBuffer & buf, typename Base::ValueType & value, Arena * arena) const override
    {
        readOneItem(buf, arena, value);
    }

    static T itemValue(typename Base::ValueType & value) { return value; }
};
}

