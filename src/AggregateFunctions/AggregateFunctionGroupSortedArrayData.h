#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#define DEFAULT_THRESHOLD 10

namespace DB
{
template <typename Storage>
struct AggregateFunctionGroupSortedArrayDataBase
{
    typedef typename Storage::value_type ValueType;
    AggregateFunctionGroupSortedArrayDataBase(UInt64 threshold_ = DEFAULT_THRESHOLD) : threshold(threshold_) { }

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
        writeVarUInt(item, buf);
}

template <typename T>
static void readOneItem(ReadBuffer & buf, Arena * arena, T item)
{
    if constexpr (std::is_same_v<T, StringRef>)
        item = readStringBinaryInto(*arena, buf);
    else
        readVarUInt(item, buf);
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
        if (weight <= last || count < Base::threshold)
        {
            ++ count;
            Base::values.insert({weight, item});
            Base::narrowDown();
            last = (--Base::values.end())->first;
        }
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

    Int64 last = std::numeric_limits<Int64>::max();
    UInt64 count = 0;
};


template <typename T>
class AggregateFunctionGroupSortedArrayDataMSet : public AggregateFunctionGroupSortedArrayDataBase<std::multiset<T>>
{
public:
    using Base = AggregateFunctionGroupSortedArrayDataBase<std::multiset<T>>;
    using Base::Base;

    void serializeItem(WriteBuffer & buf, typename Base::ValueType & value) const override { writeOneItem(buf, value); }

    void deserializeItem(ReadBuffer & buf, typename Base::ValueType & value, Arena * arena) const override
    {
        readOneItem(buf, arena, value);
    }

    static T itemValue(typename Base::ValueType & value) { return value; }
};

template <>
struct AggregateFunctionGroupSortedArrayData<StringRef, false> : public AggregateFunctionGroupSortedArrayDataMSet<StringRef>
{
    using Base = AggregateFunctionGroupSortedArrayDataMSet<StringRef>;
    using Base::Base;
    void add(StringRef item)
    {
        Base::values.insert(item);
        Base::narrowDown();
    }
};

template <typename T>
struct AggregateFunctionGroupSortedArrayData<T, false> : public AggregateFunctionGroupSortedArrayDataMSet<T>
{
    using Base = AggregateFunctionGroupSortedArrayDataMSet<T>;
    using Base::Base;

    void add(T item)
    {
        if (item <= last || count < Base::threshold)
        {
            ++ count;
            Base::values.insert(item);
            Base::narrowDown();
            last = *(--Base::values.end());
        }
    }

    T last = std::numeric_limits<T>::max();
    UInt64 count = 0;
};
}

#undef DEFAULT_THRESHOLD
