
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <base/logger_useful.h>
#include <AggregateFunctions/Helpers.h>


#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#define NO_SERIALIZE    void merge(AggregateDataPtr __restrict , ConstAggregateDataPtr , Arena *) const override{} \
                        void serialize(ConstAggregateDataPtr __restrict , WriteBuffer &, std::optional<size_t>) const override{} \
                        void deserialize(AggregateDataPtr __restrict , ReadBuffer &, std::optional<size_t> , Arena * ) const override{}

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////
// Example 1   : Count()

struct TinyCountData
{
    UInt64 counter = 0;
};

class TinyCount : public IAggregateFunctionDataHelper<TinyCountData, TinyCount>
{
public:
    TinyCount(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<TinyCountData, TinyCount>(argument_types_, params)
    {
    }

    String getName() const override { return "tinyCount"; }
    
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    bool allocatesMemoryInArena() const override { return false;}

    void add(AggregateDataPtr __restrict place, const IColumn **, size_t, Arena *) const override
    {
        this->data(place).counter ++;
    }
   
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).counter);
    }

    NO_SERIALIZE
};


AggregateFunctionPtr createAggregateFunction_TinyCount(
        const std::string &, const DataTypes & argument_types, const Array & params, const Settings *)
{
    return AggregateFunctionPtr(new TinyCount(argument_types, params));
}


//////////////////////////////////////////////////////////////////////////////////////////////////////


//////////////////////////////////////////////////////////////////////////////////////////////////////
// Example 2   : Sum()

struct TinySumData
{
    UInt64 total = 0;
};

class TinySum : public IAggregateFunctionDataHelper<TinySumData, TinySum>
{
protected:
    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    TinySum(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<TinySumData, TinySum>(argument_types_, params)
    {
    }

    String getName() const override { return "tinySum"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<UInt64>>(); }

    bool allocatesMemoryInArena() const override { return false;}

    void add(AggregateDataPtr __restrict place, const IColumn **columns, size_t row_num, Arena *) const override
    {
        UInt64 value = assert_cast<const ColumnVector<UInt64> &>(*columns[0]).getData()[row_num];
        this->data(place).total += value;
    }
   
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).total);
    }

    NO_SERIALIZE
};

AggregateFunctionPtr createAggregateFunction_TinySum(
        const std::string &name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception("Aggregate function " + name + " requires one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    WhichDataType which(argument_types[0]);
    if (which.idx == TypeIndex::UInt64)
        return AggregateFunctionPtr(new TinySum(argument_types, params));

    throw Exception("Invalid parameter type. Function " + name + " only supports UInt64", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////


//////////////////////////////////////////////////////////////////////////////////////////////////////
// Example 3   : SumT()

template <typename T>struct TinySumTData
{
    T total = 0;
};

template <typename T>class TinySumT : public IAggregateFunctionDataHelper<TinySumTData<T>, TinySumT<T>>
{
protected:
    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    TinySumT(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<TinySumTData<T>, TinySumT<T>>(argument_types_, params)
    {
    }

    String getName() const override { return "tinySumT"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<T>>(); }

    bool allocatesMemoryInArena() const override { return false;}

    void add(AggregateDataPtr __restrict place, const IColumn **columns, size_t row_num, Arena *) const override
    {
        T value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        this->data(place).total += value;
    }
   
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).total);
    }

    NO_SERIALIZE
};

AggregateFunctionPtr createAggregateFunction_TinySumT(
        const std::string &name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception("Aggregate function " + name + " requires one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    WhichDataType which(argument_types[0]);

#if 0
    if (which.idx == TypeIndex::UInt8)
        return AggregateFunctionPtr(new TinySumT<UInt8>(argument_types, params));
    if (which.idx == TypeIndex::UInt16)
        return AggregateFunctionPtr(new TinySumT<UInt16>(argument_types, params));
    if (which.idx == TypeIndex::UInt32)
        return AggregateFunctionPtr(new TinySumT<UInt32>(argument_types, params));
    if (which.idx == TypeIndex::UInt64)
        return AggregateFunctionPtr(new TinySumT<UInt64>(argument_types, params));
    if (which.idx == TypeIndex::Int8)
        return AggregateFunctionPtr(new TinySumT<UInt8>(argument_types, params));
    if (which.idx == TypeIndex::Int16)
        return AggregateFunctionPtr(new TinySumT<Int16>(argument_types, params));
    if (which.idx == TypeIndex::Int32)
        return AggregateFunctionPtr(new TinySumT<Int32>(argument_types, params));
    if (which.idx == TypeIndex::Int64)
        return AggregateFunctionPtr(new TinySumT<Int64>(argument_types, params));
    if (which.idx == TypeIndex::Float32)
        return AggregateFunctionPtr(new TinySumT<Float32>(argument_types, params));
    if (which.idx == TypeIndex::Float64)
        return AggregateFunctionPtr(new TinySumT<Float64>(argument_types, params));
#else
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return AggregateFunctionPtr(new TinySumT<TYPE>(argument_types, params));
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
#endif

    throw Exception("Invalid parameter type. Function " + name + " only supports Numeric Types", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}
//////////////////////////////////////////////////////////////////////////////////////////////////////



//////////////////////////////////////////////////////////////////////////////////////////////////////
// Example 3   : SumTS()

template <typename T>struct TinySumTSData
{
    T total = 0;
};

template <typename T>class TinySumTS : public IAggregateFunctionDataHelper<TinySumTSData<T>, TinySumTS<T>>
{
protected:
    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    TinySumTS(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<TinySumTSData<T>, TinySumTS<T>>(argument_types_, params)
    {
    }

    String getName() const override { return "TinySumTS"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<T>>(); }

    bool allocatesMemoryInArena() const override { return false;}

    void add(AggregateDataPtr __restrict place, const IColumn **columns, size_t row_num, Arena *) const override
    {
        T value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        this->data(place).total += value;
    }
   
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).total);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).total += this->data(rhs).total;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer &buf, std::optional<size_t>) const override
    {
        if constexpr (std::numeric_limits<T>::is_signed)
        {
            writeVarInt(Int64(this->data(place).total), buf);
        }
        else
        {
            writeVarUInt(UInt64(this->data(place).total), buf);
        }
    }
    
    void deserialize(AggregateDataPtr __restrict place, ReadBuffer &buf, std::optional<size_t> , Arena * ) const override
    {
        if constexpr (std::numeric_limits<T>::is_signed)
        {
            DB::Int64 val;
            readVarT(val, buf);
            this->data(place).total = val;
        }
        else
        {
            DB::UInt64 val;
            readVarT(val, buf);
            this->data(place).total = val;
        }
    }
};

AggregateFunctionPtr createAggregateFunction_TinySumTS(
        const std::string &name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception("Aggregate function " + name + " requires one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    WhichDataType which(argument_types[0]);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return AggregateFunctionPtr(new TinySumTS<TYPE>(argument_types, params));
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    throw Exception("Invalid parameter type. Function " + name + " only supports Numeric Types", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////


void registerAggregateFunctionGroupTinySamples(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("tinyCount", {createAggregateFunction_TinyCount, properties});
    factory.registerFunction("tinySum", {createAggregateFunction_TinySum, properties});
    factory.registerFunction("tinySumT", {createAggregateFunction_TinySumT, properties});
    factory.registerFunction("tinySumTS", {createAggregateFunction_TinySumT, properties});
}

}
