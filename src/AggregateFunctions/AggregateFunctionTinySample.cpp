
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/Helpers.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename T>struct TinySumData
{
    T total = 0;
};

template <typename T>class TinySum : public IAggregateFunctionDataHelper<TinySumData<T>, TinySum<T>>
{
public:
    TinySum(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<TinySumData<T>, TinySum<T>>(argument_types_, params)
    {
    }

    String getName() const override { return "TinySum"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<T>>(); }

    bool allocatesMemoryInArena() const override { return false;}

    void add(AggregateDataPtr __restrict place, const IColumn **columns, size_t row_num, Arena *) const override
    {
        T value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        this->data(place).total += value;
    }

    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena *, ssize_t if_argument_pos) const override
    {
        StringRef ref = columns[0]->getRawData();
        const T *values = reinterpret_cast<const T *>(ref.data);

        if (if_argument_pos >= 0)
        {
            StringRef refFilter = columns[if_argument_pos]->getRawData();
            const UInt8 * filter = reinterpret_cast<const UInt8 *>(refFilter.data);
            for (size_t i = 0; i < batch_size; i ++)
                if (filter[i])
                    this->data(place).total += values[i];
        }
        else
        {
            for (size_t i = 0; i < batch_size; i ++)
                this->data(place).total += values[i];
        }
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

AggregateFunctionPtr createAggregateFunction_TinySum(
        const std::string &name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception("Aggregate function " + name + " requires one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    WhichDataType which(argument_types[0]);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return AggregateFunctionPtr(new TinySum<TYPE>(argument_types, params));
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    throw Exception("Invalid parameter type. Function " + name + " only supports Numeric Types", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

void registerAggregateFunctionGroupTinySamples(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("tinySum", {createAggregateFunction_TinySum, properties});
}

}
