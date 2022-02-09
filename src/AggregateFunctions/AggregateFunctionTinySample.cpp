
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>

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

struct TinySumData
{
    UInt64 total = 0;
};

class TinySum : public IAggregateFunctionDataHelper<TinySumData, TinySum>
{
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

void registerAggregateFunctionGroupTinySamples(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("tinySum", {createAggregateFunction_TinySum, properties});
}

}
