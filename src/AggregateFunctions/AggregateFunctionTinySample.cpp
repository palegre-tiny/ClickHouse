
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

void registerAggregateFunctionGroupTinySamples(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("tinyCount", {createAggregateFunction_TinyCount, properties});
}

}
