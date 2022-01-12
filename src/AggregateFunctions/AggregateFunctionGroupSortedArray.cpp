#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupSortedArray.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <Common/FieldVisitorConvertToNumber.h>


static inline constexpr UInt64 TOP_K_MAX_SIZE = 0xFFFFFF;


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{
    template <typename T, bool is_weighted>
    class AggregateFunctionGroupSortedArrayNumeric : public AggregateFunctionGroupSortedArray<false, T, is_weighted>
    {
        using AggregateFunctionGroupSortedArray<false, T, is_weighted>::AggregateFunctionGroupSortedArray;
    };

    template <typename T, bool is_weighted>
    class AggregateFunctionGroupSortedArrayFieldType : public AggregateFunctionGroupSortedArray<false, typename T::FieldType, is_weighted>
    {
        using AggregateFunctionGroupSortedArray<false, typename T::FieldType, is_weighted>::AggregateFunctionGroupSortedArray;
        DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<T>()); }
    };

    template <bool is_weighted>
    static IAggregateFunction *
    createWithExtraTypes(const DataTypes & argument_types, UInt64 threshold, UInt64 load_factor, const Array & params)
    {
        if (argument_types.empty())
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Got empty arguments list");

        WhichDataType which(argument_types[0]);
        if (which.idx == TypeIndex::Date)
            return new AggregateFunctionGroupSortedArrayFieldType<DataTypeDate, is_weighted>(threshold, load_factor, argument_types, params);
        if (which.idx == TypeIndex::DateTime)
            return new AggregateFunctionGroupSortedArrayFieldType<DataTypeDateTime, is_weighted>(threshold, load_factor, argument_types, params);

        if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            return new AggregateFunctionGroupSortedArray<true, StringRef, false>(threshold, load_factor, argument_types, params);
        }
        else
        {
            return new AggregateFunctionGroupSortedArray<false, StringRef, false>(threshold, load_factor, argument_types, params);
        }
    }

    template <bool is_weighted>
    AggregateFunctionPtr createAggregateFunctionGroupSortedArray(
        const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        if (!is_weighted)
        {
            assertUnary(name, argument_types);
        }
        else
        {
            assertBinary(name, argument_types);
            if (!isInteger(argument_types[1]))
                throw Exception(
                    "The second argument for aggregate function 'groupSortedArray' must have integer type",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        UInt64 threshold = 10; /// default values
        UInt64 load_factor = 3;

        if (!params.empty())
        {
            if (params.size() > 2)
                throw Exception(
                    "Aggregate function " + name + " requires two parameters or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            UInt64 k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
            if (params.size() == 2)
            {
                load_factor = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[1]);

                if (load_factor < 1)
                    throw Exception(
                        "Too small parameter 'load_factor' for aggregate function " + name + ". Minimum: 1",
                        ErrorCodes::ARGUMENT_OUT_OF_BOUND);
            }

            if (k > TOP_K_MAX_SIZE || load_factor > TOP_K_MAX_SIZE || k * load_factor > TOP_K_MAX_SIZE)
                throw Exception(
                    "Too large parameter(s) for aggregate function " + name + ". Maximum: " + toString(TOP_K_MAX_SIZE),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            if (k == 0)
                throw Exception("Parameter 0 is illegal for aggregate function " + name, ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            threshold = k;
        }

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupSortedArrayNumeric, is_weighted>(
            *argument_types[0], threshold, load_factor, argument_types, params));

        if (!res)
            res = AggregateFunctionPtr(createWithExtraTypes<is_weighted>(argument_types, threshold, load_factor, params));

        if (!res)
            throw Exception(
                "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function 4 " + name,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;
    }
}

void registerAggregateFunctionGroupSortedArray(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("groupSortedArray", {createAggregateFunctionGroupSortedArray<false>, properties});
    factory.registerFunction("groupSortedArrayWeighted", {createAggregateFunctionGroupSortedArray<true>, properties});
}

}
