#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupSortedArray.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


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

AggregateFunctionPtr createAggregateFunctionGroupSortedArray(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    assertBinary(name, argument_types);
    if (!isInteger(argument_types[1]))
        throw Exception("The second argument for aggregate function 'groupSortedArray' must have integer type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    UInt64 threshold = 10;  /// default values
    UInt64 load_factor = 3;

    if (!params.empty())
    {
        if (params.size() > 2)
            throw Exception("Aggregate function " + name + " requires two parameters or less.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
        if (params.size() == 2)
        {
            load_factor = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[1]);

            if (load_factor < 1)
                throw Exception("Too small parameter 'load_factor' for aggregate function " + name + ". Minimum: 1",
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        if (k > TOP_K_MAX_SIZE || load_factor > TOP_K_MAX_SIZE || k * load_factor > TOP_K_MAX_SIZE)
            throw Exception("Too large parameter(s) for aggregate function " + name + ". Maximum: " + toString(TOP_K_MAX_SIZE),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (k == 0)
            throw Exception("Parameter 0 is illegal for aggregate function " + name,
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = k;
    }

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupSortedArray>(
        *argument_types[0], threshold, load_factor, argument_types, params));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() +
            " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionGroupSortedArray(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("groupSortedArray", { createAggregateFunctionGroupSortedArray, properties });
}

}
