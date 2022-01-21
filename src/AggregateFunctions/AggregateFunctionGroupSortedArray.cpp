#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupSortedArray.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <Common/FieldVisitorConvertToNumber.h>


static inline constexpr UInt64 GROUP_SORTED_ARRAY_MAX_SIZE = 0xFFFFFF;
static inline constexpr UInt64 GROUP_SORTED_ARRAY_DEFAULT_THRESHOLD = 10;


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
    template <typename T, bool is_weighted, typename TIndex>
    class AggregateFunctionGroupSortedArrayNumeric : public AggregateFunctionGroupSortedArray<false, T, is_weighted, TIndex>
    {
        using AggregateFunctionGroupSortedArray<false, T, is_weighted, TIndex>::AggregateFunctionGroupSortedArray;
    };

    template <typename T, bool is_weighted, typename TIndex>
    class AggregateFunctionGroupSortedArrayFieldType : public AggregateFunctionGroupSortedArray<false, typename T::FieldType, is_weighted, TIndex>
    {
        using AggregateFunctionGroupSortedArray<false, typename T::FieldType, is_weighted, TIndex>::AggregateFunctionGroupSortedArray;
        DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<T>()); }
    };

    template <bool is_weighted, typename TIndex>
    static IAggregateFunction *
    createWithExtraTypes(const DataTypes & argument_types, UInt64 threshold, const Array & params)
    {
        if (argument_types.empty())
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Got empty arguments list");

        WhichDataType which(argument_types[0]);
        if (which.idx == TypeIndex::Date)
            return new AggregateFunctionGroupSortedArrayFieldType<DataTypeDate, is_weighted, TIndex>(
                threshold, argument_types, params);
        if (which.idx == TypeIndex::DateTime)
            return new AggregateFunctionGroupSortedArrayFieldType<DataTypeDateTime, is_weighted, TIndex>(
                threshold, argument_types, params);

        if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            return new AggregateFunctionGroupSortedArray<true, StringRef, is_weighted, TIndex>(threshold, argument_types, params);
        }
        else
        {
            return new AggregateFunctionGroupSortedArray<false, StringRef, is_weighted, TIndex>(threshold, argument_types, params);
        }
    }

    template <template <typename, bool, typename> class AggregateFunctionTemplate, bool bool_param, typename TIndex, typename... TArgs>
    static IAggregateFunction * createWithNumericType2(const IDataType & argument_type, TArgs && ... args)
    {
        WhichDataType which(argument_type);
    #define DISPATCH(TYPE) \
        if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<TYPE, bool_param, TIndex>(std::forward<TArgs>(args)...);
        FOR_NUMERIC_TYPES(DISPATCH)
    #undef DISPATCH
        if (which.idx == TypeIndex::Enum8) return new AggregateFunctionTemplate<Int8, bool_param, TIndex>(std::forward<TArgs>(args)...);
        if (which.idx == TypeIndex::Enum16) return new AggregateFunctionTemplate<Int16, bool_param, TIndex>(std::forward<TArgs>(args)...);
        return nullptr;
    }

    template <bool is_weighted, typename TIndex>
    AggregateFunctionPtr createAggregateFunctionGroupSortedArrayTyped(
        const std::string & name, const DataTypes & argument_types, const Array & params, UInt64 threshold)
    {
         AggregateFunctionPtr res(createWithNumericType2<AggregateFunctionGroupSortedArrayNumeric, is_weighted, TIndex>(
            *argument_types[0], threshold, argument_types, params));

        if (!res)
            res = AggregateFunctionPtr(createWithExtraTypes<is_weighted, TIndex>(argument_types, threshold, params));

        if (!res)
            throw Exception(
                "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        
        return res;
    }

    template <bool is_weighted>
    AggregateFunctionPtr createAggregateFunctionGroupSortedArray(
        const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        if constexpr(!is_weighted)
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

        UInt64 threshold = GROUP_SORTED_ARRAY_DEFAULT_THRESHOLD;

        if (!params.empty())
        {
            if (params.size() > 2)
                throw Exception(
                    "Aggregate function " + name + " requires two parameters or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            UInt64 k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

            if (k > GROUP_SORTED_ARRAY_MAX_SIZE)
                throw Exception(
                    "Too large parameter(s) for aggregate function " + name + ". Maximum: " + toString(GROUP_SORTED_ARRAY_MAX_SIZE),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            if (k == 0)
                throw Exception("Parameter 0 is illegal for aggregate function " + name, ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            threshold = k;
        }

        if (is_weighted && isUnsignedInteger(argument_types[1]))
            return createAggregateFunctionGroupSortedArrayTyped<is_weighted, UInt64>(name, argument_types, params, threshold);
        else
            return createAggregateFunctionGroupSortedArrayTyped<is_weighted, Int64>(name, argument_types, params, threshold);
    }
}

void registerAggregateFunctionGroupSortedArray(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("groupSortedArray", {createAggregateFunctionGroupSortedArray<false>, properties});
    factory.registerFunction("groupSortedArrayWeighted", {createAggregateFunctionGroupSortedArray<true>, properties});
}

}
