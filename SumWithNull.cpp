#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>

using namespace Vertica;

class SumWithNull : public AggregateFunction
{
    virtual void initAggregate(ServerInterface &srvInterface, 
                               IntermediateAggs &aggs) {
        try {
            vfloat &sum = aggs.getFloatRef(0);
            sum = 0;
        } catch (std::exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while initializing intermediate aggregates: [%s]", e.what());
        }
    }
    
    void aggregate(ServerInterface &srvInterface, 
                   BlockReader &argReader, 
                   IntermediateAggs &aggs)
    {
        try {
            vfloat &sum = aggs.getFloatRef(0);
            if (vfloatIsNull(sum)) {
                return;
            }
            do {
                const vfloat &input = argReader.getFloatRef(0);
                if (vfloatIsNull(input)) {
                    sum = vfloat_null;
                    return;
                }
                sum += input;
            } while (argReader.next());
        } catch (std::exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing aggregate: [%s]", e.what());
        }
    }

    virtual void combine(ServerInterface &srvInterface, 
                         IntermediateAggs &aggs, 
                         MultipleIntermediateAggs &aggsOther)
    {
        try {
            vfloat &mySum = aggs.getFloatRef(0);
            if (vfloatIsNull(mySum)) {
                return;
            }

            // Combine all the other intermediate aggregates
            do {
                const vfloat &otherSum = aggsOther.getFloatRef(0);
                if (vfloatIsNull(otherSum)) {
                    mySum = vfloat_null;
                    return;
                }
            
                // Do the actual accumulation 
                mySum += otherSum;
            } while (aggsOther.next());
        } catch (std::exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while combining intermediate aggregates: [%s]", e.what());
        }
    }

    virtual void terminate(ServerInterface &srvInterface, 
                           BlockWriter &resWriter, 
                           IntermediateAggs &aggs)
    {
        try {
            const vfloat &sum = aggs.getFloatRef(0);
            resWriter.setFloat(sum);
        } catch (std::exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while computing aggregate output: [%s]", e.what());
        }
    }

    InlineAggregate()
};


/*
 * This class provides the meta-data associated with the aggregate function
 * shown above, as well as a way of instantiating objects of the class. 
 */
class SumWithNullFactory : public AggregateFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes, 
                              ColumnTypes &returnType)
    {
        argTypes.addNumeric();
        returnType.addNumeric();
    }

    // Provide return type length/scale/precision information (given the input
    // type length/scale/precision), as well as column names
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes)
    {
        const VerticaType &inType = inputTypes.getColumnType(0);
        outputTypes.addNumeric(inType.getNumericPrecision(), inType.getNumericScale());
    }

    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes, 
                                      SizedColumnTypes &intermediateTypeMetaData)
    {
        const VerticaType &inType = inputTypes.getColumnType(0);

        // intermediate sum
        int32 interPrec = inType.getNumericPrecision() + 3; // provision 1000x precision if possible
        const int32 MAX_NUMERIC_PREC = 1024;
        if (interPrec > MAX_NUMERIC_PREC) {
            interPrec = MAX_NUMERIC_PREC;
        }
        intermediateTypeMetaData.addNumeric(interPrec, inType.getNumericScale());
    }

    // Create an instance of the AggregateFunction
    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface)
    { return vt_createFuncObject<SumWithNull>(srvInterface.allocator); }

};

RegisterFactory(SumWithNullFactory);
