#include <vector>
#include <boost/assign.hpp>

#include "query/Operator.h"
#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "query/TypeSystem.h"
#include "system/ErrorsLibrary.h"
#include "query/TileFunctions.h"
#include "query/Aggregate.h"

using namespace std;
using namespace scidb;
using namespace boost::assign;

#include "bin_functions.h"

vector<AggregatePtr> _aggregates;
EXPORTED_FUNCTION const vector<AggregatePtr>& GetAggregates()
{
    return _aggregates;
}

static class BinLibrary
{
public:
    // Registering objects
    BinLibrary()
    {
        // Aggregates
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin, double>("bin", TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(make_shared<SignedCountAggregate>(TypeLibrary::getType(TID_VOID)));
    }

    ~BinLibrary()
    {
        scidb::ErrorsLibrary::getInstance()->unregisterErrors("libbin");
    }

private:
    scidb::ErrorsLibrary::ErrorsMessages _errors;
} _instance;
