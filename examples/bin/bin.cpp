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

AGGBIN(1, 10);
AGGBIN(2, 10);
AGGBIN(4, 10);
AGGBIN(8, 10);
AGGBIN(16, 10);
AGGBIN(32, 10);
AGGBIN(64, 10);
AGGBIN(128, 10);
AGGBIN(256, 10);
AGGBIN(512, 10);
AGGBIN(1024, 10);

static class BinLibrary
{
public:
    // Registering objects
    BinLibrary()
    {
        // Aggregates
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin1,    double>("bin1",       1, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin2,    double>("bin2",       2, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin4,    double>("bin4",       4, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin8,    double>("bin8",       8, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin16,   double>("bin16",     16, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin32,   double>("bin32",     32, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin64,   double>("bin64",     64, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin128,  double>("bin128",   128, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin256,  double>("bin256",   256, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin512,  double>("bin512",   512, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(AggregatePtr(new BinAggregate<AggBin1024, double>("bin1024", 1024, TypeLibrary::getType(TID_DOUBLE))));
      _aggregates.push_back(make_shared<SignedCountAggregate>(TypeLibrary::getType(TID_VOID)));
    }

    ~BinLibrary()
    {
        scidb::ErrorsLibrary::getInstance()->unregisterErrors("libbin");
    }

private:
    scidb::ErrorsLibrary::ErrorsMessages _errors;
} _instance;
