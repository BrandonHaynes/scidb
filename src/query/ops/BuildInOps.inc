/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/*
 * @file BuildInOps.inc
 *
 * @author: roman.simakov@gmail.com
 *
 * @brief Brief declaration of macroses for built-in operators to simplify adding new operators
 *
 * Add new declaration following by existing examples and use the real class names
 * which implementing logical and physical operators.
 */

//Example: see XExample.cpp files for details
//LOGICAL_BUILDIN_OPERATOR(LogicalExample);
//PHYSICAL_BUILDIN_OPERATOR(PhysicalExample);

// unfold
LOGICAL_BUILDIN_OPERATOR(LogicalUnfold);
PHYSICAL_BUILDIN_OPERATOR(PhysicalUnfold);

// CreateArray
LOGICAL_BUILDIN_OPERATOR(LogicalCreateArray);
PHYSICAL_BUILDIN_OPERATOR(PhysicalCreateArray);

// Load
LOGICAL_BUILDIN_OPERATOR(LogicalLoad);

// Project
LOGICAL_BUILDIN_OPERATOR(LogicalProject);
PHYSICAL_BUILDIN_OPERATOR(PhysicalProject);

// Cumulate
LOGICAL_BUILDIN_OPERATOR(LogicalCumulate);
PHYSICAL_BUILDIN_OPERATOR(PhysicalCumulate);

// Scan
LOGICAL_BUILDIN_OPERATOR(LogicalScan);
PHYSICAL_BUILDIN_OPERATOR(PhysicalScan);

// SubArray
LOGICAL_BUILDIN_OPERATOR(LogicalSubArray);
PHYSICAL_BUILDIN_OPERATOR(PhysicalSubArray);

// Between
LOGICAL_BUILDIN_OPERATOR(LogicalBetween);
PHYSICAL_BUILDIN_OPERATOR(PhysicalBetween);
LOGICAL_BUILDIN_OPERATOR(LogicalCrossBetween);
PHYSICAL_BUILDIN_OPERATOR(PhysicalCrossBetween);

// SCATTER/GATHER
LOGICAL_BUILDIN_OPERATOR(LogicalSG);
PHYSICAL_BUILDIN_OPERATOR(PhysicalSG);

// Apply
LOGICAL_BUILDIN_OPERATOR(LogicalApply);
PHYSICAL_BUILDIN_OPERATOR(PhysicalApply);

// Filter
LOGICAL_BUILDIN_OPERATOR(LogicalFilter);
PHYSICAL_BUILDIN_OPERATOR(PhysicalFilter);

// Count
//LOGICAL_BUILDIN_OPERATOR(LogicalCount);

// Average
//LOGICAL_BUILDIN_OPERATOR(LogicalAverage);

// Sum
//LOGICAL_BUILDIN_OPERATOR(LogicalSum);

// Store
LOGICAL_BUILDIN_OPERATOR(LogicalStore);
PHYSICAL_BUILDIN_OPERATOR(PhysicalStore);

// Redimension
LOGICAL_BUILDIN_OPERATOR(LogicalRedimension);
PHYSICAL_BUILDIN_OPERATOR(PhysicalRedimension);

// Redimension_Store
//LOGICAL_BUILDIN_OPERATOR(LogicalRedimensionStore);
//PHYSICAL_BUILDIN_OPERATOR(PhysicalRedimensionStore);

// Reshape
LOGICAL_BUILDIN_OPERATOR(LogicalReshape);
PHYSICAL_BUILDIN_OPERATOR(PhysicalReshape);

// Repart
LOGICAL_BUILDIN_OPERATOR(LogicalRepart);
PHYSICAL_BUILDIN_OPERATOR(PhysicalRepart);

// DeclarePhysicalOperator
LOGICAL_BUILDIN_OPERATOR(LogicalLoadLibrary);
PHYSICAL_BUILDIN_OPERATOR(PhysicalLoadLibrary);

// LoadModule
LOGICAL_BUILDIN_OPERATOR(LogicalLoadModule);
PHYSICAL_BUILDIN_OPERATOR(PhysicalLoadModule);

// Join
LOGICAL_BUILDIN_OPERATOR(LogicalJoin);
PHYSICAL_BUILDIN_OPERATOR(PhysicalJoin);

// CrossJoin
LOGICAL_BUILDIN_OPERATOR(LogicalCrossJoin);
PHYSICAL_BUILDIN_OPERATOR(PhysicalCrossJoin);

// Substitute
LOGICAL_BUILDIN_OPERATOR(LogicalSubstitute);
PHYSICAL_BUILDIN_OPERATOR(PhysicalSubstitute);

// Aggregate
LOGICAL_BUILDIN_OPERATOR(LogicalAggregate);
PHYSICAL_BUILDIN_OPERATOR(PhysicalAggregate);

// Regrid
LOGICAL_BUILDIN_OPERATOR(LogicalRegrid);
PHYSICAL_BUILDIN_OPERATOR(PhysicalRegrid);

// Window
LOGICAL_BUILDIN_OPERATOR(LogicalWindow);
PHYSICAL_BUILDIN_OPERATOR(PhysicalWindow);

// Slice
LOGICAL_BUILDIN_OPERATOR(LogicalSlice);
PHYSICAL_BUILDIN_OPERATOR(PhysicalSlice);

// Sort
LOGICAL_BUILDIN_OPERATOR(LogicalSort);
PHYSICAL_BUILDIN_OPERATOR(PhysicalSort);

// Sort
LOGICAL_BUILDIN_OPERATOR(LogicalSort2);
PHYSICAL_BUILDIN_OPERATOR(PhysicalSort2);

// List
LOGICAL_BUILDIN_OPERATOR(LogicalList);
PHYSICAL_BUILDIN_OPERATOR(PhysicalList);

// Attributes
LOGICAL_BUILDIN_OPERATOR(LogicalAttributes);
PHYSICAL_BUILDIN_OPERATOR(PhysicalAttributes);

// Dimensions
LOGICAL_BUILDIN_OPERATOR(LogicalDimensions);
PHYSICAL_BUILDIN_OPERATOR(PhysicalDimensions);

// Rename
LOGICAL_BUILDIN_OPERATOR(LogicalRename);
PHYSICAL_BUILDIN_OPERATOR(PhysicalRename);


// Concat
LOGICAL_BUILDIN_OPERATOR(LogicalConcat);
PHYSICAL_BUILDIN_OPERATOR(PhysicalConcat);

// Unpack
LOGICAL_BUILDIN_OPERATOR(LogicalUnpack);
PHYSICAL_BUILDIN_OPERATOR(PhysicalUnpack);

// Unpack
LOGICAL_BUILDIN_OPERATOR(LogicalOldUnpack);
PHYSICAL_BUILDIN_OPERATOR(PhysicalOldUnpack);

// Build
LOGICAL_BUILDIN_OPERATOR(LogicalBuild);
PHYSICAL_BUILDIN_OPERATOR(PhysicalBuild);

// BuildSparse
//LOGICAL_BUILDIN_OPERATOR(LogicalBuildSparse);
//PHYSICAL_BUILDIN_OPERATOR(PhysicalBuildSparse);

// Transpose
LOGICAL_BUILDIN_OPERATOR(LogicalTranspose);
PHYSICAL_BUILDIN_OPERATOR(PhysicalTranspose);

// Cast
LOGICAL_BUILDIN_OPERATOR(LogicalCast);
PHYSICAL_BUILDIN_OPERATOR(PhysicalCast);

// Remove
LOGICAL_BUILDIN_OPERATOR(LogicalRemove);
PHYSICAL_BUILDIN_OPERATOR(PhysicalRemove);

// RemoveVersions
LOGICAL_BUILDIN_OPERATOR(LogicalRemoveVersions);
PHYSICAL_BUILDIN_OPERATOR(PhysicalRemoveVersions);

// Merge
LOGICAL_BUILDIN_OPERATOR(LogicalMerge);
PHYSICAL_BUILDIN_OPERATOR(PhysicalMerge);

// Min
//LOGICAL_BUILDIN_OPERATOR(LogicalMin);

// Max
//LOGICAL_BUILDIN_OPERATOR(LogicalMax);

// Adddim
LOGICAL_BUILDIN_OPERATOR(LogicalAdddim);
PHYSICAL_BUILDIN_OPERATOR(PhysicalAdddim);

// Deldim
LOGICAL_BUILDIN_OPERATOR(LogicalDeldim);
PHYSICAL_BUILDIN_OPERATOR(PhysicalDeldim);

// Normalize
LOGICAL_BUILDIN_OPERATOR(LogicalNormalize);
PHYSICAL_BUILDIN_OPERATOR(PhysicalNormalize);

// Save
LOGICAL_BUILDIN_OPERATOR(LogicalSave);
PHYSICAL_BUILDIN_OPERATOR(PhysicalSave);

// Xgrid
LOGICAL_BUILDIN_OPERATOR(LogicalXgrid);
PHYSICAL_BUILDIN_OPERATOR(PhysicalXgrid);

// Thin
LOGICAL_BUILDIN_OPERATOR(LogicalThin);
PHYSICAL_BUILDIN_OPERATOR(PhysicalThin);

// Cross
//LOGICAL_BUILDIN_OPERATOR(LogicalCross);
//PHYSICAL_BUILDIN_OPERATOR(PhysicalCross);

// Help
LOGICAL_BUILDIN_OPERATOR(LogicalHelp);
PHYSICAL_BUILDIN_OPERATOR(PhysicalHelp);

// Lookup
LOGICAL_BUILDIN_OPERATOR(LogicalLookup);
PHYSICAL_BUILDIN_OPERATOR(PhysicalLookup);

// Reverse
LOGICAL_BUILDIN_OPERATOR(LogicalReverse);
PHYSICAL_BUILDIN_OPERATOR(PhysicalReverse);

// Input
LOGICAL_BUILDIN_OPERATOR(LogicalInput);
PHYSICAL_BUILDIN_OPERATOR(PhysicalInput);

// ExplainPhysical
LOGICAL_BUILDIN_OPERATOR(LogicalExplainPhysical);
PHYSICAL_BUILDIN_OPERATOR(PhysicalExplainPhysical);

// Versions
LOGICAL_BUILDIN_OPERATOR(LogicalVersions);
PHYSICAL_BUILDIN_OPERATOR(PhysicalVersions);

// AllVersions
LOGICAL_BUILDIN_OPERATOR(LogicalAllVersions);
PHYSICAL_BUILDIN_OPERATOR(PhysicalAllVersions);

// ExplainLogical
LOGICAL_BUILDIN_OPERATOR(LogicalExplainLogical);
PHYSICAL_BUILDIN_OPERATOR(PhysicalExplainLogical);

// UnLoadLibrary
LOGICAL_BUILDIN_OPERATOR(LogicalUnLoadLibrary);
PHYSICAL_BUILDIN_OPERATOR(PhysicalUnLoadLibrary);

// Solve
//LOGICAL_BUILDIN_OPERATOR(LogicalSolve);
//PHYSICAL_BUILDIN_OPERATOR(PhysicalSolve);

// Solve
//LOGICAL_BUILDIN_OPERATOR(LogicalCovariance);
//PHYSICAL_BUILDIN_OPERATOR(PhysicalCovariance);

// AttributeRename
LOGICAL_BUILDIN_OPERATOR(LogicalAttributeRename);
PHYSICAL_BUILDIN_OPERATOR(PhysicalAttributeRename);

// Bernoulli
LOGICAL_BUILDIN_OPERATOR(LogicalBernoulli);
PHYSICAL_BUILDIN_OPERATOR(PhysicalBernoulli);

// Cancel
LOGICAL_BUILDIN_OPERATOR(LogicalCancel);
PHYSICAL_BUILDIN_OPERATOR(PhysicalCancel);

// Var
//LOGICAL_BUILDIN_OPERATOR(LogicalVar);

// Stdev
//LOGICAL_BUILDIN_OPERATOR(LogicalStdev);

//Show
LOGICAL_BUILDIN_OPERATOR(LogicalShow);
PHYSICAL_BUILDIN_OPERATOR(PhysicalShow);

//Echo
LOGICAL_BUILDIN_OPERATOR(LogicalEcho);
PHYSICAL_BUILDIN_OPERATOR(PhysicalEcho);

//Reduce Distro
LOGICAL_BUILDIN_OPERATOR(LogicalReduceDistro);
PHYSICAL_BUILDIN_OPERATOR(PhysicalReduceDistro);

//Setopt
LOGICAL_BUILDIN_OPERATOR(LogicalSetopt);
PHYSICAL_BUILDIN_OPERATOR(PhysicalSetopt);

//Materialize
LOGICAL_BUILDIN_OPERATOR(LogicalMaterialize);
PHYSICAL_BUILDIN_OPERATOR(PhysicalMaterialize);

//DiskInfo
LOGICAL_BUILDIN_OPERATOR(LogicalDiskInfo);
PHYSICAL_BUILDIN_OPERATOR(PhysicalDiskInfo);

//Analyze
LOGICAL_BUILDIN_OPERATOR(LogicalAnalyze);
PHYSICAL_BUILDIN_OPERATOR(PhysicalAnalyze);

//ApproxDC
//LOGICAL_BUILDIN_OPERATOR(LogicalApproxDC);

// MStat
LOGICAL_BUILDIN_OPERATOR(LogicalMStat);
PHYSICAL_BUILDIN_OPERATOR(PhysicalMStat);

// Rank-Quantile
LOGICAL_BUILDIN_OPERATOR(LogicalRank);
PHYSICAL_BUILDIN_OPERATOR(PhysicalRank);
LOGICAL_BUILDIN_OPERATOR(LogicalAverageRank);
PHYSICAL_BUILDIN_OPERATOR(PhysicalAverageRank);
LOGICAL_BUILDIN_OPERATOR(LogicalQuantile);
PHYSICAL_BUILDIN_OPERATOR(PhysicalQuantile);

// insert
LOGICAL_BUILDIN_OPERATOR(LogicalInsert);
PHYSICAL_BUILDIN_OPERATOR(PhysicalInsert);

// variable_window
LOGICAL_BUILDIN_OPERATOR(LogicalVariableWindow);
PHYSICAL_BUILDIN_OPERATOR(PhysicalVariableWindow);

// splitarraytest
LOGICAL_BUILDIN_OPERATOR(LogicalSplitArrayTest);
PHYSICAL_BUILDIN_OPERATOR(PhysicalSplitArrayTest);

// consume
LOGICAL_BUILDIN_OPERATOR(LogicalConsume);
PHYSICAL_BUILDIN_OPERATOR(PhysicalConsume);

// uniq
LOGICAL_BUILDIN_OPERATOR(LogicalUniq);
PHYSICAL_BUILDIN_OPERATOR(PhysicalUniq);

// index_lookup
LOGICAL_BUILDIN_OPERATOR(LogicalIndexLookup);
PHYSICAL_BUILDIN_OPERATOR(PhysicalIndexLookup);
