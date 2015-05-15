########################################
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
########################################

# One can create subdirectory here for every component
set(ops_lib_src
    BuildInOps.inc
    create_array/LogicalCreateArray.cpp
    create_array/PhysicalCreateArray.cpp
    project/LogicalProject.cpp
    project/PhysicalProject.cpp
    scan/LogicalScan.cpp
    scan/PhysicalScan.cpp
    bernoulli/LogicalBernoulli.cpp
    bernoulli/PhysicalBernoulli.cpp
    bernoulli/NumericOps.cpp
    subarray/SubArray.cpp
    subarray/LogicalSubArray.cpp
    subarray/PhysicalSubArray.cpp
    between/BetweenArray.cpp
    between/LogicalBetween.cpp
    between/PhysicalBetween.cpp
    between/LogicalCrossBetween.cpp
    between/PhysicalCrossBetween.cpp
    filter/FilterArray.cpp
    filter/LogicalFilter.cpp
    filter/PhysicalFilter.cpp
    apply/ApplyArray.cpp
    apply/LogicalApply.cpp
    apply/PhysicalApply.cpp
    sg/LogicalSG.cpp
    sg/PhysicalSG.cpp
    load/LogicalLoad.cpp
    store/LogicalStore.cpp
    store/PhysicalStore.cpp
    redimension/LogicalRedimension.cpp
    redimension/PhysicalRedimension.cpp
    redimension/RedimensionCommon.cpp
    redimension/SyntheticDimChunkMerger.cpp
    reshape/LogicalReshape.cpp
    reshape/PhysicalReshape.cpp
    reshape/ReshapeArray.cpp
    reshape/ShiftArray.cpp
    repart/LogicalRepart.cpp
    repart/PhysicalRepart.cpp
    load_library/LogicalLoadLibrary.cpp
    load_library/PhysicalLoadLibrary.cpp
    load_module/LogicalLoadModule.cpp
    load_module/PhysicalLoadModule.cpp
    aggregates/LogicalAggregate.cpp
    aggregates/PhysicalAggregate.cpp
    aggregates/LogicalRegrid.cpp
    aggregates/PhysicalRegrid.cpp
    aggregates/LogicalWindow.cpp
    aggregates/WindowArray.cpp
    aggregates/PhysicalWindow.cpp
    aggregates/LogicalCumulate.cpp
    aggregates/PhysicalCumulate.cpp
    join/JoinArray.cpp
    join/LogicalJoin.cpp
    join/PhysicalJoin.cpp
    cross_join/CrossJoinArray.cpp
    cross_join/LogicalCrossJoin.cpp
    cross_join/PhysicalCrossJoin.cpp
    substitute/LogicalSubstitute.cpp
    substitute/PhysicalSubstitute.cpp
    slice/LogicalSlice.cpp
    slice/PhysicalSlice.cpp
    slice/SliceArray.cpp
    sort/LogicalSort.cpp
    sort/PhysicalSort.cpp
    sort/ArrayBreaker.cpp
    sort/DistributedSort.cpp
    sort/LogicalOldSort.cpp
    sort/PhysicalOldSort.cpp
    sort2/LogicalSort.cpp
    sort2/PhysicalSort.cpp
    list/ListArrayBuilder.cpp
    list/LogicalList.cpp
    list/PhysicalList.cpp
    attributes/LogicalAttributes.cpp
    attributes/PhysicalAttributes.cpp
    dimensions/LogicalDimensions.cpp
    dimensions/PhysicalDimensions.cpp
    rename/LogicalRename.cpp
    rename/PhysicalRename.cpp
    concat/LogicalConcat.cpp
    concat/PhysicalConcat.cpp
    concat/ConcatArray.cpp
    unpack/LogicalUnpack.cpp
    unpack/PhysicalUnpack.cpp
    build/LogicalBuild.cpp
    build/PhysicalBuild.cpp
    build/BuildArray.cpp
    transpose/PhysicalTranspose.cpp
    transpose/LogicalTranspose.cpp
    transpose/TransposeArray.cpp
    cast/LogicalCast.cpp
    cast/PhysicalCast.cpp
    remove/LogicalRemove.cpp
    remove/PhysicalRemove.cpp
    remove/LogicalRemoveVersions.cpp
    remove/PhysicalRemoveVersions.cpp
    merge/LogicalMerge.cpp
    merge/PhysicalMerge.cpp
    merge/MergeArray.cpp
    normalize/LogicalNormalize.cpp
    normalize/PhysicalNormalize.cpp
    normalize/NormalizeArray.cpp
    save/LogicalSave.cpp
    save/PhysicalSave.cpp
    xgrid/LogicalXgrid.cpp
    xgrid/PhysicalXgrid.cpp
    xgrid/XgridArray.cpp
    thin/LogicalThin.cpp
    thin/PhysicalThin.cpp
    thin/ThinArray.cpp
    help/LogicalHelp.cpp
    help/PhysicalHelp.cpp
    lookup/LogicalLookup.cpp
    lookup/PhysicalLookup.cpp
    lookup/LookupArray.cpp
    reverse/LogicalReverse.cpp
    reverse/PhysicalReverse.cpp
    reverse/ReverseArray.cpp
    input/LogicalInput.cpp
    input/PhysicalInput.cpp
    input/InputArray.cpp
    input/ChunkLoader.cpp
    input/CsvChunkLoader.cpp
    explainPhysical/LogicalExplainPhysical.cpp
    explainPhysical/PhysicalExplainPhysical.cpp
    versions/LogicalVersions.cpp
    versions/PhysicalVersions.cpp
    allversions/LogicalAllVersions.cpp
    allversions/PhysicalAllVersions.cpp
    allversions/AllVersionsArray.cpp
    explainLogical/LogicalExplainLogical.cpp
    explainLogical/PhysicalExplainLogical.cpp
    unload_library/LogicalUnLoadLibrary.cpp
    unload_library/PhysicalUnLoadLibrary.cpp
    attribute_rename/LogicalAttributeRename.cpp
    attribute_rename/PhysicalAttributeRename.cpp
    cancel/LogicalCancel.cpp
    cancel/PhysicalCancel.cpp
    show/LogicalShow.cpp
    show/PhysicalShow.cpp
    reduce_distro/LogicalReduceDistro.cpp
    reduce_distro/PhysicalReduceDistro.cpp
    setopt/LogicalSetopt.cpp
    setopt/PhysicalSetopt.cpp
    diskinfo/LogicalDiskInfo.cpp
    diskinfo/PhysicalDiskInfo.cpp
    materialize/LogicalMaterialize.cpp
    materialize/PhysicalMaterialize.cpp
    analyze/LogicalAnalyze.cpp
    analyze/PhysicalAnalyze.cpp
    rankquantile/LogicalRank.cpp
    rankquantile/PhysicalRank.cpp
    rankquantile/LogicalQuantile.cpp
    rankquantile/PhysicalQuantile.cpp
    rankquantile/LogicalAverageRank.cpp
    rankquantile/PhysicalAverageRank.cpp
    rankquantile/RankCommon.cpp
    unfold/LogicalUnfold.cpp
    unfold/PhysicalUnfold.cpp
    unfold/UnfoldArray.cpp
    variable_window/LogicalVariableWindow.cpp
    variable_window/PhysicalVariableWindow.cpp
    insert/LogicalInsert.cpp
    insert/PhysicalInsert.cpp
    test/splitarray/LogicalSplitArrayTest.cpp
    test/splitarray/PhysicalSplitArrayTest.cpp
    consume/LogicalConsume.cpp
    consume/PhysicalConsume.cpp
    uniq/LogicalUniq.cpp
    uniq/PhysicalUniq.cpp
    index_lookup/LogicalIndexLookup.cpp
    index_lookup/PhysicalIndexLookup.cpp
)

find_package(Libcsv REQUIRED)
include_directories(${libcsv_INCLUDE_DIR})

file(GLOB_RECURSE ops_lib_include "*.h")

add_library(ops_lib STATIC ${ops_lib_src} ${ops_lib_include})
target_link_libraries(ops_lib compression_lib network_lib MurmurHash_lib ${libcsv_LIBRARY})
