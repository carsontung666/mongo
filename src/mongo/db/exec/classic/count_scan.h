// Copyright (c) MongoDB, Inc.
// SPDX-License-Identifier: SSPL-1.0

#pragma once

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/classic/plan_stage.h"
#include "mongo/db/exec/classic/recordid_deduplicator.h"
#include "mongo/db/exec/classic/requires_index_stage.h"
#include "mongo/db/exec/classic/working_set.h"
#include "mongo/db/exec/plan_stats.h"
#include "mongo/db/memory_tracking/operation_memory_usage_tracker.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/query/compiler/physical_model/query_solution/stage_types.h"
#include "mongo/db/shard_role/shard_catalog/collection.h"
#include "mongo/db/shard_role/shard_catalog/index_catalog_entry.h"
#include "mongo/db/shard_role/shard_catalog/index_descriptor.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/modules.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace mongo {
using namespace std::literals::string_view_literals;

class WorkingSet;

struct CountScanParams {
    CountScanParams(const IndexCatalogEntry* entry,
                    std::string indexName,
                    BSONObj keyPattern,
                    MultikeyPaths multikeyPaths,
                    bool multikey)
        : indexEntry(entry),
          name(std::move(indexName)),
          keyPattern(std::move(keyPattern)),
          multikeyPaths(std::move(multikeyPaths)),
          isMultiKey(multikey) {
        tassert(11051649, "Expecting non-null index entry", entry);
    }

    CountScanParams(OperationContext* opCtx,
                    const CollectionPtr& collection,
                    const IndexCatalogEntry* entry)
        : CountScanParams(
              entry,
              entry->descriptor()->indexName(),
              entry->descriptor()->keyPattern(),
              [&]() {
                  MultikeyPaths paths;
                  collection->isIndexMultikey(opCtx, entry->descriptor()->indexName(), &paths);
                  return paths;
              }(),
              collection->isIndexMultikey(opCtx, entry->descriptor()->indexName(), nullptr)) {}

    const IndexCatalogEntry* indexEntry;
    std::string name;

    BSONObj keyPattern;

    MultikeyPaths multikeyPaths;
    bool isMultiKey;

    BSONObj startKey;
    bool startKeyInclusive{true};

    BSONObj endKey;
    bool endKeyInclusive{true};
};

/**
 * Used when don't need to return the actual records from the index or the collection (e.g. count
 * command and some cases of aggregation).
 *
 * Scans an index from a start key to an end key. Creates a WorkingSetMember for each matching index
 * key in RID_AND_OBJ state. It holds the index entry's record id and an empty object with a null
 * snapshot id rather than real data. Returning real data is unnecessary since all we need is the
 * count.
 *
 * The exception is a direct CountStage parent, which never reads that member and so disables
 * materialization when it adopts us. doWork() then returns ADVANCED with WorkingSet::INVALID_ID and
 * builds nothing.
 */
class CountScan final : public RequiresIndexStage {
public:
    CountScan(ExpressionContext* expCtx,
              CollectionAcquisition collection,
              CountScanParams params,
              WorkingSet* workingSet);

    StageState doWork(WorkingSetID* out) final;
    bool isEOF() const final;
    void doDetachFromOperationContext() final;
    void doReattachToOperationContext() final;

    StageType stageType() const final {
        return STAGE_COUNT_SCAN;
    }

    std::unique_ptr<PlanStageStats> getStats() final;

    const SpecificStats* getSpecificStats() const final;

    static constexpr std::string_view kStageType = "COUNT_SCAN"sv;

protected:
    void doSaveStateRequiresIndex() final;

    void doRestoreStateRequiresIndex() final;

private:
    // Private because WorkingSet::get() does not check for INVALID_ID outside debug builds, so any
    // consumer other than CountStage that called this would read out of bounds in a release build
    // instead of failing an assertion.
    friend class CountStage;
    void disableResultMaterialization() {
        // Two of the four places that construct a CountStage wrap an already-built rejected plan
        // root when explaining a count -- ClassicPlannerInterface::makeExecutor() and
        // buildRejectedExecutableTreesForExplain(). A rejected plan may have been worked during
        // multiplanning and may hold live WorkingSetIDs, so opting it out is not safe. Today a
        // COUNT_SCAN solution is never a rejected plan, because QueryPlanner::plan returns it as
        // the single solution; SERVER-118659 proposes to bring count scans under cost-based
        // ranking, which would change that. Decline rather than assert, so that landing it costs
        // this optimization instead of a user's explain. dassert catches the change in testing.
        if (_commonStats.works != 0) {
            dassert(false, "CountStage adopted a CountScan that had already been worked");
            return;
        }
        _materializeResults = false;
    }

    // The WorkingSet we annotate with results.  Not owned by us.
    WorkingSet* _workingSet;

    const BSONObj _keyPattern;

    const bool _shouldDedup;

    // When false, doWork() returns ADVANCED with WorkingSet::INVALID_ID rather than building a
    // member. Only a direct CountStage parent may clear this.
    bool _materializeResults = true;

    const BSONObj _startKey;
    const bool _startKeyInclusive = true;

    const BSONObj _endKey;
    const bool _endKeyInclusive = true;

    std::unique_ptr<SortedDataInterface::Cursor> _cursor;

    // The set of record ids we've returned so far. Used to avoid returning duplicates, if
    // '_shouldDedup' is set to true.
    RecordIdDeduplicator _recordIdDeduplicator;

    CountScanStats _specificStats;

    // Check memory usage of the stage.
    SimpleMemoryUsageTracker _memoryTracker;

    // Tracks memory usage of the record ID deduplicator and reports metrics to serverStatus.
    DeduplicatorReporter _dedupReporter;
};

}  // namespace mongo
