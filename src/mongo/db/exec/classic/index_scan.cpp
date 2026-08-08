// Copyright (c) MongoDB, Inc.
// SPDX-License-Identifier: SSPL-1.0


// IWYU pragma: no_include "boost/intrusive/detail/iterator.hpp"
#include "mongo/db/exec/classic/index_scan.h"

#include "mongo/db/exec/classic/filter.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/memory_tracking/operation_memory_usage_tracker.h"
#include "mongo/db/query/compiler/optimizer/index_bounds_builder/index_bounds_builder.h"
#include "mongo/db/query/plan_executor_impl.h"
#include "mongo/db/query/stage_memory_limit_knobs/knobs.h"
#include "mongo/db/shard_role/transaction_resources.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/storage/exceptions.h"
#include "mongo/db/storage/key_string/key_string.h"
#include "mongo/util/assert_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery


namespace {

// Return a value in the set {-1, 0, 1} to represent the sign of parameter i.
int sgn(int i) {
    if (i == 0)
        return 0;
    return i > 0 ? 1 : -1;
}

/**
 * Hands 'obj' to 'member' as an owned object, reusing the DocumentStorage already on the member.
 * Mirrors mongo::(anonymous)::transitionMemberToOwnedObj in exec/classic/projection.cpp, which is
 * what the PROJECTION_COVERED parent calls when the projection is not folded into the scan.
 */
void transitionMemberToOwnedObj(const mongo::BSONObj& obj, mongo::WorkingSetMember* member) {
    mongo::MutableDocument md(std::move(member->doc.value()));
    md.reset(obj, false);
    member->keyData.clear();
    member->recordId = {};
    member->doc = {{}, md.freeze()};
    member->transitionToOwnedObj();
}

}  // namespace

namespace mongo {

MONGO_FAIL_POINT_DEFINE(throwDuringIndexScanRestore);


IndexScan::IndexScan(ExpressionContext* expCtx,
                     CollectionAcquisition collection,
                     IndexScanParams params,
                     WorkingSet* workingSet,
                     const MatchExpression* filter,
                     std::unique_ptr<CoveredProjection> coveredProjection)
    : RequiresIndexStage(kStageType, expCtx, collection, params.indexEntry, workingSet),
      _workingSet(workingSet),
      _coveredProjection(std::move(coveredProjection)),
      _keyPattern(params.keyPattern.getOwned()),
      _bounds(std::move(params.bounds)),
      _filter((filter && !filter->isTriviallyTrue()) ? filter : nullptr),
      _direction(params.direction),
      _forward(params.direction == 1),
      _addKeyMetadata(params.addKeyMetadata),
      _dedup(params.shouldDedup),
      _recordIdDeduplicator(expCtx),
      _startKeyInclusive(IndexBounds::isStartIncludedInBound(_bounds.boundInclusion)),
      _endKeyInclusive(IndexBounds::isEndIncludedInBound(_bounds.boundInclusion)),
      _memoryTracker(OperationMemoryUsageTracker::createSimpleMemoryUsageTrackerForStage(
          *expCtx, loadMemoryLimit(StageMemoryLimit::IndexScanStageMaxMemoryBytes))),
      _dedupReporter(OperationMemoryUsageTracker::createDeduplicatorReporter(
          [](int64_t deduplicatedBytes, int64_t deduplicatedRecords) {
              ixScanCounters.incrementPerDeduplication(deduplicatedBytes, deduplicatedRecords);
          },
          internalQueryMaxWriteToServerStatusMemoryUsageBytes.loadRelaxed())) {
    _specificStats.indexName = params.name;
    _specificStats.keyPattern = _keyPattern;
    _specificStats.isMultiKey = params.isMultiKey;
    _specificStats.multiKeyPaths = params.multikeyPaths;
    _specificStats.isUnique = indexDescriptor()->unique();
    _specificStats.isSetSparseByUser = indexDescriptor()->isSetSparseByUser();
    _specificStats.isPartial = indexDescriptor()->isPartial();
    _specificStats.indexVersion = static_cast<int>(indexDescriptor()->version());
    _specificStats.collation = indexDescriptor()
                                   ->infoObj()
                                   .getObjectField(IndexDescriptor::kCollationFieldName)
                                   .getOwned();
    _specificStats.coveredProjection = static_cast<bool>(_coveredProjection);
}

void IndexScan::projectKeyValueView(const SortedDataKeyValueView& view, WorkingSetMember* member) {
    auto keyString = view.getKeyStringWithoutRecordIdView();
    auto typeBitsView = view.getTypeBitsView();
    BufReader typeBitsReaderBuf(typeBitsView.data(), typeBitsView.size());
    auto typeBitsReader =
        key_string::TypeBits::getReaderFromBuffer(view.getVersion(), &typeBitsReaderBuf);

    // The object has to own its bytes: the member's Document holds on to it past this call.
    BSONObjBuilder bob;
    key_string::toBsonProjectedSafe(keyString,
                                    _coveredProjection->ordering,
                                    typeBitsReader,
                                    _coveredProjection->includeKey,
                                    _coveredProjection->fieldNames,
                                    bob,
                                    _coveredProjection->discardBuf);
    transitionMemberToOwnedObj(bob.obj(), member);
}

void IndexScan::projectMaterialisedKey(const BSONObj& key, WorkingSetMember* member) {
    BSONObjBuilder bob;
    size_t i = 0;
    for (auto&& elt : key) {
        if (i < _coveredProjection->includeKey.size() && _coveredProjection->includeKey[i]) {
            bob.appendAs(elt, _coveredProjection->fieldNames[i]);
        }
        ++i;
    }
    transitionMemberToOwnedObj(bob.obj(), member);
}

boost::optional<IndexKeyEntry> IndexScan::initIndexScan() {
    auto& ru = *shard_role_details::getRecoveryUnit(opCtx());
    // Perform the possibly heavy-duty initialization of the underlying index cursor.
    _indexCursor = indexAccessMethod()->newCursor(opCtx(), ru, _forward);

    // We always seek once to establish the cursor position.
    ++_specificStats.seeks;

    if (_bounds.isSimpleRange) {
        // Start at one key, end at another.
        _startKey = _bounds.startKey;
        _endKey = _bounds.endKey;
        _indexCursor->setEndPosition(_endKey, _endKeyInclusive);

        key_string::Builder builder(
            indexAccessMethod()->getSortedDataInterface()->getKeyStringVersion());
        auto keyStringForSeek = IndexEntryComparison::makeKeyStringFromBSONKeyForSeek(
            _startKey,
            indexAccessMethod()->getSortedDataInterface()->getOrdering(),
            _forward,
            _startKeyInclusive,
            builder);
        return _indexCursor->seek(ru, keyStringForSeek);
    } else {
        // For single intervals, we can use an optimized scan which checks against the position
        // of an end cursor.  For all other index scans, we fall back on using
        // IndexBoundsChecker to determine when we've finished the scan.
        if (IndexBoundsBuilder::isSingleInterval(
                _bounds, &_startKey, &_startKeyInclusive, &_endKey, &_endKeyInclusive)) {
            _indexCursor->setEndPosition(_endKey, _endKeyInclusive);

            key_string::Builder builder(
                indexAccessMethod()->getSortedDataInterface()->getKeyStringVersion());
            auto keyStringForSeek = IndexEntryComparison::makeKeyStringFromBSONKeyForSeek(
                _startKey,
                indexAccessMethod()->getSortedDataInterface()->getOrdering(),
                _forward,
                _startKeyInclusive,
                builder);
            return _indexCursor->seek(ru, keyStringForSeek);
        } else {
            _checker.reset(new IndexBoundsChecker(&_bounds, _keyPattern, _direction));

            if (!_checker->getStartSeekPoint(&_seekPoint))
                return boost::none;
            key_string::Builder builder(
                indexAccessMethod()->getSortedDataInterface()->getKeyStringVersion(),
                indexAccessMethod()->getSortedDataInterface()->getOrdering());
            return _indexCursor->seek(ru,
                                      IndexEntryComparison::makeKeyStringFromSeekPointForSeek(
                                          _seekPoint, _forward, builder));
        }
    }
}

PlanStage::StageState IndexScan::doWork(WorkingSetID* out) {
    // Steady-state path for a folded-in covered projection: decode the wanted key components
    // straight out of the KeyString into the output object, so the key is never materialised in
    // full only to be walked and copied again.
    //
    // Restricted to GETTING_NEXT with no IndexBoundsChecker, because the checker is the one
    // remaining consumer of the materialised key that the stage builder cannot rule out in advance
    // -- it is created inside initIndexScan(). The seek paths fall through to the general path and
    // project from the materialised key; they run once per scan, not once per key.
    if (_coveredProjection && _scanState == GETTING_NEXT && !_checker) {
        SortedDataKeyValueView view;
        const auto viewRet = handlePlanStageYield(
            expCtx(),
            "IndexScan",
            [&] {
                view =
                    _indexCursor->nextKeyValueView(*shard_role_details::getRecoveryUnit(opCtx()));
                return PlanStage::ADVANCED;
            },
            [&] {
                // yieldHandler
                *out = WorkingSet::INVALID_ID;
            });

        if (viewRet != PlanStage::ADVANCED) {
            return viewRet;
        }

        if (view.isEmpty()) {
            _scanState = HIT_END;
            _commonStats.isEOF = true;
            _indexCursor.reset();
            _specificStats.peakTrackedMemBytes = _memoryTracker.peakTrackedMemoryBytes();
            return PlanStage::IS_EOF;
        }

        ++_specificStats.keysExamined;

        WorkingSetID id = _workingSet->allocate();
        projectKeyValueView(view, _workingSet->get(id));
        *out = id;
        return PlanStage::ADVANCED;
    }

    // Get the next kv pair from the index, if any.
    boost::optional<IndexKeyEntry> kv;

    const auto ret = handlePlanStageYield(
        expCtx(),
        "IndexScan",
        [&] {
            auto& ru = *shard_role_details::getRecoveryUnit(opCtx());
            switch (_scanState) {
                case INITIALIZING:
                    kv = initIndexScan();
                    break;
                case GETTING_NEXT:
                    kv = _indexCursor->next(ru);
                    break;
                case NEED_SEEK: {
                    ++_specificStats.seeks;
                    key_string::Builder builder(
                        indexAccessMethod()->getSortedDataInterface()->getKeyStringVersion(),
                        indexAccessMethod()->getSortedDataInterface()->getOrdering());
                    kv = _indexCursor->seek(ru,
                                            IndexEntryComparison::makeKeyStringFromSeekPointForSeek(
                                                _seekPoint, _forward, builder));
                    break;
                }
                case HIT_END:
                    return PlanStage::IS_EOF;
            }
            return PlanStage::ADVANCED;
        },
        [&] {
            // yieldHandler
            *out = WorkingSet::INVALID_ID;
        });

    if (ret != PlanStage::ADVANCED) {
        return ret;
    }

    if (kv) {
        // In debug mode, check that the cursor isn't lying to us.
        if (kDebugBuild && !_startKey.isEmpty()) {
            int cmp = kv->key.woCompare(_startKey,
                                        Ordering::make(_keyPattern),
                                        /*compareFieldNames*/ false);
            if (cmp == 0)
                dassert(_startKeyInclusive);
            dassert(_forward ? cmp >= 0 : cmp <= 0);
        }

        if (kDebugBuild && !_endKey.isEmpty()) {
            int cmp = kv->key.woCompare(_endKey,
                                        Ordering::make(_keyPattern),
                                        /*compareFieldNames*/ false);
            if (cmp == 0)
                dassert(_endKeyInclusive);
            dassert(_forward ? cmp <= 0 : cmp >= 0);
        }

        ++_specificStats.keysExamined;
    }

    if (kv && _checker) {
        switch (_checker->checkKey(kv->key, &_seekPoint)) {
            case IndexBoundsChecker::VALID:
                break;

            case IndexBoundsChecker::DONE:
                kv = boost::none;
                break;

            case IndexBoundsChecker::MUST_ADVANCE:
                _scanState = NEED_SEEK;
                return PlanStage::NEED_TIME;
        }
    }

    if (!kv) {
        _scanState = HIT_END;
        _commonStats.isEOF = true;
        _indexCursor.reset();
        _specificStats.peakTrackedMemBytes = _memoryTracker.peakTrackedMemoryBytes();
        return PlanStage::IS_EOF;
    }

    _scanState = GETTING_NEXT;

    // If we're deduping
    if (_dedup) {
        ++_specificStats.dupsTested;

        // ... check whether we have seen the record id.
        // Do not add the recordId to recordIdDeduplicator unless we know that the
        // scan will return the recordId.
        uint64_t dedupBytesPrev = _recordIdDeduplicator.getApproximateSize();
        bool duplicate = _filter == nullptr ? !_recordIdDeduplicator.insert(kv->loc)
                                            : _recordIdDeduplicator.contains(kv->loc);
        uint64_t dedupBytes = _recordIdDeduplicator.getApproximateSize();
        _memoryTracker.add(dedupBytes - dedupBytesPrev);
        _specificStats.peakTrackedMemBytes = _memoryTracker.peakTrackedMemoryBytes();
        uassert(11130305,
                "Exceeded memory limit in record id deduplicator for IXSCAN stage",
                _memoryTracker.withinMemoryLimit(opCtx()));

        // If we've seen the RecordId before
        if (duplicate) {
            // ...skip it
            ++_specificStats.dupsDropped;
            return PlanStage::NEED_TIME;
        } else if (!_filter) {
            _dedupReporter.add(dedupBytes - dedupBytesPrev);
        }
    }

    if (!Filter::passes(kv->key, _keyPattern, _filter)) {
        return PlanStage::NEED_TIME;
    }

    // If we're deduping and the record matches a non-null filter
    if (_dedup && _filter != nullptr) {
        // ... now we can add the RecordId to the Deduplicator.
        uint64_t dedupBytesPrev = _recordIdDeduplicator.getApproximateSize();
        _recordIdDeduplicator.insert(kv->loc);
        uint64_t dedupBytes = _recordIdDeduplicator.getApproximateSize();
        _memoryTracker.add(dedupBytes - dedupBytesPrev);
        _dedupReporter.add(dedupBytes - dedupBytesPrev);
        _specificStats.peakTrackedMemBytes = _memoryTracker.peakTrackedMemoryBytes();
        uassert(11130304,
                "Exceeded memory limit in record id deduplicator for IXSCAN stage",
                _memoryTracker.withinMemoryLimit(opCtx()));
    }

    // A folded-in covered projection reached the general path -- a seek, or a scan that turned out
    // to need an IndexBoundsChecker. Project from the materialised key so the output is the same
    // whichever path produced it. Done before getOwned(): the projected object owns its own bytes,
    // so owning the whole key first would be a copy thrown away on every key of such a scan.
    if (_coveredProjection) {
        WorkingSetID projectedId = _workingSet->allocate();
        projectMaterialisedKey(kv->key, _workingSet->get(projectedId));
        *out = projectedId;
        return PlanStage::ADVANCED;
    }

    if (!kv->key.isOwned())
        kv->key = kv->key.getOwned();

    // We found something to return, so fill out the WSM.
    WorkingSetID id = _workingSet->allocate();
    WorkingSetMember* member = _workingSet->get(id);
    member->recordId = std::move(kv->loc);
    member->keyData.push_back(
        IndexKeyDatum(_keyPattern,
                      kv->key,
                      workingSetIndexId(),
                      shard_role_details::getRecoveryUnit(opCtx())->getSnapshotId()));
    _workingSet->transitionToRecordIdAndIdx(id);

    if (_addKeyMetadata) {
        member->metadata().setIndexKey(IndexKeyEntry::rehydrateKey(_keyPattern, kv->key));
    }

    *out = id;
    return PlanStage::ADVANCED;
}

bool IndexScan::isEOF() const {
    return _commonStats.isEOF;
}

void IndexScan::doSaveStateRequiresIndex() {
    if (!_indexCursor)
        return;

    if (_scanState == NEED_SEEK) {
        _indexCursor->saveUnpositioned();
        return;
    }

    _indexCursor->save();
}

void IndexScan::doRestoreStateRequiresIndex() {
    if (_indexCursor) {
        if (MONGO_unlikely(throwDuringIndexScanRestore.shouldFail())) {
            throwTemporarilyUnavailableException(str::stream()
                                                 << "Hit failpoint '"
                                                 << throwDuringIndexScanRestore.getName() << "'.");
        }
        auto& ru = *shard_role_details::getRecoveryUnit(opCtx());
        _indexCursor->restore(ru);
    }
}

void IndexScan::doDetachFromOperationContext() {
    if (_indexCursor)
        _indexCursor->detachFromOperationContext();
}

void IndexScan::doReattachToOperationContext() {
    if (_indexCursor)
        _indexCursor->reattachToOperationContext(opCtx());
}

std::unique_ptr<PlanStageStats> IndexScan::getStats() {
    // WARNING: this could be called even if the collection was dropped.  Do not access any
    // catalog information here.

    // Add a BSON representation of the filter to the stats tree, if there is one.
    if (nullptr != _filter) {
        _commonStats.filter = _filter->serialize();
    }

    // These specific stats fields never change.
    if (_specificStats.indexType.empty()) {
        _specificStats.indexType = "BtreeCursor";  // TODO amName;

        _specificStats.indexBounds = _bounds.toBSON(!_specificStats.collation.isEmpty());

        _specificStats.direction = _direction;
    }

    std::unique_ptr<PlanStageStats> ret =
        std::make_unique<PlanStageStats>(_commonStats, STAGE_IXSCAN);
    ret->specific = std::make_unique<IndexScanStats>(_specificStats);
    return ret;
}

}  // namespace mongo
