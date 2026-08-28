// Copyright (c) MongoDB, Inc.
// SPDX-License-Identifier: SSPL-1.0

#include "mongo/db/query/get_executor_fast_paths.h"

#include "mongo/db/client.h"
#include "mongo/db/curop.h"
#include "mongo/db/exec/classic/delete_stage.h"
#include "mongo/db/exec/classic/plan_stage.h"
#include "mongo/db/exec/classic/sort_key_generator.h"
#include "mongo/db/exec/classic/subplan.h"
#include "mongo/db/exec/express/plan_executor_express.h"
#include "mongo/db/exec/runtime_planners/planner_interface.h"
#include "mongo/db/matcher/extensions_callback_real.h"
#include "mongo/db/pipeline/sbe_pushdown.h"
#include "mongo/db/query/canonical_query.h"
#include "mongo/db/query/collection_query_info.h"
#include "mongo/db/query/compiler/parsers/matcher/expression_parser.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/field_ref.h"
#include "mongo/db/query/planner_analysis.h"
#include "mongo/db/query/query_execution_knobs_gen.h"
#include "mongo/db/query/query_planner_common.h"
#include "mongo/db/query/query_planner_params.h"
#include "mongo/db/query/query_utils.h"
#include "mongo/db/query/wildcard_multikey_paths.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/server_parameter.h"
#include "mongo/db/service_context.h"
#include "mongo/db/shard_role/shard_catalog/index_catalog.h"
#include "mongo/db/shard_role/shard_catalog/index_descriptor.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/update/update_driver.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/container/vector.hpp>
#include <boost/cstdint.hpp>
#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <boost/optional/optional.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
// IWYU pragma: no_include "ext/alloc_traits.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo {

namespace {

boost::optional<ScopedCollectionFilter> getScopedCollectionFilter(
    OperationContext* opCtx,
    const MultipleCollectionAccessor& collections,
    const QueryPlannerParams& plannerParams) {
    if (plannerParams.mainCollectionInfo.options & QueryPlannerParams::INCLUDE_SHARD_FILTER) {
        auto collFilter = collections.getMainCollectionPtrOrAcquisition().getShardingFilter();
        tassert(11321302,
                "Attempting to use shard filter when there's no shard filter available for "
                "the collection",
                collFilter);
        return collFilter;
    }
    return boost::none;
}


struct PrefixScanTarget {
    const IndexEntry* index;
    std::vector<BSONElement> prefixValues;
};

// Hinted/sorted queries skip isExpressEligible(); re-check those refusals here.
bool isPrefixScanExpressEligibleQuery(const CanonicalQuery& cq) {
    const auto& findCommand = cq.getFindCommandRequest();
    return !cq.getExpCtx()->getQueryKnobConfiguration().getDisableSingleFieldExpressExecutor() &&
        !cq.metadataDeps().any() && !cq.getExpCtx()->getQuerySettings().getQueryFramework() &&
        (cq.getProj() == nullptr || cq.getProj()->isSimple()) && !findCommand.getReturnKey() &&
        !findCommand.getBatchSize() && !findCommand.getShowRecordId() &&
        findCommand.getMin().isEmpty() && findCommand.getMax().isEmpty() &&
        !findCommand.getSkip() && !findCommand.getTailable();
}

boost::optional<PrefixScanTarget> getIndexForExpressPrefixScan(
    const CanonicalQuery& cq,
    const CollectionPtr& collection,
    const QueryPlannerParams& params) {
    if (!internalQueryEnableExpressPrefixScan.load()) {
        return boost::none;
    }
    if (!isPrefixScanExpressEligibleQuery(cq)) {
        return boost::none;
    }
    if (!collection || collection->getClusteredInfo()) {
        return boost::none;
    }
    // An orphan rejected by the shard filter yields no document, so the batch size stops
    // bounding how many keys a single getNext() walks. Leaving shard filtering out keeps the
    // scan bounded without a yield point, matching the limit getIndexForExpressEquality puts
    // on non-unique indexes (TODO SERVER-87016).
    if (params.mainCollectionInfo.options & QueryPlannerParams::INCLUDE_SHARD_FILTER) {
        return boost::none;
    }
    // tryExpress() runs before planning; a distinct would lose DISTINCT_SCAN.
    if (cq.getDistinct()) {
        return boost::none;
    }

    const auto& findCommand = cq.getFindCommandRequest();

    const MatchExpression* root = cq.getPrimaryMatchExpression();
    std::vector<const ComparisonMatchExpressionBase*> equalities;
    if (root->matchType() == MatchExpression::EQ) {
        equalities.push_back(static_cast<const ComparisonMatchExpressionBase*>(root));
    } else if (root->matchType() == MatchExpression::AND) {
        for (size_t i = 0; i < root->numChildren(); ++i) {
            const auto* child = root->getChild(i);
            if (child->matchType() != MatchExpression::EQ) {
                return boost::none;
            }
            equalities.push_back(static_cast<const ComparisonMatchExpressionBase*>(child));
        }
    } else {
        return boost::none;
    }
    if (equalities.empty()) {
        return boost::none;
    }
    for (size_t i = 0; i < equalities.size(); ++i) {
        const auto* eq = equalities[i];
        if (eq->getCollator() != nullptr) {
            return boost::none;
        }
        // {$eq: null} is not exact; no residual filter here.
        if (!Indexability::isExactBoundsGenerating(eq->getData())) {
            return boost::none;
        }
        // Two equalities on the same path cannot both be a prefix.
        for (size_t j = 0; j < i; ++j) {
            if (eq->path() == equalities[j]->path()) {
                return boost::none;
            }
        }
    }

    const auto& sortObj = findCommand.getSort();

    // This path skips planning, so it must honour the hint itself.
    const BSONObj& hintObj = findCommand.getHint();

    // Ambiguous key-pattern hint is error 27; don't hide it by picking the first.
    if (!hintObj.isEmpty() && hintObj.firstElementFieldNameStringData() != "$hint") {
        size_t matches = 0;
        for (const auto& index : params.mainCollectionInfo.indexes) {
            if (hintObj.woCompare(index.keyPattern) == 0 && ++matches > 1) {
                return boost::none;
            }
        }
    }

    boost::optional<PrefixScanTarget> chosen;
    for (const auto& index : params.mainCollectionInfo.indexes) {
        if (!hintObj.isEmpty() &&
            !hintMatchesNameOrPattern(hintObj, index.identifier.catalogName, index.keyPattern)) {
            continue;
        }
        if (index.type != INDEX_BTREE || index.multikey || index.sparse || index.filterExpr ||
            !CollatorInterface::collatorsMatch(index.collator, cq.getCollator())) {
            continue;
        }

        std::vector<std::string_view> fields;
        bool allAscending = true;
        for (auto&& elt : index.keyPattern) {
            if (!elt.isNumber() || elt.numberInt() != 1) {
                allAscending = false;
                break;
            }
            // Numeric path components are not marked multikey; no residual filter.
            FieldRef path{elt.fieldNameStringData()};
            for (FieldIndex i = 0; i < path.numParts(); ++i) {
                if (FieldRef::isNumericPathComponentStrict(path.getPart(i))) {
                    allAscending = false;
                    break;
                }
            }
            if (!allAscending) {
                break;
            }
            fields.push_back(elt.fieldNameStringData());
        }
        if (!allAscending || fields.size() < equalities.size()) {
            continue;
        }

        std::vector<BSONElement> values(equalities.size());
        std::vector<std::string_view> covered;
        bool matched = true;
        for (size_t i = 0; i < equalities.size(); ++i) {
            const ComparisonMatchExpressionBase* found = nullptr;
            for (const auto* eq : equalities) {
                if (eq->path() == fields[i]) {
                    found = eq;
                    break;
                }
            }
            if (!found) {
                matched = false;
                break;
            }
            values[i] = found->getData();
            covered.push_back(fields[i]);
        }
        if (!matched) {
            continue;
        }
        // Repeated key-pattern fields can leave an equality unused.
        bool allEqualitiesUsed = true;
        for (const auto* eq : equalities) {
            if (std::find(covered.begin(), covered.end(), eq->path()) == covered.end()) {
                allEqualitiesUsed = false;
                break;
            }
        }
        if (!allEqualitiesUsed) {
            continue;
        }

        if (!sortObj.isEmpty()) {
            size_t pos = equalities.size();
            bool sortSatisfied = true;
            for (auto&& elt : sortObj) {
                if (pos >= fields.size() || !elt.isNumber() || elt.numberInt() != 1 ||
                    elt.fieldNameStringData() != fields[pos]) {
                    sortSatisfied = false;
                    break;
                }
                ++pos;
            }
            if (!sortSatisfied) {
                continue;
            }
        }

        // Always fetches. If any eligible index covers the projection, decline the query.
        if (const auto* proj = cq.getProj();
            proj && proj->type() == projection_ast::ProjectType::kInclusion) {
            const auto& required = proj->getRequiredFields();
            StringDataSet keyPatternFields;
            for (auto&& elt : index.keyPattern) {
                if (elt.isNumber()) {
                    keyPatternFields.insert(elt.fieldNameStringData());
                }
            }
            if (!index.multikey && std::ranges::all_of(required, [&](const auto& path) {
                    return keyPatternFields.contains(std::string_view{path});
                })) {
                return boost::none;
            }
        }

        if (!chosen) {
            chosen = PrefixScanTarget{&index, std::move(values)};
        }
    }
    return chosen;
}

}  // namespace

ExpressResult tryExpress(OperationContext* opCtx,
                         const MultipleCollectionAccessor& collections,
                         std::unique_ptr<CanonicalQuery>& canonicalQuery,
                         std::size_t plannerOptions,
                         const MakePlannerParamsFn& makePlannerParams) {
    // First try to use the express id point query fast path.
    const auto& mainColl = collections.getMainCollection();
    const auto expressEligibility = isExpressEligible(opCtx, mainColl, *canonicalQuery);
    if (expressEligibility == ExpressEligibility::IdPointQueryEligible) {
        planCacheCounters.incrementClassicSkippedCounter();
        auto plannerParams =
            std::make_unique<QueryPlannerParams>(QueryPlannerParams::ArgsForExpress{
                opCtx, *canonicalQuery, collections, plannerOptions});
        auto collectionFilter = getScopedCollectionFilter(opCtx, collections, *plannerParams);
        const bool isClusteredOnId = plannerParams->clusteredInfo
            ? clustered_util::isClusteredOnId(plannerParams->clusteredInfo)
            : false;

        auto expressExecutor = isClusteredOnId
            ? makeExpressExecutorForFindByClusteredId(
                  opCtx,
                  std::move(canonicalQuery),
                  collections.getMainCollectionPtrOrAcquisition(),
                  std::move(collectionFilter),
                  plannerOptions & QueryPlannerParams::RETURN_OWNED_DATA)
            : makeExpressExecutorForFindById(opCtx,
                                             std::move(canonicalQuery),
                                             collections.getMainCollectionPtrOrAcquisition(),
                                             std::move(collectionFilter),
                                             plannerOptions &
                                                 QueryPlannerParams::RETURN_OWNED_DATA);

        return {.executor = std::move(expressExecutor)};
    }

    // The query might still be eligible for express execution via the index equality fast path.
    // However, that requires the full set of planner parameters for the main collection to be
    // available and creating those now allows them to be reused for subsequent strategies if
    // the express index equality one fails.
    auto paramsForSingleCollectionQuery =
        makePlannerParams(*canonicalQuery, plannerOptions, boost::none /* replanningData */);
    if (expressEligibility == ExpressEligibility::IndexedEqualityEligible) {
        if (auto indexEntry =
                getIndexForExpressEquality(*canonicalQuery, *paramsForSingleCollectionQuery)) {
            auto expressExecutor = makeExpressExecutorForFindByUserIndex(
                opCtx,
                std::move(canonicalQuery),
                collections.getMainCollectionPtrOrAcquisition(),
                *indexEntry,
                getScopedCollectionFilter(opCtx, collections, *paramsForSingleCollectionQuery),
                plannerOptions & QueryPlannerParams::RETURN_OWNED_DATA);

            return {.executor = std::move(expressExecutor)};
        }
    }

    if (auto target = getIndexForExpressPrefixScan(
            *canonicalQuery, mainColl, *paramsForSingleCollectionQuery)) {
        planCacheCounters.incrementClassicSkippedCounter();
        auto expressExecutor = makeExpressExecutorForPrefixScan(
            opCtx,
            std::move(canonicalQuery),
            collections.getMainCollectionPtrOrAcquisition(),
            *target->index,
            std::move(target->prefixValues),
            boost::none /* collectionFilter, refused above */,
            plannerOptions & QueryPlannerParams::RETURN_OWNED_DATA);
        return {.executor = std::move(expressExecutor)};
    }

    // Allow reuse of the planner params, in case other planning logic needs it.
    return {.plannerParams = std::move(paramsForSingleCollectionQuery)};
}

std::unique_ptr<classic_runtime_planner::IdHackPlanner> tryIdHack(
    OperationContext* opCtx,
    const MultipleCollectionAccessor& collections,
    CanonicalQuery* cq,
    const std::function<PlannerData()>& makePlannerData) {
    const auto& mainCollection = collections.getMainCollection();
    if (!isIdHackEligibleQuery(mainCollection, *cq)) {
        return nullptr;
    }

    const auto indexEntry = mainCollection->getIndexCatalog()->findIdIndex(opCtx);
    if (!indexEntry) {
        return nullptr;
    }

    LOGV2_DEBUG(20922,
                2,
                "Using classic engine idhack",
                "canonicalQuery"_attr = redact(cq->toStringShort()));
    planCacheCounters.incrementClassicSkippedCounter();
    fastPathQueryCounters.incrementIdHackQueryCounter();
    return std::make_unique<classic_runtime_planner::IdHackPlanner>(makePlannerData(), indexEntry);
}

}  // namespace mongo
