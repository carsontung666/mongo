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
#include "mongo/db/query/planner_analysis.h"
#include "mongo/db/query/query_planner.h"
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
#include "mongo/util/fail_point.h"

#include <utility>

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
extern FailPoint pauseAfterFillingOutIndexEntries;

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

}  // namespace

ExpressResult tryExpress(OperationContext* opCtx,
                         const MultipleCollectionAccessor& collections,
                         std::unique_ptr<CanonicalQuery>& canonicalQuery,
                         std::size_t plannerOptions,
                         const MakePlannerParamsFn& makePlannerParams) {
    // First try to use the express id point query fast path.
    const auto& mainColl = collections.getMainCollection();
    ExpressEqualityList equalities;
    const auto expressEligibility =
        isExpressEligible(opCtx, mainColl, *canonicalQuery, &equalities);
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

    // Only the candidate indexes are needed here, not the full planner parameters.
    if (expressEligibility == ExpressEligibility::IndexedEqualityEligible) {
        QueryPlannerParams expressParams{QueryPlannerParams::ArgsForExpress{
            opCtx, *canonicalQuery, collections, plannerOptions}};
        expressParams.fillOutIndexEntriesForExpressEquality(
            opCtx, *canonicalQuery, collections, equalities);
        if (auto indexEntry =
                getIndexForExpressEquality(*canonicalQuery, expressParams, equalities)) {
            // Fires once here; a miss falls through to fillOutIndexEntries, which has its own.
            pauseAfterFillingOutIndexEntries.pauseWhileSet();
            auto expressExecutor = makeExpressExecutorForFindByUserIndex(
                opCtx,
                std::move(canonicalQuery),
                collections.getMainCollectionPtrOrAcquisition(),
                *indexEntry,
                getScopedCollectionFilter(opCtx, collections, expressParams),
                plannerOptions & QueryPlannerParams::RETURN_OWNED_DATA);

            return {.executor = std::move(expressExecutor)};
        }
    }

    return {.plannerParams = makePlannerParams(
                *canonicalQuery, plannerOptions, boost::none /* replanningData */)};
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
