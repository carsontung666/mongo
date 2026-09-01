// Copyright (c) MongoDB, Inc.
// SPDX-License-Identifier: SSPL-1.0

#pragma once

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/query/canonical_query.h"
#include "mongo/db/query/collation/collator_interface.h"
#include "mongo/db/query/compiler/logical_model/sort_pattern/sort_pattern.h"
#include "mongo/db/query/indexability.h"
#include "mongo/db/query/query_execution_knobs_gen.h"
#include "mongo/db/query/query_feature_flags_gen.h"
#include "mongo/db/query/query_integration_knobs_gen.h"
#include "mongo/db/query/query_optimization_knobs_gen.h"
#include "mongo/db/shard_role/shard_catalog/clustered_collection_util.h"
#include "mongo/db/shard_role/shard_catalog/collection.h"
#include "mongo/util/modules.h"

#include <string_view>

#include <boost/container/small_vector.hpp>

namespace mongo {
/**
 * Returns 'true' if 'sortPattern' contains any sort pattern parts that share a common prefix, false
 * otherwise.
 */
bool sortPatternHasPartsWithCommonPrefix(const SortPattern& sortPattern);

/**
 * Returns 'true' if the given match expression is of the shape {_id: {$eq: ...}}.
 */
bool isMatchIdHackEligible(MatchExpression* me);

/**
 * Returns true if 'query' describes an exact-match query on _id.
 */
[[MONGO_MOD_NEEDS_REPLACEMENT]] bool isSimpleIdQuery(const BSONObj& query);

/**
 * Returns 'true' if 'query' on the given 'collection' can be answered using a special IDHACK plan,
 * without taking into account the collators.
 */
inline bool isIdHackEligibleQueryWithoutCollator(const FindCommandRequest& findCommand,
                                                 MatchExpression* me = nullptr) {
    return !findCommand.getShowRecordId() && findCommand.getHint().isEmpty() &&
        findCommand.getMin().isEmpty() && findCommand.getMax().isEmpty() &&
        !findCommand.getSkip() &&
        (isSimpleIdQuery(findCommand.getFilter()) || isMatchIdHackEligible(me)) &&
        !findCommand.getTailable();
}

/**
 * Returns 'true' if 'query' on the given 'collection' can be answered using a special IDHACK plan.
 * TODO SERVER-123100: Remove isIdHackEligibleQuery() in favor of ExpCtx::isIdHackQuery() checks.
 */
inline bool isIdHackEligibleQuery(const CollectionPtr& collection, const CanonicalQuery& cq) {
    return isIdHackEligibleQueryWithoutCollator(cq.getFindCommandRequest(),
                                                cq.getPrimaryMatchExpression()) &&
        CollatorInterface::collatorsMatch(cq.getCollator(), collection->getDefaultCollator());
}

/**
 * One equality operand of an express-eligible predicate, together with the path it constrains.
 */
struct ExpressEquality {
    std::string_view path;
    BSONElement data;  // Unowned, borrowed from the match expression.
};

// Held inline: express predicates are short, so decomposing one does not allocate.
using ExpressEqualityList = boost::container::small_vector<ExpressEquality, 4>;
// Equality operands in index-key order, borrowed from the match expression.
using ExpressKeyOperands = boost::container::small_vector<BSONElement, 4>;

/**
 * Decomposes 'me' into equalities and returns 'true' for a single equality, or a conjunction of
 * equalities on distinct paths, all generating exact bounds. 'out' is unspecified when 'false'.
 */
inline bool collectExpressEqualities(const MatchExpression* me, ExpressEqualityList* out) {
    const auto addOne = [&](const MatchExpression* node) {
        if (node->matchType() != MatchExpression::EQ) {
            return false;
        }
        const auto* cmp = static_cast<const ComparisonMatchExpressionBase*>(node);
        if (!Indexability::isExactBoundsGenerating(cmp->getData())) {
            return false;
        }
        if (out->size() >= static_cast<size_t>(Ordering::kMaxCompoundIndexKeys)) {
            return false;
        }
        out->push_back(ExpressEquality{node->path(), cmp->getData()});
        return true;
    };

    // Accept nested $and so eligibility does not depend on CanonicalQuery's flattening.
    const auto collectFrom = [&](auto&& self, const MatchExpression* node) -> bool {
        if (node->matchType() == MatchExpression::AND) {
            if (node->numChildren() == 0) {
                return false;
            }
            for (size_t i = 0; i < node->numChildren(); ++i) {
                if (!self(self, node->getChild(i))) {
                    return false;
                }
            }
            return true;
        }
        return addOne(node);
    };

    out->clear();
    if (!collectFrom(collectFrom, me) || out->empty()) {
        return false;
    }
    // Two equalities on one path are redundant or contradictory; leave them to the planner.
    for (size_t i = 1; i < out->size(); ++i) {
        for (size_t j = 0; j < i; ++j) {
            if ((*out)[i].path == (*out)[j].path) {
                return false;
            }
        }
    }
    return true;
}

/**
 * Returns 'true' if 'query' can be answered using the user-index express path, subject to finding
 * a suitable index.
 */
inline bool isEqualityExpressEligibleQuery(const CanonicalQuery& cq,
                                           ExpressEqualityList* equalitiesOut = nullptr) {
    const auto& findCommand = cq.getFindCommandRequest();
    auto me = cq.getPrimaryMatchExpression();

    const bool isProjectionEligible = cq.getProj() == nullptr || cq.getProj()->isSimple();

    if (!isProjectionEligible || findCommand.getShowRecordId() ||
        !findCommand.getHint().isEmpty() || !findCommand.getMin().isEmpty() ||
        !findCommand.getMax().isEmpty() || !findCommand.getSort().isEmpty() ||
        findCommand.getSkip() || findCommand.getTailable()) {
        return false;
    }

    ExpressEqualityList local;
    ExpressEqualityList* out = equalitiesOut ? equalitiesOut : &local;
    if (!collectExpressEqualities(me, out)) {
        return false;
    }
    const auto& knobs = cq.getExpCtx()->getQueryKnobConfiguration();
    if (out->size() == 1 && knobs.getDisableSingleFieldExpressExecutor()) {
        return false;
    }
    if (out->size() > 1 &&
        (!feature_flags::gFeatureFlagExpressCompoundEquality.isEnabled() ||
         knobs.getDisableCompoundFieldExpressExecutor())) {
        return false;
    }
    return true;
}

/**
 * Describes whether or not a query is eligible for the express executor.
 */
enum ExpressEligibility {
    // For an ineligible query that should go through regular query optimization and execution.
    Ineligible = 0,
    // For a point query that can fulfilled via a single lookup into the _id index or with a direct
    // lookup into a clustered collection.
    IdPointQueryEligible,
    // For an equality query that *may* use the express executor if a suitable index is found.
    IndexedEqualityEligible,
};
inline ExpressEligibility isExpressEligible(OperationContext* opCtx,
                                            const CollectionPtr& coll,
                                            const CanonicalQuery& cq,
                                            ExpressEqualityList* equalities = nullptr) {
    // Not eligible to use the express path if a particular query framework is set.
    if (auto queryFramework = cq.getExpCtx()->getQuerySettings().getQueryFramework()) {
        return ExpressEligibility::Ineligible;
    }

    // If a query needs metadata, it is ineligible for express path.
    if (cq.metadataDeps().any()) {
        return ExpressEligibility::Ineligible;
    }

    const auto& findCommandReq = cq.getFindCommandRequest();

    if (!coll || findCommandReq.getReturnKey() || findCommandReq.getBatchSize() ||
        (cq.getProj() != nullptr && !cq.getProj()->isSimple())) {
        return ExpressEligibility::Ineligible;
    }
    if (isIdHackEligibleQuery(coll, cq) &&
        (coll->getIndexCatalog()->haveIdIndex(opCtx) ||
         clustered_util::isClusteredOnId(coll->getClusteredInfo()))) {
        return ExpressEligibility::IdPointQueryEligible;
    }

    if (isEqualityExpressEligibleQuery(cq, equalities) &&
        coll->getIndexCatalog()->haveAnyIndexes() && !coll->getClusteredInfo()) {
        return ExpressEligibility::IndexedEqualityEligible;
    }

    return ExpressEligibility::Ineligible;
}

inline bool isInternalOrDirectClient(Client* client) {
    return client->isInternalClient() || client->isInDirectClient();
}

/**
 * Verifies that users did not specify the internal fields 'originalQueryShapeHash' and
 * 'querySettings' directly.
 */
template <typename T>
concept hasOriginalQueryShapeHash = requires(const T& t) { t.getOriginalQueryShapeHash(); };
template <typename T>
concept hasQuerySettings = requires(const T& t) { t.getQuerySettings(); };

template <typename T>
requires hasOriginalQueryShapeHash<T>
void assertInternalParamsAreSetByInternalClients(Client* client, T& req) {
    const bool isInternalOrDirect = isInternalOrDirectClient(client);

    // Only check 'querySettings' if the command accepts it.
    if constexpr (hasQuerySettings<T>) {
        uassert(7923000,
                "BSON field 'querySettings' is an unknown field",
                isInternalOrDirect || !req.getQuerySettings().has_value() ||
                    feature_flags::gFeatureFlagAllowUserFacingQuerySettings.isEnabled());
    }

    uassert(10742702,
            "BSON field 'originalQueryShapeHash' is an unknown field",
            isInternalOrDirect || !req.getOriginalQueryShapeHash().has_value());
}

bool isSortSbeCompatible(const SortPattern& sortPattern);
}  // namespace mongo
