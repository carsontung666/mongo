// Copyright (c) MongoDB, Inc.
// SPDX-License-Identifier: SSPL-1.0

#include "mongo/db/exec/express/plan_executor_express.h"

#include "mongo/bson/json.h"
#include "mongo/db/pipeline/expression_context_builder.h"
#include "mongo/db/query/collation/collator_interface_mock.h"
#include "mongo/db/query/query_utils.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/unittest/server_parameter_guard.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {
IndexForExpressEquality wantIndex(IndexEntry index, bool coversProjection) {
    return IndexForExpressEquality(std::move(index), coversProjection, {});
}

boost::optional<IndexForExpressEquality> chooseExpressIndex(const CanonicalQuery& cq,
                                                            const QueryPlannerParams& params) {
    ExpressEqualityList equalities;
    if (!collectExpressEqualities(cq.getPrimaryMatchExpression(), &equalities)) {
        return boost::none;
    }
    return getIndexForExpressEquality(cq, params, equalities);
}

IndexEntry createIndexEntry(BSONObj keyPattern,
                            bool unique,
                            bool sparse = false,
                            std::string name = "ident") {
    return IndexEntry(keyPattern,
                      IndexNames::nameToType(IndexNames::findPluginName(keyPattern)),
                      IndexConfig::kLatestIndexVersion,
                      false /*multikey*/,
                      {} /*mutikeyPaths*/,
                      {} /*multikeyPathSet*/,
                      sparse,
                      unique,
                      IndexEntry::Identifier{std::move(name)},
                      BSONObj() /*infoObj*/,
                      nullptr /*wildcardProjection*/);
}

class ExpressTest : public ServiceContextTest {
public:
    std::unique_ptr<CanonicalQuery> canonicalize(const char* queryStr,
                                                 bool setLimit1 = false,
                                                 const char* projectionStr = nullptr) {
        auto nss = NamespaceString::createNamespaceString_forTest("test.collection");
        auto findCommand = std::make_unique<FindCommandRequest>(nss);
        findCommand->setFilter(fromjson(queryStr));
        if (setLimit1) {
            findCommand->setLimit(1);
        }
        if (projectionStr) {
            findCommand->setProjection(fromjson(projectionStr));
        }
        return std::make_unique<CanonicalQuery>(CanonicalQueryParams{
            .expCtx = ExpressionContextBuilder{}.fromRequest(opCtx(), *findCommand).build(),
            .parsedFind = ParsedFindCommandParams{std::move(findCommand)}});
    }

    OperationContext* opCtx() {
        if (!_opCtxOwned) {
            _opCtxOwned = makeOperationContext();
        }

        return _opCtxOwned.get();
    }

    ServiceContext::UniqueOperationContext _opCtxOwned;
    unittest::ServerParameterGuard _enableCompoundExpress{"featureFlagExpressCompoundEquality",
                                                          true};
};

}  // namespace

TEST_F(ExpressTest, idIndexEligibility) {
    auto cq = canonicalize("{_id: 2}");
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto want = wantIndex(createIndexEntry(BSON("_id" << 1), true), false);
    params.mainCollectionInfo.indexes = {createIndexEntry(BSON("_id" << 1), false),
                                         createIndexEntry(BSON("a" << 1), true),
                                         createIndexEntry(BSON("_id" << 1 << "f"
                                                                     << "1"),
                                                          true),
                                         want.index};
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);

    // Shard filtering shouldn't change anything since _id indexes are always unique.
    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);
}

TEST_F(ExpressTest, compoundIdIndexEligibility) {
    auto cq = canonicalize("{_id: 2}", /* limit 1*/ true);
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto want = wantIndex(createIndexEntry(BSON("_id" << 1 << "f"
                                                      << "1"),
                                           true),
                          false);
    params.mainCollectionInfo.indexes = {createIndexEntry(BSON("a" << 1), true),
                                         createIndexEntry(BSON("_id" << 1 << "f"
                                                                     << "1"),
                                                          false),
                                         want.index};
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);

    // Unique compound indexes ineligible when shard filtering is required.
    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);

    // Unique compound indexes + no shard filtering still ineligible if no limit(1).
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto cqNoLimit1 = canonicalize("{_id: 2}", /* limit 1*/ false);
    ent = chooseExpressIndex(*cqNoLimit1, params);
    ASSERT_EQUALS(ent, boost::none);
}

// Non-_id query prefers unique, single-field index.
TEST_F(ExpressTest, singleFieldIndexEligibility) {
    auto cq = canonicalize("{a: 2}");
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto want = wantIndex(createIndexEntry(BSON("a" << 1), true), false);
    params.mainCollectionInfo.indexes = {createIndexEntry(BSON("_id" << 1), true),
                                         createIndexEntry(BSON("a" << 1), false),
                                         createIndexEntry(BSON("a" << 1 << "f"
                                                                   << "1"),
                                                          true),
                                         want.index};
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);

    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);
}

// Non-unique single-field or compound indexes are ineligible without limit(1) and no
// shard filtering. The query must be guaranteed to produce at most 1 result.
TEST_F(ExpressTest, nonUniqueIndexEligibility) {
    auto cq = canonicalize("{a: 2}");
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("_id" << 1), true),
        createIndexEntry(BSON("a" << 1), false),
        createIndexEntry(BSON("a" << 1 << "f"
                                  << "1"),
                         false),
    };
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);

    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);

    // Non-unique index also ineligible with .limit(1) if shard filtering is needed
    cq = canonicalize("{a: 2}", true /* limit 1 */);
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);

    // Non-unique index is eligible with limit(1) and no shard filtering
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent->index, params.mainCollectionInfo.indexes[1]);
}

TEST_F(ExpressTest, compoundUniqueIndexEligibility) {
    auto cq = canonicalize("{a: 2}", true /* limit 1 */);
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto want = wantIndex(createIndexEntry(BSON("a" << 1 << "f"
                                                    << "1"),
                                           true),
                          false);
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("_id" << 1), true),
        createIndexEntry(BSON("a" << 1), false),
        want.index,
        createIndexEntry(BSON("a" << 1 << "f"
                                  << "1"),
                         false),
    };
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);

    // Compound unique indexes are ineligible when shard filtering is required.
    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);

    // Compound unique indexes always ineligible when there's no .limit(1).
    cq = canonicalize("{a: 2}", false /* limit 1 */);
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);

    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);
}

TEST_F(ExpressTest, sparseIndexIneligibility) {
    auto cq = canonicalize("{a: 2}");
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto want =
        wantIndex(createIndexEntry(BSON("a" << 1), true /* unique */, true /* sparse */), false);
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("_id" << 1), true),
        want.index,
    };
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);

    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);

    // Sparse indexes are ineligible when matching against null.
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    cq = canonicalize("{a: null}");
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);


    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);
}

TEST_F(ExpressTest, nonBtreeEligibility) {
    auto cq = canonicalize("{a: 2}");
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("$**" << 1), true),  // wildcard index is not a btree index type
        createIndexEntry(BSON("a" << 1), false /* unique */),
    };
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);
}

TEST_F(ExpressTest, collationMismatchEligibility) {
    CollatorInterfaceMock revCollator(CollatorInterfaceMock::MockType::kReverseString);
    auto cq = canonicalize("{a: \"foo\"}");
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto revCollatorIdxEnt = createIndexEntry(BSON("a" << 1), true /* unique */);
    revCollatorIdxEnt.collator = &revCollator;

    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("a" << 1), false /* unique */),
        revCollatorIdxEnt,
    };
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);
}

TEST_F(ExpressTest, partialIndexEligibility) {
    auto cq = canonicalize("{a: 1}");
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto partialIdxEnt = createIndexEntry(BSON("a" << 1), true /* unique */);
    auto idxExpr = canonicalize("{a: {$exists: true}}");
    partialIdxEnt.filterExpr = idxExpr->getPrimaryMatchExpression();

    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("a" << 1), false /* unique */),
        partialIdxEnt,
    };
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent->index, partialIdxEnt);

    // Partial index is ineligible if query is NOT a subset of index filter expr.
    idxExpr = canonicalize("{b:  {$exists: true}}");
    params.mainCollectionInfo.indexes[1].filterExpr = idxExpr->getPrimaryMatchExpression();
    ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, boost::none);
}

TEST_F(ExpressTest, coveringIndexEligibilityWithUniqueIndex) {
    auto cq = canonicalize("{a: 2}", false /*setLimit1*/, "{_id: 0, a: 1, c: 1}");
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    auto want = wantIndex(createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1), false), true);
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("_id" << 1), true),
        createIndexEntry(BSON("a" << 1), true),
        createIndexEntry(BSON("a" << 1 << "b"
                                  << "1" << "c" << 1 << "d" << 1),
                         false),
        want.index,
    };
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);
}

// A conjunction can also be answered from a covering index rather than the unique one that
// established eligibility.
TEST_F(ExpressTest, conjunctionCoveringIndexEligibility) {
    auto cq = canonicalize("{a: 1, b: 2}", false /*setLimit1*/, "{_id: 0, a: 1, b: 1, c: 1}");
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto want = wantIndex(createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1), false), true);
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("_id" << 1), true),
        createIndexEntry(BSON("a" << 1 << "b" << 1), true),
        want.index,
    };
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), want);
}

TEST_F(ExpressTest, coveringIndexEligibilityWithoutUniqueIndex) {
    auto cqNoLimit = canonicalize("{a: 2}", false /*setLimit1*/, "{_id: 0, a: 1, c: 1}");
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto want = wantIndex(createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1), false), true);
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("_id" << 1), true),
        createIndexEntry(BSON("a" << 1), false),
        createIndexEntry(BSON("a" << 1 << "b"
                                  << "1" << "c" << 1 << "d" << 1),
                         false),
        want.index,
    };
    auto ent = chooseExpressIndex(*cqNoLimit, params);
    ASSERT_EQUALS(ent, boost::none);

    auto cqLimit = canonicalize("{a: 2}", true /*setLimit1*/, "{_id: 0, a: 1, c: 1}");
    ent = chooseExpressIndex(*cqLimit, params);
    ASSERT_EQUALS(ent, want);

    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ent = chooseExpressIndex(*cqLimit, params);
    ASSERT_EQUALS(ent, boost::none);
}

TEST_F(ExpressTest, coveringIndexEligibilityWithCollation) {
    CollatorInterfaceMock revCollator(CollatorInterfaceMock::MockType::kReverseString);

    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto wantCovered = wantIndex(createIndexEntry(BSON("a" << 1), false), true);
    auto wantNotCovered = wantIndex(createIndexEntry(BSON("a" << 1), false), false);


    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("_id" << 1), true),
        createIndexEntry(BSON("a" << 1 << "b"
                                  << "1" << "c" << 1 << "d" << 1),
                         false),
        wantNotCovered.index,
    };

    for (auto& indexEntry : params.mainCollectionInfo.indexes) {
        indexEntry.collator = &revCollator;
    }

    {
        auto cq = canonicalize("{a: 2}", true /*setLimit1*/, "{_id: 0, a: 1, c: 1}");
        cq->setCollator(std::make_unique<CollatorInterfaceMock>(
            CollatorInterfaceMock::MockType::kReverseString));

        auto ent = chooseExpressIndex(*cq, params);
        // Can't cover because "c" might be a string
        ASSERT_EQUALS(ent, wantNotCovered);
    }

    {
        auto cq = canonicalize("{a: 2}", true /*setLimit1*/, "{_id: 0, c: 1}");
        cq->setCollator(std::make_unique<CollatorInterfaceMock>(
            CollatorInterfaceMock::MockType::kReverseString));

        auto ent = chooseExpressIndex(*cq, params);
        // Can't cover because "c" might be a string
        ASSERT_EQUALS(ent, wantNotCovered);
    }

    {
        auto cq = canonicalize("{a: 2}", true /*setLimit1*/, "{_id: 0, a: 1}");
        cq->setCollator(std::make_unique<CollatorInterfaceMock>(
            CollatorInterfaceMock::MockType::kReverseString));

        auto ent = chooseExpressIndex(*cq, params);
        // Can cover because we know that "a" is not a string
        ASSERT_EQUALS(ent, wantCovered);
    }

    {
        auto cq = canonicalize("{a: \"abacaba\"}", true /*setLimit1*/, "{_id: 0, a: 1}");
        cq->setCollator(std::make_unique<CollatorInterfaceMock>(
            CollatorInterfaceMock::MockType::kReverseString));

        auto ent = chooseExpressIndex(*cq, params);
        // Can't cover because we know that "a" is a string
        ASSERT_EQUALS(ent, wantNotCovered);
    }
}

TEST_F(ExpressTest, coveringIndexEligibilityWithDuplicateKeys) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto want = wantIndex(
        createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1 << "b" << 1), false), true);


    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("_id" << 1), true),
        createIndexEntry(BSON("a" << 1), true),
        createIndexEntry(BSON("b" << 1 << "c" << 1 << "b" << 1), false),
        want.index,
    };

    auto cq = canonicalize("{a: 2}", true /*setLimit1*/, "{_id: 0, a: 1, b: 1}");
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);
}

TEST_F(ExpressTest, coveringIndexEligibilityWithWrongKeyOrder) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    auto want = wantIndex(createIndexEntry(BSON("a" << 1), true), false);

    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("_id" << 1), true),
        createIndexEntry(BSON("b" << 1 << "a" << 1), false),
        want.index,
    };

    auto cq = canonicalize("{a: 2}", true /*setLimit1*/, "{_id: 0, a: 1, b: 1}");
    auto ent = chooseExpressIndex(*cq, params);
    ASSERT_EQUALS(ent, want);
}

TEST_F(ExpressTest, coveringIndexEligibilityWithMultiKey) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;

    const auto cq = canonicalize("{a: 2}", true /*setLimit1*/, "{_id: 0, a: 1, c: 1}");

    {  // Can cover by a default index.
        auto indexEntry = createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1), false);
        params.mainCollectionInfo.indexes = {indexEntry};

        const auto ent = chooseExpressIndex(*cq, params);
        const auto want = wantIndex(indexEntry, true);
        ASSERT_EQUALS(ent, want);
    }
    {  // Can cover if unrelated field is multi-key.
        auto indexEntry = createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1), false);
        indexEntry.multikey = true;
        indexEntry.multikeyPaths = MultikeyPaths{{}, {0U}, {}};

        params.mainCollectionInfo.indexes = {indexEntry};
        auto ent = chooseExpressIndex(*cq, params);

        const auto want = wantIndex(indexEntry, true);
        ASSERT_EQUALS(ent, want);
    }
    {  // Can't cover if required field is multi-key.
        auto indexEntry = createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1), false);
        indexEntry.multikey = true;
        indexEntry.multikeyPaths = MultikeyPaths{{0U}, {}, {}};

        params.mainCollectionInfo.indexes = {indexEntry};
        auto ent = chooseExpressIndex(*cq, params);

        const auto want = wantIndex(indexEntry, false);
        ASSERT_EQUALS(ent, want);
    }
    {  // Can cover if filter field is multi-key and is not included into the projection.
        const auto cq = canonicalize("{a: 2}", true /*setLimit1*/, "{_id: 0, c: 1}");
        auto indexEntry = createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1), false);
        indexEntry.multikey = true;
        indexEntry.multikeyPaths = MultikeyPaths{{0U}, {}, {}};

        params.mainCollectionInfo.indexes = {indexEntry};
        auto ent = chooseExpressIndex(*cq, params);

        const auto want = wantIndex(indexEntry, true);
        ASSERT_EQUALS(ent, want);
    }
    {  // Can't cover if multikey paths are missing.
        auto indexEntry = createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1), false);
        indexEntry.multikey = true;

        params.mainCollectionInfo.indexes = {indexEntry};
        auto ent = chooseExpressIndex(*cq, params);

        const auto want = wantIndex(indexEntry, false);
        ASSERT_EQUALS(ent, want);
    }
}

TEST_F(ExpressTest, coveringIndexEligibilityWithSubFields) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;

    const auto cq = canonicalize("{'a.b': 2}", true /*setLimit1*/, "{_id: 0, c: 1}");

    auto indexEntry = createIndexEntry(BSON("a.b" << 1 << "c" << 1), false);
    params.mainCollectionInfo.indexes = {indexEntry};

    const auto ent = chooseExpressIndex(*cq, params);
    const auto want = wantIndex(indexEntry, true);
    ASSERT_EQUALS(ent, want);
}

// Binding every field of a unique index needs no limit: at most one key can match.
TEST_F(ExpressTest, conjunctionFullyBindingUniqueIndexEligibility) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;

    const auto want = wantIndex(createIndexEntry(BSON("a" << 1 << "b" << 1), true), false);
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("_id" << 1), true),
        createIndexEntry(BSON("a" << 1), false),
        want.index,
    };

    auto cq = canonicalize("{a: 1, b: 2}");
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), want);

    // The predicate's order is not the index's; the operands are lined up against the key pattern.
    cq = canonicalize("{b: 2, a: 1}");
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), want);

    // An explicit $and is the same predicate after parsing.
    cq = canonicalize("{$and: [{a: 1}, {b: 2}]}");
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), want);
}

// Uniqueness is over the whole key, so a prefix seek can still match several documents.
TEST_F(ExpressTest, conjunctionPartiallyBindingUniqueIndexIneligibility) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1), true)};

    auto cq = canonicalize("{a: 1, b: 2}");
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), boost::none);

    // With a limit of one it is eligible again, on the limit-1 rule rather than on uniqueness.
    cq = canonicalize("{a: 1, b: 2}", true /*setLimit1*/);
    ASSERT_NOT_EQUALS(chooseExpressIndex(*cq, params), boost::none);
}

// An unconstrained key field cannot be skipped: express has no residual filter.
TEST_F(ExpressTest, conjunctionLeavingLeadingFieldUnboundIneligibility) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("a" << 1 << "b" << 1 << "c" << 1), true)};

    // "b" sits between the two constrained fields.
    const auto cq = canonicalize("{a: 1, c: 3}");
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), boost::none);
}

// A key pattern may repeat a field name; without the injectivity check the repeat would bind
// one equality twice and leave the other field unchecked.
TEST_F(ExpressTest, conjunctionAgainstDuplicateKeyPatternFieldIneligibility) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;

    const auto duplicated = BSON("a" << 1 << "a" << 1);
    ASSERT_EQUALS(duplicated.nFields(), 2);
    params.mainCollectionInfo.indexes = {createIndexEntry(duplicated, true)};

    const auto cq = canonicalize("{a: 1, b: 2}");
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), boost::none);
}

// Every conjunct must be an equality generating exact bounds.
TEST_F(ExpressTest, conjunctionWithNonEqualityIneligibility) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    params.mainCollectionInfo.indexes = {createIndexEntry(BSON("a" << 1 << "b" << 1), true)};

    auto cq = canonicalize("{a: 1, b: {$gte: 2}}");
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), boost::none);

    // Two equalities on one path are redundant or contradictory; either way the planner decides.
    cq = canonicalize("{$and: [{a: 1}, {a: 2}]}");
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), boost::none);
}

// A multikey bound field makes one document produce several keys.
TEST_F(ExpressTest, conjunctionAgainstMultikeyIndexIneligibility) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;

    auto indexEntry = createIndexEntry(BSON("a" << 1 << "b" << 1), true);
    indexEntry.multikey = true;
    indexEntry.multikeyPaths = {{0U}, {}};
    params.mainCollectionInfo.indexes = {indexEntry};

    const auto cq = canonicalize("{a: 1, b: 2}");
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), boost::none);
}

// A non-unique index cannot bound the result set on its own, whatever the predicate binds.
TEST_F(ExpressTest, conjunctionAgainstNonUniqueIndexEligibility) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    params.mainCollectionInfo.indexes = {createIndexEntry(BSON("a" << 1 << "b" << 1), false)};

    auto cq = canonicalize("{a: 1, b: 2}");
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), boost::none);

    cq = canonicalize("{a: 1, b: 2}", true /*setLimit1*/);
    ASSERT_NOT_EQUALS(chooseExpressIndex(*cq, params), boost::none);

    // ... and not even with the limit once shard filtering is required.
    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), boost::none);
}

// The compound path's numeric-key-field check must not reach the single-equality case.
TEST_F(ExpressTest, singleEqualityUnaffectedByCompoundSupport) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;

    const auto want = wantIndex(createIndexEntry(BSON("a" << 1 << "f"
                                                          << "1"),
                                                 true),
                                false);
    params.mainCollectionInfo.indexes = {want.index};

    const auto cq = canonicalize("{a: 2}", true /*setLimit1*/);
    ASSERT_EQUALS(chooseExpressIndex(*cq, params), want);
}

TEST_F(ExpressTest, nestedAndOfEqualitiesIsExpressEligible) {
    auto cq = canonicalize("{$and: [{$and: [{a: 1}, {b: 2}]}]}");
    ASSERT_TRUE(isEqualityExpressEligibleQuery(*cq));
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options &= ~QueryPlannerParams::INCLUDE_SHARD_FILTER;
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("a" << 1 << "b" << 1), true, false, "ab_full")};
    ASSERT_NOT_EQUALS(chooseExpressIndex(*cq, params), boost::none);
}

TEST_F(ExpressTest, singleFieldKnobDoesNotDisableConjunction) {
    unittest::ServerParameterGuard guard{"internalQueryDisableSingleFieldExpressExecutor", true};
    ASSERT_TRUE(isEqualityExpressEligibleQuery(*canonicalize("{a: 1, b: 2}")));
    ASSERT_FALSE(isEqualityExpressEligibleQuery(*canonicalize("{a: 1}", true /*setLimit1*/)));
}

TEST_F(ExpressTest, compoundKnobDisablesConjunctionOnly) {
    unittest::ServerParameterGuard guard{"internalQueryDisableCompoundFieldExpressExecutor", true};
    ASSERT_FALSE(isEqualityExpressEligibleQuery(*canonicalize("{a: 1, b: 2}")));
    ASSERT_TRUE(isEqualityExpressEligibleQuery(*canonicalize("{a: 1}", true /*setLimit1*/)));
}

TEST_F(ExpressTest, shardFilterAllowsFullyBoundUnique) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    const auto want =
        wantIndex(createIndexEntry(BSON("a" << 1 << "b" << 1), true, false, "ab_full"), false);
    params.mainCollectionInfo.indexes = {want.index};
    ASSERT_EQUALS(chooseExpressIndex(*canonicalize("{a: 1, b: 2}"), params), want);
}

TEST_F(ExpressTest, shardFilterRejectsUniquePrefixLimitOne) {
    QueryPlannerParams params{QueryPlannerParams::ArgsForTest{}};
    params.mainCollectionInfo.options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
    params.mainCollectionInfo.indexes = {
        createIndexEntry(BSON("a" << 1 << "b" << 1), true, false, "ab_full")};
    ASSERT_EQUALS(chooseExpressIndex(*canonicalize("{a: 1}", true /*setLimit1*/), params),
                  boost::none);
}

}  // namespace mongo
