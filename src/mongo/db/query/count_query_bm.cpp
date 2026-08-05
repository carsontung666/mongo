// Copyright (c) MongoDB, Inc.
// SPDX-License-Identifier: SSPL-1.0

#include "mongo/db/query/query_bm_fixture.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/version.h"

#include <cstdint>
#include <string_view>
#include <utility>

#include <benchmark/benchmark.h>

namespace mongo {
using namespace std::literals::string_view_literals;

namespace {

class CountQueryBenchmark : public QueryBenchmarkFixture {
protected:
    // explain() needs version info to be available, and these benchmarks run outside the server
    // startup that would normally provide it.
    BSONObj explain(const BSONObj& command) {
        const auto& versionInfo =
            VersionInfoInterface::instance(VersionInfoInterface::NotEnabledAction::kFallback);
        VersionInfoInterface::enable(&versionInfo);
        auto reply = runCommand(BSON("explain" << command.removeField("$db") << "verbosity"
                                               << "executionStats"
                                               << "$db" << kNss.db_forTest()));
        uassertStatusOK(getStatusFromCommandResult(reply));
        return reply;
    }

    void runCountScanBenchmark(benchmark::State& state,
                               BSONObj query,
                               std::string_view hint,
                               int64_t indexedKeys,
                               bool expectedMultiKey) {
        const auto numDocs = static_cast<int64_t>(docs().size());
        auto command = BSON("count" << kNss.coll() << "$db" << kNss.db_forTest() << "query" << query
                                    << "hint" << hint);

        const auto reply = runCommand(command);
        uassertStatusOK(getStatusFromCommandResult(reply));
        invariant(reply.getField("n").safeNumberLong() == numDocs);

        // Keep these benchmarks pinned to the optimized classic plan shape. An index hint alone
        // would not prevent a future planner change from silently benchmarking another stage tree.
        const auto explainReply = explain(command);
        const auto winningPlan =
            explainReply.getObjectField("queryPlanner").getObjectField("winningPlan");
        invariant(winningPlan.getField("stage").String() == "COUNT", winningPlan.toString());
        const auto countScanPlan = winningPlan.getObjectField("inputStage");
        invariant(countScanPlan.getField("stage").String() == "COUNT_SCAN", winningPlan.toString());
        invariant(countScanPlan.getBoolField("isMultiKey") == expectedMultiKey,
                  countScanPlan.toString());
        const auto executionStats = explainReply.getObjectField("executionStats");
        // CountScan includes its terminal EOF probe in keysExamined, in addition to each matching
        // index entry represented by the throughput denominator.
        invariant(executionStats.getField("totalKeysExamined").safeNumberLong() == indexedKeys + 1,
                  executionStats.toString());
        invariant(executionStats.getField("totalDocsExamined").safeNumberLong() == 0,
                  executionStats.toString());
        const auto executionStages = executionStats.getObjectField("executionStages");
        invariant(executionStages.getField("nCounted").safeNumberLong() == numDocs,
                  executionStages.toString());
        const auto countScanStats = executionStages.getObjectField("inputStage");
        invariant(countScanStats.getField("works").safeNumberLong() == indexedKeys + 1,
                  countScanStats.toString());
        invariant(countScanStats.getField("advanced").safeNumberLong() == numDocs,
                  countScanStats.toString());
        invariant(countScanStats.getField("needTime").safeNumberLong() == indexedKeys - numDocs,
                  countScanStats.toString());

        runCommandBenchmark(std::move(command), state);
        state.SetItemsProcessed(state.iterations() * indexedKeys);
    }
};

class ScalarCountQueryBenchmark : public CountQueryBenchmark {
private:
    BSONObj generateDocument(size_t index, size_t approximateSize) override {
        return BSON("_id" << static_cast<long long>(index) << "scanKey"
                          << static_cast<long long>(index) << "padding"
                          << randomLowercaseAlphaString(approximateSize));
    }

    std::vector<BSONObj> getIndexSpecs() const override {
        return {buildIndexSpec("scanKey", false)};
    }
};

BENCHMARK_DEFINE_F(ScalarCountQueryBenchmark, DirectNonDeduplicatingCountScan)
(benchmark::State& state) {
    const auto numDocs = static_cast<int64_t>(docs().size());
    runCountScanBenchmark(
        state, BSON("scanKey" << GTE << 0 << LT << numDocs), "scanKey_1"sv, numDocs, false);
}

BENCHMARK_REGISTER_F(ScalarCountQueryBenchmark, DirectNonDeduplicatingCountScan)
    ->Args({10'000, 64})
    ->Args({400'000, 64});

class MultikeyCountQueryBenchmark : public CountQueryBenchmark {
private:
    BSONObj generateDocument(size_t index, size_t approximateSize) override {
        const auto firstKey = static_cast<long long>(index) * 2;
        return BSON("_id" << static_cast<long long>(index) << "scanKey"
                          << BSON_ARRAY(firstKey << firstKey + 1) << "padding"
                          << randomLowercaseAlphaString(approximateSize));
    }

    std::vector<BSONObj> getIndexSpecs() const override {
        return {buildIndexSpec("scanKey", false)};
    }
};

BENCHMARK_DEFINE_F(MultikeyCountQueryBenchmark, DirectDeduplicatingCountScan)
(benchmark::State& state) {
    const auto numDocs = static_cast<int64_t>(docs().size());
    runCountScanBenchmark(state, BSONObj{}, "scanKey_1"sv, numDocs * 2, true);
}

BENCHMARK_REGISTER_F(MultikeyCountQueryBenchmark, DirectDeduplicatingCountScan)
    ->Args({10'000, 64})
    ->Args({200'000, 64});

class CompoundWildcardCountQueryBenchmark : public CountQueryBenchmark {
private:
    BSONObj generateDocument(size_t index, size_t approximateSize) override {
        return BSON("_id" << static_cast<long long>(index) << "anchor" << 0 << "obj"
                          << BSON("value" << static_cast<long long>(index)) << "padding"
                          << randomLowercaseAlphaString(approximateSize));
    }

    std::vector<BSONObj> getIndexSpecs() const override {
        return {BSON("v" << IndexConfig::kLatestIndexVersion << "key"
                         << BSON("anchor" << 1 << "obj.$**" << 1) << "name"
                         << "anchor_1_obj.$**_1")};
    }
};

BENCHMARK_DEFINE_F(CompoundWildcardCountQueryBenchmark, DirectNonMultikeyCountScan)
(benchmark::State& state) {
    const auto numDocs = static_cast<int64_t>(docs().size());
    runCountScanBenchmark(state,
                          BSON("anchor" << 0 << "obj.value" << GTE << 0),
                          "anchor_1_obj.$**_1"sv,
                          numDocs,
                          false);
}

BENCHMARK_REGISTER_F(CompoundWildcardCountQueryBenchmark, DirectNonMultikeyCountScan)
    ->Args({10'000, 64})
    ->Args({200'000, 64});

// A count whose winning plan is COUNT -> FETCH -> IXSCAN, so CountStage's child is a FETCH rather
// than a CountScan. This is the control for counts that cannot use the COUNT_SCAN fast path: it
// drives on the order of one work() call per document through stages a change to the direct
// CountStage -> CountScan edge must leave untouched.
class UnoptimizedCountQueryBenchmark : public CountQueryBenchmark {
protected:
    void runFetchingCountBenchmark(benchmark::State& state, BSONObj query, std::string_view hint) {
        const auto numDocs = static_cast<int64_t>(docs().size());
        auto command = BSON("count" << kNss.coll() << "$db" << kNss.db_forTest() << "query" << query
                                    << "hint" << hint);

        const auto reply = runCommand(command);
        uassertStatusOK(getStatusFromCommandResult(reply));
        invariant(reply.getField("n").safeNumberLong() == numDocs);

        // Pin the un-optimized plan shape. If this ever became a COUNT_SCAN the control would
        // silently start measuring the optimized path instead of the un-optimized one.
        const auto explainReply = explain(command);
        const auto winningPlan =
            explainReply.getObjectField("queryPlanner").getObjectField("winningPlan");
        invariant(winningPlan.getField("stage").String() == "COUNT", winningPlan.toString());
        const auto fetchPlan = winningPlan.getObjectField("inputStage");
        invariant(fetchPlan.getField("stage").String() == "FETCH", winningPlan.toString());
        invariant(fetchPlan.getObjectField("inputStage").getField("stage").String() == "IXSCAN",
                  winningPlan.toString());

        const auto executionStats = explainReply.getObjectField("executionStats");
        invariant(executionStats.getField("totalDocsExamined").safeNumberLong() == numDocs,
                  executionStats.toString());

        runCommandBenchmark(std::move(command), state);
    }

private:
    BSONObj generateDocument(size_t index, size_t approximateSize) override {
        return BSON("_id" << static_cast<long long>(index) << "scanKey"
                          << static_cast<long long>(index) << "residual" << 0 << "padding"
                          << randomLowercaseAlphaString(approximateSize));
    }

    std::vector<BSONObj> getIndexSpecs() const override {
        return {buildIndexSpec("scanKey", false)};
    }
};

BENCHMARK_DEFINE_F(UnoptimizedCountQueryBenchmark, FetchingCountWithoutCountScan)
(benchmark::State& state) {
    const auto numDocs = static_cast<int64_t>(docs().size());
    // "residual" is deliberately not indexed, so its predicate cannot be discharged by the index
    // and the planner must FETCH each document, which rules out the COUNT_SCAN transform.
    runFetchingCountBenchmark(
        state, BSON("scanKey" << GTE << 0 << LT << numDocs << "residual" << 0), "scanKey_1"sv);
}

BENCHMARK_REGISTER_F(UnoptimizedCountQueryBenchmark, FetchingCountWithoutCountScan)
    ->Args({10'000, 64})
    ->Args({200'000, 64});

}  // namespace
}  // namespace mongo
