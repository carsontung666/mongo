// Copyright (c) MongoDB, Inc.
// SPDX-License-Identifier: SSPL-1.0

#include "mongo/db/query/query_bm_fixture.h"
#include "mongo/unittest/server_parameter_guard.h"
#include "mongo/util/processinfo.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

namespace mongo {
namespace {

class PointQueryBenchmark : public QueryBenchmarkFixture {
private:
    BSONObj generateDocument(size_t index, size_t approximateSize) override {
        std::string str;
        str.reserve(approximateSize);
        for (size_t i = 0; i < approximateSize; ++i) {
            str.push_back(randomLowercaseAlpha());
        }
        auto uniqueField = static_cast<long long>(index);
        auto nonUniqueField = static_cast<long long>(index / 2);
        return BSONObjBuilder{}
            .append("_id", OID::gen())
            .append("uniqueField", uniqueField)
            .append("nonUniqueField", nonUniqueField)
            .append("arrayField", std::vector<long long>{uniqueField, nonUniqueField})
            // A natural key: a low-cardinality prefix plus an id unique within it. This is the
            // shape a document keyed by (tenant, id) rather than by _id has, and the pair is
            // unique, so a conjunction of equalities on it identifies one document.
            .append("tenantField", static_cast<long long>(index % 16))
            .append("keyField", static_cast<long long>(index / 16))
            .append("str", str)
            .obj();
    }

    std::vector<BSONObj> getIndexSpecs() const override {
        return {buildIndexSpec("uniqueField", true),
                buildIndexSpec("nonUniqueField", false),
                buildIndexSpec("arrayField", false),
                BSONObjBuilder{}
                    .append("v", IndexConfig::kLatestIndexVersion)
                    .append("key", BSON("tenantField" << 1 << "keyField" << 1))
                    .append("name", "tenantField_1_keyField_1")
                    .append("unique", true)
                    .obj()};
    }
};

BENCHMARK_DEFINE_F(PointQueryBenchmark, IdPointQuery)
(benchmark::State& state) {
    auto id = docs()[docs().size() / 2].getField("_id").OID();
    runBenchmark(BSON("_id" << id), BSONObj{} /*projection*/, state);
}

BENCHMARK_DEFINE_F(PointQueryBenchmark, UniqueFieldPointQuery)
(benchmark::State& state) {
    int64_t fieldValue = docs().size() / 2;
    runBenchmark(BSON("uniqueField" << fieldValue), BSONObj{} /*projection*/, state);
}

BENCHMARK_DEFINE_F(PointQueryBenchmark, CompoundUniqueFieldPointQuery)
(benchmark::State& state) {
    const auto& doc = docs()[docs().size() / 2];
    runBenchmark(BSON("tenantField" << doc.getField("tenantField").numberLong() << "keyField"
                                    << doc.getField("keyField").numberLong()),
                 BSONObj{} /*projection*/,
                 state);
}

// The control arm for the one above: the identical query with the compound express path switched
// off, so it takes the regular planner. Same binary, same collection, same predicate -- the only
// difference is the knob, so nothing but the express path can account for a difference.
//
// The knob must be set through its ServerParameter and not by storing to the underlying atomic:
// eligibility reads the value from the process-wide QueryKnobSnapshot, which is rebuilt by the
// on-update hook that only ServerParameter::set() fires. A bare store leaves the snapshot holding
// the old value, so the arm silently measures express against itself.
BENCHMARK_DEFINE_F(PointQueryBenchmark, CompoundUniqueFieldPointQueryExpressDisabled)
(benchmark::State& state) {
    unittest::ServerParameterGuard knob{"internalQueryDisableCompoundFieldExpressExecutor", true};
    const auto& doc = docs()[docs().size() / 2];
    runBenchmark(BSON("tenantField" << doc.getField("tenantField").numberLong() << "keyField"
                                    << doc.getField("keyField").numberLong()),
                 BSONObj{} /*projection*/,
                 state);
}

BENCHMARK_DEFINE_F(PointQueryBenchmark, NonUniqueFieldPointQuery)
(benchmark::State& state) {
    int64_t fieldValue = docs().size() / 3;
    runBenchmark(BSON("nonUniqueField" << fieldValue), BSONObj{} /*projection*/, state);
}

// Calibration for the pair above: the same ablation applied to the single-field express path that
// already ships. If this shows no effect either, the harness cannot resolve express at this
// collection size and neither pair says anything about the compound path.
BENCHMARK_DEFINE_F(PointQueryBenchmark, UniqueFieldPointQueryExpressDisabled)
(benchmark::State& state) {
    unittest::ServerParameterGuard knob{"internalQueryDisableSingleFieldExpressExecutor", true};
    int64_t fieldValue = docs().size() / 2;
    runBenchmark(BSON("uniqueField" << fieldValue), BSONObj{} /*projection*/, state);
}

BENCHMARK_DEFINE_F(PointQueryBenchmark, ArrayFieldPointQuery)
(benchmark::State& state) {
    int64_t fieldValue = docs().size() / 3;
    runBenchmark(BSON("arrayField" << fieldValue), BSONObj{} /*projection*/, state);
}

BENCHMARK_DEFINE_F(PointQueryBenchmark, UniqueFieldPointQueryWithCoveredProjection)
(benchmark::State& state) {
    int64_t fieldValue = docs().size() / 2;
    runBenchmark(BSON("uniqueField" << fieldValue), BSON("_id" << 0 << "uniqueField" << 1), state);
}

BENCHMARK_DEFINE_F(PointQueryBenchmark, UniqueFieldPointQueryWithNotCoveredProjection)
(benchmark::State& state) {
    int64_t fieldValue = docs().size() / 2;
    runBenchmark(
        BSON("uniqueField" << fieldValue), BSON("_id" << 0 << "nonUniqueField" << 1), state);
}

BENCHMARK_DEFINE_F(PointQueryBenchmark, NonUniqueFieldPointQueryWithCoveredProjection)
(benchmark::State& state) {
    int64_t fieldValue = docs().size() / 3;
    runBenchmark(
        BSON("nonUniqueField" << fieldValue), BSON("_id" << 0 << "nonUniqueField" << 1), state);
}

BENCHMARK_DEFINE_F(PointQueryBenchmark, NonUniqueFieldPointQueryWithNotCoveredProjection)
(benchmark::State& state) {
    int64_t fieldValue = docs().size() / 3;
    runBenchmark(
        BSON("nonUniqueField" << fieldValue), BSON("_id" << 0 << "uniqueField" << 1), state);
}

BENCHMARK_DEFINE_F(PointQueryBenchmark, IdPointQueryWithCoveredProjection)
(benchmark::State& state) {
    auto id = docs()[docs().size() / 2].getField("_id").OID();
    runBenchmark(BSON("_id" << id), BSON("_id" << 1), state);
}


BENCHMARK_DEFINE_F(PointQueryBenchmark, IdPointQueryWithNotCoveredProjection)
(benchmark::State& state) {
    auto id = docs()[docs().size() / 2].getField("_id").OID();
    runBenchmark(BSON("_id" << id), BSON("_id" << 0 << "uniqueField" << 1), state);
}

/**
 * ASAN can't handle the # of threads the benchmark creates. With sanitizers, run this in a
 * diminished "correctness check" mode. See SERVER-73168.
 */
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
const auto kMaxThreads = 1;
#else
/** 2x to benchmark the case of more threads than cores for curiosity's sake. */
const auto kMaxThreads = 2 * ProcessInfo::getNumLogicalCores();
#endif

static void configureBenchmarks(benchmark::internal::Benchmark* bm) {
    bm->ThreadRange(1, kMaxThreads)->Args({10, 1024});
}

static void configureProjectionBenchmarks(benchmark::internal::Benchmark* bm) {
    // Varying document size allows us to measure the effect of covering the projection with an
    // index.
    bm->ThreadRange(1, kMaxThreads)->Args({10, 256 * 1024})->Args({10, 4096 * 1024});
}

BENCHMARK_REGISTER_F(PointQueryBenchmark, IdPointQuery)->Apply(configureBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, CompoundUniqueFieldPointQuery)
    ->Apply(configureBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, CompoundUniqueFieldPointQueryExpressDisabled)
    ->Apply(configureBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, UniqueFieldPointQuery)->Apply(configureBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, UniqueFieldPointQueryExpressDisabled)
    ->Apply(configureBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, NonUniqueFieldPointQuery)->Apply(configureBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, ArrayFieldPointQuery)->Apply(configureBenchmarks);

BENCHMARK_REGISTER_F(PointQueryBenchmark, UniqueFieldPointQueryWithCoveredProjection)
    ->Apply(configureProjectionBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, UniqueFieldPointQueryWithNotCoveredProjection)
    ->Apply(configureProjectionBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, NonUniqueFieldPointQueryWithCoveredProjection)
    ->Apply(configureProjectionBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, NonUniqueFieldPointQueryWithNotCoveredProjection)
    ->Apply(configureProjectionBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, IdPointQueryWithCoveredProjection)
    ->Apply(configureProjectionBenchmarks);
BENCHMARK_REGISTER_F(PointQueryBenchmark, IdPointQueryWithNotCoveredProjection)
    ->Apply(configureProjectionBenchmarks);

}  // namespace
}  // namespace mongo
