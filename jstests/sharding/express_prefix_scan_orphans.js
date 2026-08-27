/**
 * Prefix scan must filter orphans across getMore. No sort: a sharded sort never expresses.
 *
 * @tags: [
 *   requires_sharding,
 * ]
 */
import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {getPlanStages, getWinningPlanFromExplain} from "jstests/libs/query/analyze_plan.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

const st = new ShardingTest({shards: 2});

const dbName = "express_prefix_scan_orphans";
const collName = "nodes";
const ns = dbName + "." + collName;

const mongosDB = st.s.getDB(dbName);
const coll = mongosDB[collName];

assert.commandWorked(st.s.adminCommand({enableSharding: dbName, primaryShard: st.shard0.shardName}));
assert.commandWorked(st.s.adminCommand({shardCollection: ns, key: {sk: 1}}));

// Index omits the shard key so both shards run the query.
assert.commandWorked(coll.createIndex({tree: 1, parent: 1, ord: 1}));

const kNumDocs = 250;  // > first-batch 101, so getMore runs with orphan filtering.
const kNumDecoys = 10;

const docs = [];
for (let i = 0; i < kNumDocs; i++) {
    docs.push({_id: i, sk: i % 2 === 0 ? -(i + 1) : i + 1, tree: "t", parent: "p", ord: i});
}
// Same prefix, different parent: end position must still hold.
for (let i = 0; i < kNumDecoys; i++) {
    docs.push({_id: 100000 + i, sk: -(100000 + i), tree: "t", parent: "q", ord: i});
}
assert.commandWorked(coll.insert(docs));

const suspendRangeDeletion = configureFailPoint(st.shard0, "suspendRangeDeletion");
assert.commandWorked(st.s.adminCommand({split: ns, middle: {sk: 0}}));
assert.commandWorked(st.s.adminCommand(
    {moveChunk: ns, find: {sk: 0}, to: st.shard1.shardName, _waitForDelete: false}));

const shard0Coll = st.rs0.getPrimary().getDB(dbName)[collName];
assert.eq(kNumDocs + kNumDecoys,
          shard0Coll.count(),
          "shard0 should still hold every document, the migrated half as orphans");

function setKnob(enabled) {
    for (const rs of [st.rs0, st.rs1]) {
        assert.commandWorked(rs.getPrimary().adminCommand(
            {setParameter: 1, internalQueryEnableExpressPrefixScan: enabled}));
    }
}

function find() {
    return coll.find({tree: "t", parent: "p"}, {_id: 1, ord: 1}).hint({tree: 1, parent: 1, ord: 1});
}

function byId(docs) {
    return docs.slice().sort((a, b) => a._id - b._id);
}

setKnob(false);
const baseline = byId(find().toArray());
assert.eq(kNumDocs, baseline.length, "control arm did not see every document exactly once");

setKnob(true);

const explain = assert.commandWorked(find().explain());
const shardPlans = explain.queryPlanner.winningPlan.shards;
assert(shardPlans, "expected a per-shard plan breakdown: " + tojson(explain));
assert.eq(2, shardPlans.length, "expected the query to reach both shards: " + tojson(explain));
for (const shardPlan of shardPlans) {
    const stages = getPlanStages(getWinningPlanFromExplain(shardPlan), "EXPRESS_PREFIX_IXSCAN");
    assert.eq(1, stages.length, "shard did not use the express prefix scan: " + tojson(shardPlan));
}

const withFastPath = byId(find().toArray());

assert.eq(kNumDocs,
          withFastPath.length,
          "expected each owned document exactly once; a longer result means orphans leaked and a " +
              "shorter one means the scan stopped at a rejected document");
assert.eq(kNumDocs,
          new Set(withFastPath.map((doc) => doc._id)).size,
          "duplicate _ids in the result, which is what a leaked orphan looks like");
assert.eq(baseline,
          withFastPath,
          "the express prefix scan returned a different result set than the stage-based plan");

suspendRangeDeletion.off();
st.stop();
