/**
 * Verify that simple equality predicates against compound unique
 * indexes filter orphans correctly.
 *
 * Previously these queries were Express eligible and were known to
 * produce incorrect responses (see SERVER-97860).
 *
 * @tags: [
 *   requires_sharding,
 * ]
 */

import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

// Deliberately inserts orphans outside of migration.
TestData.skipCheckOrphans = true;
const st = new ShardingTest({shards: 2});
const collName = "test.shardfilter";
const mongosDb = st.s.getDB("test");
const mongosColl = st.s.getCollection(collName);

assert.commandWorked(st.s.adminCommand({enableSharding: "test", primaryShard: st.shard1.name}));
assert.commandWorked(
    st.s.adminCommand({shardCollection: collName, key: {a: 1, b: 1}, unique: true}),
);

// shard0 gets small chunk so that we can create orphans on shard1
assert.commandWorked(st.s.adminCommand({split: collName, middle: {a: 1, b: 10}}));
assert.commandWorked(st.s.adminCommand({split: collName, middle: {a: 1, b: 20}}));
assert.commandWorked(
    st.s.adminCommand({moveChunk: collName, find: {a: 1, b: 15}, to: st.shard0.shardName}),
);

const orphanDocs = [{_id: 9, a: 1, b: 10}];
assert.commandWorked(st.shard1.getCollection(collName).insert(orphanDocs));

// Insert legit doc.
assert.commandWorked(mongosColl.insert([{_id: 2, a: 1, b: 22}]));

assert.eq(mongosColl.find().itcount(), 1);
// Prefix + limit 1 must not take express (SERVER-97860): uniqueness is over the
// whole key, so a prefix seek can land on the orphan first.
assert.eq(mongosColl.find({a: 1}).limit(1).toArray(), [{_id: 2, a: 1, b: 22}]);

// Fully bound unique equality is express-eligible even with shard filtering.
// The orphan {a:1,b:10} must not surface; the owned {a:1,b:22} must.
if (FeatureFlagUtil.isPresentAndEnabled(mongosDb, "ExpressCompoundEquality")) {
    assert.eq(mongosColl.find({a: 1, b: 22}).toArray(), [{_id: 2, a: 1, b: 22}]);
    assert.eq(mongosColl.find({a: 1, b: 10}).toArray(), []);
}

st.stop();
