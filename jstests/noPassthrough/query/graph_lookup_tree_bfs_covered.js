/**
 * With internalQueryEnableGraphLookupIndexScan, $graphLookup runs as a covered index walk only
 * when the walk reproduces the stock search, and never on a shard. A walk fetches no documents,
 * so the profiler's docsExamined tells which path ran.
 *
 * @tags: [
 *   requires_profiling,
 *   requires_sharding,
 *   requires_timeseries,
 * ]
 */
import {getAggPlanStages} from "jstests/libs/query/analyze_plan.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

const kGate = "internalQueryEnableGraphLookupIndexScan";
const kIndex = {tree_id: 1, parent_id: 1, path: 1, node_id: 1};
const kIds = {$project: {_id: 0, ids: "$desc.node_id"}};
const kCaseInsensitive = {locale: "en", strength: 2};
const kTree = [
    {_id: 0, tree_id: "t0", node_id: "n0", parent_id: null, path: "/n0"},
    {_id: 1, tree_id: "t0", node_id: "n1", parent_id: "n0", path: "/n0/n1"},
    {_id: 2, tree_id: "t0", node_id: "n2", parent_id: "n0", path: "/n0/n2"},
    {_id: 3, tree_id: "t0", node_id: "n3", parent_id: "n1", path: "/n0/n1/n3"},
];

function makeColl(db, name, docs, options = {}) {
    assert.commandWorked(db.createCollection(name, options));
    assert.commandWorked(db[name].insert(docs));
    assert.commandWorked(db[name].createIndex(kIndex));
    return db[name];
}

function pipeline(coll, spec = {}, tail = [kIds]) {
    const graphLookup = {
        from: coll.getName(),
        startWith: "$node_id",
        connectFromField: "node_id",
        connectToField: "parent_id",
        restrictSearchWithMatch: {tree_id: "t0"},
        as: "desc",
    };
    return [{$match: {_id: 0}}, {$graphLookup: Object.assign(graphLookup, spec)}, ...tail];
}

let nextComment = 0;
function docsExamined(coll, profileDB, pipeline, options = {}) {
    const comment = "graph_lookup_tree_bfs_" + nextComment++;
    coll.aggregate(pipeline, Object.assign({comment}, options)).toArray();
    return profileDB.system.profile.findOne({"command.comment": comment}).docsExamined;
}

// Standalone.
{
    const conn = MongoRunner.runMongod({setParameter: {[kGate]: true}});
    const db = conn.getDB("test");
    const tree = makeColl(db, "tree", kTree);
    assert.commandWorked(db.setProfilingLevel(2));

    // Only the input document is fetched, and $indexStats counts the walk.
    const kIndexName = "tree_id_1_parent_id_1_path_1_node_id_1";
    const indexOps = () =>
        tree.aggregate([{$indexStats: {}}, {$match: {name: kIndexName}}]).next().accesses.ops;
    const opsBefore = indexOps();
    assert.eq(1, docsExamined(tree, db, pipeline(tree)));
    assert.eq(opsBefore + 1, indexOps());

    // explain still shows the $project.
    assert.eq(1, getAggPlanStages(tree.explain().aggregate(pipeline(tree)), "$project").length);

    // Collections the walk must not serve.
    const noFrom = {_id: 4, tree_id: "t0", parent_id: "n1"};
    const missingFrom = makeColl(db, "missing_from", [...kTree, noFrom]);
    const collatedIndex = db.collated_index;
    assert.commandWorked(collatedIndex.insert(kTree));
    assert.commandWorked(collatedIndex.createIndex(kIndex, {collation: kCaseInsensitive}));
    const collatedColl = makeColl(db, "collated_coll", kTree, {collation: kCaseInsensitive});

    // Each of these runs the stock search, which fetches the descendants too.
    for (const [name, coll, p, options] of [
        ["whole as", tree, pipeline(tree, {}, [{$project: {n: {$size: "$desc"}}}])],
        ["depthField", tree, pipeline(tree, {depthField: "d"})],
        ["$unwind", tree, pipeline(tree, {}, [{$unwind: "$desc"}])],
        ["regex filter", tree, pipeline(tree, {restrictSearchWithMatch: {tree_id: /^t0$/}})],
        ["null startWith", tree, pipeline(tree, {startWith: "$parent_id"})],
        ["missing connectFrom", missingFrom, pipeline(missingFrom)],
        ["aggregate collation", tree, pipeline(tree), {collation: kCaseInsensitive}],
        ["collated index", collatedIndex, pipeline(collatedIndex)],
        ["collection collation", collatedColl, pipeline(collatedColl)],
    ]) {
        assert.gt(docsExamined(coll, db, p, options), 1, name);
    }

    // A timeseries collection stores buckets; the result counts measurements.
    assert.commandWorked(
        db.createCollection("ts", {timeseries: {timeField: "t", metaField: "meta"}}),
    );
    const t = ISODate();
    assert.commandWorked(db.ts.insert([{_id: 0, t, meta: "a"}, {_id: 1, t, meta: "a"}]));
    const spec = {
        from: "ts",
        startWith: "a",
        connectFromField: "meta",
        connectToField: "meta",
        as: "g",
    };
    const [ts] = tree
        .aggregate([{$match: {_id: 0}}, {$graphLookup: spec}, {$project: {n: {$size: "$g.meta"}}}])
        .toArray();
    assert.eq(2, ts.n, tojson(ts));

    assert.commandWorked(db.adminCommand({setParameter: 1, [kGate]: false}));
    assert.gt(docsExamined(tree, db, pipeline(tree)), 1, "knob off");
    assert.commandWorked(db.adminCommand({setParameter: 1, [kGate]: true}));

    // The walk needs more than the memory limit, so the stock search runs and fails.
    const kMemoryKnob = "internalDocumentSourceGraphLookupMaxMemoryBytes";
    assert.commandWorked(db.adminCommand({setParameter: 1, [kMemoryKnob]: 1}));
    assert.throwsWithCode(
        () => tree.aggregate(pipeline(tree), {allowDiskUse: false}).toArray(),
        ErrorCodes.QueryExceededMemoryLimitNoDiskUseAllowed,
    );

    MongoRunner.stopMongod(conn);
}

// Sharded cluster: the result keeps its shape, and the shard runs the stock search.
{
    const st = new ShardingTest({shards: 1, mongos: 1});
    for (const node of [st.s, st.shard0]) {
        assert.commandWorked(node.adminCommand({setParameter: 1, [kGate]: true}));
    }
    const db = st.s.getDB("test");
    const tree = makeColl(db, "tree", kTree);
    const shardDB = st.shard0.getDB("test");
    assert.commandWorked(shardDB.setProfilingLevel(2));

    const [out] = tree.aggregate(pipeline(tree)).toArray();
    assert.sameMembers(["n1", "n2", "n3"], out.ids, tojson(out));
    assert.eq(["ids"], Object.keys(out), tojson(out));
    assert.gt(docsExamined(tree, shardDB, pipeline(tree)), 1);

    st.stop();
}
