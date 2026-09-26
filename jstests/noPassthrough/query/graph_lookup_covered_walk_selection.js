/**
 * With internalQueryEnableGraphLookupCoveredWalk, $graphLookup runs as a covered index walk only
 * when the walk answers exactly, and never on a shard. A walk fetches no documents, so the
 * profiler's docsExamined tells which path ran.
 *
 * @tags: [
 *   requires_profiling,
 *   requires_sharding,
 *   requires_timeseries,
 * ]
 */
import {getAggPlanStages} from "jstests/libs/query/analyze_plan.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

const kGate = "internalQueryEnableGraphLookupCoveredWalk";
const kIndex = {tree_id: 1, parent_id: 1, path: 1, node_id: 1};
const kIds = {$project: {_id: 0, ids: "$desc.node_id"}};
const kCaseInsensitive = {locale: "en", strength: 2};
const kTree = [
    {_id: 0, tree_id: "t0", node_id: "n0", parent_id: null, path: "/n0"},
    {_id: 1, tree_id: "t0", node_id: "n1", parent_id: "n0", path: "/n0/n1"},
    {_id: 2, tree_id: "t0", node_id: "n2", parent_id: "n0", path: "/n0/n2"},
    {_id: 3, tree_id: "t0", node_id: "n3", parent_id: "n1", path: "/n0/n1/n3"},
];

function makeColl(db, name, docs, index = kIndex, options = {}) {
    assert.commandWorked(db.createCollection(name, options));
    assert.commandWorked(db[name].insert(docs));
    assert.commandWorked(db[name].createIndex(index));
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

// Runs 'pipeline'; returns the first result's sorted 'ids' and the profiled docsExamined.
let nextComment = 0;
function run(coll, profileDB, pipeline, options = {}) {
    const comment = "graph_lookup_covered_walk_" + nextComment++;
    const [out] = coll.aggregate(pipeline, Object.assign({comment}, options)).toArray();
    const entry = profileDB.system.profile.findOne({"command.comment": comment});
    return {ids: (out.ids || []).slice().sort(), docsExamined: entry.docsExamined};
}

// Standalone.
{
    const conn = MongoRunner.runMongod({setParameter: {[kGate]: true}});
    const db = conn.getDB("test");
    const tree = makeColl(db, "tree", kTree);
    assert.commandWorked(db.setProfilingLevel(2));

    // The walk runs: only the input document is fetched, and $indexStats counts the walk.
    const kIndexName = "tree_id_1_parent_id_1_path_1_node_id_1";
    const indexOps = () =>
        tree.aggregate([{$indexStats: {}}, {$match: {name: kIndexName}}]).next().accesses.ops;
    const opsBefore = indexOps();
    assert.eq({ids: ["n1", "n2", "n3"], docsExamined: 1}, run(tree, db, pipeline(tree)));
    assert.eq(opsBefore + 1, indexOps());

    // explain still shows the $project, and executionStats counts the walk.
    const explain = tree.explain("executionStats").aggregate(pipeline(tree));
    assert.eq(1, getAggPlanStages(explain, "$project").length);
    assert.eq(1, getAggPlanStages(explain, "$graphLookup")[0].coveredWalks, tojson(explain));

    const reordered = makeColl(
        db,
        "reordered",
        kTree.map((d) => Object.assign({org: "o"}, d)),
        {parent_id: 1, org: 1, tree_id: 1, node_id: 1},
    );
    const descending = makeColl(db, "descending", kTree, {tree_id: 1, parent_id: 1, node_id: -1});
    const ancestors = makeColl(db, "ancestors", kTree, {tree_id: 1, node_id: 1, parent_id: 1});
    const toRoot = {
        startWith: "$parent_id",
        connectFromField: "parent_id",
        connectToField: "node_id",
    };

    // Each of these runs the walk.
    for (const [name, coll, p, ids] of [
        ["$eq filter", tree, pipeline(tree, {restrictSearchWithMatch: {tree_id: {$eq: "t0"}}})],
        [
            "output other than connectFrom",
            tree,
            pipeline(tree, {}, [{$project: {_id: 0, ids: "$desc.path"}}]),
            ["/n0/n1", "/n0/n1/n3", "/n0/n2"],
        ],
        ["array startWith", tree, pipeline(tree, {startWith: ["n1", "n2"]}), ["n3"]],
        [
            "dotted as",
            tree,
            pipeline(tree, {as: "a.b"}, [{$project: {_id: 0, ids: "$a.b.node_id"}}]),
        ],
        [
            "two equalities, reordered prefix",
            reordered,
            pipeline(reordered, {restrictSearchWithMatch: {tree_id: "t0", org: "o"}}),
        ],
        ["descending key", descending, pipeline(descending)],
        [
            "ancestors, null parent at the root",
            ancestors,
            [{$match: {_id: 3}}, ...pipeline(ancestors, toRoot).slice(1)],
            ["n0", "n1"],
        ],
    ]) {
        assert.eq({ids: ids || ["n1", "n2", "n3"], docsExamined: 1}, run(coll, db, p), name);
    }

    // Collections the walk must not serve.
    const noFrom = {_id: 4, tree_id: "t0", parent_id: "n1"};
    const missingFrom = makeColl(db, "missing_from", [...kTree, noFrom]);
    const collatedIndex = db.collated_index;
    assert.commandWorked(collatedIndex.insert(kTree));
    assert.commandWorked(collatedIndex.createIndex(kIndex, {collation: kCaseInsensitive}));
    const collatedColl = makeColl(db, "collated_coll", kTree, kIndex, {
        collation: kCaseInsensitive,
    });
    const objectNode = {_id: 4, tree_id: "t0", node_id: {a: 1}, parent_id: "n3"};
    const objectIds = makeColl(db, "object_ids", [...kTree, objectNode]);
    const zeroField = makeColl(
        db,
        "zero_field",
        kTree.map((d) => Object.assign({0: d.node_id}, d)),
        {tree_id: 1, parent_id: 1, 0: 1, node_id: 1},
    );
    const fractional = makeColl(db, "fractional", kTree, {tree_id: 1, parent_id: 1, node_id: 0.5});

    // Each of these runs the subpipeline search, which fetches the descendants too, and still
    // returns the descendants.
    const kIdsAndCount = {$project: {ids: "$desc.node_id", n: {$size: "$desc"}}};
    for (const [name, coll, p, options] of [
        ["whole as", tree, pipeline(tree, {}, [kIdsAndCount])],
        ["depthField", tree, pipeline(tree, {depthField: "d"})],
        ["regex filter", tree, pipeline(tree, {restrictSearchWithMatch: {tree_id: /^t0$/}})],
        ["missing connectFrom", missingFrom, pipeline(missingFrom)],
        ["object connectFrom", objectIds, pipeline(objectIds)],
        ["numeric subfield", zeroField, pipeline(zeroField, {}, [{$project: {ids: "$desc.0"}}])],
        ["fractional key pattern", fractional, pipeline(fractional)],
        ["aggregate collation", tree, pipeline(tree), {collation: kCaseInsensitive}],
        ["collated index", collatedIndex, pipeline(collatedIndex)],
        ["collection collation", collatedColl, pipeline(collatedColl)],
    ]) {
        const {ids, docsExamined} = run(coll, db, p, options);
        assert.gt(docsExamined, 1, name);
        assert.eq(["n1", "n2", "n3"], ids.filter((id) => typeof id === "string"), name);
    }
    for (const [name, p] of [
        ["regex startWith", pipeline(tree, {startWith: {$literal: /^n/}})],
        ["null startWith", pipeline(tree, {startWith: "$parent_id"})],
    ]) {
        assert.gt(run(tree, db, p).docsExamined, 1, name);
    }
    // $unwind is absorbed into $graphLookup, so check it on its own.
    const unwind = pipeline(tree, {}, [{$unwind: "$desc"}]);
    assert.eq(3, tree.aggregate(unwind, {comment: "unwind"}).itcount());
    assert.gt(db.system.profile.findOne({"command.comment": "unwind"}).docsExamined, 1);

    // The subpipeline parses a '$' object as an operator and fails.
    const dollar = makeColl(db, "dollar", kTree);
    assert.commandWorked(
        db.runCommand({
            insert: dollar.getName(),
            documents: [{_id: 4, tree_id: "t0", node_id: {$x: 1}, parent_id: "n3"}],
        }),
    );
    assert.throwsWithCode(() => dollar.aggregate(pipeline(dollar)).toArray(), ErrorCodes.BadValue);

    // A write conflict inside the walk is retried.
    const kWriteConflict = "throwWriteConflictInGraphLookupCoveredWalk";
    assert.commandWorked(db.adminCommand({configureFailPoint: kWriteConflict, mode: {times: 1}}));
    const retried = tree.explain("executionStats").aggregate(pipeline(tree));
    assert.eq(1, getAggPlanStages(retried, "$graphLookup")[0].coveredWalks, tojson(retried));
    const fp = assert.commandWorked(
        db.adminCommand({configureFailPoint: kWriteConflict, mode: "off"}),
    );
    assert.eq(1, fp.count, tojson(fp));

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
    assert.gt(run(tree, db, pipeline(tree)).docsExamined, 1, "knob off");
    assert.commandWorked(db.adminCommand({setParameter: 1, [kGate]: true}));

    // The walk needs more than the memory limit, so the subpipeline search runs and fails.
    const kMemoryKnob = "internalDocumentSourceGraphLookupMaxMemoryBytes";
    assert.commandWorked(db.adminCommand({setParameter: 1, [kMemoryKnob]: 1}));
    assert.throwsWithCode(
        () => tree.aggregate(pipeline(tree), {allowDiskUse: false}).toArray(),
        ErrorCodes.QueryExceededMemoryLimitNoDiskUseAllowed,
    );

    MongoRunner.stopMongod(conn);
}

// Sharded cluster: the result keeps its shape, and the shard runs the subpipeline search.
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
    assert.eq(["ids"], Object.keys(out), tojson(out));
    const {ids, docsExamined} = run(tree, shardDB, pipeline(tree));
    assert.eq(["n1", "n2", "n3"], ids);
    assert.gt(docsExamined, 1);

    st.stop();
}
