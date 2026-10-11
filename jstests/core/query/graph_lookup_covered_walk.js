/**
 * internalQueryEnableGraphLookupCoveredWalk must not change $graphLookup results. Every pipeline
 * runs with the knob off and on; the results must match.
 *
 * @tags: [
 *   # We modify a query knob. setParameter is not persistent.
 *   does_not_support_stepdowns,
 *   # This test runs commands that are not allowed with security token: setParameter.
 *   not_allowed_with_signed_security_token,
 *   # This test sets a server parameter via setParameterOnAllNonConfigNodes. To keep the host list
 *   # consistent, no add/remove shard operations should occur during the test.
 *   assumes_stable_shard_list,
 *   requires_fcv_91,
 * ]
 */

import {runWithParamsAllNonConfigNodes} from "jstests/noPassthrough/libs/server_parameter_helpers.js";

const kGate = "internalQueryEnableGraphLookupCoveredWalk";
const kTreeId = "t0";
const kIndex = {tree_id: 1, parent_id: 1, path: 1, node_id: 1};
const kIds = {$project: {_id: 0, ids: "$desc.node_id"}};

function makeColl(suffix, docs) {
    const coll = db[jsTestName() + suffix];
    coll.drop();
    assert.commandWorked(coll.insert(docs));
    assert.commandWorked(coll.createIndex(kIndex));
    return coll;
}

function graphLookup(coll, spec = {}) {
    return {
        $graphLookup: Object.assign(
            {
                from: coll.getName(),
                startWith: "$node_id",
                connectFromField: "node_id",
                connectToField: "parent_id",
                restrictSearchWithMatch: {tree_id: kTreeId},
                as: "desc",
            },
            spec,
        ),
    };
}

function descendants(coll, start, spec = {}, tail = [kIds]) {
    return [{$match: {tree_id: kTreeId, node_id: start}}, graphLookup(coll, spec), ...tail];
}

// $graphLookup output order is unspecified, so compare arrays as sorted.
function normalize(docs) {
    return docs.map((doc) => {
        const out = {};
        for (const [k, v] of Object.entries(doc)) {
            out[k] = Array.isArray(v) ? v.slice().sort((a, b) => bsonWoCompare({x: a}, {x: b})) : v;
        }
        return out;
    });
}

function runWithGate(gateOn, coll, pipeline, options) {
    return runWithParamsAllNonConfigNodes(db, {[kGate]: gateOn}, () =>
        normalize(coll.aggregate(pipeline, options).toArray()),
    );
}

function assertParity(coll, pipeline, expected, options = {}) {
    const off = runWithGate(false, coll, pipeline, options);
    const on = runWithGate(true, coll, pipeline, options);
    assert.eq(off, on, tojson({pipeline, options}));
    assert.eq(normalize(expected), on, tojson({pipeline, options}));
}

//   n0
//   ├── n1
//   │   ├── n3
//   │   │   └── n6
//   │   └── n4
//   └── n2
//       └── n5
const tree = makeColl("", [
    {tree_id: kTreeId, node_id: "n0", parent_id: null, path: "/n0"},
    {tree_id: kTreeId, node_id: "n1", parent_id: "n0", path: "/n0/n1"},
    {tree_id: kTreeId, node_id: "n2", parent_id: "n0", path: "/n0/n2"},
    {tree_id: kTreeId, node_id: "n3", parent_id: "n1", path: "/n0/n1/n3"},
    {tree_id: kTreeId, node_id: "n4", parent_id: "n1", path: "/n0/n1/n4"},
    {tree_id: kTreeId, node_id: "n5", parent_id: "n2", path: "/n0/n2/n5"},
    {tree_id: kTreeId, node_id: "n6", parent_id: "n3", path: "/n0/n1/n3/n6"},
    {tree_id: "t1", node_id: "n0", parent_id: null, path: "/n0"},
    {tree_id: "t1", node_id: "n99", parent_id: "n0", path: "/n0/n99"},
]);

for (const [start, maxDepth, ids] of [
    ["missing", 0, null],
    ["n4", 0, []],
    ["n6", 2, []],
    ["n0", 0, ["n1", "n2"]],
    ["n0", 1, ["n1", "n2", "n3", "n4", "n5"]],
    ["n0", 2, ["n1", "n2", "n3", "n4", "n5", "n6"]],
    ["n1", 0, ["n3", "n4"]],
    ["n1", 2, ["n3", "n4", "n6"]],
]) {
    assertParity(tree, descendants(tree, start, {maxDepth}), ids ? [{ids}] : []);
}

// No maxDepth.
assertParity(tree, descendants(tree, "n1"), [{ids: ["n3", "n4", "n6"]}]);

// A regex restrictSearchWithMatch is not an equality.
assertParity(
    tree,
    descendants(tree, "n0", {maxDepth: 0, restrictSearchWithMatch: {tree_id: /^t0$/}}),
    [{ids: ["n1", "n2"]}],
);

// The rest of the pipeline reads all of 'as'.
const kIdsAndCount = {$project: {_id: 0, ids: "$desc.node_id", n: {$size: "$desc"}}};
assertParity(tree, descendants(tree, "n0", {maxDepth: 0}, [kIdsAndCount]), [
    {ids: ["n1", "n2"], n: 2},
]);

// Stages after the $project that read its output.
assertParity(tree, descendants(tree, "n0", {maxDepth: 2}, [kIds, {$match: {ids: "n3"}}]), [
    {ids: ["n1", "n2", "n3", "n4", "n5", "n6"]},
]);
assertParity(
    tree,
    [
        {$match: {tree_id: kTreeId, node_id: {$in: ["n0", "n1", "n2"]}}},
        graphLookup(tree, {maxDepth: 0}),
        kIds,
        {$sort: {ids: -1}},
    ],
    [{ids: ["n5"]}, {ids: ["n3", "n4"]}, {ids: ["n1", "n2"]}],
);

// A regex startWith matches connectTo by pattern.
assertParity(tree, descendants(tree, "n0", {maxDepth: 0, startWith: {$literal: /^n1$/}}), [
    {ids: ["n3", "n4"]},
]);

// A missing startWith matches nothing.
assertParity(tree, descendants(tree, "n0", {maxDepth: 0, startWith: "$missing"}), [{ids: []}]);

// A null startWith matches only an explicit null connectTo, never a missing one.
{
    const coll = makeColl("_null_start", [
        {tree_id: kTreeId, node_id: "n0", parent_id: null, path: "/n0"},
        {tree_id: kTreeId, node_id: "orphan", path: "/orphan"},
    ]);
    assertParity(coll, descendants(coll, "n0", {maxDepth: 0, startWith: "$parent_id"}), [
        {ids: ["n0"]},
    ]);
}

// A document without connectFrom is returned but not expanded, and '$desc.node_id' skips it.
{
    const coll = makeColl("_missing_from", [
        {tree_id: kTreeId, node_id: "n0", parent_id: null, path: "/n0"},
        {tree_id: kTreeId, node_id: "n1", parent_id: "n0", path: "/n0/n1"},
        {tree_id: kTreeId, node_id: "n3", parent_id: "n1", path: "/n0/n1/n3"},
        {tree_id: kTreeId, parent_id: "n1", path: "/n0/n1/x"},
    ]);
    const withStage = (stage) => descendants(coll, "n1", {maxDepth: 2}, [kIds, stage]);
    assertParity(coll, descendants(coll, "n1", {maxDepth: 2}), [{ids: ["n3"]}]);
    assertParity(coll, withStage({$project: {n: {$size: "$ids"}}}), [{n: 1}]);
    assertParity(coll, withStage({$unwind: "$ids"}), [{ids: "n3"}]);
}

// A case-insensitive collation.
{
    const coll = makeColl("_collation", [
        {tree_id: kTreeId, node_id: "n0", parent_id: null, path: "/n0"},
        {tree_id: kTreeId, node_id: "n1", parent_id: "N0", path: "/n0/n1"},
        {tree_id: kTreeId, node_id: "n2", parent_id: "n0", path: "/n0/n2"},
    ]);
    assertParity(coll, descendants(coll, "n0", {maxDepth: 0}), [{ids: ["n1", "n2"]}], {
        collation: {locale: "en", strength: 2},
    });
}

// An output field other than connectFrom.
assertParity(tree, descendants(tree, "n1", {}, [{$project: {_id: 0, ids: "$desc.path"}}]), [
    {ids: ["/n0/n1/n3", "/n0/n1/n3/n6", "/n0/n1/n4"]},
]);

// Ancestors: the root's null parent_id is on the frontier. The second collection also has a
// document without node_id, which a null connectTo seek finds.
for (const extra of [[], [{tree_id: kTreeId, parent_id: "n9", path: "/n9/x"}]]) {
    const coll = makeColl("_ancestors" + extra.length, [
        {tree_id: kTreeId, node_id: "n0", parent_id: null, path: "/n0"},
        {tree_id: kTreeId, node_id: "n1", parent_id: "n0", path: "/n0/n1"},
        {tree_id: kTreeId, node_id: "n3", parent_id: "n1", path: "/n0/n1/n3"},
        ...extra,
    ]);
    assert.commandWorked(coll.createIndex({tree_id: 1, node_id: 1, parent_id: 1}));
    const toRoot = {
        startWith: "$parent_id",
        connectFromField: "parent_id",
        connectToField: "node_id",
    };
    assertParity(coll, descendants(coll, "n3", toRoot), [{ids: ["n0", "n1"]}]);
}

// Numeric ids of different types match by value and keep their types.
{
    const coll = makeColl("_numeric", [
        {tree_id: kTreeId, node_id: NumberInt(0), parent_id: null, path: "/0"},
        {tree_id: kTreeId, node_id: NumberLong(1), parent_id: 0.0, path: "/0/1"},
        {tree_id: kTreeId, node_id: 2.5, parent_id: NumberDecimal("0"), path: "/0/2"},
        {tree_id: kTreeId, node_id: NumberDecimal("3"), parent_id: NumberInt(1), path: "/0/1/3"},
    ]);
    assertParity(coll, descendants(coll, NumberInt(0)), [
        {ids: [NumberLong(1), 2.5, NumberDecimal("3")]},
    ]);
}

// A cycle must stop, and each document is returned once.
{
    const coll = makeColl("_cycle", [
        {tree_id: kTreeId, node_id: "A", parent_id: "B", path: "/B/A"},
        {tree_id: kTreeId, node_id: "B", parent_id: "A", path: "/A/B"},
    ]);
    assertParity(coll, descendants(coll, "A"), [{ids: ["A", "B"]}]);
}

// Two documents share node_id n3: both are returned, n3 is expanded once.
{
    const coll = makeColl("_dag", [
        {tree_id: kTreeId, node_id: "n0", parent_id: null, path: "/n0"},
        {tree_id: kTreeId, node_id: "n1", parent_id: "n0", path: "/n0/n1"},
        {tree_id: kTreeId, node_id: "n2", parent_id: "n0", path: "/n0/n2"},
        {tree_id: kTreeId, node_id: "n3", parent_id: "n1", path: "/n0/n1/n3"},
        {tree_id: kTreeId, node_id: "n3", parent_id: "n2", path: "/n0/n2/n3"},
        {tree_id: kTreeId, node_id: "n4", parent_id: "n3", path: "/n0/n3/n4"},
    ]);
    assertParity(coll, descendants(coll, "n0", {maxDepth: 2}), [
        {ids: ["n1", "n2", "n3", "n3", "n4"]},
    ]);
}

// An index {tree_id, parent_id, node_id}: connectFrom and the output field follow the key prefix.
{
    const kAdjacency = {tree_id: 1, parent_id: 1, node_id: 1};
    const makeAdjacency = (suffix, docs) => {
        const coll = makeColl(suffix, docs);
        assert.commandWorked(coll.dropIndex(kIndex));
        assert.commandWorked(coll.createIndex(kAdjacency));
        return coll;
    };
    const node = (id, parent, tree = kTreeId) => ({tree_id: tree, node_id: id, parent_id: parent});

    // Strings with NULs, long and non-ASCII strings, numbers, a cycle, a node with two parents, a
    // second tree, and children far apart in the index.
    const docs = [node("r", null), node("r", null, "t1"), node("x", "r", "t1")];
    for (let i = 0; i < 40; ++i) {
        docs.push(node("c" + i, "r"));
        docs.push(node("filler" + i, "elsewhere"));
    }
    docs.push(
        node("a\u0000b", "c0"),
        node("leaf", "a\u0000b"),
        node("a very long node identifier", "c1"),
        node("ünïcødé", "a very long node identifier"),
        node(NumberInt(7), "c2"),
        node("seven", 7.0),
        node("loop", "c3"),
        node("c3", "loop"),
        node("shared", "c4"),
        node("shared", "c5"),
        node("under shared", "shared"),
        node("deep", "c39"),
    );
    const coll = makeAdjacency("_adjacency", docs);
    for (const maxDepth of [0, 1, 2, 5]) {
        const pipeline = descendants(coll, "r", {maxDepth});
        assertParity(coll, pipeline, runWithGate(false, coll, pipeline));
    }
    assertParity(coll, descendants(coll, "r", {maxDepth: 1}), [
        {ids: [...docs.slice(3).filter((d) => d.parent_id === "r").map((d) => d.node_id),
               "a\u0000b", "a very long node identifier", NumberInt(7), "loop", "shared", "shared",
               "deep"]},
    ]);

    // The $project that follows reads '$desc.node_id' in several ways.
    const tails = [
        [{$project: {_id: 0, ids: "$desc.node_id"}}],
        [{$project: {_id: 0, ids: "$desc.node_id", nested: {n: {$size: "$desc.node_id"}}}}],
        [{$project: {_id: 0, desc: "$desc.node_id"}}],
        [{$project: {_id: 0, first: {$min: "$desc.node_id"}, node_id: 1}}],
        // An inclusion of 'desc.node_id' keeps 'desc' documents.
        [{$project: {_id: 0, "desc.node_id": 1}}],
        [{$addFields: {ids: "$desc.node_id"}}, {$project: {_id: 0, ids: 1}}],
        [{$project: {_id: 0, ids: "$desc.node_id"}}, {$project: {n: {$size: "$ids"}}}],
    ];
    for (const tail of tails) {
        const pipeline = descendants(coll, "r", {maxDepth: 5}, tail);
        assertParity(coll, pipeline, runWithGate(false, coll, pipeline));
    }

    // The walk gives up at run time but the $project still reads '$desc': a null node_id, and a
    // collection without a usable index.
    const withNull = makeAdjacency("_adjacency_null", [node("r", null), node("a", "r"), node(null, "a")]);
    const noIndex = makeColl("_adjacency_noindex", [node("r", null), node("a", "r"), node("b", "a")]);
    assert.commandWorked(noIndex.dropIndex(kIndex));
    for (const c of [withNull, noIndex]) {
        for (const tail of tails) {
            const pipeline = descendants(c, "r", {maxDepth: 5}, tail);
            assertParity(c, pipeline, runWithGate(false, c, pipeline));
        }
    }
}
