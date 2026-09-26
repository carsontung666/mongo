/**
 * internalQueryEnableGraphLookupIndexScan must not change $graphLookup results. Every pipeline
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
 *   assumes_no_implicit_index_creation,
 *   requires_fcv_91,
 * ]
 */

import {runWithParamsAllNonConfigNodes} from "jstests/noPassthrough/libs/server_parameter_helpers.js";

const kGate = "internalQueryEnableGraphLookupIndexScan";
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
