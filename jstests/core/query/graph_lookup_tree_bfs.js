/**
 * Gate internalQueryEnableGraphLookupIndexScan: covered tree BFS vs stock $graphLookup.
 * connectFrom = node_id, connectTo = parent_id, restrictSearchWithMatch = {tree_id}.
 * Index (tree_id, parent_id, path, node_id). Gate off and on must match.
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
 * ]
 */

import {runWithParamsAllNonConfigNodes} from "jstests/noPassthrough/libs/server_parameter_helpers.js";

const kGate = "internalQueryEnableGraphLookupIndexScan";
assert.commandWorked(db.adminCommand({getParameter: 1, [kGate]: 1}));

const kTreeId = "t0";
const kOtherTree = "t1";
const coll = db[jsTestName()];
coll.drop();

//   n0
//   ├── n1
//   │   ├── n3
//   │   │   └── n6
//   │   └── n4
//   └── n2
//       └── n5
const docs = [
    {tree_id: kTreeId, node_id: "n0", parent_id: null, path: "/n0"},
    {tree_id: kTreeId, node_id: "n1", parent_id: "n0", path: "/n0/n1"},
    {tree_id: kTreeId, node_id: "n2", parent_id: "n0", path: "/n0/n2"},
    {tree_id: kTreeId, node_id: "n3", parent_id: "n1", path: "/n0/n1/n3"},
    {tree_id: kTreeId, node_id: "n4", parent_id: "n1", path: "/n0/n1/n4"},
    {tree_id: kTreeId, node_id: "n5", parent_id: "n2", path: "/n0/n2/n5"},
    {tree_id: kTreeId, node_id: "n6", parent_id: "n3", path: "/n0/n1/n3/n6"},
    {tree_id: kOtherTree, node_id: "n0", parent_id: null, path: "/n0"},
    {tree_id: kOtherTree, node_id: "n99", parent_id: "n0", path: "/n0/n99"},
];
assert.commandWorked(coll.insert(docs));
assert.commandWorked(
    coll.createIndex(
        {tree_id: 1, parent_id: 1, path: 1, node_id: 1},
        {name: "allops_tree_parent_path"},
    ),
);

const byParent = {};
for (const doc of docs) {
    if (doc.tree_id !== kTreeId || doc.parent_id == null) {
        continue;
    }
    if (!byParent[doc.parent_id]) {
        byParent[doc.parent_id] = [];
    }
    byParent[doc.parent_id].push(doc.node_id);
}

function expectedDescendants(start, maxDepth) {
    const out = [];
    let frontier = [start];
    for (let depth = 0; depth <= maxDepth; ++depth) {
        const next = [];
        for (const parent of frontier) {
            for (const child of byParent[parent] || []) {
                out.push(child);
                next.push(child);
            }
        }
        frontier = next;
    }
    return out;
}

function descendantIds(startNodeId, maxDepth) {
    const out = coll
        .aggregate([
            {$match: {tree_id: kTreeId, node_id: startNodeId}},
            {
                $graphLookup: {
                    from: coll.getName(),
                    startWith: "$node_id",
                    connectFromField: "node_id",
                    connectToField: "parent_id",
                    restrictSearchWithMatch: {tree_id: kTreeId},
                    maxDepth: maxDepth,
                    as: "desc",
                },
            },
            {$project: {_id: 0, ids: "$desc.node_id"}},
        ])
        .toArray();
    if (out.length === 0) {
        return {found: false, ids: []};
    }
    assert.eq(1, out.length, tojson(out));
    return {found: true, ids: out[0].ids || []};
}

const cases = [
    {name: "missing_start", start: "missing", maxDepth: 0, found: false, ids: []},
    {name: "leaf_d0", start: "n4", maxDepth: 0, found: true, ids: []},
    {name: "leaf_d1", start: "n4", maxDepth: 1, found: true, ids: []},
    {name: "leaf_d2", start: "n6", maxDepth: 2, found: true, ids: []},
    {name: "root_d0", start: "n0", maxDepth: 0, found: true, ids: expectedDescendants("n0", 0)},
    {name: "root_d1", start: "n0", maxDepth: 1, found: true, ids: expectedDescendants("n0", 1)},
    {name: "root_d2", start: "n0", maxDepth: 2, found: true, ids: expectedDescendants("n0", 2)},
    {name: "internal_d0", start: "n1", maxDepth: 0, found: true, ids: expectedDescendants("n1", 0)},
    {name: "internal_d1", start: "n1", maxDepth: 1, found: true, ids: expectedDescendants("n1", 1)},
    {name: "internal_d2", start: "n1", maxDepth: 2, found: true, ids: expectedDescendants("n1", 2)},
];

function runAllCases() {
    const results = {};
    for (const c of cases) {
        results[c.name] = descendantIds(c.start, c.maxDepth);
    }
    return results;
}

const off = runWithParamsAllNonConfigNodes(db, {[kGate]: false}, runAllCases);
const on = runWithParamsAllNonConfigNodes(db, {[kGate]: true}, runAllCases);

for (const c of cases) {
    const a = off[c.name];
    const b = on[c.name];
    const msg = c.name + " off=" + tojson(a) + " on=" + tojson(b) + " expected=" + tojson(c);
    assert.eq(a.found, b.found, msg);
    assert.sameMembers(a.ids, b.ids, msg);
    assert.eq(c.found, b.found, msg);
    assert.sameMembers(c.ids, b.ids, msg);
    assert(!b.ids.includes("n99"), "restrictSearchWithMatch leaked other tree: " + msg);
}

assert.sameMembers(on.root_d0.ids, ["n1", "n2"]);
assert.eq(2, on.root_d0.ids.length);
assert.sameMembers(on.root_d1.ids, ["n1", "n2", "n3", "n4", "n5"]);
assert.sameMembers(on.root_d2.ids, ["n1", "n2", "n3", "n4", "n5", "n6"]);
assert.sameMembers(on.internal_d0.ids, ["n3", "n4"]);
assert.sameMembers(on.leaf_d0.ids, []);
assert.eq(false, on.missing_start.found);
assert.eq(0, on.missing_start.ids.length);

function idsWithGate(collName, pipeline, gateOn) {
    return runWithParamsAllNonConfigNodes(db, {[kGate]: gateOn}, () => {
        const out = db[collName].aggregate(pipeline).toArray();
        assert.eq(1, out.length, tojson(out));
        return out[0];
    });
}

function assertGateParity(collName, pipeline, msg) {
    const a = idsWithGate(collName, pipeline, false);
    const b = idsWithGate(collName, pipeline, true);
    const keys = [...new Set([...Object.keys(a), ...Object.keys(b)])];
    for (const k of keys) {
        const av = a[k];
        const bv = b[k];
        if (Array.isArray(av) || Array.isArray(bv)) {
            assert.sameMembers(av || [], bv || [], msg + " field " + k + " off=" + tojson(a) + " on=" + tojson(b));
            assert.eq((av || []).length, (bv || []).length, msg + " field " + k + " length off=" + tojson(a) + " on=" + tojson(b));
        } else {
            assert.eq(av, bv, msg + " field " + k + " off=" + tojson(a) + " on=" + tojson(b));
        }
    }
    return b;
}

// Cycle: visited-from-values must stop re-expansion. Without it this hangs or duplicates.
{
    const cyc = db[jsTestName() + "_cycle"];
    cyc.drop();
    assert.commandWorked(cyc.insert([
        {tree_id: kTreeId, node_id: "A", parent_id: "B", path: "/B/A"},
        {tree_id: kTreeId, node_id: "B", parent_id: "A", path: "/A/B"},
    ]));
    assert.commandWorked(
        cyc.createIndex({tree_id: 1, parent_id: 1, path: 1, node_id: 1}, {name: "allops_tree_parent_path"}),
    );
    const pipe = [
        {$match: {tree_id: kTreeId, node_id: "A"}},
        {
            $graphLookup: {
                from: cyc.getName(),
                startWith: "$node_id",
                connectFromField: "node_id",
                connectToField: "parent_id",
                restrictSearchWithMatch: {tree_id: kTreeId},
                as: "desc",
            },
        },
        {$project: {_id: 0, ids: "$desc.node_id"}},
    ];
    const got = assertGateParity(cyc.getName(), pipe, "cycle");
    assert.sameMembers(got.ids, ["A", "B"]);
    assert.eq(2, got.ids.length, "cycle must not re-emit around the loop: " + tojson(got));
}

// Diamond DAG via two documents sharing node_id. Emission follows documents (two n3);
// expansion of node_id is once.
{
    const dag = db[jsTestName() + "_dag"];
    dag.drop();
    assert.commandWorked(dag.insert([
        {tree_id: kTreeId, node_id: "n0", parent_id: null, path: "/n0"},
        {tree_id: kTreeId, node_id: "n1", parent_id: "n0", path: "/n0/n1"},
        {tree_id: kTreeId, node_id: "n2", parent_id: "n0", path: "/n0/n2"},
        {tree_id: kTreeId, node_id: "n3", parent_id: "n1", path: "/n0/n1/n3"},
        {tree_id: kTreeId, node_id: "n3", parent_id: "n2", path: "/n0/n2/n3"},
        {tree_id: kTreeId, node_id: "n4", parent_id: "n3", path: "/n0/n3/n4"},
    ]));
    assert.commandWorked(
        dag.createIndex({tree_id: 1, parent_id: 1, path: 1, node_id: 1}, {name: "allops_tree_parent_path"}),
    );
    const pipe = [
        {$match: {tree_id: kTreeId, node_id: "n0"}},
        {
            $graphLookup: {
                from: dag.getName(),
                startWith: "$node_id",
                connectFromField: "node_id",
                connectToField: "parent_id",
                restrictSearchWithMatch: {tree_id: kTreeId},
                maxDepth: 2,
                as: "desc",
            },
        },
        {$project: {_id: 0, ids: "$desc.node_id"}},
    ];
    const got = assertGateParity(dag.getName(), pipe, "dag");
    assert.eq(1, got.ids.filter((id) => id === "n4").length, "n3 must be expanded once: " + tojson(got));
}

// Missing startWith must not dump the tree_id prefix.
{
    const miss = db[jsTestName() + "_miss"];
    miss.drop();
    assert.commandWorked(miss.insert([
        {tree_id: kTreeId, node_id: "root", parent_id: null, path: "/root"},
        {tree_id: kTreeId, node_id: "child", parent_id: "root", path: "/root/child"},
        {tree_id: kTreeId, tag: "no-startWith", parent_id: null, path: "/in"},
    ]));
    assert.commandWorked(
        miss.createIndex({tree_id: 1, parent_id: 1, path: 1, node_id: 1}, {name: "allops_tree_parent_path"}),
    );
    const pipe = [
        {$match: {tag: "no-startWith"}},
        {
            $graphLookup: {
                from: miss.getName(),
                startWith: "$does_not_exist",
                connectFromField: "node_id",
                connectToField: "parent_id",
                restrictSearchWithMatch: {tree_id: kTreeId},
                maxDepth: 0,
                as: "desc",
            },
        },
        {$project: {_id: 0, ids: "$desc.node_id"}},
    ];
    const got = assertGateParity(miss.getName(), pipe, "missing_startWith");
    assert(!got.ids.includes("child"), "missing startWith dumped prefix: " + tojson(got));
}

// Regex restrictSearchWithMatch is not equality. Fast path must refuse and match stock.
{
    const pipe = [
        {$match: {tree_id: kTreeId, node_id: "n0"}},
        {
            $graphLookup: {
                from: coll.getName(),
                startWith: "$node_id",
                connectFromField: "node_id",
                connectToField: "parent_id",
                restrictSearchWithMatch: {tree_id: /^t0$/},
                maxDepth: 0,
                as: "desc",
            },
        },
        {$project: {_id: 0, ids: "$desc.node_id"}},
    ];
    const got = assertGateParity(coll.getName(), pipe, "regex");
    assert.sameMembers(got.ids, ["n1", "n2"]);
}

// Sibling {$size:"$desc"} must not be dropped by $project absorb.
{
    const pipe = [
        {$match: {tree_id: kTreeId, node_id: "n0"}},
        {
            $graphLookup: {
                from: coll.getName(),
                startWith: "$node_id",
                connectFromField: "node_id",
                connectToField: "parent_id",
                restrictSearchWithMatch: {tree_id: kTreeId},
                maxDepth: 0,
                as: "desc",
            },
        },
        {$project: {_id: 0, ids: "$desc.node_id", n: {$size: "$desc"}}},
    ];
    const got = assertGateParity(coll.getName(), pipe, "size_sibling");
    assert.sameMembers(got.ids, ["n1", "n2"]);
    assert.eq(2, got.n);
}

// Full `as` documents (no skinny project): fast path must not emit rehydrated keys.
{
    const pipe = [
        {$match: {tree_id: kTreeId, node_id: "n0"}},
        {
            $graphLookup: {
                from: coll.getName(),
                startWith: "$node_id",
                connectFromField: "node_id",
                connectToField: "parent_id",
                restrictSearchWithMatch: {tree_id: kTreeId},
                maxDepth: 0,
                as: "desc",
            },
        },
        {$project: {_id: 0, n: {$size: "$desc"}, fields: "$desc.node_id"}},
    ];
    const got = assertGateParity(coll.getName(), pipe, "full_as");
    assert.eq(2, got.n);
    assert.sameMembers(got.fields, ["n1", "n2"]);
}

// Array connectFrom on a non-covering {_id:1} index must not take the tree fast path.
{
    const g = db[jsTestName() + "_graph"];
    g.drop();
    assert.commandWorked(g.insert([
        {_id: "A", links: ["B", "C"]},
        {_id: "B", links: ["D"]},
        {_id: "C", links: ["D"]},
        {_id: "D", links: []},
    ]));
    assert.commandWorked(g.createIndex({_id: 1}));
    const pipe = [
        {$match: {_id: "A"}},
        {
            $graphLookup: {
                from: g.getName(),
                startWith: "$_id",
                connectFromField: "links",
                connectToField: "_id",
                as: "desc",
            },
        },
        {$project: {_id: 0, ids: "$desc._id"}},
    ];
    const got = assertGateParity(g.getName(), pipe, "array_connectFrom");
    // startWith=$_id / connectTo=_id includes the start vertex; D is reached twice but
    // stock de-dups by _id so it appears once.
    assert.sameMembers(got.ids, ["A", "B", "C", "D"]);
    assert.eq(4, got.ids.length, "diamond D must appear once: " + tojson(got));
}
