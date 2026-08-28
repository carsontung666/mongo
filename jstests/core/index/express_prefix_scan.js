/**
 * Express prefix scan. The knob is off by default; this test turns it on and restores it.
 *
 * @tags: [
 *   uses_explain,
 *   requires_fcv_90,
 *   does_not_support_stepdowns,
 *   does_not_support_transactions,
 *   not_allowed_with_signed_security_token,
 *   directly_against_shardsvrs_incompatible,
 *   transitioning_replicaset_incompatible,
 *   assumes_stable_shard_list,
 *   assumes_unsharded_collection,
 *   assumes_no_implicit_index_creation,
 *   assumes_read_concern_unchanged,
 *   requires_getmore,
 * ]
 */

import {isExpress, planHasStage} from "jstests/libs/query/analyze_plan.js";
import {
    setParameterOnAllNonConfigNodes,
} from "jstests/noPassthrough/libs/server_parameter_helpers.js";

const coll = db.getCollection(jsTestName());

// Set on every planning node, not just mongos.
function setKnob(value) {
    const previous = assert.commandWorked(
        db.adminCommand({getParameter: 1, internalQueryEnableExpressPrefixScan: 1}),
    ).internalQueryEnableExpressPrefixScan;
    setParameterOnAllNonConfigNodes(db.getMongo(), "internalQueryEnableExpressPrefixScan", value);
    return previous;
}

function plan(filter, sort, hint, projection, limit) {
    const cmd = {find: coll.getName(), filter: filter};
    if (sort !== undefined) {
        cmd.sort = sort;
    }
    if (hint !== undefined) {
        cmd.hint = hint;
    }
    if (projection !== undefined) {
        cmd.projection = projection;
    }
    if (limit !== undefined) {
        cmd.limit = limit;
    }
    return assert.commandWorked(db.runCommand({explain: cmd, verbosity: "queryPlanner"}));
}

// isExpress() also matches point lookup.
function assertExpress(filter, sort, hint, limit) {
    const explained = plan(filter, sort, hint, undefined, limit);
    assert(planHasStage(db, explained, "EXPRESS_PREFIX_IXSCAN"),
           `expected the express prefix scan for ${tojson(filter)} sort ${tojson(sort)}: ` +
               tojson(explained));
}

function assertNotExpress(filter, sort, hint, why, projection) {
    const explained = plan(filter, sort, hint, projection);
    assert(!isExpress(db, explained),
           `expected NO express (${why}) for ${tojson(filter)} sort ${tojson(sort)}: ` +
               tojson(explained));
}

function assertSameWithKnobOff(filter, sort, hint, projection = {}, limit) {
    const run = () => {
        let cur = coll.find(filter, projection);
        if (sort !== undefined) {
            cur = cur.sort(sort);
        }
        if (hint !== undefined) {
            cur = cur.hint(hint);
        }
        if (limit !== undefined) {
            cur = cur.limit(limit);
        }
        return cur.toArray();
    };
    setKnob(false);
    const off = run();
    setKnob(true);
    const on = run();
    assert.eq(off, on, `express changed the answer for ${tojson(filter)} sort ${tojson(sort)}`);
    return on.map(d => d._id);
}

const wasEnabled = setKnob(true);
try {
    coll.drop();
    assert.commandWorked(coll.insertMany([
        {_id: 0, a: 1, b: 1, c: 3, d: "x"},
        {_id: 1, a: 1, b: 1, c: 1, d: "y"},
        {_id: 2, a: 1, b: 1, c: 2, d: "z"},
        {_id: 3, a: 1, b: 2, c: 1, d: "w"},
        {_id: 4, a: 2, b: 1, c: 1, d: "v"},
    ]));
    assert.commandWorked(coll.createIndex({a: 1, b: 1, c: 1, d: 1}));

    assertExpress({a: 1, b: 1}, {c: 1, d: 1});
    assert.eq([1, 2, 0], assertSameWithKnobOff({a: 1, b: 1}, {c: 1, d: 1}));
    assertExpress({a: 1, b: 1}, {c: 1, d: 1}, undefined, 2);
    assert.eq([1, 2], assertSameWithKnobOff({a: 1, b: 1}, {c: 1, d: 1}, undefined, {}, 2));

    assertExpress({a: 1, b: 1}, {c: 1});
    assertSameWithKnobOff({a: 1, b: 1}, {c: 1});
    assertExpress({a: 1, b: 1}, undefined);
    assertExpress({a: 1}, {b: 1, c: 1});
    assertSameWithKnobOff({a: 1}, {b: 1, c: 1});

    assertNotExpress({a: 1, b: 1}, {d: 1}, undefined, "sort is not the next index fields");
    assertNotExpress({a: 1, b: 1}, {c: -1}, undefined, "sort direction does not match");
    assertNotExpress({a: 1, b: {$gte: 1}}, {c: 1}, undefined, "not an equality");
    assertNotExpress({a: {$in: [1, 2]}}, {b: 1}, undefined, "$in is not an equality");
    assertNotExpress({b: 1}, {c: 1}, undefined, "equality does not start at the index prefix");
    assertNotExpress({$and: [{a: 1}, {a: 2}]}, {b: 1}, undefined,
                     "two equalities on the same path cannot both be a prefix");

    assertNotExpress({a: null, b: 1}, {c: 1}, undefined, "null is not exact-bounds generating");
    assertNotExpress({a: {$eq: null}}, {b: 1}, undefined, "null is not exact-bounds generating");

    assertExpress({a: 1, b: 1}, {c: 1, d: 1}, "a_1_b_1_c_1_d_1");
    assertExpress({a: 1, b: 1}, {c: 1, d: 1}, {a: 1, b: 1, c: 1, d: 1});

    assert.commandWorked(coll.createIndex({a: 1, c: 1}, {name: "partial_a_c",
                                                         partialFilterExpression: {c: {$gt: 1}}}));
    assertNotExpress({a: 1, b: 1}, {c: 1}, "partial_a_c", "hinted an index the fast path refuses");
    assertSameWithKnobOff({a: 1}, {c: 1}, "partial_a_c");

    assert.commandWorked(coll.createIndex({a: 1, b: 1}, {name: "sparse_ab", sparse: true}));
    assertNotExpress({a: 1, b: 1}, undefined, "sparse_ab", "hinted a sparse index");

    coll.drop();
    assert.commandWorked(coll.insertMany([
        {_id: 0, a: 1, arr: [1, 2], c: 1},
        {_id: 1, a: 1, arr: [2, 3], c: 2},
    ]));
    assert.commandWorked(coll.createIndex({arr: 1, c: 1}));
    assertNotExpress({arr: 1}, {c: 1}, undefined, "multikey index");
    assertSameWithKnobOff({arr: 1}, {c: 1});

    // Identical keys spanning getMore.
    coll.drop();
    const dupDocs = [];
    for (let i = 0; i < 500; i++) {
        dupDocs.push({_id: i, a: 1, b: 2, c: 3, d: 4});
    }
    assert.commandWorked(coll.insertMany(dupDocs));
    assert.commandWorked(coll.createIndex({a: 1, b: 1, c: 1, d: 1}));

    assertExpress({a: 1, b: 2}, {c: 1, d: 1});
    assert.eq(500,
              coll.find({a: 1, b: 2}, {_id: 1}).sort({c: 1, d: 1}).itcount(),
              "documents were dropped across a getMore boundary");
    const ids = coll.find({a: 1, b: 2}, {_id: 1}).sort({c: 1, d: 1}).toArray().map(d => d._id);
    assert.eq(500, new Set(ids).size, "documents were duplicated across a getMore boundary");

    coll.drop();
    const mixDocs = [];
    for (let i = 0; i < 700; i++) {
        mixDocs.push({_id: i, a: 1, b: 2, c: Math.floor(i / 37), d: "same"});
    }
    assert.commandWorked(coll.insertMany(mixDocs));
    assert.commandWorked(coll.createIndex({a: 1, b: 1, c: 1, d: 1}));
    assertExpress({a: 1, b: 2}, {c: 1, d: 1});
    assert.eq(700, coll.find({a: 1, b: 2}).sort({c: 1, d: 1}).itcount());
    assert.eq(mixDocs.map(d => d._id),
              assertSameWithKnobOff({a: 1, b: 2}, {c: 1, d: 1}),
              "order or contents changed across a run of duplicate keys");

    // Overflow the 16MB batch cap.
    coll.drop();
    const big = "x".repeat(200 * 1024);
    const bigDocs = [];
    for (let i = 0; i < 220; i++) {
        bigDocs.push({_id: i, a: 7, b: 7, c: i, pad: big});
    }
    assert.commandWorked(coll.insertMany(bigDocs));
    assert.commandWorked(coll.createIndex({a: 1, b: 1, c: 1}));
    assertExpress({a: 7, b: 7}, {c: 1});
    const bigIds = coll.find({a: 7, b: 7}, {_id: 1}).sort({c: 1}).toArray().map(d => d._id);
    assert.eq(220, bigIds.length, "documents lost when a batch overflowed on size");
    assert.eq(220, new Set(bigIds).size, "documents duplicated when a batch overflowed on size");
    assert.eq(bigDocs.map(d => d._id), bigIds, "order changed when a batch overflowed on size");
    const bigWhole = coll.find({a: 7, b: 7}).sort({c: 1}).toArray();
    assert.eq(220, bigWhole.length);
    for (const d of bigWhole) {
        assert.eq(big.length, d.pad.length, `stashed document corrupted at _id ${d._id}`);
    }

    // No batchSize (eligibility refuses it). 220 docs parks the cursor.
    assertExpress({a: 7, b: 7}, {c: 1});
    const killCur = coll.find({a: 7, b: 7}).sort({c: 1});
    killCur.hasNext();
    const killId = killCur.getId();
    assert.neq(0, killId, "expected a parked express cursor");
    assert.commandWorked(db.runCommand({killCursors: coll.getName(), cursors: [killId]}));
    assert.commandFailedWithCode(
        db.runCommand({getMore: killId, collection: coll.getName()}),
        ErrorCodes.CursorNotFound,
        "a killed cursor must report CursorNotFound, not an empty successful batch");

    coll.drop();
    assert.commandWorked(coll.insertMany([
        {_id: 0, a: 1, b: 1},
        {_id: 1, a: 1, b: 2},
        {_id: 2, a: 1, b: 2},
        {_id: 3, a: 2, b: 3},
    ]));
    assert.commandWorked(coll.createIndex({a: 1, b: 1}));

    assertNotExpress({a: 1}, undefined, undefined, "the projection is covered by the index",
                     {b: 1, _id: 0});
    assert.commandWorked(coll.createIndex({a: 1, c: 1}));
    assertNotExpress({a: 1}, undefined, undefined,
                     "a later non-covering index must not take a covered query",
                     {b: 1, _id: 0});
    coll.drop();
    assert.commandWorked(coll.insertMany([
        {_id: 0, a: 1, b: 1},
        {_id: 1, a: 1, b: 2},
    ]));
    assert.commandWorked(coll.createIndex({a: 1, c: 1}));
    assert.commandWorked(coll.createIndex({a: 1, b: 1}));
    assertNotExpress({a: 1}, undefined, undefined,
                     "an earlier non-covering index must not take a covered query",
                     {b: 1, _id: 0});

    const distinctPlan =
        assert.commandWorked(db.runCommand({explain: {distinct: coll.getName(), key: "b",
                                                      query: {a: 1}},
                                            verbosity: "queryPlanner"}));
    assert(!isExpress(db, distinctPlan),
           `distinct must keep its DISTINCT_SCAN, not take express: ${tojson(distinctPlan)}`);
    assert.eq([1, 2], coll.distinct("b", {a: 1}).sort());
} finally {
    setKnob(wasEnabled);
}
