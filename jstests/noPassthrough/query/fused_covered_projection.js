/**
 * Differential test for internalQueryEnableFusedCoveredProjection.
 *
 * When the knob is on, a covered projection sitting directly on an index scan is folded into the
 * scan, which decodes only the projected key components straight out of the KeyString instead of
 * materialising the whole key for the projection stage to copy a second time.
 *
 * The PROJECTION_COVERED stage stays in the plan either way -- removing it would leave the
 * plan-stage tree with fewer nodes than the QuerySolution, which explain attribution and exact
 * cardinality estimation both walk in lockstep -- so it becomes a pass-through. Activation is
 * therefore read from the IXSCAN's `coveredProjection` flag, and the stage trees of the two arms
 * must be identical.
 *
 * Every query below runs with the knob off and on and the two result sets must match element for
 * element, in order. A decode that loses a component, mis-orders one, or desynchronises the
 * positional TypeBits reader shows up as a diff rather than as plausible-looking output.
 *
 * Fails on an unpatched server: the setParameter below is not recognised there.
 */
const conn = MongoRunner.runMongod();
assert.neq(null, conn, "mongod failed to start");
const db = conn.getDB(jsTestName());

function setFused(enabled) {
    assert.commandWorked(
        db.adminCommand({setParameter: 1, internalQueryEnableFusedCoveredProjection: enabled}));
}

// The knob has to exist for this test to mean anything.
setFused(false);

function walk(node, fn) {
    if (node === null || typeof node !== "object") {
        return;
    }
    fn(node);
    for (const key of Object.keys(node)) {
        const child = node[key];
        if (Array.isArray(child)) {
            child.forEach((c) => walk(c, fn));
        } else if (child && typeof child === "object") {
            walk(child, fn);
        }
    }
}

function stageNames(plan) {
    const names = [];
    walk(plan, (n) => {
        if (typeof n.stage === "string") {
            names.push(n.stage);
        }
    });
    return names;
}

/** True when some IXSCAN in the plan reports a folded-in covered projection. */
function ixscanFused(plan) {
    let found = false;
    walk(plan, (n) => {
        if (n.stage === "IXSCAN" && n.coveredProjection) {
            found = true;
        }
    });
    return found;
}

function totalKeysExamined(explain) {
    return explain.executionStats ? explain.executionStats.totalKeysExamined : null;
}

/**
 * Runs 'query' with the knob off and on and asserts the results match exactly. Returns both plans
 * and whether the fused arm actually fused.
 */
function differential({coll, filter, projection, hint, sort, expectFused}) {
    const build = () => {
        let c = coll.find(filter, projection).hint(hint);
        if (sort) {
            c = c.sort(sort);
        }
        return c;
    };

    setFused(false);
    const baseRows = build().toArray();
    const baseExplain = build().explain("executionStats");
    setFused(true);
    const fusedRows = build().toArray();
    const fusedExplain = build().explain("executionStats");

    const basePlan = baseExplain.queryPlanner.winningPlan;
    const fusedPlan = fusedExplain.queryPlanner.winningPlan;
    const label = `${tojsononeline(filter)} / ${tojsononeline(projection)}`;

    assert.gt(baseRows.length, 0, () => `query returned no rows, comparison is vacuous: ${label}`);
    assert.eq(baseRows.length, fusedRows.length, () => `row count differs for ${label}`);
    for (let i = 0; i < baseRows.length; ++i) {
        assert.eq(baseRows[i],
                  fusedRows[i],
                  () => `row ${i} differs for ${label}: ${tojson(baseRows[i])} vs ` +
                      `${tojson(fusedRows[i])}`);
    }

    assert(!ixscanFused(basePlan), () => `base arm reported a fused scan: ${tojson(basePlan)}`);
    assert.eq(ixscanFused(fusedPlan),
              expectFused,
              () => `expected fused=${expectFused} for ${label}: ${tojson(fusedPlan)}`);

    // The fold must not change the shape of the plan-stage tree.
    assert.eq(stageNames(basePlan),
              stageNames(fusedPlan),
              () => `the fold changed the plan-stage tree for ${label}`);

    // Nor how much of the index it read.
    assert.eq(totalKeysExamined(baseExplain),
              totalKeysExamined(fusedExplain),
              () => `totalKeysExamined differs for ${label}`);

    return {baseRows, basePlan, fusedPlan};
}

// ---------------------------------------------------------------------------
// The shape this change targets: a compound covering index, projecting a strict
// subset of its components, with one component decoded and discarded.
// ---------------------------------------------------------------------------
{
    const coll = db.fused_basic;
    coll.drop();
    const docs = [];
    for (let i = 0; i < 200; ++i) {
        docs.push({
            path: "/p/" + i.toString().padStart(4, "0"),
            node_id: "n" + i,
            title: "title-" + i,
            summary: "summary text for node " + i + " ".repeat(i % 17),
        });
    }
    assert.commandWorked(coll.insert(docs));
    assert.commandWorked(
        coll.createIndex({path: 1, node_id: 1, title: 1, summary: 1}, {name: "cover"}));

    const r = differential({
        coll,
        filter: {path: {$gte: "/p/0000", $lt: "/p/9999"}},
        projection: {_id: 0, node_id: 1, title: 1, summary: 1},
        hint: "cover",
        sort: {path: 1, node_id: 1},
        expectFused: true,
    });
    assert.eq(200, r.baseRows.length);
    assert(stageNames(r.fusedPlan).includes("PROJECTION_COVERED"),
           () => `the projection stage must stay in the tree: ${tojson(r.fusedPlan)}`);
}

// ---------------------------------------------------------------------------
// Values that stress the decoder: embedded NUL bytes take readCStringWithNuls'
// scratch path, and the mixed types exercise the per-CType decode dispatch and
// the positional TypeBits reads.
// ---------------------------------------------------------------------------
{
    const coll = db.fused_types;
    coll.drop();
    assert.commandWorked(coll.insert([
        {k: 1, s: "plain", v: 1},
        {k: 2, s: "with\0nul", v: NumberLong("9007199254740993")},
        {k: 3, s: "two\0nuls\0here", v: NumberDecimal("1.0000000000000000000000001")},
        {k: 4, s: "", v: null},
        {k: 5, s: "trailing\0", v: true},
        {k: 6, s: "\0leading", v: new Date(0)},
        {k: 7, s: "unicode é中", v: -0.0},
        {k: 8, s: "x".repeat(500), v: MinKey},
        {k: 9, s: "y", v: MaxKey},
        {k: 10, s: "\0", v: 3.5},
        {k: 11, s: "a\0\0b", v: NumberLong(-1)},
    ]));
    assert.commandWorked(coll.createIndex({k: 1, s: 1, v: 1}, {name: "kv"}));

    const cases = [
        {projection: {_id: 0, s: 1, v: 1}, note: "skip leading"},
        {projection: {_id: 0, k: 1}, note: "skip trailing"},
        {projection: {_id: 0, s: 1}, note: "skip both sides"},
        {projection: {_id: 0, k: 1, s: 1, v: 1}, note: "skip nothing"},
    ];
    for (const c of cases) {
        const r = differential({
            coll,
            filter: {k: {$gte: 0}},
            projection: c.projection,
            hint: "kv",
            sort: {k: 1},
            expectFused: true,
        });
        assert.eq(11, r.baseRows.length, c.note);
    }
}

// ---------------------------------------------------------------------------
// A descending component inverts the KeyString encoding; the decoder must invert
// exactly the components the Ordering says are descending.
// ---------------------------------------------------------------------------
{
    const coll = db.fused_desc;
    coll.drop();
    const docs = [];
    for (let i = 0; i < 50; ++i) {
        docs.push({a: i % 5, b: i % 3 ? "b" + i + "\0tail" : "b" + i, c: i});
    }
    assert.commandWorked(coll.insert(docs));
    assert.commandWorked(coll.createIndex({a: 1, b: -1, c: 1}, {name: "mixed"}));

    for (const projection of [{_id: 0, b: 1, c: 1}, {_id: 0, a: 1, c: 1}]) {
        const r = differential({
            coll,
            filter: {a: {$gte: 0}},
            projection,
            hint: "mixed",
            sort: {a: 1, b: -1},
            expectFused: true,
        });
        assert.eq(50, r.baseRows.length);
    }
}

// ---------------------------------------------------------------------------
// Multi-interval bounds build an IndexBoundsChecker, which the fused stage
// refuses at runtime -- every key then goes through the materialised-key
// fallback instead of the fast path. Output must be unchanged either way.
// A backward scan and an included _id go through the same helper.
// ---------------------------------------------------------------------------
{
    const coll = db.fused_bounds;
    coll.drop();
    const docs = [];
    for (let i = 0; i < 100; ++i) {
        docs.push({a: i, b: i % 7, c: "c" + i});
    }
    assert.commandWorked(coll.insert(docs));
    assert.commandWorked(coll.createIndex({a: 1, b: 1, c: 1}, {name: "abc"}));

    differential({
        coll,
        filter: {a: {$in: [1, 5, 9, 42, 77]}},
        projection: {_id: 0, a: 1, c: 1},
        hint: "abc",
        sort: {a: 1},
        expectFused: true,
    });

    differential({
        coll,
        filter: {a: {$gte: 0}},
        projection: {_id: 0, a: 1, c: 1},
        hint: "abc",
        sort: {a: -1},
        expectFused: true,
    });

    const ids = db.fused_ids;
    ids.drop();
    assert.commandWorked(ids.insert(Array.from({length: 20}, (_, i) => ({_id: i, a: "a" + i}))));
    assert.commandWorked(ids.createIndex({_id: 1, a: 1}, {name: "ida"}));
    differential({
        coll: ids,
        filter: {_id: {$gte: 0}},
        projection: {_id: 1, a: 1},
        hint: "ida",
        sort: {_id: 1},
        expectFused: true,
    });
}

// ---------------------------------------------------------------------------
// Shapes that must be refused, because something other than the projection
// consumes the index key.
// ---------------------------------------------------------------------------
{
    // Residual filter. The predicate has to be one the planner cannot turn into index bounds --
    // an equality on a trailing field becomes bounds and leaves no filter behind, which would
    // make this pass for the wrong reason. A non-anchored regex stays a filter.
    const coll = db.fused_filter;
    coll.drop();
    assert.commandWorked(
        coll.insert(Array.from({length: 100}, (_, i) => ({a: i, b: i % 7, c: "c" + i}))));
    assert.commandWorked(coll.createIndex({a: 1, b: 1, c: 1}, {name: "abc"}));

    differential({
        coll,
        filter: {a: {$gte: 0}, c: {$regex: "9"}},
        projection: {_id: 0, a: 1, c: 1},
        hint: "abc",
        sort: {a: 1},
        expectFused: false,
    });
}
{
    // Deduplicating scan. An index that is multikey overall can still fully provide a field whose
    // own path is not multikey, so a covered projection over a deduplicating IXSCAN is reachable
    // -- with no sort, so nothing forces a FETCH. This is the guard that matters: fusing here
    // would emit one row per index entry instead of one per document.
    const coll = db.fused_multikey;
    coll.drop();
    assert.commandWorked(coll.insert([{a: [1, 2, 3], b: "x"}, {a: [4, 5], b: "y"}]));
    assert.commandWorked(coll.createIndex({a: 1, b: 1}, {name: "ab"}));

    const r = differential({
        coll,
        filter: {a: {$gte: 0}},
        projection: {_id: 0, b: 1},
        hint: "ab",
        expectFused: false,
    });
    assert.eq(2, r.baseRows.length, () => `deduplication was lost: ${tojson(r.baseRows)}`);
    assert(stageNames(r.basePlan).includes("PROJECTION_COVERED"),
           () => `expected a covered projection over a deduplicating scan: ` +
               `${tojson(r.basePlan)}`);
}

// ---------------------------------------------------------------------------
// Non-intrusion: yielding on every work() must not change the answer. The fused
// path holds an unowned view of the key across the decode, so a yield landing in
// the wrong place would show up here.
// ---------------------------------------------------------------------------
{
    const coll = db.fused_yield;
    coll.drop();
    const docs = [];
    for (let i = 0; i < 2000; ++i) {
        docs.push({
            path: "/y/" + i.toString().padStart(6, "0"),
            node_id: "n" + i,
            title: i % 5 === 0 ? "t" + i + "\0z" : "t" + i,
        });
    }
    assert.commandWorked(coll.insert(docs));
    assert.commandWorked(
        coll.createIndex({path: 1, node_id: 1, title: 1}, {name: "ycover"}));

    const previous =
        assert.commandWorked(db.adminCommand({getParameter: 1, internalQueryExecYieldIterations: 1}))
            .internalQueryExecYieldIterations;
    assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryExecYieldIterations: 1}));
    try {
        const r = differential({
            coll,
            filter: {path: {$gte: "/y/", $lt: "/y0"}},
            projection: {_id: 0, node_id: 1, title: 1},
            hint: "ycover",
            sort: {path: 1, node_id: 1},
            expectFused: true,
        });
        assert.eq(2000, r.baseRows.length);
    } finally {
        assert.commandWorked(
            db.adminCommand({setParameter: 1, internalQueryExecYieldIterations: previous}));
    }
}

// ---------------------------------------------------------------------------
// A single-key result takes the seek path, which is the fused stage's fallback
// rather than its fast path. Small results are the majority of find traffic.
// ---------------------------------------------------------------------------
{
    const coll = db.fused_single;
    coll.drop();
    assert.commandWorked(coll.insert([{a: 1, b: "only", c: 7}]));
    assert.commandWorked(coll.createIndex({a: 1, b: 1, c: 1}, {name: "abc1"}));

    const r = differential({
        coll,
        filter: {a: 1},
        projection: {_id: 0, b: 1, c: 1},
        hint: "abc1",
        expectFused: true,
    });
    assert.eq(1, r.baseRows.length);
    assert.eq({b: "only", c: 7}, r.baseRows[0]);
}

setFused(false);
MongoRunner.stopMongod(conn);
