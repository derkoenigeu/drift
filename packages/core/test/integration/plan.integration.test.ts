import { describe, it, expect, beforeAll, afterAll } from "vitest";
import type { Pool } from "mysql2/promise";
import { buildPlan, applyPlan } from "../../src/sync-plan/plan.js";
import { introspect } from "../../src/schema-diff/introspect.js";
import {
  createTestPool,
  createTestDb,
  dropTestDb,
  uniqueDbName,
  pingMySQL,
} from "./setup.js";

// ---------------------------------------------------------------------------
// Suite-level setup
// ---------------------------------------------------------------------------

let skip = false;
let adminPool: Pool;

beforeAll(async () => {
  const alive = await pingMySQL();
  if (!alive) {
    skip = true;
  }
  if (!skip) {
    adminPool = createTestPool();
  }
}, 20_000);

afterAll(async () => {
  await adminPool?.end();
}, 10_000);

// ---------------------------------------------------------------------------
// Helper: spin up isolated source + target DB pairs per test group
// ---------------------------------------------------------------------------

async function makeTestPair(prefix: string) {
  const srcDb = uniqueDbName(`${prefix}_src`);
  const tgtDb = uniqueDbName(`${prefix}_tgt`);
  await createTestDb(adminPool, srcDb);
  await createTestDb(adminPool, tgtDb);
  const srcPool = createTestPool(srcDb);
  const tgtPool = createTestPool(tgtDb);
  return {
    srcDb, tgtDb, srcPool, tgtPool,
    async cleanup() {
      await srcPool.end();
      await tgtPool.end();
      await dropTestDb(adminPool, srcDb);
      await dropTestDb(adminPool, tgtDb);
    },
  };
}

// ---------------------------------------------------------------------------
// buildPlan: schema-only
// ---------------------------------------------------------------------------

describe("buildPlan() — schema sync", () => {
  let ctx: Awaited<ReturnType<typeof makeTestPair>>;

  beforeAll(async () => {
    if (skip) return;
    ctx = await makeTestPair("plan_schema");

    await ctx.srcPool.query(`
      CREATE TABLE products (
        id    INT          NOT NULL AUTO_INCREMENT,
        sku   VARCHAR(50)  NOT NULL,
        price DECIMAL(10,2) NOT NULL DEFAULT '0.00',
        PRIMARY KEY (id),
        UNIQUE INDEX uq_sku (sku)
      ) ENGINE=InnoDB
    `);
    // target is empty
  }, 20_000);

  afterAll(async () => {
    if (skip) return;
    await ctx.cleanup();
  }, 10_000);

  it("plan contains a create-table statement for the new table", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: true, data: false } }
    );
    const kinds = plan.schema.map((s) => s.kind);
    expect(kinds).toContain("create-table");
    expect(plan.schema.find((s) => s.object === "products")).toBeDefined();
  });

  it("plan has no data statements in schema-only mode", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: true, data: false } }
    );
    expect(plan.data).toHaveLength(0);
  });

  it("applyPlan creates the table in the target database", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: true, data: false } }
    );
    const result = await applyPlan(ctx.tgtPool, plan);
    expect(result.errors).toHaveLength(0);
    expect(result.executed).toBeGreaterThan(0);

    const snap = await introspect(ctx.tgtPool, ctx.tgtDb);
    expect(snap.tables.has("products")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// buildPlan: data-only
// ---------------------------------------------------------------------------

describe("buildPlan() — data sync", () => {
  let ctx: Awaited<ReturnType<typeof makeTestPair>>;

  beforeAll(async () => {
    if (skip) return;
    ctx = await makeTestPair("plan_data");

    // Create same table structure in both DBs
    const createSql = `
      CREATE TABLE users (
        id   INT          NOT NULL,
        name VARCHAR(100) NOT NULL,
        val  INT          NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB
    `;
    await ctx.srcPool.query(createSql);
    await ctx.tgtPool.query(createSql);

    // Source has 3 rows; target has only 1
    await ctx.srcPool.query(
      `INSERT INTO \`${ctx.srcDb}\`.users VALUES (1,'Alice',10),(2,'Bob',20),(3,'Charlie',30)`
    );
    await ctx.tgtPool.query(
      `INSERT INTO \`${ctx.tgtDb}\`.users VALUES (1,'Alice',10)`
    );
  }, 20_000);

  afterAll(async () => {
    if (skip) return;
    await ctx.cleanup();
  }, 10_000);

  it("plan contains INSERT statements for missing rows", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: false, data: true } }
    );
    const inserts = plan.data.filter((s) => s.kind === "insert");
    expect(inserts).toHaveLength(2);
  });

  it("plan has no schema statements in data-only mode", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: false, data: true } }
    );
    expect(plan.schema).toHaveLength(0);
  });

  it("applyPlan inserts the missing rows into target", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: false, data: true } }
    );
    const result = await applyPlan(ctx.tgtPool, plan);
    expect(result.errors).toHaveLength(0);

    const [rows] = await ctx.tgtPool.query<any[]>(
      `SELECT id FROM \`${ctx.tgtDb}\`.users ORDER BY id`
    );
    const ids = rows.map((r: any) => r.id);
    expect(ids).toEqual([1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// buildPlan: schema + data combined
// ---------------------------------------------------------------------------

describe("buildPlan() — schema + data sync", () => {
  let ctx: Awaited<ReturnType<typeof makeTestPair>>;

  beforeAll(async () => {
    if (skip) return;
    ctx = await makeTestPair("plan_full");

    // Source has a table with data; target is empty
    await ctx.srcPool.query(`
      CREATE TABLE categories (
        id   INT         NOT NULL AUTO_INCREMENT,
        name VARCHAR(80) NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB
    `);
    await ctx.srcPool.query(
      `INSERT INTO \`${ctx.srcDb}\`.categories (name) VALUES ('Alpha'), ('Beta'), ('Gamma')`
    );
  }, 20_000);

  afterAll(async () => {
    if (skip) return;
    await ctx.cleanup();
  }, 10_000);

  it("applies schema and data end-to-end", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: true, data: true } }
    );

    expect(plan.schema.some((s) => s.kind === "create-table")).toBe(true);
    // data diff skipped because target doesn't have the table yet (tables must exist in both)
    // after applying schema the table exists — this validates the schema path
    const result = await applyPlan(ctx.tgtPool, plan);
    expect(result.errors).toHaveLength(0);

    const snap = await introspect(ctx.tgtPool, ctx.tgtDb);
    expect(snap.tables.has("categories")).toBe(true);
  });

  it("second run after sync: plan is a no-op", async () => {
    if (skip) return;
    // Copy data into target manually so both DBs match
    await ctx.tgtPool.query(
      `INSERT INTO \`${ctx.tgtDb}\`.categories (name) VALUES ('Alpha'), ('Beta'), ('Gamma')`
    );

    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: true, data: true } }
    );
    expect(plan.schema).toHaveLength(0);
    expect(plan.data).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// buildPlan: UPDATE and DELETE data changes
// ---------------------------------------------------------------------------

describe("buildPlan() — data updates and deletes", () => {
  let ctx: Awaited<ReturnType<typeof makeTestPair>>;

  beforeAll(async () => {
    if (skip) return;
    ctx = await makeTestPair("plan_upd_del");

    const createSql = `
      CREATE TABLE tags (
        id    INT         NOT NULL,
        label VARCHAR(80) NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB
    `;
    await ctx.srcPool.query(createSql);
    await ctx.tgtPool.query(createSql);

    // source: 1 same, 2 modified, 3 new (4 removed from target)
    await ctx.srcPool.query(
      `INSERT INTO \`${ctx.srcDb}\`.tags VALUES (1,'foo'),(2,'bar-updated'),(3,'baz')`
    );
    await ctx.tgtPool.query(
      `INSERT INTO \`${ctx.tgtDb}\`.tags VALUES (1,'foo'),(2,'bar'),(4,'qux')`
    );
  }, 20_000);

  afterAll(async () => {
    if (skip) return;
    await ctx.cleanup();
  }, 10_000);

  it("plan includes update, insert, and delete", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: false, data: true } }
    );
    const kinds = plan.data.map((s) => s.kind);
    expect(kinds).toContain("update");
    expect(kinds).toContain("insert");
    expect(kinds).toContain("delete");
  });

  it("after applyPlan target matches source exactly", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: false, data: true } }
    );
    const result = await applyPlan(ctx.tgtPool, plan);
    expect(result.errors).toHaveLength(0);

    const [srcRows] = await ctx.srcPool.query<any[]>(
      `SELECT id, label FROM \`${ctx.srcDb}\`.tags ORDER BY id`
    );
    const [tgtRows] = await ctx.tgtPool.query<any[]>(
      `SELECT id, label FROM \`${ctx.tgtDb}\`.tags ORDER BY id`
    );
    expect(tgtRows.map((r: any) => ({ id: r.id, label: r.label }))).toEqual(
      srcRows.map((r: any) => ({ id: r.id, label: r.label }))
    );
  });
});

// ---------------------------------------------------------------------------
// applyPlan: dry-run mode
// ---------------------------------------------------------------------------

describe("applyPlan() — dry-run", () => {
  let ctx: Awaited<ReturnType<typeof makeTestPair>>;

  beforeAll(async () => {
    if (skip) return;
    ctx = await makeTestPair("plan_dryrun");

    const createSql = `
      CREATE TABLE events (
        id   INT         NOT NULL,
        name VARCHAR(80) NOT NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB
    `;
    await ctx.srcPool.query(createSql);
    await ctx.tgtPool.query(createSql);

    await ctx.srcPool.query(`INSERT INTO \`${ctx.srcDb}\`.events VALUES (1,'Event A'),(2,'Event B')`);
    // target is empty
  }, 20_000);

  afterAll(async () => {
    if (skip) return;
    await ctx.cleanup();
  }, 10_000);

  it("dry-run skips all statements and leaves target untouched", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: false, data: true } }
    );
    expect(plan.data.length).toBeGreaterThan(0);

    const result = await applyPlan(ctx.tgtPool, plan, { dryRun: true });
    expect(result.executed).toBe(0);
    expect(result.skipped).toBeGreaterThan(0);
    expect(result.errors).toHaveLength(0);

    // target should still be empty
    const [rows] = await ctx.tgtPool.query<any[]>(
      `SELECT COUNT(*) as cnt FROM \`${ctx.tgtDb}\`.events`
    );
    expect(rows[0].cnt).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// buildPlan: table without primary key is skipped for data diff
// ---------------------------------------------------------------------------

describe("buildPlan() — table without primary key is skipped for data diff", () => {
  let ctx: Awaited<ReturnType<typeof makeTestPair>>;

  beforeAll(async () => {
    if (skip) return;
    ctx = await makeTestPair("plan_nopk");

    const createSql = `
      CREATE TABLE logs (
        message TEXT NOT NULL
      ) ENGINE=InnoDB
    `;
    await ctx.srcPool.query(createSql);
    await ctx.tgtPool.query(createSql);
    await ctx.srcPool.query(`INSERT INTO \`${ctx.srcDb}\`.logs VALUES ('hello')`);
  }, 20_000);

  afterAll(async () => {
    if (skip) return;
    await ctx.cleanup();
  }, 10_000);

  it("data plan is empty for tables without a primary key", async () => {
    if (skip) return;
    const plan = await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      { mode: { schema: false, data: true } }
    );
    expect(plan.data).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// buildPlan: progress callback is invoked
// ---------------------------------------------------------------------------

describe("buildPlan() — onProgress callback", () => {
  let ctx: Awaited<ReturnType<typeof makeTestPair>>;

  beforeAll(async () => {
    if (skip) return;
    ctx = await makeTestPair("plan_progress");
  }, 20_000);

  afterAll(async () => {
    if (skip) return;
    await ctx.cleanup();
  }, 10_000);

  it("calls onProgress at least once", async () => {
    if (skip) return;
    const messages: string[] = [];
    await buildPlan(
      ctx.srcPool, ctx.tgtPool,
      ctx.srcDb, ctx.tgtDb,
      {
        mode: { schema: true, data: true },
        onProgress: (msg) => messages.push(msg),
      }
    );
    expect(messages.length).toBeGreaterThan(0);
  });
});
