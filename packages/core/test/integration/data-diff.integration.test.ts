import { describe, it, expect, beforeAll, afterAll } from "vitest";
import type { Pool } from "mysql2/promise";
import { diffTableData } from "../../src/data-diff/data-diff.js";
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
let srcDb: string;
let tgtDb: string;
let srcPool: Pool;
let tgtPool: Pool;

beforeAll(async () => {
  const alive = await pingMySQL();
  if (!alive) {
    skip = true;
    return;
  }

  adminPool = createTestPool();
  srcDb = uniqueDbName("test_dd_src");
  tgtDb = uniqueDbName("test_dd_tgt");
  await createTestDb(adminPool, srcDb);
  await createTestDb(adminPool, tgtDb);
  srcPool = createTestPool(srcDb);
  tgtPool = createTestPool(tgtDb);
}, 20_000);

afterAll(async () => {
  if (skip) return;
  await srcPool?.end();
  await tgtPool?.end();
  await dropTestDb(adminPool, srcDb);
  await dropTestDb(adminPool, tgtDb);
  await adminPool?.end();
}, 15_000);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function createSimpleTable(pool: Pool, db: string, table: string) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${db}\`.\`${table}\` (
      id   INT          NOT NULL,
      name VARCHAR(100) NOT NULL,
      val  INT          NULL,
      PRIMARY KEY (id)
    ) ENGINE=InnoDB
  `);
}

async function truncateTable(pool: Pool, db: string, table: string) {
  await pool.query(`TRUNCATE TABLE \`${db}\`.\`${table}\``);
}

async function dropTable(pool: Pool, db: string, table: string) {
  await pool.query(`DROP TABLE IF EXISTS \`${db}\`.\`${table}\``);
}

const simpleDiffOpts = (srcD: string, tgtD: string, table: string) => ({
  sourceDatabase: srcD,
  targetDatabase: tgtD,
  table,
  primaryKey: ["id"],
  columns: ["id", "name", "val"],
  queryTimeout: 10_000,
});

// ---------------------------------------------------------------------------
// Tests: single-column primary key
// ---------------------------------------------------------------------------

describe("diffTableData() — identical data", () => {
  const TABLE = "items_same";

  beforeAll(async () => {
    if (skip) return;
    await createSimpleTable(srcPool, srcDb, TABLE);
    await createSimpleTable(tgtPool, tgtDb, TABLE);
    const rows = [[1, "Alice", 10], [2, "Bob", 20], [3, "Charlie", 30]];
    for (const [id, name, val] of rows) {
      await srcPool.query(`INSERT INTO \`${srcDb}\`.\`${TABLE}\` VALUES (?, ?, ?)`, [id, name, val]);
      await tgtPool.query(`INSERT INTO \`${tgtDb}\`.\`${TABLE}\` VALUES (?, ?, ?)`, [id, name, val]);
    }
  }, 10_000);

  afterAll(async () => {
    if (skip) return;
    await dropTable(srcPool, srcDb, TABLE);
    await dropTable(tgtPool, tgtDb, TABLE);
  }, 10_000);

  it("returns no statements when data is identical", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    expect(stmts).toHaveLength(0);
  });
});

describe("diffTableData() — source has extra rows", () => {
  const TABLE = "items_src_extra";

  beforeAll(async () => {
    if (skip) return;
    await createSimpleTable(srcPool, srcDb, TABLE);
    await createSimpleTable(tgtPool, tgtDb, TABLE);
    // source has rows 1, 2, 3 — target has only row 1
    await srcPool.query(`INSERT INTO \`${srcDb}\`.\`${TABLE}\` VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 30)`);
    await tgtPool.query(`INSERT INTO \`${tgtDb}\`.\`${TABLE}\` VALUES (1, 'Alice', 10)`);
  }, 10_000);

  afterAll(async () => {
    if (skip) return;
    await dropTable(srcPool, srcDb, TABLE);
    await dropTable(tgtPool, tgtDb, TABLE);
  }, 10_000);

  it("generates INSERT statements for missing rows", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    expect(stmts).toHaveLength(2);
    expect(stmts.every((s) => s.kind === "insert")).toBe(true);
    expect(stmts.every((s) => s.destructive === false)).toBe(true);
  });

  it("INSERT statements reference the target database", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    for (const s of stmts) {
      expect(s.sql).toContain(tgtDb);
    }
  });

  it("INSERT params contain the correct values", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    const ids = stmts.map((s) => s.params[0]);
    expect(ids).toContain(2);
    expect(ids).toContain(3);
  });
});

describe("diffTableData() — target has extra rows (deletes)", () => {
  const TABLE = "items_tgt_extra";

  beforeAll(async () => {
    if (skip) return;
    await createSimpleTable(srcPool, srcDb, TABLE);
    await createSimpleTable(tgtPool, tgtDb, TABLE);
    // source has row 1 only; target has rows 1, 2, 3
    await srcPool.query(`INSERT INTO \`${srcDb}\`.\`${TABLE}\` VALUES (1, 'Alice', 10)`);
    await tgtPool.query(`INSERT INTO \`${tgtDb}\`.\`${TABLE}\` VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 30)`);
  }, 10_000);

  afterAll(async () => {
    if (skip) return;
    await dropTable(srcPool, srcDb, TABLE);
    await dropTable(tgtPool, tgtDb, TABLE);
  }, 10_000);

  it("generates DELETE statements for extra target rows", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    expect(stmts).toHaveLength(2);
    expect(stmts.every((s) => s.kind === "delete")).toBe(true);
    expect(stmts.every((s) => s.destructive === true)).toBe(true);
  });

  it("DELETE statements contain WHERE clause with PK values", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    for (const s of stmts) {
      expect(s.sql).toMatch(/WHERE/i);
      expect(s.params.length).toBeGreaterThan(0);
    }
    const deletedIds = stmts.map((s) => s.params[0]);
    expect(deletedIds).toContain(2);
    expect(deletedIds).toContain(3);
  });
});

describe("diffTableData() — modified rows", () => {
  const TABLE = "items_modified";

  beforeAll(async () => {
    if (skip) return;
    await createSimpleTable(srcPool, srcDb, TABLE);
    await createSimpleTable(tgtPool, tgtDb, TABLE);
    // Both have rows 1-3, but row 2 and 3 differ
    await srcPool.query(`INSERT INTO \`${srcDb}\`.\`${TABLE}\` VALUES (1, 'Alice', 10), (2, 'Bob-updated', 99), (3, 'Charlie', 30)`);
    await tgtPool.query(`INSERT INTO \`${tgtDb}\`.\`${TABLE}\` VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 999)`);
  }, 10_000);

  afterAll(async () => {
    if (skip) return;
    await dropTable(srcPool, srcDb, TABLE);
    await dropTable(tgtPool, tgtDb, TABLE);
  }, 10_000);

  it("generates UPDATE statements for changed rows", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    expect(stmts).toHaveLength(2);
    expect(stmts.every((s) => s.kind === "update")).toBe(true);
    expect(stmts.every((s) => s.destructive === false)).toBe(true);
  });

  it("UPDATE statements contain SET and WHERE", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    for (const s of stmts) {
      expect(s.sql).toMatch(/SET/i);
      expect(s.sql).toMatch(/WHERE/i);
    }
  });

  it("UPDATE params carry the new values and PK last", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    const updatedIds = stmts.map((s) => s.params[s.params.length - 1]); // PK is last
    expect(updatedIds).toContain(2);
    expect(updatedIds).toContain(3);
  });
});

describe("diffTableData() — mixed changes", () => {
  const TABLE = "items_mixed";

  beforeAll(async () => {
    if (skip) return;
    await createSimpleTable(srcPool, srcDb, TABLE);
    await createSimpleTable(tgtPool, tgtDb, TABLE);
    // source: 1 (same), 2 (modified), 3 (new)
    // target: 1 (same), 2 (stale), 4 (to delete)
    await srcPool.query(`INSERT INTO \`${srcDb}\`.\`${TABLE}\` VALUES (1, 'Alice', 10), (2, 'Bob-v2', 99), (3, 'Charlie', 30)`);
    await tgtPool.query(`INSERT INTO \`${tgtDb}\`.\`${TABLE}\` VALUES (1, 'Alice', 10), (2, 'Bob-v1', 20), (4, 'David', 40)`);
  }, 10_000);

  afterAll(async () => {
    if (skip) return;
    await dropTable(srcPool, srcDb, TABLE);
    await dropTable(tgtPool, tgtDb, TABLE);
  }, 10_000);

  it("returns insert, update, and delete statements", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    const kinds = stmts.map((s) => s.kind);
    expect(kinds).toContain("insert");
    expect(kinds).toContain("update");
    expect(kinds).toContain("delete");
    // 1 insert (id=3) + 1 update (id=2) + 1 delete (id=4) = 3 total
    expect(stmts).toHaveLength(3);
  });
});

describe("diffTableData() — empty tables", () => {
  const TABLE = "items_empty";

  beforeAll(async () => {
    if (skip) return;
    await createSimpleTable(srcPool, srcDb, TABLE);
    await createSimpleTable(tgtPool, tgtDb, TABLE);
    // both empty — no rows inserted
  }, 10_000);

  afterAll(async () => {
    if (skip) return;
    await dropTable(srcPool, srcDb, TABLE);
    await dropTable(tgtPool, tgtDb, TABLE);
  }, 10_000);

  it("returns no statements for two empty tables", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, simpleDiffOpts(srcDb, tgtDb, TABLE));
    expect(stmts).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Tests: multi-column primary key
// ---------------------------------------------------------------------------

describe("diffTableData() — multi-column primary key", () => {
  const TABLE = "order_items";

  beforeAll(async () => {
    if (skip) return;
    await srcPool.query(`
      CREATE TABLE IF NOT EXISTS \`${srcDb}\`.\`${TABLE}\` (
        order_id   INT          NOT NULL,
        product_id INT          NOT NULL,
        qty        INT          NOT NULL,
        price      DECIMAL(8,2) NOT NULL,
        PRIMARY KEY (order_id, product_id)
      ) ENGINE=InnoDB
    `);
    await tgtPool.query(`
      CREATE TABLE IF NOT EXISTS \`${tgtDb}\`.\`${TABLE}\` (
        order_id   INT          NOT NULL,
        product_id INT          NOT NULL,
        qty        INT          NOT NULL,
        price      DECIMAL(8,2) NOT NULL,
        PRIMARY KEY (order_id, product_id)
      ) ENGINE=InnoDB
    `);

    // source: (1,1) same, (1,2) modified qty, (2,1) new
    // target: (1,1) same, (1,2) stale,        (1,3) to delete
    await srcPool.query(
      `INSERT INTO \`${srcDb}\`.\`${TABLE}\` VALUES (1,1,2,'9.99'), (1,2,5,'4.50'), (2,1,1,'99.00')`
    );
    await tgtPool.query(
      `INSERT INTO \`${tgtDb}\`.\`${TABLE}\` VALUES (1,1,2,'9.99'), (1,2,3,'4.50'), (1,3,1,'1.00')`
    );
  }, 10_000);

  afterAll(async () => {
    if (skip) return;
    await dropTable(srcPool, srcDb, TABLE);
    await dropTable(tgtPool, tgtDb, TABLE);
  }, 10_000);

  it("handles composite PKs: insert + update + delete", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, {
      sourceDatabase: srcDb,
      targetDatabase: tgtDb,
      table: TABLE,
      primaryKey: ["order_id", "product_id"],
      columns: ["order_id", "product_id", "qty", "price"],
      queryTimeout: 10_000,
    });

    const kinds = stmts.map((s) => s.kind);
    expect(kinds).toContain("insert");
    expect(kinds).toContain("update");
    expect(kinds).toContain("delete");
    expect(stmts).toHaveLength(3);
  });
});

// ---------------------------------------------------------------------------
// Tests: ignoreColumns
// ---------------------------------------------------------------------------

describe("diffTableData() — ignoreColumns", () => {
  const TABLE = "items_ignore";

  beforeAll(async () => {
    if (skip) return;
    await srcPool.query(`
      CREATE TABLE IF NOT EXISTS \`${srcDb}\`.\`${TABLE}\` (
        id         INT          NOT NULL,
        name       VARCHAR(100) NOT NULL,
        updated_at DATETIME     NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB
    `);
    await tgtPool.query(`
      CREATE TABLE IF NOT EXISTS \`${tgtDb}\`.\`${TABLE}\` (
        id         INT          NOT NULL,
        name       VARCHAR(100) NOT NULL,
        updated_at DATETIME     NULL,
        PRIMARY KEY (id)
      ) ENGINE=InnoDB
    `);
    // Same name, different updated_at — should be treated as equal when ignoring updated_at
    await srcPool.query(`INSERT INTO \`${srcDb}\`.\`${TABLE}\` VALUES (1, 'Alice', '2024-01-01 00:00:00')`);
    await tgtPool.query(`INSERT INTO \`${tgtDb}\`.\`${TABLE}\` VALUES (1, 'Alice', '2023-06-15 12:00:00')`);
  }, 10_000);

  afterAll(async () => {
    if (skip) return;
    await dropTable(srcPool, srcDb, TABLE);
    await dropTable(tgtPool, tgtDb, TABLE);
  }, 10_000);

  it("treats rows as equal when differing column is ignored", async () => {
    if (skip) return;
    const stmts = await diffTableData(srcPool, tgtPool, {
      sourceDatabase: srcDb,
      targetDatabase: tgtDb,
      table: TABLE,
      primaryKey: ["id"],
      columns: ["id", "name", "updated_at"],
      ignoreColumns: ["updated_at"],
      queryTimeout: 10_000,
    });
    expect(stmts).toHaveLength(0);
  });

  it("detects update when non-ignored column differs", async () => {
    if (skip) return;
    // Update name in target so it differs even ignoring updated_at
    await tgtPool.query(`UPDATE \`${tgtDb}\`.\`${TABLE}\` SET name = 'Alicia' WHERE id = 1`);
    const stmts = await diffTableData(srcPool, tgtPool, {
      sourceDatabase: srcDb,
      targetDatabase: tgtDb,
      table: TABLE,
      primaryKey: ["id"],
      columns: ["id", "name", "updated_at"],
      ignoreColumns: ["updated_at"],
      queryTimeout: 10_000,
    });
    expect(stmts).toHaveLength(1);
    expect(stmts[0].kind).toBe("update");
  });
});
