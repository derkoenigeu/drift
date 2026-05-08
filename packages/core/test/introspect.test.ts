import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Pool, PoolConnection, RowDataPacket } from "mysql2/promise";
import { introspect } from "../src/schema-diff/introspect.js";

// ── Mock pool builder ──────────────────────────────────────────────────────
//
// introspect() calls pool.query() many times in sequence. The implementation
// uses a private helper `q()` which calls `pool.query<T[]>(sql, params)` and
// unpacks `[rows]`. For SHOW CREATE … statements it calls pool.query directly
// and also unpacks `[rows]`.
//
// We build a queue-based mock so each successive call returns the next result.

type QueryResult = RowDataPacket[][] | [RowDataPacket[]];

function makePool(responses: QueryResult[]): Pool {
  let idx = 0;
  const query = vi.fn().mockImplementation(() => {
    const result = responses[idx++];
    if (result === undefined) {
      throw new Error(`Unexpected extra pool.query call at index ${idx - 1}`);
    }
    return Promise.resolve(result);
  });
  return { query } as unknown as Pool;
}

// ── Helpers ────────────────────────────────────────────────────────────────

function tableRow(name: string, type: "BASE TABLE" | "VIEW" = "BASE TABLE") {
  return {
    TABLE_NAME: name,
    ENGINE: "InnoDB",
    TABLE_COLLATION: "utf8mb4_general_ci",
    TABLE_COMMENT: "",
    TABLE_TYPE: type,
  } as RowDataPacket;
}

function columnRow(opts: {
  name: string;
  type?: string;
  nullable?: "YES" | "NO";
  default?: string | null;
  extra?: string;
  collation?: string | null;
  comment?: string;
  pos?: number;
}): RowDataPacket {
  return {
    COLUMN_NAME: opts.name,
    COLUMN_TYPE: opts.type ?? "int",
    IS_NULLABLE: opts.nullable ?? "NO",
    COLUMN_DEFAULT: opts.default ?? null,
    EXTRA: opts.extra ?? "",
    COLLATION_NAME: opts.collation ?? null,
    COLUMN_COMMENT: opts.comment ?? "",
    ORDINAL_POSITION: opts.pos ?? 1,
  } as unknown as RowDataPacket;
}

function indexRow(opts: {
  name: string;
  nonUnique?: 0 | 1;
  column: string;
  seq?: number;
  type?: string;
}): RowDataPacket {
  return {
    INDEX_NAME: opts.name,
    NON_UNIQUE: opts.nonUnique ?? 0,
    COLUMN_NAME: opts.column,
    SEQ_IN_INDEX: opts.seq ?? 1,
    INDEX_TYPE: opts.type ?? "BTREE",
  } as unknown as RowDataPacket;
}

// A full sequence of pool.query responses for a single table named `name`
// with the given columns and index rows.
function tableQuerySequence(
  name: string,
  columns: RowDataPacket[],
  indexes: RowDataPacket[],
): QueryResult[] {
  return [
    // SHOW CREATE TABLE — returns [[{ 'Create Table': '...' }]]
    [[{ "Create Table": `CREATE TABLE \`${name}\` (...)` } as RowDataPacket]],
    // COLUMNS
    [columns],
    // STATISTICS
    [indexes],
  ];
}

// ── Tests ──────────────────────────────────────────────────────────────────

describe("introspect", () => {
  it("returns empty snapshot when no tables, views, routines, triggers", async () => {
    const pool = makePool([
      [[]], // TABLES → []
      [[]], // VIEWS → []
      [[]], // ROUTINES → []
      [[]], // TRIGGERS → []
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.database).toBe("testdb");
    expect(snap.tables.size).toBe(0);
    expect(snap.views.size).toBe(0);
    expect(snap.routines.size).toBe(0);
    expect(snap.triggers.size).toBe(0);
  });

  it("skips VIEW-type rows from the TABLES query", async () => {
    const pool = makePool([
      // TABLES: one base table, one view-type row
      [[tableRow("users"), tableRow("v_users", "VIEW")]],
      // SHOW CREATE TABLE users
      [[{ "Create Table": "CREATE TABLE `users` (...)" } as RowDataPacket]],
      // COLUMNS for users
      [[columnRow({ name: "id", pos: 1 })]],
      // STATISTICS for users (no indexes)
      [[]],
      // VIEWS
      [[]],
      // ROUTINES
      [[]],
      // TRIGGERS
      [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.tables.has("users")).toBe(true);
    expect(snap.tables.has("v_users")).toBe(false);
    expect(snap.tables.size).toBe(1);
  });

  it("populates TableInfo correctly for a single table", async () => {
    const cols = [
      columnRow({ name: "id", type: "int unsigned", nullable: "NO", extra: "auto_increment", pos: 1 }),
      columnRow({ name: "email", type: "varchar(255)", nullable: "YES", default: null, collation: "utf8mb4_general_ci", pos: 2 }),
    ];
    const idxs = [
      indexRow({ name: "PRIMARY", nonUnique: 0, column: "id", seq: 1 }),
    ];
    const pool = makePool([
      [[tableRow("users")]],
      ...tableQuerySequence("users", cols, idxs),
      [[]], // VIEWS
      [[]], // ROUTINES
      [[]], // TRIGGERS
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    const t = snap.tables.get("users")!;
    expect(t).toBeDefined();
    expect(t.name).toBe("users");
    expect(t.engine).toBe("InnoDB");
    expect(t.collation).toBe("utf8mb4_general_ci");
    expect(t.columns).toHaveLength(2);
    expect(t.columns[0]).toMatchObject({ name: "id", type: "int unsigned", nullable: false, extra: "auto_increment" });
    expect(t.columns[1]).toMatchObject({ name: "email", type: "varchar(255)", nullable: true, collation: "utf8mb4_general_ci" });
  });

  it("extracts primaryKey from PRIMARY index", async () => {
    const idxs = [indexRow({ name: "PRIMARY", nonUnique: 0, column: "id", seq: 1 })];
    const pool = makePool([
      [[tableRow("orders")]],
      ...tableQuerySequence("orders", [columnRow({ name: "id" })], idxs),
      [[]], [[]], [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.tables.get("orders")!.primaryKey).toEqual(["id"]);
  });

  it("extracts multi-column primary key in correct order", async () => {
    const idxs = [
      indexRow({ name: "PRIMARY", nonUnique: 0, column: "tenant_id", seq: 1 }),
      indexRow({ name: "PRIMARY", nonUnique: 0, column: "order_id", seq: 2 }),
    ];
    const pool = makePool([
      [[tableRow("order_items")]],
      ...tableQuerySequence(
        "order_items",
        [columnRow({ name: "tenant_id", pos: 1 }), columnRow({ name: "order_id", pos: 2 })],
        idxs,
      ),
      [[]], [[]], [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.tables.get("order_items")!.primaryKey).toEqual(["tenant_id", "order_id"]);
  });

  it("sets primaryKey=[] when no PRIMARY index exists", async () => {
    const pool = makePool([
      [[tableRow("log_entries")]],
      ...tableQuerySequence("log_entries", [columnRow({ name: "msg" })], []),
      [[]], [[]], [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.tables.get("log_entries")!.primaryKey).toEqual([]);
  });

  it("parses composite non-unique indexes correctly", async () => {
    const idxs = [
      indexRow({ name: "PRIMARY", nonUnique: 0, column: "id", seq: 1 }),
      indexRow({ name: "idx_name_email", nonUnique: 1, column: "name", seq: 1, type: "BTREE" }),
      indexRow({ name: "idx_name_email", nonUnique: 1, column: "email", seq: 2, type: "BTREE" }),
    ];
    const pool = makePool([
      [[tableRow("users")]],
      ...tableQuerySequence("users", [columnRow({ name: "id" }), columnRow({ name: "name" }), columnRow({ name: "email" })], idxs),
      [[]], [[]], [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    const t = snap.tables.get("users")!;
    const composite = t.indexes.find((i) => i.name === "idx_name_email")!;
    expect(composite).toBeDefined();
    expect(composite.unique).toBe(false);
    expect(composite.columns).toEqual(["name", "email"]);
    expect(composite.type).toBe("BTREE");
  });

  it("stores the createStatement from SHOW CREATE TABLE", async () => {
    const stmt = "CREATE TABLE `products` (`id` int NOT NULL)";
    const pool = makePool([
      [[tableRow("products")]],
      [[{ "Create Table": stmt } as RowDataPacket]],
      [[columnRow({ name: "id" })]],
      [[]],
      [[]], [[]], [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.tables.get("products")!.createStatement).toBe(stmt);
  });

  it("handles missing 'Create Table' key gracefully (empty string)", async () => {
    const pool = makePool([
      [[tableRow("t")]],
      [[{} as RowDataPacket]], // no 'Create Table' key
      [[columnRow({ name: "x" })]],
      [[]],
      [[]], [[]], [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.tables.get("t")!.createStatement).toBe("");
  });

  it("populates views map correctly", async () => {
    const pool = makePool([
      [[]], // TABLES → no base tables
      [[{ TABLE_NAME: "v_active_users" } as RowDataPacket]], // VIEWS
      [[{ "Create View": "CREATE VIEW `v_active_users` AS SELECT ..." } as RowDataPacket]],
      [[]], // ROUTINES
      [[]], // TRIGGERS
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.views.size).toBe(1);
    const v = snap.views.get("v_active_users")!;
    expect(v.name).toBe("v_active_users");
    expect(v.definition).toContain("CREATE VIEW");
  });

  it("handles missing 'Create View' key gracefully", async () => {
    const pool = makePool([
      [[]],
      [[{ TABLE_NAME: "v_empty" } as RowDataPacket]],
      [[{} as RowDataPacket]], // no 'Create View' key
      [[]], [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.views.get("v_empty")!.definition).toBe("");
  });

  it("populates routines map with PROCEDURE key", async () => {
    const pool = makePool([
      [[]], // TABLES
      [[]], // VIEWS
      [[{ ROUTINE_NAME: "do_something", ROUTINE_TYPE: "PROCEDURE" } as RowDataPacket]], // ROUTINES
      [[{ "Create Procedure": "CREATE PROCEDURE `do_something`() BEGIN END" } as RowDataPacket]],
      [[]], // TRIGGERS
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.routines.size).toBe(1);
    expect(snap.routines.has("PROCEDURE:do_something")).toBe(true);
    const r = snap.routines.get("PROCEDURE:do_something")!;
    expect(r.name).toBe("do_something");
    expect(r.kind).toBe("PROCEDURE");
    expect(r.definition).toContain("CREATE PROCEDURE");
  });

  it("populates routines map with FUNCTION key", async () => {
    const pool = makePool([
      [[]], [[]],
      [[{ ROUTINE_NAME: "get_total", ROUTINE_TYPE: "FUNCTION" } as RowDataPacket]],
      [[{ "Create Function": "CREATE FUNCTION `get_total`() RETURNS int BEGIN END" } as RowDataPacket]],
      [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.routines.has("FUNCTION:get_total")).toBe(true);
    expect(snap.routines.get("FUNCTION:get_total")!.kind).toBe("FUNCTION");
  });

  it("populates triggers map correctly", async () => {
    const pool = makePool([
      [[]], [[]], [[]],
      [[{ TRIGGER_NAME: "before_insert_users" } as RowDataPacket]], // TRIGGERS
      [[{ "SQL Original Statement": "CREATE TRIGGER ..." } as RowDataPacket]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.triggers.size).toBe(1);
    const t = snap.triggers.get("before_insert_users")!;
    expect(t.name).toBe("before_insert_users");
    expect(t.definition).toContain("CREATE TRIGGER");
  });

  it("falls back to 'Statement' column for trigger definition", async () => {
    const pool = makePool([
      [[]], [[]], [[]],
      [[{ TRIGGER_NAME: "trg" } as RowDataPacket]],
      [[{ Statement: "CREATE TRIGGER trg ..." } as RowDataPacket]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.triggers.get("trg")!.definition).toContain("CREATE TRIGGER trg");
  });

  it("handles multiple tables sequentially", async () => {
    const pool = makePool([
      [[tableRow("users"), tableRow("orders")]],
      // users
      [[{ "Create Table": "CREATE TABLE `users` (...)" } as RowDataPacket]],
      [[columnRow({ name: "id" })]],
      [[indexRow({ name: "PRIMARY", nonUnique: 0, column: "id" })]],
      // orders
      [[{ "Create Table": "CREATE TABLE `orders` (...)" } as RowDataPacket]],
      [[columnRow({ name: "id" })]],
      [[indexRow({ name: "PRIMARY", nonUnique: 0, column: "id" })]],
      [[]], [[]], [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    expect(snap.tables.size).toBe(2);
    expect(snap.tables.has("users")).toBe(true);
    expect(snap.tables.has("orders")).toBe(true);
  });

  it("maps column nullable correctly (YES → true, NO → false)", async () => {
    const pool = makePool([
      [[tableRow("t")]],
      [[{ "Create Table": "..." } as RowDataPacket]],
      [[
        columnRow({ name: "required", nullable: "NO", pos: 1 }),
        columnRow({ name: "optional", nullable: "YES", pos: 2 }),
      ]],
      [[]],
      [[]], [[]], [[]],
    ]);
    const snap = await introspect(pool as Pool, "testdb");
    const cols = snap.tables.get("t")!.columns;
    expect(cols.find((c) => c.name === "required")!.nullable).toBe(false);
    expect(cols.find((c) => c.name === "optional")!.nullable).toBe(true);
  });
});
