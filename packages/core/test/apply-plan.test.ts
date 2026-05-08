import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Pool, PoolConnection, RowDataPacket } from "mysql2/promise";
import { applyPlan } from "../src/sync-plan/plan.js";
import type { SyncPlan } from "../src/sync-plan/plan.js";
import type { SchemaStatement } from "../src/schema-diff/diff.js";
import type { DataStatement } from "../src/data-diff/data-diff.js";

// ── Helpers ────────────────────────────────────────────────────────────────

function schemaStmt(
  kind: SchemaStatement["kind"],
  object: string,
  sql?: string,
): SchemaStatement {
  return { kind, object, sql: sql ?? `-- ${kind} ${object}`, destructive: false };
}

function insertStmt(table: string, params: unknown[] = [1]): DataStatement {
  return {
    kind: "insert",
    table,
    sql: `INSERT INTO \`tgt\`.\`${table}\` (\`id\`) VALUES (?)`,
    params,
    destructive: false,
  };
}

function deleteStmt(table: string): DataStatement {
  return {
    kind: "delete",
    table,
    sql: `DELETE FROM \`tgt\`.\`${table}\` WHERE \`id\`=?`,
    params: [99],
    destructive: true,
  };
}

function emptyPlan(overrides?: Partial<SyncPlan>): SyncPlan {
  return {
    sourceDatabase: "src",
    targetDatabase: "tgt",
    schema: [],
    data: [],
    createdAt: new Date().toISOString(),
    ...overrides,
  };
}

// ── Mock pool/connection factory ───────────────────────────────────────────

interface MockConn {
  query: ReturnType<typeof vi.fn>;
  release: ReturnType<typeof vi.fn>;
}

function makePool(
  queryImpl?: (sql: string | { sql: string }) => Promise<unknown>,
): { pool: Pool; conn: MockConn } {
  const conn: MockConn = {
    query: vi.fn().mockImplementation(async (arg: string | { sql: string }) => {
      if (queryImpl) return queryImpl(arg);
      // Default: always succeed with empty result for schema; for FK info query return []
      return [[]];
    }),
    release: vi.fn(),
  };
  const pool = {
    getConnection: vi.fn().mockResolvedValue(conn),
  } as unknown as Pool;
  return { pool, conn };
}

// The FK gatherFks query returns rows: we match by SQL content
function fkQueryResult(rows: Record<string, string>[] = []): [RowDataPacket[]] {
  return [rows as RowDataPacket[]];
}

// ── Tests ──────────────────────────────────────────────────────────────────

describe("applyPlan – dryRun", () => {
  it("returns {executed:0, skipped:total, errors:[]} without querying", async () => {
    const { pool, conn } = makePool();
    const plan = emptyPlan({
      schema: [schemaStmt("create-table", "users")],
      data: [insertStmt("users")],
    });
    const result = await applyPlan(pool, plan, { dryRun: true });
    expect(result.executed).toBe(0);
    expect(result.skipped).toBe(2); // 1 schema + 1 data
    expect(result.errors).toEqual([]);
    // pool.getConnection should NOT have been called
    expect(pool.getConnection).not.toHaveBeenCalled();
  });

  it("counts skipped correctly for schema-only plan", async () => {
    const { pool } = makePool();
    const plan = emptyPlan({ schema: [schemaStmt("alter-table", "t1"), schemaStmt("drop-table", "t2")] });
    const result = await applyPlan(pool, plan, { dryRun: true });
    expect(result.skipped).toBe(2);
  });

  it("counts skipped correctly for data-only plan", async () => {
    const { pool } = makePool();
    const plan = emptyPlan({ data: [insertStmt("t"), insertStmt("t"), deleteStmt("t")] });
    const result = await applyPlan(pool, plan, { dryRun: true });
    expect(result.skipped).toBe(3);
  });
});

describe("applyPlan – empty plan", () => {
  it("executes setup queries and releases connection", async () => {
    const { pool, conn } = makePool();
    const result = await applyPlan(pool, emptyPlan());
    expect(result.executed).toBe(0);
    expect(result.errors).toEqual([]);
    expect(conn.release).toHaveBeenCalledOnce();
  });
});

describe("applyPlan – schema ordering", () => {
  it("executes alter-table statements before create-table before others", async () => {
    const executedKinds: string[] = [];
    const { pool, conn } = makePool();

    conn.query.mockImplementation(async (arg: string | { sql: string }) => {
      const sql = typeof arg === "string" ? arg : arg.sql;
      // Capture which kind of schema statement is running by checking its comment
      if (sql.includes("-- alter-table")) executedKinds.push("alter-table");
      else if (sql.includes("-- create-table")) executedKinds.push("create-table");
      else if (sql.includes("-- create-view")) executedKinds.push("create-view");
      return [[]];
    });

    const plan = emptyPlan({
      schema: [
        schemaStmt("create-view", "v1", "-- create-view v1"),
        schemaStmt("create-table", "t1", "-- create-table t1"),
        schemaStmt("alter-table", "t2", "-- alter-table t2"),
      ],
    });

    await applyPlan(pool, plan);

    const schemaIdx = (kind: string) => executedKinds.indexOf(kind);
    expect(schemaIdx("alter-table")).toBeLessThan(schemaIdx("create-table"));
    expect(schemaIdx("create-table")).toBeLessThan(schemaIdx("create-view"));
  });

  it("executes multiple alter-table statements all before create-table", async () => {
    const order: string[] = [];
    const { pool, conn } = makePool();
    conn.query.mockImplementation(async (arg: string | { sql: string }) => {
      const sql = typeof arg === "string" ? arg : arg.sql;
      if (sql.startsWith("-- ")) order.push(sql.replace("-- ", ""));
      return [[]];
    });

    const plan = emptyPlan({
      schema: [
        schemaStmt("create-table", "new_t", "-- create-table new_t"),
        schemaStmt("alter-table", "a", "-- alter-table a"),
        schemaStmt("alter-table", "b", "-- alter-table b"),
      ],
    });
    await applyPlan(pool, plan);
    expect(order.indexOf("alter-table a")).toBeLessThan(order.indexOf("create-table new_t"));
    expect(order.indexOf("alter-table b")).toBeLessThan(order.indexOf("create-table new_t"));
  });
});

describe("applyPlan – FK handling", () => {
  it("gathers FKs and drops them before schema execution, then restores", async () => {
    const calls: string[] = [];
    const { pool, conn } = makePool();

    conn.query.mockImplementation(async (arg: string | { sql: string }) => {
      const sql = typeof arg === "string" ? arg : arg.sql;
      calls.push(sql.slice(0, 80).replace(/\s+/g, " ").trim());

      // FK gather query returns one FK
      if (sql.includes("INFORMATION_SCHEMA.KEY_COLUMN_USAGE")) {
        return [[{
          TABLE_NAME: "orders",
          CONSTRAINT_NAME: "fk_user",
          COLUMN_NAME: "user_id",
          REFERENCED_TABLE_NAME: "users",
          REFERENCED_COLUMN_NAME: "id",
          DELETE_RULE: "CASCADE",
          UPDATE_RULE: "RESTRICT",
        }]];
      }
      return [[]];
    });

    const plan = emptyPlan({
      schema: [schemaStmt("alter-table", "users", "ALTER TABLE `users` CHANGE ...")],
    });
    await applyPlan(pool, plan);

    const dropIdx = calls.findIndex((s) => s.includes("DROP FOREIGN KEY"));
    const schemaIdx = calls.findIndex((s) => s.includes("ALTER TABLE") && s.includes("CHANGE"));
    const restoreIdx = calls.findIndex((s) => s.includes("ADD CONSTRAINT"));

    expect(dropIdx).toBeGreaterThan(-1);
    expect(schemaIdx).toBeGreaterThan(dropIdx);
    expect(restoreIdx).toBeGreaterThan(schemaIdx);
  });

  it("continues gracefully when FK drop fails", async () => {
    const { pool, conn } = makePool();
    conn.query.mockImplementation(async (arg: string | { sql: string }) => {
      const sql = typeof arg === "string" ? arg : arg.sql;
      if (sql.includes("INFORMATION_SCHEMA.KEY_COLUMN_USAGE")) {
        return [[{
          TABLE_NAME: "orders",
          CONSTRAINT_NAME: "fk_user",
          COLUMN_NAME: "user_id",
          REFERENCED_TABLE_NAME: "users",
          REFERENCED_COLUMN_NAME: "id",
          DELETE_RULE: "RESTRICT",
          UPDATE_RULE: "RESTRICT",
        }]];
      }
      if (sql.includes("DROP FOREIGN KEY")) throw new Error("FK drop failed");
      return [[]];
    });

    // Should not throw even when DROP FK fails (it's swallowed)
    await expect(applyPlan(pool, emptyPlan())).resolves.toBeDefined();
  });
});

describe("applyPlan – error handling", () => {
  it("throws on first schema error when continueOnError=false (default)", async () => {
    const { pool, conn } = makePool();
    conn.query.mockImplementation(async (arg: string | { sql: string }) => {
      const sql = typeof arg === "string" ? arg : arg.sql;
      if (sql.includes("FAIL")) throw new Error("statement failed");
      return [[]];
    });

    const plan = emptyPlan({
      schema: [
        schemaStmt("alter-table", "t", "FAIL this statement"),
        schemaStmt("create-table", "t2", "CREATE TABLE t2 ..."),
      ],
    });
    await expect(applyPlan(pool, plan, { continueOnError: false })).rejects.toThrow("statement failed");
  });

  it("collects errors and continues when continueOnError=true", async () => {
    const { pool, conn } = makePool();
    conn.query.mockImplementation(async (arg: string | { sql: string }) => {
      const sql = typeof arg === "string" ? arg : arg.sql;
      if (sql.includes("FAIL")) throw new Error("statement failed");
      return [[]];
    });

    const plan = emptyPlan({
      schema: [
        schemaStmt("alter-table", "t1", "FAIL"),
        schemaStmt("create-table", "t2", "CREATE TABLE t2"),
        schemaStmt("create-view", "v1", "FAIL"),
      ],
    });
    const result = await applyPlan(pool, plan, { continueOnError: true });
    expect(result.errors).toHaveLength(2);
    expect(result.executed).toBe(1); // only t2 succeeded
  });

  it("throws on first data error when continueOnError=false", async () => {
    const { pool, conn } = makePool();
    conn.query.mockImplementation(async (arg: string | { sql: string }) => {
      const sql = typeof arg === "string" ? arg : arg.sql;
      if (sql.includes("INSERT") && sql.includes("bad_table")) throw new Error("data insert failed");
      return [[]];
    });

    const plan = emptyPlan({
      data: [insertStmt("bad_table"), insertStmt("good_table")],
    });
    await expect(applyPlan(pool, plan, { continueOnError: false })).rejects.toThrow("data insert failed");
  });

  it("collects data errors and continues when continueOnError=true", async () => {
    const { pool, conn } = makePool();
    conn.query.mockImplementation(async (arg: string | { sql: string }) => {
      const sql = typeof arg === "string" ? arg : arg.sql;
      if (sql.includes("fail_table")) throw new Error("data error");
      return [[]];
    });

    const plan = emptyPlan({
      data: [
        insertStmt("fail_table"),
        insertStmt("ok_table"),
        deleteStmt("fail_table"),
      ],
    });
    const result = await applyPlan(pool, plan, { continueOnError: true });
    expect(result.errors.length).toBeGreaterThanOrEqual(1);
    expect(result.executed).toBeGreaterThanOrEqual(1);
  });

  it("always releases connection even when error is thrown", async () => {
    const { pool, conn } = makePool();
    conn.query.mockImplementation(async (arg: string | { sql: string }) => {
      const sql = typeof arg === "string" ? arg : arg.sql;
      if (sql.includes("FAIL")) throw new Error("boom");
      return [[]];
    });
    await expect(
      applyPlan(pool, emptyPlan({ schema: [schemaStmt("alter-table", "t", "FAIL")] })),
    ).rejects.toThrow("boom");
    expect(conn.release).toHaveBeenCalledOnce();
  });
});

describe("applyPlan – data statements and batching", () => {
  it("executes data statements with sql and values from plan", async () => {
    const dataQueries: Array<{ sql: string; values: unknown }> = [];
    const { pool, conn } = makePool();
    conn.query.mockImplementation(async (arg: string | { sql: string; values?: unknown }) => {
      if (typeof arg === "object" && arg.sql.includes("INSERT")) {
        dataQueries.push({ sql: arg.sql, values: (arg as any).values });
      }
      return [[]];
    });

    const plan = emptyPlan({
      data: [insertStmt("users", [42])],
    });
    await applyPlan(pool, plan);
    // batchInserts with a single insert leaves it unchanged
    expect(dataQueries).toHaveLength(1);
    expect(dataQueries[0].values).toEqual([42]);
  });

  it("batches consecutive INSERT statements for the same table", async () => {
    const dataQueries: string[] = [];
    const { pool, conn } = makePool();
    conn.query.mockImplementation(async (arg: string | { sql: string }) => {
      if (typeof arg === "object" && arg.sql.includes("INSERT")) {
        dataQueries.push(arg.sql);
      }
      return [[]];
    });

    // 3 inserts for same table → should be batched into 1
    const plan = emptyPlan({
      data: [
        insertStmt("orders", [1]),
        insertStmt("orders", [2]),
        insertStmt("orders", [3]),
      ],
    });
    await applyPlan(pool, plan);
    expect(dataQueries).toHaveLength(1);
    // The batched SQL should contain exactly 3 (?) groups
    expect((dataQueries[0].match(/\(\?\)/g) ?? []).length).toBe(3);
  });

  it("counts executed correctly across schema and data", async () => {
    const { pool, conn } = makePool();
    conn.query.mockImplementation(async () => [[]]);

    const plan = emptyPlan({
      schema: [schemaStmt("create-table", "t1"), schemaStmt("alter-table", "t2")],
      data: [insertStmt("t1"), insertStmt("t2")],
    });
    const result = await applyPlan(pool, plan);
    // 2 schema + 2 data (batched into 2 since different tables)
    expect(result.executed).toBe(4);
    expect(result.errors).toEqual([]);
  });
});

describe("applyPlan – onProgress callback", () => {
  it("calls onProgress with messages during execution", async () => {
    const messages: string[] = [];
    const { pool } = makePool();
    const plan = emptyPlan({
      schema: [schemaStmt("create-table", "users")],
    });
    await applyPlan(pool, plan, { onProgress: (msg) => messages.push(msg) });
    expect(messages.length).toBeGreaterThan(0);
    expect(messages.some((m) => m.includes("tgt"))).toBe(true);
  });

  it("reports dry-run message when dryRun=true", async () => {
    const messages: string[] = [];
    const { pool } = makePool();
    await applyPlan(pool, emptyPlan({ schema: [schemaStmt("create-table", "t")] }), {
      dryRun: true,
      onProgress: (m) => messages.push(m),
    });
    expect(messages.some((m) => m.toLowerCase().includes("dry"))).toBe(true);
  });
});
