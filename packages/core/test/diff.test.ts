import { describe, it, expect } from "vitest";
import { diffSchemas, type SchemaStatement } from "../src/schema-diff/diff.js";
import type { SchemaSnapshot, TableInfo, ColumnInfo, IndexInfo } from "../src/schema-diff/introspect.js";

// ── helpers ────────────────────────────────────────────────────────────────

function col(overrides: Partial<ColumnInfo> & { name: string; type: string }): ColumnInfo {
  return {
    nullable: false,
    default: null,
    extra: "",
    collation: null,
    comment: "",
    ordinalPosition: 1,
    ...overrides,
  };
}

function table(name: string, columns: ColumnInfo[], indexes: IndexInfo[] = []): TableInfo {
  return {
    name,
    engine: "InnoDB",
    collation: "utf8mb4_unicode_ci",
    comment: "",
    createStatement: `CREATE TABLE \`${name}\` (\`id\` int NOT NULL, PRIMARY KEY (\`id\`))`,
    columns,
    indexes,
    primaryKey: columns.filter((c) => c.extra.includes("auto_increment")).map((c) => c.name),
  };
}

function snapshot(
  tables: TableInfo[] = [],
  views: [string, string][] = [],
  routines: [string, { name: string; kind: "PROCEDURE" | "FUNCTION"; definition: string }][] = [],
  triggers: [string, { name: string; definition: string }][] = [],
): SchemaSnapshot {
  return {
    database: "test",
    tables: new Map(tables.map((t) => [t.name, t])),
    views: new Map(views.map(([k, def]) => [k, { name: k, definition: def }])),
    routines: new Map(routines.map(([k, r]) => [k, r])),
    triggers: new Map(triggers.map(([k, t]) => [k, t])),
  };
}

function kinds(stmts: SchemaStatement[]) {
  return stmts.map((s) => s.kind);
}

// ── tables ──────────────────────────────────────────────────────────────────

describe("diffSchemas — tables", () => {
  it("emits create-table when source has a table missing in target", () => {
    const src = snapshot([table("users", [col({ name: "id", type: "int" })])]);
    const tgt = snapshot();
    const stmts = diffSchemas(src, tgt);
    expect(kinds(stmts)).toContain("create-table");
    expect(stmts[0].destructive).toBe(false);
  });

  it("emits drop-table when target has a table missing in source", () => {
    const src = snapshot();
    const tgt = snapshot([table("old_table", [col({ name: "id", type: "int" })])]);
    const stmts = diffSchemas(src, tgt);
    expect(kinds(stmts)).toContain("drop-table");
    expect(stmts[0].destructive).toBe(true);
  });

  it("emits no statements when source and target tables are identical", () => {
    const cols = [col({ name: "id", type: "int" })];
    const snap = snapshot([table("users", cols)]);
    expect(diffSchemas(snap, snap)).toHaveLength(0);
  });

  it("emits alter-table ADD COLUMN when source has a new column", () => {
    const src = snapshot([table("t", [col({ name: "id", type: "int" }), col({ name: "email", type: "varchar(255)" })])]);
    const tgt = snapshot([table("t", [col({ name: "id", type: "int" })])]);
    const stmts = diffSchemas(src, tgt);
    expect(stmts).toHaveLength(1);
    expect(stmts[0].kind).toBe("alter-table");
    expect(stmts[0].sql).toContain("ADD COLUMN");
    expect(stmts[0].destructive).toBe(false);
  });

  it("emits alter-table DROP COLUMN (destructive) when target has an extra column", () => {
    const src = snapshot([table("t", [col({ name: "id", type: "int" })])]);
    const tgt = snapshot([table("t", [col({ name: "id", type: "int" }), col({ name: "obsolete", type: "text" })])]);
    const stmts = diffSchemas(src, tgt);
    expect(stmts).toHaveLength(1);
    expect(stmts[0].sql).toContain("DROP COLUMN");
    expect(stmts[0].destructive).toBe(true);
  });

  it("emits alter-table MODIFY COLUMN when column type changes", () => {
    const src = snapshot([table("t", [col({ name: "id", type: "bigint" })])]);
    const tgt = snapshot([table("t", [col({ name: "id", type: "int" })])]);
    const stmts = diffSchemas(src, tgt);
    expect(stmts[0].sql).toContain("MODIFY COLUMN");
  });

  it("normalises int(11) ≡ int across MySQL 8 / MariaDB", () => {
    const src = snapshot([table("t", [col({ name: "id", type: "int" })])]);
    const tgt = snapshot([table("t", [col({ name: "id", type: "int(11)" })])]);
    expect(diffSchemas(src, tgt)).toHaveLength(0);
  });

  it("normalises json ≡ longtext across MySQL 8 / MariaDB", () => {
    const src = snapshot([table("t", [col({ name: "meta", type: "json" })])]);
    const tgt = snapshot([table("t", [col({ name: "meta", type: "longtext" })])]);
    expect(diffSchemas(src, tgt)).toHaveLength(0);
  });

  it("treats null collation as compatible with any collation", () => {
    const src = snapshot([table("t", [col({ name: "name", type: "varchar(100)", collation: "utf8mb4_unicode_ci" })])]);
    const tgt = snapshot([table("t", [col({ name: "name", type: "varchar(100)", collation: null })])]);
    expect(diffSchemas(src, tgt)).toHaveLength(0);
  });

  it("treats MySQL null default ≡ MariaDB 'NULL' string", () => {
    const src = snapshot([table("t", [col({ name: "x", type: "int", nullable: true, default: null })])]);
    const tgt = snapshot([table("t", [col({ name: "x", type: "int", nullable: true, default: "NULL" })])]);
    expect(diffSchemas(src, tgt)).toHaveLength(0);
  });
});

// ── indexes ─────────────────────────────────────────────────────────────────

describe("diffSchemas — indexes", () => {
  function idx(name: string, columns: string[], unique = false): IndexInfo {
    return { name, unique, columns, type: "BTREE" };
  }

  it("emits ADD INDEX when source has a new index", () => {
    const idCol = col({ name: "id", type: "int" });
    const src = snapshot([table("t", [idCol], [idx("idx_id", ["id"])])]);
    const tgt = snapshot([table("t", [idCol], [])]);
    const stmts = diffSchemas(src, tgt);
    expect(stmts[0].sql).toContain("ADD");
    expect(stmts[0].sql).toContain("INDEX");
  });

  it("emits DROP INDEX (destructive) when target has an extra index", () => {
    const idCol = col({ name: "id", type: "int" });
    const src = snapshot([table("t", [idCol], [])]);
    const tgt = snapshot([table("t", [idCol], [idx("stale_idx", ["id"])])]);
    const stmts = diffSchemas(src, tgt);
    expect(stmts[0].sql).toContain("DROP INDEX");
    expect(stmts[0].destructive).toBe(true);
  });
});

// ── views / routines / triggers ──────────────────────────────────────────────

describe("diffSchemas — views", () => {
  it("emits create-view for new view", () => {
    const src = snapshot([], [["v_users", "CREATE VIEW `v_users` AS SELECT 1"]]);
    expect(kinds(diffSchemas(src, snapshot()))).toContain("create-view");
  });

  it("emits drop-view (destructive) for removed view", () => {
    const tgt = snapshot([], [["v_old", "CREATE VIEW `v_old` AS SELECT 1"]]);
    const stmts = diffSchemas(snapshot(), tgt);
    expect(kinds(stmts)).toContain("drop-view");
    expect(stmts[0].destructive).toBe(true);
  });

  it("emits alter-view when definition changes", () => {
    const src = snapshot([], [["v", "CREATE VIEW `v` AS SELECT 2"]]);
    const tgt = snapshot([], [["v", "CREATE VIEW `v` AS SELECT 1"]]);
    expect(kinds(diffSchemas(src, tgt))).toContain("alter-view");
  });

  it("emits nothing when view definitions are identical", () => {
    const def = "CREATE VIEW `v` AS SELECT 1";
    const snap = snapshot([], [["v", def]]);
    expect(diffSchemas(snap, snap)).toHaveLength(0);
  });
});

describe("diffSchemas — routines", () => {
  const proc = { name: "proc1", kind: "PROCEDURE" as const, definition: "CREATE PROCEDURE `proc1`() BEGIN END" };

  it("emits create-routine (non-destructive) for new routine", () => {
    const src = snapshot([], [], [["PROCEDURE.proc1", proc]]);
    const stmts = diffSchemas(src, snapshot());
    expect(kinds(stmts)).toContain("create-routine");
    expect(stmts[0].destructive).toBe(false);
  });

  it("emits create-routine (destructive=true) when routine definition changes", () => {
    const src = snapshot([], [], [["PROCEDURE.proc1", { ...proc, definition: "CREATE PROCEDURE `proc1`() BEGIN SELECT 2; END" }]]);
    const tgt = snapshot([], [], [["PROCEDURE.proc1", proc]]);
    const stmts = diffSchemas(src, tgt);
    expect(stmts[0].kind).toBe("create-routine");
    expect(stmts[0].destructive).toBe(true);
  });

  it("emits drop-routine for removed routine", () => {
    const tgt = snapshot([], [], [["PROCEDURE.proc1", proc]]);
    expect(kinds(diffSchemas(snapshot(), tgt))).toContain("drop-routine");
  });
});

describe("diffSchemas — triggers", () => {
  const trig = { name: "trg1", definition: "CREATE TRIGGER `trg1` AFTER INSERT ON `t` FOR EACH ROW BEGIN END" };

  it("emits create-trigger for new trigger", () => {
    const src = snapshot([], [], [], [["trg1", trig]]);
    expect(kinds(diffSchemas(src, snapshot()))).toContain("create-trigger");
  });

  it("emits drop-trigger for removed trigger", () => {
    const tgt = snapshot([], [], [], [["trg1", trig]]);
    expect(kinds(diffSchemas(snapshot(), tgt))).toContain("drop-trigger");
  });
});
