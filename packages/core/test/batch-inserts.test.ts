import { describe, it, expect } from "vitest";
import { batchInserts } from "../src/sync-plan/plan.js";
import type { DataStatement } from "../src/data-diff/data-diff.js";

function insert(table: string, params: unknown[]): DataStatement {
  const cols = params.map((_, i) => `\`col${i}\``).join(",");
  const placeholders = params.map(() => "?").join(",");
  return {
    kind: "insert",
    table,
    sql: `INSERT INTO \`tgt\`.\`${table}\` (${cols}) VALUES (${placeholders})`,
    params,
    destructive: false,
  };
}

function del(table: string): DataStatement {
  return { kind: "delete", table, sql: `DELETE FROM \`tgt\`.\`${table}\` WHERE \`id\`=?`, params: [1], destructive: true };
}

describe("batchInserts", () => {
  it("returns empty array unchanged", () => {
    expect(batchInserts([])).toEqual([]);
  });

  it("leaves a single INSERT untouched", () => {
    const stmts = [insert("t", [1, "a"])];
    expect(batchInserts(stmts)).toHaveLength(1);
    expect(batchInserts(stmts)[0].sql).toEqual(stmts[0].sql);
  });

  it("merges two consecutive INSERTs for the same table into one", () => {
    const stmts = [insert("t", [1, "a"]), insert("t", [2, "b"])];
    const result = batchInserts(stmts);
    expect(result).toHaveLength(1);
    expect(result[0].sql).toContain("VALUES (?,?),(?,?)");
    expect(result[0].params).toEqual([1, "a", 2, "b"]);
  });

  it("does NOT merge INSERTs for different tables", () => {
    const stmts = [insert("a", [1]), insert("b", [2])];
    expect(batchInserts(stmts)).toHaveLength(2);
  });

  it("does NOT merge across a non-INSERT statement", () => {
    const stmts = [insert("t", [1]), del("t"), insert("t", [2])];
    const result = batchInserts(stmts);
    expect(result).toHaveLength(3);
    expect(result[0].kind).toBe("insert");
    expect(result[1].kind).toBe("delete");
    expect(result[2].kind).toBe("insert");
  });

  it("preserves DELETE and UPDATE statements unchanged", () => {
    const stmts = [del("t"), del("t")];
    const result = batchInserts(stmts);
    expect(result).toHaveLength(2);
    expect(result.every((s) => s.kind === "delete")).toBe(true);
  });

  it("merges up to 500 rows per batch and starts a new batch after", () => {
    const stmts = Array.from({ length: 501 }, (_, i) => insert("t", [i]));
    const result = batchInserts(stmts);
    expect(result).toHaveLength(2);
    expect(result[0].params).toHaveLength(500);
    expect(result[1].params).toHaveLength(1);
  });

  it("sets destructive=false on merged INSERT batch", () => {
    const stmts = [insert("t", [1]), insert("t", [2])];
    expect(batchInserts(stmts)[0].destructive).toBe(false);
  });
});
