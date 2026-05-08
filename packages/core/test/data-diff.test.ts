import { describe, it, expect, vi } from "vitest";
import { diffTableData, type DataDiffOptions } from "../src/data-diff/data-diff.js";
import type { Pool } from "mysql2/promise";

function mockPool(rows: Record<string, unknown>[]): Pool {
  return {
    query: vi.fn().mockResolvedValue([rows, []]),
  } as unknown as Pool;
}

const baseOpts: DataDiffOptions = {
  sourceDatabase: "src",
  targetDatabase: "tgt",
  table: "users",
  primaryKey: ["id"],
  columns: ["id", "name", "email"],
};

describe("diffTableData", () => {
  it("produces INSERT for rows present in source but not target", async () => {
    const src = mockPool([{ id: 1, name: "Alice", email: "a@x.com" }]);
    const tgt = mockPool([]);
    const stmts = await diffTableData(src, tgt, baseOpts);
    expect(stmts).toHaveLength(1);
    expect(stmts[0].kind).toBe("insert");
    expect(stmts[0].destructive).toBe(false);
    expect(stmts[0].sql).toContain("INSERT INTO");
    expect(stmts[0].params).toEqual([1, "Alice", "a@x.com"]);
  });

  it("produces DELETE for rows present in target but not source", async () => {
    const src = mockPool([]);
    const tgt = mockPool([{ id: 99, name: "Ghost", email: "g@x.com" }]);
    const stmts = await diffTableData(src, tgt, baseOpts);
    expect(stmts).toHaveLength(1);
    expect(stmts[0].kind).toBe("delete");
    expect(stmts[0].destructive).toBe(true);
    expect(stmts[0].params).toEqual([99]);
  });

  it("produces UPDATE when row exists but content differs", async () => {
    const row = { id: 1, name: "Alice", email: "a@x.com" };
    const src = mockPool([{ ...row, name: "Alice Updated" }]);
    const tgt = mockPool([row]);
    const stmts = await diffTableData(src, tgt, baseOpts);
    expect(stmts).toHaveLength(1);
    expect(stmts[0].kind).toBe("update");
    expect(stmts[0].sql).toContain("UPDATE");
    expect(stmts[0].sql).toContain("WHERE");
  });

  it("emits no statements when source and target are identical", async () => {
    const rows = [{ id: 1, name: "Alice", email: "a@x.com" }];
    const src = mockPool(rows);
    const tgt = mockPool(rows);
    expect(await diffTableData(src, tgt, baseOpts)).toHaveLength(0);
  });

  it("respects ignoreColumns when comparing rows", async () => {
    const srcRows = [{ id: 1, name: "Alice", email: "a@x.com" }];
    const tgtRows = [{ id: 1, name: "Alice CHANGED", email: "a@x.com" }];
    const src = mockPool(srcRows);
    const tgt = mockPool(tgtRows);
    const stmts = await diffTableData(src, tgt, { ...baseOpts, ignoreColumns: ["name"] });
    expect(stmts).toHaveLength(0);
  });

  it("handles composite primary keys correctly", async () => {
    const opts: DataDiffOptions = {
      sourceDatabase: "src",
      targetDatabase: "tgt",
      table: "order_items",
      primaryKey: ["order_id", "product_id"],
      columns: ["order_id", "product_id", "qty"],
    };
    const src = mockPool([{ order_id: 1, product_id: 2, qty: 5 }]);
    const tgt = mockPool([]);
    const stmts = await diffTableData(src, tgt, opts);
    expect(stmts[0].kind).toBe("insert");
    expect(stmts[0].params).toEqual([1, 2, 5]);
  });

  it("handles null values in rows without crashing", async () => {
    const src = mockPool([{ id: 1, name: null, email: null }]);
    const tgt = mockPool([]);
    const stmts = await diffTableData(src, tgt, baseOpts);
    expect(stmts[0].kind).toBe("insert");
    expect(stmts[0].params).toContain(null);
  });

  it("handles multiple mixed changes in one pass", async () => {
    const srcRows = [
      { id: 1, name: "Alice", email: "a@x.com" },  // update (name changed)
      { id: 3, name: "Carol", email: "c@x.com" },  // insert
    ];
    const tgtRows = [
      { id: 1, name: "Alice OLD", email: "a@x.com" },
      { id: 2, name: "Bob",  email: "b@x.com" },   // delete
    ];
    const src = mockPool(srcRows);
    const tgt = mockPool(tgtRows);
    const stmts = await diffTableData(src, tgt, baseOpts);
    expect(stmts.map((s) => s.kind).sort()).toEqual(["delete", "insert", "update"]);
  });
});
