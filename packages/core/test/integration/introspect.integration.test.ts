import { describe, it, expect, beforeAll, afterAll } from "vitest";
import type { Pool } from "mysql2/promise";
import { introspect } from "../../src/schema-diff/introspect.js";
import {
  createTestPool,
  createTestDb,
  dropTestDb,
  uniqueDbName,
  pingMySQL,
} from "./setup.js";

// ---------------------------------------------------------------------------
// Skip the entire suite if no MySQL is reachable
// ---------------------------------------------------------------------------

let skip = false;
let adminPool: Pool;
let testDb: string;
let dbPool: Pool;

beforeAll(async () => {
  const alive = await pingMySQL();
  if (!alive) {
    skip = true;
    return;
  }

  testDb = uniqueDbName("test_introspect");
  adminPool = createTestPool();
  await createTestDb(adminPool, testDb);

  dbPool = createTestPool(testDb);

  // ---- seed the database ----
  await dbPool.query(`
    CREATE TABLE users (
      id   INT          NOT NULL AUTO_INCREMENT,
      name VARCHAR(100) NOT NULL,
      email VARCHAR(255) NULL,
      age  INT          NULL,
      PRIMARY KEY (id),
      UNIQUE INDEX uq_email (email),
      INDEX idx_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);

  await dbPool.query(`
    CREATE TABLE orders (
      id         INT  NOT NULL AUTO_INCREMENT,
      user_id    INT  NOT NULL,
      amount     DECIMAL(10,2) NOT NULL DEFAULT '0.00',
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      INDEX idx_user (user_id)
    ) ENGINE=InnoDB
  `);

  await dbPool.query(`
    CREATE VIEW v_user_orders AS
      SELECT u.id AS user_id, u.name, o.amount
      FROM users u
      JOIN orders o ON o.user_id = u.id
  `);

  await dbPool.query(`
    CREATE PROCEDURE sp_get_user(IN p_id INT)
    BEGIN
      SELECT * FROM users WHERE id = p_id;
    END
  `);

  // Triggers require a table — attach to users
  await dbPool.query(`
    CREATE TRIGGER trg_before_user_insert
    BEFORE INSERT ON users
    FOR EACH ROW
    BEGIN
      SET NEW.name = TRIM(NEW.name);
    END
  `);
}, 30_000);

afterAll(async () => {
  if (skip) return;
  await dbPool?.end();
  await dropTestDb(adminPool, testDb);
  await adminPool?.end();
}, 15_000);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("introspect() — tables", () => {
  it("skips if MySQL is unavailable", () => {
    if (skip) return;
  });

  it("returns the correct database name", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    expect(snap.database).toBe(testDb);
  });

  it("detects both tables", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    expect(snap.tables.has("users")).toBe(true);
    expect(snap.tables.has("orders")).toBe(true);
  });

  it("reads column info for users table", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    const users = snap.tables.get("users")!;
    expect(users).toBeDefined();

    const colNames = users.columns.map((c) => c.name);
    expect(colNames).toContain("id");
    expect(colNames).toContain("name");
    expect(colNames).toContain("email");
    expect(colNames).toContain("age");
  });

  it("correctly marks nullable / not-null columns", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    const users = snap.tables.get("users")!;
    const idCol = users.columns.find((c) => c.name === "id")!;
    const emailCol = users.columns.find((c) => c.name === "email")!;

    expect(idCol.nullable).toBe(false);
    expect(emailCol.nullable).toBe(true);
  });

  it("detects the PRIMARY KEY", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    const users = snap.tables.get("users")!;
    expect(users.primaryKey).toEqual(["id"]);
  });

  it("detects secondary indexes including UNIQUE", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    const users = snap.tables.get("users")!;
    const idxNames = users.indexes.map((i) => i.name);
    expect(idxNames).toContain("uq_email");
    expect(idxNames).toContain("idx_name");

    const uqEmail = users.indexes.find((i) => i.name === "uq_email")!;
    expect(uqEmail.unique).toBe(true);
    expect(uqEmail.columns).toEqual(["email"]);

    const idxName = users.indexes.find((i) => i.name === "idx_name")!;
    expect(idxName.unique).toBe(false);
  });

  it("reads the createStatement for each table", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    const users = snap.tables.get("users")!;
    expect(users.createStatement).toMatch(/CREATE TABLE/i);
    expect(users.createStatement).toContain("users");
  });

  it("does not include views in tables map", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    expect(snap.tables.has("v_user_orders")).toBe(false);
  });

  it("reads engine and collation", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    const users = snap.tables.get("users")!;
    expect(users.engine).toBe("InnoDB");
    expect(users.collation).toMatch(/utf8mb4/i);
  });
});

describe("introspect() — views", () => {
  it("detects the view", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    expect(snap.views.has("v_user_orders")).toBe(true);
  });

  it("stores a non-empty view definition", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    const view = snap.views.get("v_user_orders")!;
    expect(view.definition).toMatch(/CREATE.*VIEW/i);
  });
});

describe("introspect() — routines", () => {
  it("detects the stored procedure", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    expect(snap.routines.has("PROCEDURE:sp_get_user")).toBe(true);
  });

  it("stores routine kind and definition", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    const proc = snap.routines.get("PROCEDURE:sp_get_user")!;
    expect(proc.kind).toBe("PROCEDURE");
    expect(proc.definition).toMatch(/sp_get_user/i);
  });
});

describe("introspect() — triggers", () => {
  it("detects the trigger", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    expect(snap.triggers.has("trg_before_user_insert")).toBe(true);
  });

  it("stores a non-empty trigger definition", async () => {
    if (skip) return;
    const snap = await introspect(dbPool, testDb);
    const trg = snap.triggers.get("trg_before_user_insert")!;
    expect(trg.definition).toMatch(/TRIGGER/i);
  });
});

describe("introspect() — empty database", () => {
  let emptyDb: string;
  let emptyPool: Pool;

  beforeAll(async () => {
    if (skip) return;
    emptyDb = uniqueDbName("test_empty");
    await createTestDb(adminPool, emptyDb);
    emptyPool = createTestPool(emptyDb);
  }, 10_000);

  afterAll(async () => {
    if (skip) return;
    await emptyPool?.end();
    await dropTestDb(adminPool, emptyDb);
  }, 10_000);

  it("returns empty maps for an empty database", async () => {
    if (skip) return;
    const snap = await introspect(emptyPool, emptyDb);
    expect(snap.tables.size).toBe(0);
    expect(snap.views.size).toBe(0);
    expect(snap.routines.size).toBe(0);
    expect(snap.triggers.size).toBe(0);
  });
});
