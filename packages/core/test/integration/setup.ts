/**
 * Integration test helpers.
 *
 * Requires a running MySQL/MariaDB instance. Configure via env vars:
 *   MYSQL_HOST     (default: 127.0.0.1)
 *   MYSQL_PORT     (default: 3306)
 *   MYSQL_USER     (default: root)
 *   MYSQL_PASSWORD (default: testpass)
 *   MYSQL_DATABASE (default: test_source)
 */

import mysql from "mysql2/promise";
import type { Pool } from "mysql2/promise";

export interface TestDbConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

export function getTestConfig(): TestDbConfig {
  return {
    host: process.env.MYSQL_HOST ?? "127.0.0.1",
    port: Number(process.env.MYSQL_PORT ?? 3306),
    user: process.env.MYSQL_USER ?? "root",
    password: process.env.MYSQL_PASSWORD ?? "testpass",
    database: process.env.MYSQL_DATABASE ?? "test_source",
  };
}

/** Returns true when we can be confident a MySQL server is available to test against. */
export function mysqlAvailable(): boolean {
  return Boolean(
    process.env.MYSQL_HOST ||
    process.env.CI ||
    process.env.INTEGRATION_TESTS
  );
}

/**
 * Create a connection pool for the given database (or no database if omitted,
 * useful for CREATE/DROP DATABASE statements).
 */
export function createTestPool(database?: string): Pool {
  const cfg = getTestConfig();
  return mysql.createPool({
    host: cfg.host,
    port: cfg.port,
    user: cfg.user,
    password: cfg.password,
    database: database,
    waitForConnections: true,
    connectionLimit: 5,
    queueLimit: 0,
    multipleStatements: false,
  });
}

/** Create an isolated test database with a unique name and return its name. */
export async function createTestDb(adminPool: Pool, dbName: string): Promise<void> {
  await adminPool.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``);
}

/** Drop a test database. */
export async function dropTestDb(adminPool: Pool, dbName: string): Promise<void> {
  await adminPool.query(`DROP DATABASE IF EXISTS \`${dbName}\``);
}

/** Generate a unique database name for a test run. */
export function uniqueDbName(prefix: string): string {
  const suffix = Math.random().toString(36).slice(2, 8);
  return `${prefix}_${suffix}`;
}

/**
 * Attempt a lightweight ping to confirm the MySQL server is reachable.
 * Returns true on success, false on any connection error.
 */
export async function pingMySQL(): Promise<boolean> {
  const pool = createTestPool();
  try {
    await pool.query("SELECT 1");
    return true;
  } catch {
    return false;
  } finally {
    await pool.end();
  }
}
