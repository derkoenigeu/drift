import type { Pool, PoolConnection, RowDataPacket } from "mysql2/promise";
import type { SchemaStatement } from "../schema-diff/diff.js";
import { diffSchemas } from "../schema-diff/diff.js";
import { introspect } from "../schema-diff/introspect.js";
import { diffTableData, type DataStatement } from "../data-diff/data-diff.js";
import { applyTableFilter } from "../filters/filters.js";
import type { TableFilter } from "../vault/types.js";

export interface SyncPlan {
  sourceDatabase: string;
  targetDatabase: string;
  schema: SchemaStatement[];
  data: DataStatement[];
  createdAt: string;
}

export interface BuildPlanOptions {
  mode: { schema: boolean; data: boolean };
  filter?: TableFilter;
  onProgress?: (msg: string) => void;
  /** Per-table data diff timeout in ms (default: 120 000). */
  tableTimeout?: number;
  /** Per-query mysql2 timeout in ms (default: 30 000). */
  queryTimeout?: number;
}

export async function buildPlan(
  sourcePool: Pool,
  targetPool: Pool,
  sourceDatabase: string,
  targetDatabase: string,
  opts: BuildPlanOptions,
): Promise<SyncPlan> {
  const plan: SyncPlan = {
    sourceDatabase,
    targetDatabase,
    schema: [],
    data: [],
    createdAt: new Date().toISOString(),
  };

  const log = opts.onProgress ?? (() => {});
  const tableTimeout = opts.tableTimeout ?? 120_000;
  const queryTimeout = opts.queryTimeout ?? 30_000;

  // Healthcheck both connections before doing any work
  await Promise.all([
    sourcePool.query({ sql: "SELECT 1", timeout: 10_000 }).catch((e) => {
      throw new Error(`Quell-Datenbank nicht erreichbar: ${(e as Error).message}`);
    }),
    targetPool.query({ sql: "SELECT 1", timeout: 10_000 }).catch((e) => {
      throw new Error(`Ziel-Datenbank nicht erreichbar: ${(e as Error).message}`);
    }),
  ]);

  log(`Verbinde und lese Schema: ${sourceDatabase} …`);
  const srcSnap = await introspect(sourcePool, sourceDatabase);
  log(`Schema gelesen: ${srcSnap.tables.size} Tabellen in ${sourceDatabase}`);

  log(`Verbinde und lese Schema: ${targetDatabase} …`);
  const tgtSnap = await introspect(targetPool, targetDatabase);
  log(`Schema gelesen: ${tgtSnap.tables.size} Tabellen in ${targetDatabase}`);

  if (opts.mode.schema) {
    log("Berechne Schema-Diff …");
    const all = diffSchemas(srcSnap, tgtSnap, log);
    const allowed = new Set(applyTableFilter(Array.from(srcSnap.tables.keys()).concat(Array.from(tgtSnap.tables.keys())), opts.filter));
    plan.schema = all.filter((s) => {
      if (s.kind.endsWith("-table")) return allowed.has(s.object);
      return true;
    });
    log(`Schema-Diff: ${plan.schema.length} Statement(s)`);
  }

  if (opts.mode.data) {
    const commonTables = Array.from(srcSnap.tables.keys()).filter((t) => tgtSnap.tables.has(t));
    const allowed = applyTableFilter(commonTables, opts.filter);
    log(`Daten-Diff: ${allowed.length} Tabelle(n) werden verglichen …`);
    for (const t of allowed) {
      const src = srcSnap.tables.get(t)!;
      if (src.primaryKey.length === 0) {
        log(`  ${t}: übersprungen (kein Primary Key)`);
        continue;
      }
      log(`  ${t}: vergleiche Zeilen …`);
      const tgt = tgtSnap.tables.get(t)!;
      const tgtColSet = new Set(tgt.columns.map((c) => c.name));
      const cols = src.columns.map((c) => c.name).filter((n) => tgtColSet.has(n));
      if (cols.length === 0 || !src.primaryKey.every((k) => tgtColSet.has(k))) {
        log(`  ${t}: übersprungen (Spalten-Schema zu unterschiedlich)`);
        continue;
      }
      let stmts: Awaited<ReturnType<typeof diffTableData>>;
      try {
        const timeoutMs = tableTimeout;
        stmts = await Promise.race([
          diffTableData(sourcePool, targetPool, {
            sourceDatabase,
            targetDatabase,
            table: t,
            primaryKey: src.primaryKey,
            columns: cols,
            ignoreColumns: opts.filter?.ignoreColumns,
            queryTimeout,
          }),
          new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error(`Timeout nach ${timeoutMs / 1000}s`)), timeoutMs),
          ),
        ]);
      } catch (e) {
        log(`  ${t}: übersprungen — ${(e as Error).message}`);
        continue;
      }
      for (const s of stmts) plan.data.push(s);
      log(`  ${t}: ${stmts.length} Änderung(en)`);
    }
  }

  return plan;
}

export interface ApplyOptions {
  dryRun?: boolean;
  /** Continue on per-statement errors instead of rolling back. */
  continueOnError?: boolean;
  onProgress?: (msg: string) => void;
  /** Per-statement query timeout in ms (default: 60 000). */
  queryTimeout?: number;
}

export interface ApplyResult {
  executed: number;
  skipped: number;
  errors: { sql: string; error: string }[];
}

interface FkInfo {
  table: string;
  constraint: string;
  columns: string[];
  refTable: string;
  refColumns: string[];
  onDelete: string;
  onUpdate: string;
}

async function gatherFks(conn: PoolConnection, database: string): Promise<FkInfo[]> {
  const [rows] = await conn.query<RowDataPacket[]>(
    `SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME,
            kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME,
            COALESCE(rc.DELETE_RULE, 'RESTRICT') AS DELETE_RULE,
            COALESCE(rc.UPDATE_RULE, 'RESTRICT') AS UPDATE_RULE
     FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
     LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
       ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
     WHERE kcu.TABLE_SCHEMA = ? AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
     ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`,
    [database],
  );
  const map = new Map<string, FkInfo>();
  for (const r of rows as any[]) {
    const key = `${r.TABLE_NAME}\0${r.CONSTRAINT_NAME}`;
    if (!map.has(key)) {
      map.set(key, { table: r.TABLE_NAME, constraint: r.CONSTRAINT_NAME, columns: [], refTable: r.REFERENCED_TABLE_NAME, refColumns: [], onDelete: r.DELETE_RULE, onUpdate: r.UPDATE_RULE });
    }
    const fk = map.get(key)!;
    fk.columns.push(r.COLUMN_NAME);
    fk.refColumns.push(r.REFERENCED_COLUMN_NAME);
  }
  return Array.from(map.values());
}

function fkToSql(fk: FkInfo): string {
  const cols = fk.columns.map((c) => `\`${c}\``).join(", ");
  const refCols = fk.refColumns.map((c) => `\`${c}\``).join(", ");
  return `ALTER TABLE \`${fk.table}\` ADD CONSTRAINT \`${fk.constraint}\` FOREIGN KEY (${cols}) REFERENCES \`${fk.refTable}\` (${refCols}) ON DELETE ${fk.onDelete} ON UPDATE ${fk.onUpdate}`;
}

const INSERT_BATCH_SIZE = 500;

/** Merges consecutive INSERT statements for the same table into multi-row INSERTs. */
export function batchInserts(data: DataStatement[]): DataStatement[] {
  const result: DataStatement[] = [];
  let i = 0;
  while (i < data.length) {
    const s = data[i];
    if (s.kind !== "insert") {
      result.push(s);
      i++;
      continue;
    }
    const batch: DataStatement[] = [s];
    while (
      batch.length < INSERT_BATCH_SIZE &&
      i + 1 < data.length &&
      data[i + 1].kind === "insert" &&
      data[i + 1].table === s.table
    ) {
      i++;
      batch.push(data[i]);
    }
    if (batch.length === 1) {
      result.push(s);
    } else {
      const placeholder = `(${Array(s.params.length).fill("?").join(",")})`;
      const valuesIdx = s.sql.toUpperCase().lastIndexOf(" VALUES ");
      result.push({
        kind: "insert",
        table: s.table,
        sql: `${s.sql.slice(0, valuesIdx)} VALUES ${batch.map(() => placeholder).join(",")}`,
        params: batch.flatMap((st) => st.params as unknown[]),
        destructive: false,
      });
    }
    i++;
  }
  return result;
}

export async function applyPlan(pool: Pool, plan: SyncPlan, opts: ApplyOptions = {}): Promise<ApplyResult> {
  const result: ApplyResult = { executed: 0, skipped: 0, errors: [] };
  const log = opts.onProgress ?? (() => {});
  const queryTimeout = opts.queryTimeout ?? 60_000;
  const total = plan.schema.length + plan.data.length;

  if (opts.dryRun) {
    log(`Dry-run: ${total} statement(s) would be executed`);
    result.skipped = total;
    return result;
  }

  log(`Connecting to target database \`${plan.targetDatabase}\`…`);
  const conn = await pool.getConnection();
  try {
    await conn.query(`USE \`${plan.targetDatabase}\``);
    log(`[apply] target database: ${plan.targetDatabase}`);
    await conn.query("SET autocommit=1");
    await conn.query("SET FOREIGN_KEY_CHECKS=0");
    try { await conn.query("SET check_constraint_checks=0"); } catch {} // MariaDB only

    // Gather all FK constraints from target so we can restore them after schema changes.
    // MariaDB (unlike MySQL 8) blocks ALTER TABLE on FK-constrained columns even with
    // FOREIGN_KEY_CHECKS=0, so we must drop them all upfront.
    let gatheredFks: FkInfo[] = [];
    try {
      gatheredFks = await gatherFks(conn, plan.targetDatabase);
      for (const fk of gatheredFks) {
        try { await conn.query({ sql: `ALTER TABLE \`${fk.table}\` DROP FOREIGN KEY \`${fk.constraint}\``, timeout: queryTimeout }); } catch {}
      }
      log(`[apply] dropped ${gatheredFks.length} FK constraint(s) — will restore after schema changes`);
    } catch (e) {
      log(`[apply] warning: FK pre-drop failed — ${(e as Error).message}`);
    }

    // Run ALTER TABLE before CREATE TABLE so that collation changes on existing tables
    // are applied before new tables try to create FKs referencing those columns.
    const schemaOrdered = [
      ...plan.schema.filter((s) => s.kind === "alter-table"),
      ...plan.schema.filter((s) => s.kind === "create-table"),
      ...plan.schema.filter((s) => s.kind !== "alter-table" && s.kind !== "create-table"),
    ];

    log(`Executing ${plan.schema.length} schema + ${plan.data.length} data statement(s)…`);

    for (const s of schemaOrdered) {
      try {
        await conn.query({ sql: s.sql, timeout: queryTimeout });
        result.executed++;
        log(`✓ [schema] ${s.kind} ${s.object}: ${s.sql.slice(0, 120).replace(/\n/g, " ")}`);
      } catch (e) {
        const msg = (e as Error).message;
        result.errors.push({ sql: s.sql, error: msg });
        log(`✗ [schema] ${s.kind} ${s.object}: ${msg}`);
        if (!opts.continueOnError) throw e;
      }
    }

    // Restore FK constraints for existing tables (new tables already have FKs from CREATE TABLE).
    if (gatheredFks.length) {
      let restored = 0;
      let skippedFks = 0;
      for (const fk of gatheredFks) {
        try {
          await conn.query({ sql: fkToSql(fk), timeout: queryTimeout });
          restored++;
        } catch {
          skippedFks++;
        }
      }
      log(`[apply] restored ${restored} FK constraint(s), ${skippedFks} skipped (table/column removed or incompatible)`);
    }
    let dataOk = 0;
    const dataErrors: string[] = [];
    const dataTotal = plan.data.length;
    const batched = batchInserts(plan.data);
    log(`[data] ${dataTotal.toLocaleString()} statement(s) → ${batched.length.toLocaleString()} batch(es) (INSERT batch size: ${INSERT_BATCH_SIZE})`);

    // Precompute per-table column count so we can track approximate row progress.
    const insertColCount = new Map<string, number>();
    for (const s of plan.data) {
      if (s.kind === "insert" && !insertColCount.has(s.table)) insertColCount.set(s.table, s.params.length);
    }

    let approxRows = 0;
    for (const s of batched) {
      const rowCount = s.kind === "insert" ? s.params.length / (insertColCount.get(s.table) ?? 1) : 1;
      if (approxRows > 0 && Math.floor(approxRows / 5_000) < Math.floor((approxRows + rowCount) / 5_000)) {
        log(`[data] ~${Math.round(approxRows).toLocaleString()} / ${dataTotal.toLocaleString()} (${Math.round(approxRows / dataTotal * 100)}%) …`);
      }
      try {
        await conn.query({ sql: s.sql, values: s.params, timeout: queryTimeout });
        result.executed++;
        dataOk++;
      } catch (e) {
        const msg = (e as Error).message;
        result.errors.push({ sql: s.sql, error: msg });
        dataErrors.push(`✗ [data] ${s.kind} ${s.table}: ${msg}`);
        if (!opts.continueOnError) throw e;
      }
      approxRows += rowCount;
    }
    if (plan.data.length) log(`✓ Data: ${dataOk} ok, ${dataErrors.length} error(s) out of ${plan.data.length} statement(s)`);
    if (dataErrors.length) {
      for (const line of dataErrors.slice(0, 10)) log(line);
      if (dataErrors.length > 10) log(`  … and ${dataErrors.length - 10} more data error(s)`);
    }

    await conn.query("COMMIT");
    await conn.query("SET FOREIGN_KEY_CHECKS=1");
    try { await conn.query("SET check_constraint_checks=1"); } catch {}
  } finally {
    conn.release();
  }
  log(`✓ Done — ${result.executed} executed, ${result.errors.length} error(s)`);
  return result;
}
