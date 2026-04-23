import { ipcMain, dialog } from "electron";
import { homedir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import {
  Vault, openConnection, buildPlan, applyPlan, fullOverwrite, dumpToFile,
  k8s as k8sNs, docker as dockerNs,
  type Profile, type SyncPlan,
} from "@db-mirror/core";
import { IPC } from "../shared/ipc-channels.js";

const defaultVaultPath = (): string =>
  process.env.DB_MIRROR_VAULT ?? join(homedir(), ".config", "db-mirror", "vault.db-mirror");

let currentVault: Vault | null = null;

function requireVault(): Vault {
  if (!currentVault) throw new Error("Vault is locked");
  return currentVault;
}

function safeParam(v: unknown): unknown {
  if (v === null || v === undefined) return null;
  if (typeof v === "bigint") return Number(v);
  if (Buffer.isBuffer(v)) return v.toString("hex");
  if (v instanceof Date) return v.toISOString();
  if (typeof v === "object") return JSON.stringify(v); // JSON columns
  return v;
}

function safePlan(plan: import("@db-mirror/core").SyncPlan): import("@db-mirror/core").SyncPlan {
  return {
    sourceDatabase: plan.sourceDatabase,
    targetDatabase: plan.targetDatabase,
    createdAt: plan.createdAt,
    schema: plan.schema.map((s) => ({
      kind: s.kind,
      object: String(s.object),
      sql: String(s.sql),
      destructive: Boolean(s.destructive),
    })),
    data: plan.data.map((s) => ({
      kind: s.kind,
      table: String(s.table),
      sql: String(s.sql),
      params: s.params.map(safeParam),
      destructive: Boolean(s.destructive),
    })),
  };
}

export function registerIpcHandlers(): void {
  ipcMain.handle(IPC.DefaultVaultPath, () => defaultVaultPath());

  ipcMain.handle(IPC.VaultExists, async (_e, path?: string) => Vault.exists(path ?? defaultVaultPath()));

  ipcMain.handle(IPC.VaultCreate, async (_e, args: { path?: string; passphrase: string }) => {
    currentVault = await Vault.create(args.path ?? defaultVaultPath(), args.passphrase);
    return true;
  });

  ipcMain.handle(IPC.VaultOpen, async (_e, args: { path?: string; passphrase: string }) => {
    currentVault = await Vault.open(args.path ?? defaultVaultPath(), args.passphrase);
    return true;
  });

  ipcMain.handle(IPC.VaultLock, async () => { currentVault = null; return true; });

  ipcMain.handle(IPC.VaultListProfiles, async () => requireVault().listProfiles());

  ipcMain.handle(IPC.VaultUpsertProfile, async (_e, p: Profile) => requireVault().upsertProfile(p));

  ipcMain.handle(IPC.VaultRemoveProfile, async (_e, id: string) => requireVault().removeProfile(id));

  ipcMain.handle(IPC.VaultDuplicateProfile, async (_e, id: string) => {
    const vault = requireVault();
    const original = vault.getProfile(id);
    if (!original) throw new Error("Profile not found");
    const copy: Profile = { ...original, id: randomUUID(), name: `${original.name} (copy)` };
    return vault.upsertProfile(copy);
  });

  // K8s
  ipcMain.handle(IPC.K8sContexts, async (_e, kubeconfig?: string) => new k8sNs.K8sClient(kubeconfig).listContexts());
  ipcMain.handle(IPC.K8sNamespaces, async (_e, args: { kubeconfig?: string; context: string }) =>
    new k8sNs.K8sClient(args.kubeconfig).listNamespaces(args.context));
  ipcMain.handle(IPC.K8sPods, async (_e, args: { kubeconfig?: string; context: string; namespace: string }) =>
    new k8sNs.K8sClient(args.kubeconfig).listPods(args.namespace, args.context));
  ipcMain.handle(IPC.K8sServices, async (_e, args: { kubeconfig?: string; context: string; namespace: string }) =>
    new k8sNs.K8sClient(args.kubeconfig).listServices(args.namespace, args.context));
  ipcMain.handle(IPC.K8sSecrets, async (_e, args: { kubeconfig?: string; context: string; namespace: string }) =>
    new k8sNs.K8sClient(args.kubeconfig).listSecrets(args.namespace, args.context));
  ipcMain.handle(IPC.K8sSecretKeys, async (_e, args: { kubeconfig?: string; context: string; namespace: string; secretName: string }) =>
    new k8sNs.K8sClient(args.kubeconfig).getSecretKeys(args.namespace, args.secretName, args.context));

  ipcMain.handle(IPC.DockerContainers, async () => dockerNs.listMysqlContainers());
  ipcMain.handle(IPC.DockerContainerEnvs, async (_e, containerId: string) => dockerNs.getContainerEnvs(containerId));

  // List databases from a saved profile (by ID)
  ipcMain.handle(IPC.ListDatabases, async (_e, profileId: string) => {
    const profile = requireVault().getProfile(profileId);
    if (!profile) throw new Error("Profil nicht gefunden");
    const conn = await openConnection(profile);
    try {
      const [rows] = await conn.pool.query(
        "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME"
      ) as [Record<string, unknown>[], unknown];
      return rows.map((r) => r.SCHEMA_NAME as string);
    } finally {
      await conn.close();
    }
  });

  // List databases from a draft/unsaved profile object (for form pre-fill and SSH testing)
  ipcMain.handle(IPC.ListDatabasesFromProfile, async (_e, profile: Profile) => {
    const conn = await openConnection(profile);
    try {
      const [rows] = await conn.pool.query(
        "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME"
      ) as [Record<string, unknown>[], unknown];
      return rows.map((r) => r.SCHEMA_NAME as string);
    } finally {
      await conn.close();
    }
  });

  // Mirror ops
  ipcMain.handle(IPC.Diff, async (e, args: {
    sourceProfileId: string; targetProfileId: string;
    sourceDb?: string; targetDb?: string;
    schema: boolean; data: boolean;
    include?: string[]; exclude?: string[]; ignoreColumns?: string[];
  }) => {
    const v = requireVault();
    const sp = v.getProfile(args.sourceProfileId)!;
    const tp = v.getProfile(args.targetProfileId)!;
    const src = await openConnection(sp);
    const tgt = await openConnection(tp);
    const push = (msg: string) => e.sender.send(IPC.DiffProgress, msg);
    try {
      const srcDb = args.sourceDb || sp.database;
      const tgtDb = args.targetDb || tp.database;
      if (!srcDb) throw new Error("Kein Datenbankname für die Quelle angegeben");
      if (!tgtDb) throw new Error("Kein Datenbankname für das Ziel angegeben");
      push(`[diff] src: ${src.label} db=${srcDb}`);
      push(`[diff] tgt: ${tgt.label} db=${tgtDb}`);
      const plan = await buildPlan(src.pool, tgt.pool, srcDb, tgtDb, {
        mode: { schema: args.schema, data: args.data },
        filter: { include: args.include ?? [], exclude: args.exclude ?? [], ignoreColumns: args.ignoreColumns },
        onProgress: push,
        tableTimeout: 120_000,
        queryTimeout: 30_000,
      });
      push(`✓ Fertig — ${plan.schema.length} Schema-, ${plan.data.length} Daten-Statement(s)`);
      return safePlan(plan);
    } finally {
      await src.close(); await tgt.close();
    }
  });

  ipcMain.handle(IPC.Apply, async (e, args: { targetProfileId: string; plan: SyncPlan; dryRun?: boolean }) => {
    const v = requireVault();
    const tp = v.getProfile(args.targetProfileId)!;
    const conn = await openConnection(tp);
    const push = (msg: string) => e.sender.send(IPC.ApplyProgress, msg);
    push(`[apply] connection: ${conn.label}`);
    try {
      const result = await applyPlan(conn.pool, args.plan, { dryRun: args.dryRun, continueOnError: true, onProgress: push });
      // Post-apply verification: sample one changed data table and one schema column
      if (!args.dryRun && args.plan.data.length) {
        try {
          const sampleTable = args.plan.data[0]?.table;
          if (sampleTable) {
            const [[row]] = await conn.pool.query(`SELECT COUNT(*) AS cnt FROM \`${args.plan.targetDatabase}\`.\`${sampleTable}\``) as any;
            push(`[verify] ${sampleTable} row count after apply: ${row?.cnt}`);
          }
          // Check a schema column collation to verify DDL persisted
          if (args.plan.schema.length) {
            const sampleObj = args.plan.schema[0]?.object;
            const [[col]] = await conn.pool.query(
              `SELECT COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? LIMIT 1`,
              [args.plan.targetDatabase, sampleObj]
            ) as any;
            push(`[verify] ${sampleObj} first col collation: ${col?.COLLATION_NAME ?? 'null'}`);
          }
        } catch (verr) {
          push(`[verify] error: ${(verr as Error).message}`);
        }
      }
      return result;
    } finally { await conn.close(); }
  });

  ipcMain.handle(IPC.Overwrite, async (_e, args: { sourceProfileId: string; targetProfileId: string; tables?: string[] }) => {
    const v = requireVault();
    const sp = v.getProfile(args.sourceProfileId)!;
    const tp = v.getProfile(args.targetProfileId)!;
    await fullOverwrite(sp, tp, { tables: args.tables });
    return true;
  });

  ipcMain.handle(IPC.DumpToFile, async (e, args: { profileId: string; database: string }) => {
    const result = await dialog.showOpenDialog({ properties: ["openDirectory", "createDirectory"], title: "Choose destination folder" });
    if (result.canceled || !result.filePaths[0]) return null;
    const outputDir = result.filePaths[0];

    const profile = requireVault().getProfile(args.profileId);
    if (!profile) throw new Error("Profile not found");

    const safeName = profile.name.replace(/[^a-z0-9_-]/gi, "_");
    const timestamp = new Date().toISOString().slice(0, 10);
    const outputPath = join(outputDir, `${safeName}_${args.database}_${timestamp}.sql`);

    const push = (msg: string) => e.sender.send(IPC.DumpProgress, msg);
    push(`Starting dump of ${args.database} from ${profile.name}…`);

    await dumpToFile(profile, args.database, outputPath, {}, push);
    push(`✓ Dump saved to ${outputPath}`);
    return outputPath;
  });
}
