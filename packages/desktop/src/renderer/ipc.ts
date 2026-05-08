import { IPC } from "../shared/ipc-channels.js";
import type { docker as dockerTypes } from "@db-mirror/core";

export type UpdateAvailableInfo  = { version: string; releaseNotes: string };
export type UpdateProgressInfo   = { percent: number; bytesPerSecond: number; transferred: number; total: number };
export type UpdateDownloadedInfo = { version: string };

const api = () => (window as unknown as { dbMirror: {
  invoke: <T>(c: string, p?: unknown) => Promise<T>;
  send: (channel: string) => void;
  on: (channel: string, cb: (payload: unknown) => void) => (...args: unknown[]) => void;
  off: (channel: string, wrapped: (...args: unknown[]) => void) => void;
} }).dbMirror;

export const rpc = {
  vaultExists: (path?: string) => api().invoke<boolean>(IPC.VaultExists, path),
  vaultCreate: (args: { path?: string; passphrase: string }) => api().invoke<boolean>(IPC.VaultCreate, args),
  vaultOpen: (args: { path?: string; passphrase: string }) => api().invoke<boolean>(IPC.VaultOpen, args),
  vaultLock: () => api().invoke<boolean>(IPC.VaultLock),
  listProfiles: () => api().invoke<readonly import("@db-mirror/core").Profile[]>(IPC.VaultListProfiles),
  upsertProfile: (p: import("@db-mirror/core").Profile) =>
    api().invoke<import("@db-mirror/core").Profile>(IPC.VaultUpsertProfile, p),
  removeProfile: (id: string) => api().invoke<void>(IPC.VaultRemoveProfile, id),
  duplicateProfile: (id: string) => api().invoke<import("@db-mirror/core").Profile>(IPC.VaultDuplicateProfile, id),
  k8sContexts: (kubeconfig?: string) => api().invoke<string[]>(IPC.K8sContexts, kubeconfig),
  k8sNamespaces: (args: { context: string; kubeconfig?: string }) => api().invoke<string[]>(IPC.K8sNamespaces, args),
  k8sPods: (args: { context: string; namespace: string; kubeconfig?: string }) =>
    api().invoke<string[]>(IPC.K8sPods, args),
  listDatabases: (profileId: string) => api().invoke<string[]>(IPC.ListDatabases, profileId),
  listDatabasesFromProfile: (profile: import("@db-mirror/core").Profile) =>
    api().invoke<string[]>(IPC.ListDatabasesFromProfile, profile),
  k8sSecrets: (args: { context: string; namespace: string; kubeconfig?: string }) =>
    api().invoke<string[]>(IPC.K8sSecrets, args),
  k8sSecretKeys: (args: { context: string; namespace: string; secretName: string; kubeconfig?: string }) =>
    api().invoke<string[]>(IPC.K8sSecretKeys, args),
  dockerContainers: () =>
    api().invoke<dockerTypes.DockerContainer[]>(IPC.DockerContainers),
  dockerContainerEnvs: (containerId: string) =>
    api().invoke<Record<string, string>>(IPC.DockerContainerEnvs, containerId),
  diff: (args: {
    sourceProfileId: string; targetProfileId: string;
    sourceDb?: string; targetDb?: string;
    schema: boolean; data: boolean;
    include?: string[]; exclude?: string[]; ignoreColumns?: string[];
  }) => api().invoke<import("@db-mirror/core").SyncPlan>(IPC.Diff, args),
  apply: (args: { targetProfileId: string; plan: import("@db-mirror/core").SyncPlan; dryRun?: boolean }) =>
    api().invoke<import("@db-mirror/core").ApplyResult>(IPC.Apply, args),
  overwrite: (args: { sourceProfileId: string; targetProfileId: string; tables?: string[] }) =>
    api().invoke<boolean>(IPC.Overwrite, args),
  dumpToFile: (args: { profileId: string; database: string }) =>
    api().invoke<string | null>(IPC.DumpToFile, args),
  onDiffProgress: (cb: (msg: string) => void) => {
    const wrapped = api().on(IPC.DiffProgress, (payload) => cb(payload as string));
    return () => api().off(IPC.DiffProgress, wrapped);
  },
  onApplyProgress: (cb: (msg: string) => void) => {
    const wrapped = api().on(IPC.ApplyProgress, (payload) => cb(payload as string));
    return () => api().off(IPC.ApplyProgress, wrapped);
  },
  onDumpProgress: (cb: (msg: string) => void) => {
    const wrapped = api().on(IPC.DumpProgress, (payload) => cb(payload as string));
    return () => api().off(IPC.DumpProgress, wrapped);
  },
  onUpdateAvailable: (cb: (info: UpdateAvailableInfo) => void) => {
    const wrapped = api().on(IPC.UpdateAvailable, (p) => cb(p as UpdateAvailableInfo));
    return () => api().off(IPC.UpdateAvailable, wrapped);
  },
  onUpdateDownloadProgress: (cb: (info: UpdateProgressInfo) => void) => {
    const wrapped = api().on(IPC.UpdateDownloadProgress, (p) => cb(p as UpdateProgressInfo));
    return () => api().off(IPC.UpdateDownloadProgress, wrapped);
  },
  onUpdateDownloaded: (cb: (info: UpdateDownloadedInfo) => void) => {
    const wrapped = api().on(IPC.UpdateDownloaded, (p) => cb(p as UpdateDownloadedInfo));
    return () => api().off(IPC.UpdateDownloaded, wrapped);
  },
  onUpdateError: (cb: (info: { message: string }) => void) => {
    const wrapped = api().on(IPC.UpdateError, (p) => cb(p as { message: string }));
    return () => api().off(IPC.UpdateError, wrapped);
  },
  installUpdate: () => api().invoke<void>(IPC.InstallUpdate),
  appVersion: () => api().invoke<string>(IPC.AppVersion),
  windowClose: () => api().send(IPC.WindowClose),
  windowMinimize: () => api().send(IPC.WindowMinimize),
  windowMaximize: () => api().send(IPC.WindowMaximize),
};
