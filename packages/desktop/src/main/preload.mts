import { contextBridge, ipcRenderer } from "electron";
import { IPC } from "../shared/ipc-channels.js";

const ALLOWED_PUSH_CHANNELS = [
  IPC.DiffProgress,
  IPC.DumpProgress,
  IPC.ApplyProgress,
  IPC.UpdateAvailable,
  IPC.UpdateDownloadProgress,
  IPC.UpdateDownloaded,
  IPC.UpdateError,
] as const;
type PushChannel = (typeof ALLOWED_PUSH_CHANNELS)[number];

const ALLOWED_SEND_CHANNELS = [
  IPC.WindowClose,
  IPC.WindowMinimize,
  IPC.WindowMaximize,
] as const;
type SendChannel = (typeof ALLOWED_SEND_CHANNELS)[number];

const api = {
  invoke: <T = unknown,>(channel: string, payload?: unknown): Promise<T> =>
    ipcRenderer.invoke(channel, payload) as Promise<T>,
  send: (channel: SendChannel) => ipcRenderer.send(channel),
  on: (channel: PushChannel, listener: (payload: unknown) => void) => {
    const wrapped = (_event: Electron.IpcRendererEvent, payload: unknown) => listener(payload);
    ipcRenderer.on(channel, wrapped);
    return wrapped;
  },
  off: (channel: PushChannel, wrapped: (...args: unknown[]) => void) => {
    ipcRenderer.off(channel, wrapped);
  },
  channels: IPC,
};

contextBridge.exposeInMainWorld("dbMirror", api);

export type DbMirrorApi = typeof api;
declare global {
  interface Window { dbMirror: DbMirrorApi }
}
