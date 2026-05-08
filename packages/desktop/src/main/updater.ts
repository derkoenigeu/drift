import pkg from "electron-updater";
const { autoUpdater } = pkg;
import type { BrowserWindow } from "electron";
import { IPC } from "../shared/ipc-channels.js";

export function initUpdater(win: BrowserWindow): void {
  autoUpdater.autoDownload = true;
  autoUpdater.autoInstallOnAppQuit = true;

  autoUpdater.on("update-available", (info) => {
    win.webContents.send(IPC.UpdateAvailable, {
      version: info.version,
      releaseNotes: typeof info.releaseNotes === "string"
        ? info.releaseNotes.slice(0, 300)
        : "",
    });
  });

  autoUpdater.on("download-progress", (progress) => {
    win.webContents.send(IPC.UpdateDownloadProgress, {
      percent: Math.round(progress.percent),
      bytesPerSecond: progress.bytesPerSecond,
      transferred: progress.transferred,
      total: progress.total,
    });
  });

  autoUpdater.on("update-downloaded", (info) => {
    win.webContents.send(IPC.UpdateDownloaded, { version: info.version });
  });

  autoUpdater.on("error", (err) => {
    console.warn("[updater] error:", err.message);
    if (!err.message.includes("net::") && !err.message.includes("ENOTFOUND")) {
      win.webContents.send(IPC.UpdateError, { message: err.message });
    }
  });

  setTimeout(() => autoUpdater.checkForUpdates().catch(() => {}), 3000);
  setInterval(() => autoUpdater.checkForUpdates().catch(() => {}), 5 * 60 * 1000);
}
