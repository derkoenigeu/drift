import { app, BrowserWindow } from "electron";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { registerIpcHandlers } from "./ipc.js";
import { initUpdater } from "./updater.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

function createWindow(): void {
  const win = new BrowserWindow({
    width: 1280,
    height: 820,
    frame: false,
    webPreferences: {
      preload: join(__dirname, "preload.mjs"),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });

  const devUrl = process.env.VITE_DEV_SERVER_URL;
  if (devUrl) win.loadURL(devUrl);
  else win.loadFile(join(__dirname, "..", "renderer", "index.html"));

  if (!app.isPackaged && !process.env.PLAYWRIGHT_TEST) win.webContents.openDevTools({ mode: "detach" });
  win.webContents.on("render-process-gone", (_e, details) => {
    console.error("[renderer gone]", details);
  });
  win.webContents.on("console-message", (_e, level, msg, line, src) => {
    console.error(`[renderer console ${level}] ${src}:${line} ${msg}`);
  });
  win.webContents.on("did-fail-load", (_e, code, desc, url) => {
    console.error(`[did-fail-load] ${code} ${desc} url=${url}`);
  });

  win.webContents.once("did-finish-load", () => {
    if (app.isPackaged) initUpdater(win);
  });
}

app.whenReady().then(() => {
  registerIpcHandlers();
  createWindow();
  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});
