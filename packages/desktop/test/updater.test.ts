import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock electron-updater before importing the module under test
vi.mock("electron-updater", () => {
  const listeners: Record<string, ((...args: unknown[]) => void)[]> = {};
  const autoUpdater = {
    autoDownload: false,
    autoInstallOnAppQuit: false,
    on: vi.fn((event: string, cb: (...args: unknown[]) => void) => {
      listeners[event] ??= [];
      listeners[event].push(cb);
    }),
    checkForUpdates: vi.fn().mockResolvedValue(null),
    quitAndInstall: vi.fn(),
    _emit: (event: string, ...args: unknown[]) => {
      for (const cb of listeners[event] ?? []) cb(...args);
    },
    _reset: () => {
      for (const k of Object.keys(listeners)) delete listeners[k];
    },
  };
  return { autoUpdater, default: { autoUpdater } };
});

import { autoUpdater } from "electron-updater";
import { initUpdater } from "../src/main/updater.js";
import { IPC } from "../src/shared/ipc-channels.js";

const mockAU = autoUpdater as typeof autoUpdater & {
  _emit: (event: string, ...args: unknown[]) => void;
  _reset: () => void;
};

function makeMockWin() {
  return {
    webContents: { send: vi.fn() },
  };
}

describe("initUpdater", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockAU._reset();
  });

  it("sets autoDownload=true and autoInstallOnAppQuit=true", () => {
    const win = makeMockWin();
    initUpdater(win as never);
    expect(autoUpdater.autoDownload).toBe(true);
    expect(autoUpdater.autoInstallOnAppQuit).toBe(true);
  });

  it("registers event listeners on autoUpdater", () => {
    const win = makeMockWin();
    initUpdater(win as never);
    expect(autoUpdater.on).toHaveBeenCalledWith("update-available", expect.any(Function));
    expect(autoUpdater.on).toHaveBeenCalledWith("download-progress", expect.any(Function));
    expect(autoUpdater.on).toHaveBeenCalledWith("update-downloaded", expect.any(Function));
    expect(autoUpdater.on).toHaveBeenCalledWith("error", expect.any(Function));
  });

  it("sends UpdateAvailable IPC when update-available fires", () => {
    const win = makeMockWin();
    initUpdater(win as never);
    mockAU._emit("update-available", { version: "1.2.3", releaseNotes: "# Changelog\nFixes." });
    expect(win.webContents.send).toHaveBeenCalledWith(
      IPC.UpdateAvailable,
      expect.objectContaining({ version: "1.2.3" }),
    );
  });

  it("truncates release notes to 300 characters", () => {
    const win = makeMockWin();
    initUpdater(win as never);
    const longNotes = "x".repeat(400);
    mockAU._emit("update-available", { version: "2.0.0", releaseNotes: longNotes });
    const payload = win.webContents.send.mock.calls[0][1] as { releaseNotes: string };
    expect(payload.releaseNotes).toHaveLength(300);
  });

  it("sends UpdateDownloadProgress IPC with rounded percent", () => {
    const win = makeMockWin();
    initUpdater(win as never);
    mockAU._emit("download-progress", { percent: 42.7, bytesPerSecond: 1024, transferred: 100, total: 200 });
    expect(win.webContents.send).toHaveBeenCalledWith(
      IPC.UpdateDownloadProgress,
      expect.objectContaining({ percent: 43 }),
    );
  });

  it("sends UpdateDownloaded IPC when update-downloaded fires", () => {
    const win = makeMockWin();
    initUpdater(win as never);
    mockAU._emit("update-downloaded", { version: "1.2.3" });
    expect(win.webContents.send).toHaveBeenCalledWith(
      IPC.UpdateDownloaded,
      expect.objectContaining({ version: "1.2.3" }),
    );
  });

  it("swallows network errors silently (no IPC sent for net:: errors)", () => {
    const win = makeMockWin();
    initUpdater(win as never);
    mockAU._emit("error", new Error("net::ERR_INTERNET_DISCONNECTED"));
    expect(win.webContents.send).not.toHaveBeenCalled();
  });

  it("swallows ENOTFOUND errors silently", () => {
    const win = makeMockWin();
    initUpdater(win as never);
    mockAU._emit("error", new Error("getaddrinfo ENOTFOUND api.github.com"));
    expect(win.webContents.send).not.toHaveBeenCalled();
  });

  it("sends UpdateError IPC for non-network errors", () => {
    const win = makeMockWin();
    initUpdater(win as never);
    mockAU._emit("error", new Error("signature verification failed"));
    expect(win.webContents.send).toHaveBeenCalledWith(
      IPC.UpdateError,
      expect.objectContaining({ message: expect.stringContaining("signature") }),
    );
  });

  it("calls checkForUpdates with a timeout", () => {
    vi.useFakeTimers();
    const win = makeMockWin();
    initUpdater(win as never);
    expect(autoUpdater.checkForUpdates).not.toHaveBeenCalled();
    vi.advanceTimersByTime(3000);
    expect(autoUpdater.checkForUpdates).toHaveBeenCalled();
    vi.useRealTimers();
  });
});
