import { test as base, expect, _electron as electron } from "@playwright/test";
import type { ElectronApplication, Page } from "@playwright/test";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { dirname, resolve, join } from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import { unlink } from "node:fs/promises";
import { Vault } from "@db-mirror/core";

const _require = createRequire(import.meta.url);
const electronBin: string = _require("electron");
const appDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export const TEST_PASSPHRASE = "T3stPass!word";

type AppFixtures = {
  vaultPath: string;
  // Fresh vault (no pre-existing file) — shows "Create your vault"
  app: ElectronApplication;
  window: Page;
  unlocked: Page;
  // Pre-existing vault — shows "Unlock your vault"
  preExistingApp: ElectronApplication;
  preExistingWindow: Page;
};

export { expect };

function launchElectron(vaultPath: string): Promise<ElectronApplication> {
  return electron.launch({
    args: [appDir],
    executablePath: electronBin,
    env: {
      ...process.env,
      DB_MIRROR_VAULT: vaultPath,
      PLAYWRIGHT_TEST: "1",
      NODE_ENV: "test",
    },
  });
}

async function waitForReady(app: ElectronApplication): Promise<Page> {
  const win = await app.firstWindow();
  await win.waitForLoadState("domcontentloaded");
  await win.locator(".loading").waitFor({ state: "hidden" });
  return win;
}

export const test = base.extend<AppFixtures>({
  vaultPath: async ({}, use) => {
    const p = join(tmpdir(), `e2e-drift-${randomUUID()}.db-mirror`);
    await use(p);
    try { await unlink(p); } catch {}
  },

  // Launches with a fresh vault (no existing vault file → Create form)
  app: async ({ vaultPath }, use) => {
    const app = await launchElectron(vaultPath);
    await use(app);
    await app.close();
  },

  window: async ({ app }, use) => {
    await use(await waitForReady(app));
  },

  // Unlocked shell (creates vault via UI on top of `window`)
  unlocked: async ({ window }, use) => {
    await window.locator('input[autocomplete="current-password"]').fill(TEST_PASSPHRASE);
    await window.locator('input[type="password"]').nth(1).fill(TEST_PASSPHRASE);
    await window.locator("button.submit").click();
    await window.locator(".titlebar").waitFor();
    await use(window);
  },

  // Launches with a vault that was created BEFORE the app starts → Unlock form
  preExistingApp: async ({ vaultPath }, use) => {
    await Vault.create(vaultPath, TEST_PASSPHRASE);
    const app = await launchElectron(vaultPath);
    await use(app);
    await app.close();
  },

  preExistingWindow: async ({ preExistingApp }, use) => {
    await use(await waitForReady(preExistingApp));
  },
});
