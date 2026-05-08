import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  retries: process.env.CI ? 2 : 0,
  reporter: "list",
  // Electron tests must run serially — one Electron instance at a time
  workers: 1,
  use: {
    actionTimeout: 10_000,
  },
});
