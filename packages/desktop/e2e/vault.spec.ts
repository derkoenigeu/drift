import { test, expect, TEST_PASSPHRASE } from "./fixtures.js";

// ── Fresh vault (no existing vault file) ────────────────────────────────────

test("shows create-vault form for a fresh vault", async ({ window }) => {
  await expect(window.locator(".title")).toHaveText("Create your vault");
  await expect(window.locator("button.submit")).toContainText("Create vault");
  await expect(window.locator('input[autocomplete="current-password"]')).toBeVisible();
  await expect(window.locator('input[type="password"]').nth(1)).toBeVisible();
});

test("submit button is disabled when passphrase is empty", async ({ window }) => {
  await expect(window.locator("button.submit")).toBeDisabled();
});

test("rejects passphrase shorter than 8 characters", async ({ window }) => {
  await window.locator('input[autocomplete="current-password"]').fill("short");
  await window.locator('input[type="password"]').nth(1).fill("short");
  await window.locator("button.submit").click();
  await expect(window.locator(".err-body")).toContainText("8 characters");
});

test("rejects mismatched passphrases", async ({ window }) => {
  await window.locator('input[autocomplete="current-password"]').fill(TEST_PASSPHRASE);
  await window.locator('input[type="password"]').nth(1).fill("DifferentPass!99");
  await window.locator("button.submit").click();
  await expect(window.locator(".err-body")).toContainText("don't match");
});

test("creates vault and shows the main shell", async ({ window }) => {
  await window.locator('input[autocomplete="current-password"]').fill(TEST_PASSPHRASE);
  await window.locator('input[type="password"]').nth(1).fill(TEST_PASSPHRASE);
  await window.locator("button.submit").click();
  await expect(window.locator(".titlebar")).toBeVisible();
  await expect(window.locator("nav button", { hasText: "Connections" })).toBeVisible();
  await expect(window.locator("nav button", { hasText: "Workbench" })).toBeVisible();
  await expect(window.locator(".pill.dot.ok")).toHaveText("Vault open");
});

// ── Pre-existing vault (created programmatically before app launch) ──────────

test("shows unlock form when vault already exists", async ({ preExistingWindow: win }) => {
  await expect(win.locator(".title")).toHaveText("Unlock your vault");
  await expect(win.locator("button.submit")).toContainText("Unlock");
  // Confirm field should NOT be visible (only shown during creation)
  await expect(win.locator('input[type="password"]').nth(1)).not.toBeVisible();
});

test("unlocks an existing vault with correct passphrase", async ({ preExistingWindow: win }) => {
  await win.locator('input[autocomplete="current-password"]').fill(TEST_PASSPHRASE);
  await win.locator("button.submit").click();
  await expect(win.locator(".titlebar")).toBeVisible();
  await expect(win.locator(".pill.dot.ok")).toHaveText("Vault open");
});

test("shows error for wrong passphrase", async ({ preExistingWindow: win }) => {
  await win.locator('input[autocomplete="current-password"]').fill("WrongPass!999");
  await win.locator("button.submit").click();
  await expect(win.locator(".err-title")).toHaveText("Wrong passphrase");
});
