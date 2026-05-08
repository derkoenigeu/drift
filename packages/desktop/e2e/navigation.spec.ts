import { test, expect } from "./fixtures.js";

// All tests in this file start from an unlocked vault (via the `unlocked` fixture)

test("shows Connections tab by default after unlock", async ({ unlocked: win }) => {
  await expect(win.locator("nav button.on")).toHaveText(/Connections/);
});

test("switches to Workbench tab on click", async ({ unlocked: win }) => {
  await win.locator("nav button", { hasText: "Workbench" }).click();
  await expect(win.locator("nav button.on")).toHaveText(/Workbench/);
});

test("keyboard shortcut Ctrl+2 switches to Workbench", async ({ unlocked: win }) => {
  await win.keyboard.press("Control+2");
  await expect(win.locator("nav button.on")).toHaveText(/Workbench/);
});

test("keyboard shortcut Ctrl+1 switches to Connections", async ({ unlocked: win }) => {
  // First go to Workbench
  await win.locator("nav button", { hasText: "Workbench" }).click();
  // Then use shortcut to go back
  await win.keyboard.press("Control+1");
  await expect(win.locator("nav button.on")).toHaveText(/Connections/);
});

test("Lock button returns to the unlock screen", async ({ unlocked: win }) => {
  await win.locator("button", { hasText: "Lock" }).click();
  await expect(win.locator(".title")).toBeVisible();
  // Should show unlock (not create) since vault file exists
  await expect(win.locator("button.submit")).toContainText("Unlock");
});

test("vault status pill disappears after locking", async ({ unlocked: win }) => {
  await expect(win.locator(".pill.dot.ok")).toBeVisible();
  await win.locator("button", { hasText: "Lock" }).click();
  await expect(win.locator(".pill.dot.ok")).not.toBeVisible();
});
