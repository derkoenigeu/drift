import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { homedir } from "node:os";
import { join } from "node:path";
import { defaultVaultPath } from "../src/paths.js";

describe("defaultVaultPath", () => {
  const original = process.env.DB_MIRROR_VAULT;

  afterEach(() => {
    if (original === undefined) delete process.env.DB_MIRROR_VAULT;
    else process.env.DB_MIRROR_VAULT = original;
  });

  it("returns the env var when DB_MIRROR_VAULT is set", () => {
    process.env.DB_MIRROR_VAULT = "/custom/path/vault.db";
    expect(defaultVaultPath()).toBe("/custom/path/vault.db");
  });

  it("returns the default path when env var is unset", () => {
    delete process.env.DB_MIRROR_VAULT;
    const expected = join(homedir(), ".config", "db-mirror", "vault.db-mirror");
    expect(defaultVaultPath()).toBe(expected);
  });

  it("default path ends with .db-mirror extension", () => {
    delete process.env.DB_MIRROR_VAULT;
    expect(defaultVaultPath()).toMatch(/\.db-mirror$/);
  });

  it("default path is under the user's home directory", () => {
    delete process.env.DB_MIRROR_VAULT;
    expect(defaultVaultPath()).toContain(homedir());
  });
});
