import { describe, it, expect, afterEach, vi } from "vitest";

// Hoist the mock so it applies before readPassphrase's static import of readFile
vi.mock("node:fs/promises", () => ({
  readFile: vi.fn(),
}));

import { readFile } from "node:fs/promises";
import { readPassphrase } from "../src/passphrase.js";

const mockReadFile = readFile as ReturnType<typeof vi.fn>;

describe("readPassphrase", () => {
  const original = process.env.DB_MIRROR_PASSPHRASE;

  afterEach(() => {
    if (original === undefined) delete process.env.DB_MIRROR_PASSPHRASE;
    else process.env.DB_MIRROR_PASSPHRASE = original;
    vi.clearAllMocks();
  });

  it("returns the env var immediately without reading a file or prompting", async () => {
    process.env.DB_MIRROR_PASSPHRASE = "from-env";
    const result = await readPassphrase({});
    expect(result).toBe("from-env");
    expect(mockReadFile).not.toHaveBeenCalled();
  });

  it("reads and trims from file when no env var but file path is given", async () => {
    delete process.env.DB_MIRROR_PASSPHRASE;
    mockReadFile.mockResolvedValue("from-file\n");
    const result = await readPassphrase({ file: "/some/secret.txt" });
    expect(result).toBe("from-file");
    expect(mockReadFile).toHaveBeenCalledWith("/some/secret.txt", "utf8");
  });

  it("env var takes precedence over file option", async () => {
    process.env.DB_MIRROR_PASSPHRASE = "env-wins";
    const result = await readPassphrase({ file: "/ignored.txt" });
    expect(result).toBe("env-wins");
    expect(mockReadFile).not.toHaveBeenCalled();
  });
});
