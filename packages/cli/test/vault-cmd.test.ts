import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

// ---------------------------------------------------------------------------
// Hoist mocks – Vitest requires vi.mock() calls to be at the top level so
// they are hoisted before any import is evaluated.
// ---------------------------------------------------------------------------

vi.mock("@db-mirror/core", () => {
  const mockVault = {
    listProfiles: vi.fn().mockReturnValue([]),
    listPairs: vi.fn().mockReturnValue([]),
    upsertProfile: vi.fn(),
    getProfile: vi.fn(),
    removeProfile: vi.fn(),
  };

  return {
    Vault: {
      exists: vi.fn(),
      create: vi.fn(),
      open: vi.fn().mockResolvedValue(mockVault),
    },
    // re-export the mock vault so tests can grab it via the module
    __mockVault: mockVault,
  };
});

vi.mock("../src/passphrase.js", () => ({
  readPassphrase: vi.fn().mockResolvedValue("testpass"),
}));

// ---------------------------------------------------------------------------
// Imports – after mocks are registered
// ---------------------------------------------------------------------------

import { Vault } from "@db-mirror/core";
import { vaultCommand } from "../src/commands/vault-cmd.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeCmd() {
  return vaultCommand();
}

// Grab the shared mock vault instance that every `Vault.open()` call returns.
// We do this via a dynamic import so the hoisted mock is already in place.
async function getMockVault() {
  const mod = await import("@db-mirror/core");
  // @ts-expect-error – __mockVault is injected by the mock factory above
  return mod.__mockVault as {
    listProfiles: ReturnType<typeof vi.fn>;
    listPairs: ReturnType<typeof vi.fn>;
    upsertProfile: ReturnType<typeof vi.fn>;
    getProfile: ReturnType<typeof vi.fn>;
    removeProfile: ReturnType<typeof vi.fn>;
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("vault init", () => {
  let exitSpy: ReturnType<typeof vi.spyOn>;
  let logSpy: ReturnType<typeof vi.spyOn>;
  let errSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    exitSpy = vi.spyOn(process, "exit").mockImplementation((code) => {
      throw new Error(`exit:${code}`);
    });
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    errSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    vi.mocked(Vault.exists).mockResolvedValue(false);
    vi.mocked(Vault.create).mockResolvedValue(undefined as never);
  });

  afterEach(() => {
    vi.clearAllMocks();
    exitSpy.mockRestore();
    logSpy.mockRestore();
    errSpy.mockRestore();
  });

  it("creates the vault when it does not yet exist", async () => {
    vi.mocked(Vault.exists).mockResolvedValue(false);

    const cmd = makeCmd();
    await cmd.parseAsync(["node", "vault", "init", "--path", "/tmp/test.vault"]);

    expect(Vault.exists).toHaveBeenCalledWith("/tmp/test.vault");
    expect(Vault.create).toHaveBeenCalledWith("/tmp/test.vault", "testpass");
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("Created vault"));
    expect(exitSpy).not.toHaveBeenCalled();
  });

  it("exits with code 1 when vault already exists", async () => {
    vi.mocked(Vault.exists).mockResolvedValue(true);

    const cmd = makeCmd();
    await expect(
      cmd.parseAsync(["node", "vault", "init", "--path", "/tmp/existing.vault"])
    ).rejects.toThrow("exit:1");

    expect(Vault.create).not.toHaveBeenCalled();
    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining("already exists"));
  });
});

describe("vault list", () => {
  let exitSpy: ReturnType<typeof vi.spyOn>;
  let logSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    exitSpy = vi.spyOn(process, "exit").mockImplementation((code) => {
      throw new Error(`exit:${code}`);
    });
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.clearAllMocks();
    exitSpy.mockRestore();
  });

  it("lists profiles and pairs from the vault", async () => {
    const mockVault = await getMockVault();
    mockVault.listProfiles.mockReturnValue([
      { kind: "direct", name: "prod", host: "db.example.com", port: 3306, database: "app", id: "abc-123" },
    ]);
    mockVault.listPairs.mockReturnValue([
      { name: "prod-pair", sourceProfileId: "src-1", targetProfileId: "tgt-1" },
    ]);

    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);

    const cmd = makeCmd();
    await cmd.parseAsync(["node", "vault", "list", "--path", "/tmp/test.vault"]);

    expect(Vault.open).toHaveBeenCalledWith("/tmp/test.vault", "testpass");
    expect(mockVault.listProfiles).toHaveBeenCalled();
    expect(mockVault.listPairs).toHaveBeenCalled();

    // Profiles header + at least one profile line
    const allLogs = logSpy.mock.calls.flat().join("\n");
    expect(allLogs).toContain("Profiles:");
    expect(allLogs).toContain("prod");
    expect(allLogs).toContain("Pairs:");
    expect(allLogs).toContain("prod-pair");
  });

  it("shows empty sections when vault has no profiles or pairs", async () => {
    const mockVault = await getMockVault();
    mockVault.listProfiles.mockReturnValue([]);
    mockVault.listPairs.mockReturnValue([]);

    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);

    const cmd = makeCmd();
    await cmd.parseAsync(["node", "vault", "list", "--path", "/tmp/test.vault"]);

    const allLogs = logSpy.mock.calls.flat().join("\n");
    expect(allLogs).toContain("Profiles:");
    expect(allLogs).toContain("Pairs:");
  });
});

describe("vault add-profile", () => {
  let exitSpy: ReturnType<typeof vi.spyOn>;
  let logSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    exitSpy = vi.spyOn(process, "exit").mockImplementation((code) => {
      throw new Error(`exit:${code}`);
    });
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.clearAllMocks();
    exitSpy.mockRestore();
  });

  it("saves a new direct profile to the vault", async () => {
    const mockVault = await getMockVault();
    mockVault.upsertProfile.mockResolvedValue({ name: "dev", id: "xyz-456" });
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "vault", "add-profile",
      "--name", "dev",
      "--host", "127.0.0.1",
      "--user", "root",
      "--password", "secret",
      "--database", "mydb",
      "--path", "/tmp/test.vault",
    ]);

    expect(mockVault.upsertProfile).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "direct",
        name: "dev",
        host: "127.0.0.1",
        user: "root",
        password: "secret",
        database: "mydb",
      })
    );
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("dev"));
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("xyz-456"));
  });
});

describe("vault remove-profile", () => {
  let exitSpy: ReturnType<typeof vi.spyOn>;
  let logSpy: ReturnType<typeof vi.spyOn>;
  let errSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    exitSpy = vi.spyOn(process, "exit").mockImplementation((code) => {
      throw new Error(`exit:${code}`);
    });
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.clearAllMocks();
    exitSpy.mockRestore();
  });

  it("removes a profile found by name", async () => {
    const mockVault = await getMockVault();
    mockVault.getProfile.mockReturnValue({ id: "abc-123", name: "dev" });
    mockVault.removeProfile.mockResolvedValue(undefined);
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);

    const cmd = makeCmd();
    await cmd.parseAsync(["node", "vault", "remove-profile", "dev", "--path", "/tmp/test.vault"]);

    expect(mockVault.getProfile).toHaveBeenCalledWith("dev");
    expect(mockVault.removeProfile).toHaveBeenCalledWith("abc-123");
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("Removed"));
  });

  it("exits with code 1 when profile not found", async () => {
    const mockVault = await getMockVault();
    mockVault.getProfile.mockReturnValue(undefined);
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);

    const cmd = makeCmd();
    await expect(
      cmd.parseAsync(["node", "vault", "remove-profile", "ghost", "--path", "/tmp/test.vault"])
    ).rejects.toThrow("exit:1");

    expect(mockVault.removeProfile).not.toHaveBeenCalled();
    expect(errSpy).toHaveBeenCalledWith("Not found");
  });
});
