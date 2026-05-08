import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

// ---------------------------------------------------------------------------
// Mocks (hoisted)
// ---------------------------------------------------------------------------

vi.mock("@db-mirror/core", () => {
  const mockSrcPool = {};
  const mockTgtPool = {};
  const mockSrcConn = { pool: mockSrcPool, close: vi.fn().mockResolvedValue(undefined) };
  const mockTgtConn = { pool: mockTgtPool, close: vi.fn().mockResolvedValue(undefined) };
  const mockVault = { getProfile: vi.fn() };

  return {
    Vault: { open: vi.fn().mockResolvedValue(mockVault) },
    openConnection: vi.fn(),
    buildPlan: vi.fn().mockResolvedValue({ schema: [], data: [] }),
    applyPlan: vi.fn().mockResolvedValue({ executed: 0, skipped: 0, errors: [] }),
    fullOverwrite: vi.fn().mockResolvedValue(undefined),
    __mockVault: mockVault,
    __mockSrcConn: mockSrcConn,
    __mockTgtConn: mockTgtConn,
  };
});

vi.mock("../src/passphrase.js", () => ({
  readPassphrase: vi.fn().mockResolvedValue("testpass"),
}));

// ---------------------------------------------------------------------------
// Imports
// ---------------------------------------------------------------------------

import { Vault, openConnection, buildPlan, applyPlan, fullOverwrite } from "@db-mirror/core";
import { mirrorCommand } from "../src/commands/mirror-cmd.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeCmd() {
  return mirrorCommand();
}

async function getMocks() {
  const mod = await import("@db-mirror/core");
  return {
    // @ts-expect-error injected by mock factory
    mockVault: mod.__mockVault as { getProfile: ReturnType<typeof vi.fn> },
    // @ts-expect-error injected by mock factory
    mockSrcConn: mod.__mockSrcConn as { pool: object; close: ReturnType<typeof vi.fn> },
    // @ts-expect-error injected by mock factory
    mockTgtConn: mod.__mockTgtConn as { pool: object; close: ReturnType<typeof vi.fn> },
  };
}

const SRC_PROFILE = { id: "src-1", name: "source", database: "srcdb" };
const TGT_PROFILE = { id: "tgt-1", name: "target", database: "tgtdb" };

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("mirror command", () => {
  let exitSpy: ReturnType<typeof vi.spyOn>;
  let logSpy: ReturnType<typeof vi.spyOn>;
  let errSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    exitSpy = vi.spyOn(process, "exit").mockImplementation((code) => {
      throw new Error(`exit:${code}`);
    });
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    errSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const { mockVault, mockSrcConn, mockTgtConn } = await getMocks();
    mockVault.getProfile.mockImplementation((id: string) => {
      if (id === "source") return SRC_PROFILE;
      if (id === "target") return TGT_PROFILE;
      return undefined;
    });
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);
    vi.mocked(openConnection)
      .mockResolvedValueOnce(mockSrcConn as never)
      .mockResolvedValueOnce(mockTgtConn as never);
    vi.mocked(buildPlan).mockResolvedValue({ schema: [], data: [] });
    vi.mocked(applyPlan).mockResolvedValue({ executed: 1, skipped: 0, errors: [] });
  });

  afterEach(() => {
    vi.clearAllMocks();
    exitSpy.mockRestore();
    logSpy.mockRestore();
    errSpy.mockRestore();
  });

  it("exits with code 1 when either profile is not found", async () => {
    const { mockVault } = await getMocks();
    mockVault.getProfile.mockReturnValue(undefined);
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);

    const cmd = makeCmd();
    await expect(
      cmd.parseAsync(["node", "mirror", "ghost", "target", "--yes", "--path", "/tmp/vault"])
    ).rejects.toThrow("exit:1");

    expect(errSpy).toHaveBeenCalledWith("Profile(s) not found");
    expect(buildPlan).not.toHaveBeenCalled();
    expect(applyPlan).not.toHaveBeenCalled();
  });

  it("calls buildPlan then applyPlan in sequence (diff mode, --yes)", async () => {
    const { mockSrcConn, mockTgtConn } = await getMocks();
    vi.mocked(buildPlan).mockResolvedValue({ schema: [{ sql: "CREATE TABLE x (id INT)", destructive: false }], data: [] });

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "mirror", "source", "target",
      "--both",
      "--yes",
      "--path", "/tmp/vault",
    ]);

    expect(buildPlan).toHaveBeenCalledWith(
      mockSrcConn.pool,
      mockTgtConn.pool,
      "srcdb",
      "tgtdb",
      expect.objectContaining({ mode: { schema: true, data: true } })
    );
    expect(applyPlan).toHaveBeenCalledWith(
      mockTgtConn.pool,
      expect.objectContaining({ schema: expect.any(Array) }),
      expect.objectContaining({ dryRun: false })
    );
    expect(mockSrcConn.close).toHaveBeenCalled();
    expect(mockTgtConn.close).toHaveBeenCalled();
  });

  it("passes dryRun=true to applyPlan when --dry-run is specified", async () => {
    const { mockTgtConn } = await getMocks();

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "mirror", "source", "target",
      "--both",
      "--dry-run",
      "--path", "/tmp/vault",
    ]);

    expect(applyPlan).toHaveBeenCalledWith(
      mockTgtConn.pool,
      expect.anything(),
      expect.objectContaining({ dryRun: true })
    );
  });

  it("uses fullOverwrite when --mode=overwrite and --yes is set", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "mirror", "source", "target",
      "--mode", "overwrite",
      "--yes",
      "--path", "/tmp/vault",
    ]);

    expect(fullOverwrite).toHaveBeenCalledWith(SRC_PROFILE, TGT_PROFILE, expect.anything());
    expect(buildPlan).not.toHaveBeenCalled();
    expect(applyPlan).not.toHaveBeenCalled();
  });

  it("prints dry-run message for overwrite mode without actually calling fullOverwrite", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "mirror", "source", "target",
      "--mode", "overwrite",
      "--dry-run",
      "--path", "/tmp/vault",
    ]);

    expect(fullOverwrite).not.toHaveBeenCalled();
    const allLogs = logSpy.mock.calls.flat().join("\n");
    expect(allLogs).toContain("dry-run");
  });

  it("closes both connections even when applyPlan throws", async () => {
    const { mockSrcConn, mockTgtConn } = await getMocks();
    vi.mocked(applyPlan).mockRejectedValue(new Error("apply failed"));

    const cmd = makeCmd();
    await expect(
      cmd.parseAsync([
        "node", "mirror", "source", "target",
        "--both",
        "--yes",
        "--path", "/tmp/vault",
      ])
    ).rejects.toThrow("apply failed");

    expect(mockSrcConn.close).toHaveBeenCalled();
    expect(mockTgtConn.close).toHaveBeenCalled();
  });
});
