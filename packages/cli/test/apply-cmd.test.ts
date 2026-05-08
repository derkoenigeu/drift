import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

// ---------------------------------------------------------------------------
// Mocks (hoisted)
// ---------------------------------------------------------------------------

vi.mock("node:fs/promises", () => ({
  readFile: vi.fn(),
}));

vi.mock("@db-mirror/core", () => {
  const mockPool = {};
  const mockConn = {
    pool: mockPool,
    close: vi.fn().mockResolvedValue(undefined),
  };
  const mockVault = {
    getProfile: vi.fn(),
  };
  return {
    Vault: {
      open: vi.fn().mockResolvedValue(mockVault),
    },
    openConnection: vi.fn().mockResolvedValue(mockConn),
    applyPlan: vi.fn().mockResolvedValue({ executed: 1, skipped: 0, errors: [] }),
    __mockVault: mockVault,
    __mockConn: mockConn,
  };
});

vi.mock("../src/passphrase.js", () => ({
  readPassphrase: vi.fn().mockResolvedValue("testpass"),
}));

// ---------------------------------------------------------------------------
// Imports
// ---------------------------------------------------------------------------

import { readFile } from "node:fs/promises";
import { Vault, openConnection, applyPlan } from "@db-mirror/core";
import { applyCommand } from "../src/commands/apply-cmd.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeCmd() {
  return applyCommand();
}

async function getMocks() {
  const mod = await import("@db-mirror/core");
  return {
    // @ts-expect-error injected by mock factory
    mockVault: mod.__mockVault as { getProfile: ReturnType<typeof vi.fn> },
    // @ts-expect-error injected by mock factory
    mockConn: mod.__mockConn as { pool: object; close: ReturnType<typeof vi.fn> },
  };
}

/** A minimal non-destructive SyncPlan JSON. */
const PLAN_NON_DESTRUCTIVE = JSON.stringify({
  schema: [{ sql: "ALTER TABLE t ADD COLUMN x INT", destructive: false }],
  data: [],
});

/** A minimal destructive SyncPlan JSON. */
const PLAN_DESTRUCTIVE = JSON.stringify({
  schema: [{ sql: "DROP TABLE t", destructive: true }],
  data: [],
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("apply command", () => {
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
    logSpy.mockRestore();
    errSpy.mockRestore();
  });

  it("reads plan from file, opens vault, gets profile, and calls applyPlan", async () => {
    const { mockVault, mockConn } = await getMocks();
    const targetProfile = { id: "tgt-1", name: "production" };
    mockVault.getProfile.mockReturnValue(targetProfile);
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);
    vi.mocked(readFile).mockResolvedValue(PLAN_NON_DESTRUCTIVE as never);
    vi.mocked(openConnection).mockResolvedValue(mockConn as never);
    vi.mocked(applyPlan).mockResolvedValue({ executed: 1, skipped: 0, errors: [] });

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "apply",
      "/tmp/plan.json",
      "--target", "production",
      "--yes",
      "--path", "/tmp/vault",
    ]);

    expect(readFile).toHaveBeenCalledWith("/tmp/plan.json", "utf8");
    expect(Vault.open).toHaveBeenCalledWith("/tmp/vault", "testpass");
    expect(mockVault.getProfile).toHaveBeenCalledWith("production");
    expect(openConnection).toHaveBeenCalledWith(targetProfile);
    expect(applyPlan).toHaveBeenCalledWith(
      mockConn.pool,
      expect.objectContaining({ schema: expect.any(Array) }),
      expect.objectContaining({ dryRun: false })
    );
    expect(mockConn.close).toHaveBeenCalled();
  });

  it("exits with code 1 when target profile is not found", async () => {
    const { mockVault } = await getMocks();
    mockVault.getProfile.mockReturnValue(undefined);
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);
    vi.mocked(readFile).mockResolvedValue(PLAN_NON_DESTRUCTIVE as never);

    const cmd = makeCmd();
    await expect(
      cmd.parseAsync([
        "node", "apply",
        "/tmp/plan.json",
        "--target", "ghost",
        "--path", "/tmp/vault",
      ])
    ).rejects.toThrow("exit:1");

    expect(errSpy).toHaveBeenCalledWith("Target profile not found");
    expect(applyPlan).not.toHaveBeenCalled();
  });

  it("passes dryRun=true when --dry-run is provided", async () => {
    const { mockVault, mockConn } = await getMocks();
    mockVault.getProfile.mockReturnValue({ id: "tgt-1", name: "staging" });
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);
    vi.mocked(readFile).mockResolvedValue(PLAN_NON_DESTRUCTIVE as never);
    vi.mocked(openConnection).mockResolvedValue(mockConn as never);
    vi.mocked(applyPlan).mockResolvedValue({ executed: 0, skipped: 1, errors: [] });

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "apply",
      "/tmp/plan.json",
      "--target", "staging",
      "--dry-run",
      "--yes",
      "--path", "/tmp/vault",
    ]);

    expect(applyPlan).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ dryRun: true })
    );
  });

  it("skips confirmation when --yes is provided even for destructive plans", async () => {
    const { mockVault, mockConn } = await getMocks();
    mockVault.getProfile.mockReturnValue({ id: "tgt-1", name: "production" });
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);
    vi.mocked(readFile).mockResolvedValue(PLAN_DESTRUCTIVE as never);
    vi.mocked(openConnection).mockResolvedValue(mockConn as never);
    vi.mocked(applyPlan).mockResolvedValue({ executed: 1, skipped: 0, errors: [] });

    const cmd = makeCmd();
    // Should not hang waiting for stdin input
    await cmd.parseAsync([
      "node", "apply",
      "/tmp/plan.json",
      "--target", "production",
      "--yes",
      "--path", "/tmp/vault",
    ]);

    expect(applyPlan).toHaveBeenCalled();
  });

  it("skips confirmation when --dry-run is provided even for destructive plans", async () => {
    const { mockVault, mockConn } = await getMocks();
    mockVault.getProfile.mockReturnValue({ id: "tgt-1", name: "production" });
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);
    vi.mocked(readFile).mockResolvedValue(PLAN_DESTRUCTIVE as never);
    vi.mocked(openConnection).mockResolvedValue(mockConn as never);
    vi.mocked(applyPlan).mockResolvedValue({ executed: 0, skipped: 1, errors: [] });

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "apply",
      "/tmp/plan.json",
      "--target", "production",
      "--dry-run",
      "--path", "/tmp/vault",
    ]);

    expect(applyPlan).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ dryRun: true })
    );
  });

  it("exits with code 2 when applyPlan returns errors", async () => {
    const { mockVault, mockConn } = await getMocks();
    mockVault.getProfile.mockReturnValue({ id: "tgt-1", name: "staging" });
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);
    vi.mocked(readFile).mockResolvedValue(PLAN_NON_DESTRUCTIVE as never);
    vi.mocked(openConnection).mockResolvedValue(mockConn as never);
    vi.mocked(applyPlan).mockResolvedValue({
      executed: 0,
      skipped: 0,
      errors: [{ sql: "ALTER TABLE t ADD COLUMN x INT", error: "syntax error" }],
    });

    const cmd = makeCmd();
    await expect(
      cmd.parseAsync([
        "node", "apply",
        "/tmp/plan.json",
        "--target", "staging",
        "--yes",
        "--path", "/tmp/vault",
      ])
    ).rejects.toThrow("exit:2");

    expect(mockConn.close).toHaveBeenCalled();
  });

  it("closes connection even when applyPlan throws", async () => {
    const { mockVault, mockConn } = await getMocks();
    mockVault.getProfile.mockReturnValue({ id: "tgt-1", name: "staging" });
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);
    vi.mocked(readFile).mockResolvedValue(PLAN_NON_DESTRUCTIVE as never);
    vi.mocked(openConnection).mockResolvedValue(mockConn as never);
    vi.mocked(applyPlan).mockRejectedValue(new Error("DB exploded"));

    const cmd = makeCmd();
    await expect(
      cmd.parseAsync([
        "node", "apply",
        "/tmp/plan.json",
        "--target", "staging",
        "--yes",
        "--path", "/tmp/vault",
      ])
    ).rejects.toThrow("DB exploded");

    expect(mockConn.close).toHaveBeenCalled();
  });
});
