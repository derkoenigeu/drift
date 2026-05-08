import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

// ---------------------------------------------------------------------------
// Mocks (hoisted)
// ---------------------------------------------------------------------------

vi.mock("node:fs/promises", () => ({
  writeFile: vi.fn().mockResolvedValue(undefined),
  readFile: vi.fn(),
}));

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

import { writeFile } from "node:fs/promises";
import { Vault, openConnection, buildPlan } from "@db-mirror/core";
import { diffCommand } from "../src/commands/diff-cmd.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeCmd() {
  return diffCommand();
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

describe("diff command", () => {
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
    // Alternate return values: first call = srcConn, second call = tgtConn
    vi.mocked(openConnection)
      .mockResolvedValueOnce(mockSrcConn as never)
      .mockResolvedValueOnce(mockTgtConn as never);
    vi.mocked(buildPlan).mockResolvedValue({ schema: [], data: [] });
  });

  afterEach(() => {
    vi.clearAllMocks();
    exitSpy.mockRestore();
    logSpy.mockRestore();
    errSpy.mockRestore();
  });

  it("exits with code 2 when neither --schema nor --data nor --both is specified", async () => {
    const cmd = makeCmd();
    await expect(
      cmd.parseAsync(["node", "diff", "source", "target", "--path", "/tmp/vault"])
    ).rejects.toThrow("exit:2");

    expect(errSpy).toHaveBeenCalledWith(expect.stringContaining("--schema"));
    expect(buildPlan).not.toHaveBeenCalled();
  });

  it("calls buildPlan with schema mode when --schema is provided", async () => {
    const { mockSrcConn, mockTgtConn } = await getMocks();

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "diff", "source", "target",
      "--schema",
      "--path", "/tmp/vault",
    ]);

    expect(buildPlan).toHaveBeenCalledWith(
      mockSrcConn.pool,
      mockTgtConn.pool,
      "srcdb",
      "tgtdb",
      expect.objectContaining({ mode: { schema: true, data: false } })
    );
  });

  it("calls buildPlan with data mode when --data is provided", async () => {
    const { mockSrcConn, mockTgtConn } = await getMocks();

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "diff", "source", "target",
      "--data",
      "--path", "/tmp/vault",
    ]);

    expect(buildPlan).toHaveBeenCalledWith(
      mockSrcConn.pool,
      mockTgtConn.pool,
      "srcdb",
      "tgtdb",
      expect.objectContaining({ mode: { schema: false, data: true } })
    );
  });

  it("calls buildPlan with both modes when --both is provided", async () => {
    const { mockSrcConn, mockTgtConn } = await getMocks();

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "diff", "source", "target",
      "--both",
      "--path", "/tmp/vault",
    ]);

    expect(buildPlan).toHaveBeenCalledWith(
      mockSrcConn.pool,
      mockTgtConn.pool,
      "srcdb",
      "tgtdb",
      expect.objectContaining({ mode: { schema: true, data: true } })
    );
  });

  it("writes JSON output to a file when --out is provided", async () => {
    vi.mocked(buildPlan).mockResolvedValue({ schema: [{ sql: "CREATE TABLE x (id INT)", destructive: false }], data: [] });

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "diff", "source", "target",
      "--schema",
      "--out", "/tmp/plan.json",
      "--path", "/tmp/vault",
    ]);

    expect(writeFile).toHaveBeenCalledWith("/tmp/plan.json", expect.stringContaining("CREATE TABLE"));
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("/tmp/plan.json"));
  });

  it("prints JSON to stdout when --out is not provided", async () => {
    vi.mocked(buildPlan).mockResolvedValue({ schema: [], data: [] });

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "diff", "source", "target",
      "--schema",
      "--path", "/tmp/vault",
    ]);

    expect(writeFile).not.toHaveBeenCalled();
    // JSON output goes to console.log
    const allLogs = logSpy.mock.calls.flat().join("\n");
    expect(allLogs).toContain('"schema"');
  });

  it("exits with code 1 when source profile is not found", async () => {
    const { mockVault } = await getMocks();
    mockVault.getProfile.mockReturnValue(undefined);
    vi.mocked(Vault.open).mockResolvedValue(mockVault as never);

    const cmd = makeCmd();
    await expect(
      cmd.parseAsync([
        "node", "diff", "ghost", "target",
        "--schema",
        "--path", "/tmp/vault",
      ])
    ).rejects.toThrow("exit:1");

    expect(buildPlan).not.toHaveBeenCalled();
  });

  it("closes both connections even when buildPlan throws", async () => {
    const { mockSrcConn, mockTgtConn } = await getMocks();
    vi.mocked(buildPlan).mockRejectedValue(new Error("connection reset"));

    const cmd = makeCmd();
    await expect(
      cmd.parseAsync([
        "node", "diff", "source", "target",
        "--schema",
        "--path", "/tmp/vault",
      ])
    ).rejects.toThrow("connection reset");

    expect(mockSrcConn.close).toHaveBeenCalled();
    expect(mockTgtConn.close).toHaveBeenCalled();
  });

  it("honours --source-db and --target-db overrides", async () => {
    const { mockSrcConn, mockTgtConn } = await getMocks();

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "diff", "source", "target",
      "--schema",
      "--source-db", "custom_src",
      "--target-db", "custom_tgt",
      "--path", "/tmp/vault",
    ]);

    expect(buildPlan).toHaveBeenCalledWith(
      mockSrcConn.pool,
      mockTgtConn.pool,
      "custom_src",
      "custom_tgt",
      expect.anything()
    );
  });
});
