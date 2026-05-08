import { describe, it, expect, vi, beforeEach } from "vitest";

// ── Static mocks ──────────────────────────────────────────────────────────
// vi.mock factories are hoisted before any imports. The factories must be
// self-contained (no references to outer let/const that are in TDZ).
// We retrieve the spies later via the mocked modules themselves.

vi.mock("mysql2/promise", () => {
  const pool = { end: vi.fn().mockResolvedValue(undefined) };
  const createPool = vi.fn().mockReturnValue(pool);
  return { default: { createPool } };
});

vi.mock("../src/ssh/tunnel.js", () => {
  const close = vi.fn().mockResolvedValue(undefined);
  const tunnel = { localPort: 12345, close };
  const createSshTunnel = vi.fn().mockResolvedValue(tunnel);
  return { createSshTunnel };
});

vi.mock("../src/docker/docker.js", () => {
  const getContainerPort = vi.fn().mockResolvedValue(55000);
  return { getContainerPort };
});

vi.mock("../src/k8s/port-forward.js", () => {
  const fwdClose = vi.fn().mockResolvedValue(undefined);
  const fwd = { localPort: 23456, close: fwdClose };
  const startPortForward = vi.fn().mockResolvedValue(fwd);
  const resolveSecretValue = vi.fn().mockResolvedValue("resolved-value");
  // expose on K8sClient so tests can access them
  const K8sClient = vi.fn().mockImplementation(() => ({ startPortForward, resolveSecretValue }));
  (K8sClient as any)._fwd = fwd;
  (K8sClient as any)._fwdClose = fwdClose;
  (K8sClient as any)._startPortForward = startPortForward;
  (K8sClient as any)._resolveSecretValue = resolveSecretValue;
  return { K8sClient };
});

// ── Imports (after mocks) ─────────────────────────────────────────────────

import mysql from "mysql2/promise";
import * as sshMod from "../src/ssh/tunnel.js";
import * as dockerMod from "../src/docker/docker.js";
import { K8sClient } from "../src/k8s/port-forward.js";
import { openConnection } from "../src/connection/connection.js";
import type { DirectProfile, SshProfile, DockerProfile, K8sProfile } from "../src/vault/types.js";

// ── Typed spy accessors ───────────────────────────────────────────────────

function getCreatePool() {
  return mysql.createPool as ReturnType<typeof vi.fn>;
}

function getPool() {
  return getCreatePool().mock.results[0]?.value as { end: ReturnType<typeof vi.fn> } | undefined;
}

function getCreateSshTunnel() {
  return sshMod.createSshTunnel as ReturnType<typeof vi.fn>;
}

function getGetContainerPort() {
  return dockerMod.getContainerPort as ReturnType<typeof vi.fn>;
}

function getK8sSpies() {
  const K = K8sClient as any;
  return {
    startPortForward: K._startPortForward as ReturnType<typeof vi.fn>,
    resolveSecretValue: K._resolveSecretValue as ReturnType<typeof vi.fn>,
    fwd: K._fwd as { localPort: number; close: ReturnType<typeof vi.fn> },
    fwdClose: K._fwdClose as ReturnType<typeof vi.fn>,
  };
}

// ── Profile factories ─────────────────────────────────────────────────────

function directProfile(overrides?: Partial<DirectProfile>): DirectProfile {
  return {
    kind: "direct",
    id: "p1",
    name: "local",
    host: "db.example.com",
    port: 3306,
    user: "root",
    password: "pass",
    database: "mydb",
    ...overrides,
  };
}

function sshProfile(overrides?: Partial<SshProfile>): SshProfile {
  return {
    kind: "ssh",
    id: "p2",
    name: "ssh-conn",
    sshHost: "bastion.example.com",
    sshPort: 22,
    sshUser: "ec2-user",
    host: "db-internal.example.com",
    port: 3306,
    user: "admin",
    password: "dbpass",
    database: "production",
    ...overrides,
  };
}

function dockerProfile(overrides?: Partial<DockerProfile>): DockerProfile {
  return {
    kind: "docker",
    id: "p3",
    name: "docker-conn",
    containerId: "abc123def456",
    containerName: "mysql-container",
    internalPort: 3306,
    user: "root",
    password: "rootpass",
    database: "testdb",
    ...overrides,
  };
}

function k8sProfile(overrides?: Partial<K8sProfile>): K8sProfile {
  return {
    kind: "k8s",
    id: "p4",
    name: "k8s-conn",
    context: "my-context",
    namespace: "default",
    target: { kind: "pod", name: "mysql-pod-abc" },
    remotePort: 3306,
    user: "admin",
    password: "k8spass",
    database: "appdb",
    ...overrides,
  };
}

// ── Setup ─────────────────────────────────────────────────────────────────

beforeEach(() => {
  vi.clearAllMocks();

  // Re-apply default return values after clearAllMocks wipes them
  const createPool = getCreatePool();
  const poolMock = { end: vi.fn().mockResolvedValue(undefined) };
  createPool.mockReturnValue(poolMock);

  getCreateSshTunnel().mockResolvedValue({ localPort: 12345, close: vi.fn().mockResolvedValue(undefined) });
  getGetContainerPort().mockResolvedValue(55000);

  const { startPortForward, resolveSecretValue, fwd, fwdClose } = getK8sSpies();
  fwdClose.mockResolvedValue(undefined);
  fwd.localPort = 23456;
  startPortForward.mockResolvedValue(fwd);
  resolveSecretValue.mockResolvedValue("resolved-value");
});

// ── Direct ─────────────────────────────────────────────────────────────────

describe("openConnection – direct", () => {
  it("calls createPool with the correct parameters", async () => {
    const conn = await openConnection(directProfile());
    expect(getCreatePool()).toHaveBeenCalledWith(
      expect.objectContaining({
        host: "db.example.com",
        port: 3306,
        user: "root",
        password: "pass",
        database: "mydb",
        multipleStatements: true,
        dateStrings: true,
        timezone: "+00:00",
        connectionLimit: 4,
      }),
    );
    await conn.close();
  });

  it("sets ssl:{} when tls is true", async () => {
    const conn = await openConnection(directProfile({ tls: true }));
    expect(getCreatePool()).toHaveBeenCalledWith(expect.objectContaining({ ssl: {} }));
    await conn.close();
  });

  it("omits ssl when tls is false/undefined", async () => {
    const conn = await openConnection(directProfile({ tls: false }));
    expect(getCreatePool()).toHaveBeenCalledWith(expect.objectContaining({ ssl: undefined }));
    await conn.close();
  });

  it("label includes profile name, host and port", async () => {
    const conn = await openConnection(directProfile());
    expect(conn.label).toContain("local");
    expect(conn.label).toContain("db.example.com");
    expect(conn.label).toContain("3306");
    await conn.close();
  });

  it("close() calls pool.end()", async () => {
    const conn = await openConnection(directProfile());
    const pool = getPool()!;
    await conn.close();
    expect(pool.end).toHaveBeenCalledOnce();
  });

  it("returns the pool from mysql.createPool", async () => {
    const conn = await openConnection(directProfile());
    expect(conn.pool).toBe(getPool());
    await conn.close();
  });
});

// ── SSH ────────────────────────────────────────────────────────────────────

describe("openConnection – ssh", () => {
  it("calls createSshTunnel with the correct parameters", async () => {
    const conn = await openConnection(sshProfile());
    expect(getCreateSshTunnel()).toHaveBeenCalledWith({
      sshHost: "bastion.example.com",
      sshPort: 22,
      sshUser: "ec2-user",
      privateKeyPath: undefined,
      password: undefined,
      remoteHost: "db-internal.example.com",
      remotePort: 3306,
    });
    await conn.close();
  });

  it("passes privateKeyPath when set", async () => {
    const conn = await openConnection(sshProfile({ sshPrivateKeyPath: "/home/user/.ssh/id_rsa" }));
    expect(getCreateSshTunnel()).toHaveBeenCalledWith(
      expect.objectContaining({ privateKeyPath: "/home/user/.ssh/id_rsa" }),
    );
    await conn.close();
  });

  it("creates pool on 127.0.0.1 using tunnel.localPort", async () => {
    getCreateSshTunnel().mockResolvedValueOnce({ localPort: 12345, close: vi.fn().mockResolvedValue(undefined) });
    const conn = await openConnection(sshProfile());
    expect(getCreatePool()).toHaveBeenCalledWith(
      expect.objectContaining({ host: "127.0.0.1", port: 12345 }),
    );
    await conn.close();
  });

  it("label includes sshUser, sshHost, remote host and port", async () => {
    const conn = await openConnection(sshProfile());
    expect(conn.label).toContain("ec2-user");
    expect(conn.label).toContain("bastion.example.com");
    expect(conn.label).toContain("db-internal.example.com");
    expect(conn.label).toContain("3306");
    await conn.close();
  });

  it("close() calls pool.end() and tunnel.close()", async () => {
    const tunnelClose = vi.fn().mockResolvedValue(undefined);
    getCreateSshTunnel().mockResolvedValueOnce({ localPort: 12345, close: tunnelClose });
    const conn = await openConnection(sshProfile());
    await conn.close();
    expect(getPool()?.end ?? vi.fn()).toHaveBeenCalled();
    expect(tunnelClose).toHaveBeenCalled();
  });

  it("calls tunnel.close() when createPool throws, then rethrows", async () => {
    const tunnelClose = vi.fn().mockResolvedValue(undefined);
    getCreateSshTunnel().mockResolvedValueOnce({ localPort: 12345, close: tunnelClose });
    getCreatePool().mockImplementationOnce(() => {
      throw new Error("createPool failed");
    });
    await expect(openConnection(sshProfile())).rejects.toThrow("createPool failed");
    expect(tunnelClose).toHaveBeenCalledOnce();
  });
});

// ── Docker ─────────────────────────────────────────────────────────────────

describe("openConnection – docker", () => {
  it("calls getContainerPort with containerId and internalPort", async () => {
    const conn = await openConnection(dockerProfile());
    expect(getGetContainerPort()).toHaveBeenCalledWith("abc123def456", 3306);
    await conn.close();
  });

  it("creates pool on 127.0.0.1 with the port from getContainerPort", async () => {
    getGetContainerPort().mockResolvedValueOnce(55001);
    const conn = await openConnection(dockerProfile());
    expect(getCreatePool()).toHaveBeenCalledWith(
      expect.objectContaining({ host: "127.0.0.1", port: 55001 }),
    );
    await conn.close();
  });

  it("label includes containerName and mapped port", async () => {
    getGetContainerPort().mockResolvedValueOnce(55002);
    const conn = await openConnection(dockerProfile());
    expect(conn.label).toContain("mysql-container");
    expect(conn.label).toContain("55002");
    await conn.close();
  });

  it("close() calls pool.end()", async () => {
    const conn = await openConnection(dockerProfile());
    const pool = getPool()!;
    await conn.close();
    expect(pool.end).toHaveBeenCalledOnce();
  });

  it("propagates getContainerPort errors", async () => {
    getGetContainerPort().mockRejectedValueOnce(new Error("no port mapping"));
    await expect(openConnection(dockerProfile())).rejects.toThrow("no port mapping");
  });
});

// ── K8s ────────────────────────────────────────────────────────────────────

describe("openConnection – k8s", () => {
  it("calls startPortForward with correct options", async () => {
    const { startPortForward } = getK8sSpies();
    const conn = await openConnection(k8sProfile());
    expect(startPortForward).toHaveBeenCalledWith({
      context: "my-context",
      namespace: "default",
      target: { kind: "pod", name: "mysql-pod-abc" },
      remotePort: 3306,
    });
    await conn.close();
  });

  it("creates pool on 127.0.0.1 using fwd.localPort", async () => {
    const { fwd } = getK8sSpies();
    fwd.localPort = 23456;
    const conn = await openConnection(k8sProfile());
    expect(getCreatePool()).toHaveBeenCalledWith(
      expect.objectContaining({ host: "127.0.0.1", port: 23456 }),
    );
    await conn.close();
  });

  it("uses profile.user/password directly when no secret refs", async () => {
    const { resolveSecretValue } = getK8sSpies();
    const conn = await openConnection(k8sProfile({ user: "k8s-user", password: "k8s-pw" }));
    expect(resolveSecretValue).not.toHaveBeenCalled();
    expect(getCreatePool()).toHaveBeenCalledWith(
      expect.objectContaining({ user: "k8s-user", password: "k8s-pw" }),
    );
    await conn.close();
  });

  it("resolves userFrom secret when set", async () => {
    const { resolveSecretValue } = getK8sSpies();
    resolveSecretValue.mockImplementation((_ns: string, ref: { key: string }) =>
      ref.key === "username" ? Promise.resolve("secret-user") : Promise.resolve("other"),
    );
    const conn = await openConnection(
      k8sProfile({ userFrom: { secretName: "db-secret", key: "username" } }),
    );
    expect(resolveSecretValue).toHaveBeenCalledWith(
      "default",
      { secretName: "db-secret", key: "username" },
      "my-context",
    );
    expect(getCreatePool()).toHaveBeenCalledWith(expect.objectContaining({ user: "secret-user" }));
    await conn.close();
  });

  it("resolves passwordFrom secret when set", async () => {
    const { resolveSecretValue } = getK8sSpies();
    resolveSecretValue.mockImplementation((_ns: string, ref: { key: string }) =>
      ref.key === "password" ? Promise.resolve("secret-pass") : Promise.resolve("other"),
    );
    const conn = await openConnection(
      k8sProfile({ passwordFrom: { secretName: "db-secret", key: "password" } }),
    );
    expect(getCreatePool()).toHaveBeenCalledWith(
      expect.objectContaining({ password: "secret-pass" }),
    );
    await conn.close();
  });

  it("label includes context, namespace and target name", async () => {
    const conn = await openConnection(k8sProfile());
    expect(conn.label).toContain("my-context");
    expect(conn.label).toContain("default");
    expect(conn.label).toContain("mysql-pod-abc");
    await conn.close();
  });

  it("close() calls pool.end() and fwd.close()", async () => {
    const { fwdClose } = getK8sSpies();
    const conn = await openConnection(k8sProfile());
    const pool = getPool()!;
    await conn.close();
    expect(pool.end).toHaveBeenCalled();
    expect(fwdClose).toHaveBeenCalled();
  });

  it("calls fwd.close() when createPool throws after port-forward succeeds", async () => {
    const { fwdClose, fwd, startPortForward } = getK8sSpies();
    startPortForward.mockResolvedValueOnce({ localPort: 23456, close: fwdClose });
    getCreatePool().mockImplementationOnce(() => {
      throw new Error("pool init failed");
    });
    await expect(openConnection(k8sProfile())).rejects.toThrow("pool init failed");
    expect(fwdClose).toHaveBeenCalled();
  });

  it("does NOT call fwd.close() when startPortForward itself throws (fwd never assigned)", async () => {
    const { startPortForward, fwdClose } = getK8sSpies();
    startPortForward.mockRejectedValueOnce(new Error("port-forward failed"));
    await expect(openConnection(k8sProfile())).rejects.toThrow("port-forward failed");
    expect(fwdClose).not.toHaveBeenCalled();
  });
});
