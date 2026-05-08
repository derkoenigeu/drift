import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock node:child_process before any imports that use it
vi.mock("node:child_process", () => ({
  execFile: vi.fn(),
}));

// Mock node:util to return our mock execFile wrapped in a passthrough promisify
vi.mock("node:util", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:util")>();
  return {
    ...actual,
    promisify:
      (fn: (...args: unknown[]) => void) =>
      (...args: unknown[]) =>
        new Promise((resolve, reject) => {
          fn(...args, (err: unknown, result: unknown) => {
            if (err) reject(err);
            else resolve(result);
          });
        }),
  };
});

import { execFile } from "node:child_process";
import { listMysqlContainers, getContainerPort, getContainerEnvs } from "../src/docker/docker.js";

const mockExecFile = execFile as unknown as ReturnType<typeof vi.fn>;

function makeExecFile(stdout: string) {
  return (_cmd: string, _args: string[], _opts: unknown, cb: (err: null, result: { stdout: string }) => void) => {
    cb(null, { stdout });
  };
}

function makeExecFileError(err: Error) {
  return (_cmd: string, _args: string[], _opts: unknown, cb: (err: Error) => void) => {
    cb(err);
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("listMysqlContainers", () => {
  it("returns empty array when docker command errors", async () => {
    mockExecFile.mockImplementation(makeExecFileError(new Error("docker not found")));
    const result = await listMysqlContainers();
    expect(result).toEqual([]);
  });

  it("returns empty array when stdout is empty", async () => {
    mockExecFile.mockImplementation(makeExecFile(""));
    const result = await listMysqlContainers();
    expect(result).toEqual([]);
  });

  it("filters out non-mysql/mariadb images", async () => {
    const stdout = [
      "abc123def456\tredis-container\tredis:7\t0.0.0.0:6379->6379/tcp",
      "111222333444\tpg-container\tpostgres:15\t0.0.0.0:5432->5432/tcp",
    ].join("\n");
    mockExecFile.mockImplementation(makeExecFile(stdout));
    const result = await listMysqlContainers();
    expect(result).toEqual([]);
  });

  it("includes containers with 'mysql' in image name", async () => {
    const stdout = "abc123def456789\tmysql-db\tmysql:8.0\t0.0.0.0:3307->3306/tcp";
    mockExecFile.mockImplementation(makeExecFile(stdout));
    const result = await listMysqlContainers();
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe("mysql-db");
    expect(result[0].image).toBe("mysql:8.0");
    expect(result[0].id).toBe("abc123def456"); // sliced to 12 chars
  });

  it("includes containers with 'mariadb' in image name", async () => {
    const stdout = "aaabbbcccdddee\tmariadb-db\tmariadb:10.11\t0.0.0.0:3308->3306/tcp";
    mockExecFile.mockImplementation(makeExecFile(stdout));
    const result = await listMysqlContainers();
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe("mariadb-db");
  });

  it("parses port mappings correctly", async () => {
    const stdout = "abc123def456789\tmysql-db\tmysql:8.0\t0.0.0.0:3307->3306/tcp";
    mockExecFile.mockImplementation(makeExecFile(stdout));
    const result = await listMysqlContainers();
    expect(result[0].ports).toEqual([{ external: 3307, internal: 3306 }]);
  });

  it("parses multiple port mappings", async () => {
    const stdout = "abc123def456789\tmysql-db\tmysql:8.0\t0.0.0.0:3307->3306/tcp, 0.0.0.0:33060->33060/tcp";
    mockExecFile.mockImplementation(makeExecFile(stdout));
    const result = await listMysqlContainers();
    expect(result[0].ports).toHaveLength(2);
    expect(result[0].ports[0]).toEqual({ external: 3307, internal: 3306 });
    expect(result[0].ports[1]).toEqual({ external: 33060, internal: 33060 });
  });

  it("handles container with no port mappings", async () => {
    const stdout = "abc123def456789\tmysql-db\tmysql:8.0\t";
    mockExecFile.mockImplementation(makeExecFile(stdout));
    const result = await listMysqlContainers();
    expect(result[0].ports).toEqual([]);
  });

  it("handles multiple containers, filtering correctly", async () => {
    const stdout = [
      "aaa111bbb222cc\tmysql-db\tmysql:8.0\t0.0.0.0:3307->3306/tcp",
      "bbb222ccc333dd\tredis\tredis:7\t0.0.0.0:6379->6379/tcp",
      "ccc333ddd444ee\tmariadb\tmariadb:10\t0.0.0.0:3308->3306/tcp",
    ].join("\n");
    mockExecFile.mockImplementation(makeExecFile(stdout));
    const result = await listMysqlContainers();
    expect(result).toHaveLength(2);
    expect(result[0].name).toBe("mysql-db");
    expect(result[1].name).toBe("mariadb");
  });

  it("truncates container ID to 12 characters", async () => {
    const stdout = "abcdef1234567890\tmysql-db\tmysql:8.0\t";
    mockExecFile.mockImplementation(makeExecFile(stdout));
    const result = await listMysqlContainers();
    expect(result[0].id).toBe("abcdef123456");
    expect(result[0].id).toHaveLength(12);
  });

  it("uses case-insensitive image matching (uppercase MYSQL)", async () => {
    const stdout = "abc123def456789\ttest\tMYSQL:8.0\t";
    mockExecFile.mockImplementation(makeExecFile(stdout));
    const result = await listMysqlContainers();
    expect(result).toHaveLength(1);
  });

  it("handles trailing newline in stdout", async () => {
    const stdout = "abc123def456789\tmysql-db\tmysql:8.0\t0.0.0.0:3307->3306/tcp\n";
    mockExecFile.mockImplementation(makeExecFile(stdout));
    const result = await listMysqlContainers();
    expect(result).toHaveLength(1);
  });
});

describe("getContainerPort", () => {
  it("parses port number from docker port output", async () => {
    mockExecFile.mockImplementation(makeExecFile("0.0.0.0:3307\n"));
    const port = await getContainerPort("abc123", 3306);
    expect(port).toBe(3307);
  });

  it("parses port when output has no host prefix", async () => {
    mockExecFile.mockImplementation(makeExecFile(":::3308\n"));
    const port = await getContainerPort("abc123", 3306);
    expect(port).toBe(3308);
  });

  it("throws when stdout has no port mapping pattern", async () => {
    mockExecFile.mockImplementation(makeExecFile("Error: No public port '3306' published for abc123\n"));
    await expect(getContainerPort("abc123", 3306)).rejects.toThrow("No port mapping found for container abc123:3306");
  });

  it("throws when stdout is empty", async () => {
    mockExecFile.mockImplementation(makeExecFile(""));
    await expect(getContainerPort("abc123", 3306)).rejects.toThrow("No port mapping found");
  });

  it("propagates docker command errors", async () => {
    mockExecFile.mockImplementation(makeExecFileError(new Error("container not found")));
    await expect(getContainerPort("nonexistent", 3306)).rejects.toThrow("container not found");
  });

  it("calls docker port with the correct containerId and port", async () => {
    mockExecFile.mockImplementation(makeExecFile("0.0.0.0:5555\n"));
    await getContainerPort("mycontainer", 3306);
    expect(mockExecFile).toHaveBeenCalledWith(
      "docker",
      ["port", "mycontainer", "3306"],
      expect.any(Object),
      expect.any(Function),
    );
  });
});

describe("getContainerEnvs", () => {
  it("parses KEY=VALUE env vars from docker inspect output", async () => {
    const envJson = JSON.stringify(["MYSQL_ROOT_PASSWORD=secret", "MYSQL_DATABASE=mydb", "PATH=/usr/bin:/bin"]);
    mockExecFile.mockImplementation(makeExecFile(envJson + "\n"));
    const envs = await getContainerEnvs("abc123");
    expect(envs).toEqual({
      MYSQL_ROOT_PASSWORD: "secret",
      MYSQL_DATABASE: "mydb",
      PATH: "/usr/bin:/bin",
    });
  });

  it("handles values containing '=' correctly", async () => {
    const envJson = JSON.stringify(["FOO=bar=baz"]);
    mockExecFile.mockImplementation(makeExecFile(envJson));
    const envs = await getContainerEnvs("abc123");
    expect(envs.FOO).toBe("bar=baz");
  });

  it("returns {} when docker command fails", async () => {
    mockExecFile.mockImplementation(makeExecFileError(new Error("container not found")));
    const envs = await getContainerEnvs("nonexistent");
    expect(envs).toEqual({});
  });

  it("returns {} when stdout is invalid JSON", async () => {
    mockExecFile.mockImplementation(makeExecFile("not valid json\n"));
    const envs = await getContainerEnvs("abc123");
    expect(envs).toEqual({});
  });

  it("returns {} when env array is empty", async () => {
    mockExecFile.mockImplementation(makeExecFile("[]"));
    const envs = await getContainerEnvs("abc123");
    expect(envs).toEqual({});
  });

  it("skips entries without '=' separator", async () => {
    const envJson = JSON.stringify(["VALID=yes", "NOEQUALSIGN", "ANOTHER=value"]);
    mockExecFile.mockImplementation(makeExecFile(envJson));
    const envs = await getContainerEnvs("abc123");
    expect(envs).not.toHaveProperty("NOEQUALSIGN");
    expect(envs.VALID).toBe("yes");
    expect(envs.ANOTHER).toBe("value");
  });
});
