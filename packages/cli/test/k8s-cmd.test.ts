import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

// ---------------------------------------------------------------------------
// Mocks (hoisted)
// ---------------------------------------------------------------------------

// vi.hoisted() runs before imports, so variables declared here can safely be
// referenced inside vi.mock() factory functions.
const { mockK8sInstance, MockK8sClient } = vi.hoisted(() => {
  const mockK8sInstance = {
    listContexts: vi.fn().mockReturnValue([]),
    listNamespaces: vi.fn().mockResolvedValue([]),
    listPods: vi.fn().mockResolvedValue([]),
    listServices: vi.fn().mockResolvedValue([]),
  };
  const MockK8sClient = vi.fn().mockImplementation(() => mockK8sInstance);
  return { mockK8sInstance, MockK8sClient };
});

vi.mock("@db-mirror/core", () => ({
  k8s: {
    K8sClient: MockK8sClient,
  },
}));

// ---------------------------------------------------------------------------
// Imports
// ---------------------------------------------------------------------------

import { k8sCommand } from "../src/commands/k8s-cmd.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeCmd() {
  return k8sCommand();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("k8s contexts", () => {
  let logSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    mockK8sInstance.listContexts.mockReturnValue(["minikube", "prod-cluster"]);
  });

  afterEach(() => {
    vi.clearAllMocks();
    logSpy.mockRestore();
  });

  it("instantiates K8sClient and calls listContexts", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync(["node", "k8s", "contexts"]);

    expect(MockK8sClient).toHaveBeenCalledWith(undefined);
    expect(mockK8sInstance.listContexts).toHaveBeenCalled();
  });

  it("prints each context name to stdout", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync(["node", "k8s", "contexts"]);

    expect(logSpy).toHaveBeenCalledWith("minikube");
    expect(logSpy).toHaveBeenCalledWith("prod-cluster");
  });

  it("passes --kubeconfig path to K8sClient constructor", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync(["node", "k8s", "contexts", "--kubeconfig", "/home/user/.kube/config"]);

    expect(MockK8sClient).toHaveBeenCalledWith("/home/user/.kube/config");
  });
});

describe("k8s namespaces", () => {
  let logSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    mockK8sInstance.listNamespaces.mockResolvedValue(["default", "kube-system", "app-ns"]);
  });

  afterEach(() => {
    vi.clearAllMocks();
    logSpy.mockRestore();
  });

  it("instantiates K8sClient and calls listNamespaces with the given context", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync(["node", "k8s", "namespaces", "--context", "minikube"]);

    expect(MockK8sClient).toHaveBeenCalledWith(undefined);
    expect(mockK8sInstance.listNamespaces).toHaveBeenCalledWith("minikube");
  });

  it("prints each namespace to stdout", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync(["node", "k8s", "namespaces", "--context", "minikube"]);

    expect(logSpy).toHaveBeenCalledWith("default");
    expect(logSpy).toHaveBeenCalledWith("kube-system");
    expect(logSpy).toHaveBeenCalledWith("app-ns");
  });

  it("passes --kubeconfig to constructor alongside --context", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "k8s", "namespaces",
      "--context", "prod-cluster",
      "--kubeconfig", "/custom/kube",
    ]);

    expect(MockK8sClient).toHaveBeenCalledWith("/custom/kube");
    expect(mockK8sInstance.listNamespaces).toHaveBeenCalledWith("prod-cluster");
  });
});

describe("k8s pods", () => {
  let logSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    mockK8sInstance.listPods.mockResolvedValue(["mysql-0", "mysql-1", "redis-0"]);
  });

  afterEach(() => {
    vi.clearAllMocks();
    logSpy.mockRestore();
  });

  it("calls listPods with namespace and context", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "k8s", "pods",
      "--context", "minikube",
      "--namespace", "production",
    ]);

    expect(mockK8sInstance.listPods).toHaveBeenCalledWith("production", "minikube");
  });

  it("prints each pod name to stdout", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "k8s", "pods",
      "--context", "minikube",
      "--namespace", "production",
    ]);

    expect(logSpy).toHaveBeenCalledWith("mysql-0");
    expect(logSpy).toHaveBeenCalledWith("mysql-1");
    expect(logSpy).toHaveBeenCalledWith("redis-0");
  });

  it("prints nothing when no pods exist", async () => {
    mockK8sInstance.listPods.mockResolvedValue([]);

    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "k8s", "pods",
      "--context", "minikube",
      "--namespace", "empty-ns",
    ]);

    expect(logSpy).not.toHaveBeenCalled();
  });
});

describe("k8s services", () => {
  let logSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    mockK8sInstance.listServices.mockResolvedValue(["mysql", "redis", "app"]);
  });

  afterEach(() => {
    vi.clearAllMocks();
    logSpy.mockRestore();
  });

  it("calls listServices with namespace and context", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "k8s", "services",
      "--context", "prod-cluster",
      "--namespace", "production",
    ]);

    expect(mockK8sInstance.listServices).toHaveBeenCalledWith("production", "prod-cluster");
  });

  it("prints each service name to stdout", async () => {
    const cmd = makeCmd();
    await cmd.parseAsync([
      "node", "k8s", "services",
      "--context", "prod-cluster",
      "--namespace", "production",
    ]);

    expect(logSpy).toHaveBeenCalledWith("mysql");
    expect(logSpy).toHaveBeenCalledWith("redis");
    expect(logSpy).toHaveBeenCalledWith("app");
  });
});
