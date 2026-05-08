import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/integration/**/*.integration.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // Run integration tests serially — database operations must not interleave
    pool: "forks",
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
  },
});
