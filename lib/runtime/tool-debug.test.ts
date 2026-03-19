import test from "node:test";
import assert from "node:assert/strict";
import { logToolDebug, runWithToolDebugLogging } from "./tool-debug.ts";

test("tool debug logging propagates through awaited calls", async () => {
  const logs: Array<{ message: string; data?: unknown }> = [];

  await runWithToolDebugLogging(async (message, data) => {
    logs.push({ message, data });
  }, async () => {
    await Promise.resolve();
    await logToolDebug("first message", { count: 2 });
    await logToolDebug("second message");
  });

  assert.deepEqual(logs, [
    { message: "first message", data: { count: 2 } },
    { message: "second message", data: undefined },
  ]);
});

test("tool debug logging is a no-op outside a traced context", async () => {
  await assert.doesNotReject(async () => {
    await logToolDebug("outside context", { ignored: true });
  });
});
