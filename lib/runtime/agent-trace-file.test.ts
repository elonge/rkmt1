import test from "node:test";
import assert from "node:assert/strict";
import { readFile, rm } from "node:fs/promises";
import { resolveAgentTraceFilePath, writeAgentToolTraceFile } from "./agent-trace-file.ts";

test("writeAgentToolTraceFile persists agent tool calls to a shareable JSON file", async () => {
  process.env.PLAN_STORE_DIR = "/tmp/rkmt-agent-trace-test";
  await rm(process.env.PLAN_STORE_DIR, { recursive: true, force: true });

  const filePath = await writeAgentToolTraceFile({
    jobId: "job-123",
    stepId: "step-1",
    stepTitle: "Build audience",
    stepToolId: "audience_builder_agent",
    agentToolCalls: [
      {
        id: "message_search-1",
        tool: "Message Search",
        toolId: "message_search",
        status: "completed",
        input: { query: "security", searchMode: "vector" },
        output: { matchedMessages: 12 },
        debugLogs: [
          {
            message: "Fetched vector message candidates from Atlas.",
            data: { candidates: 25 },
          },
        ],
      },
    ],
  });

  assert.equal(filePath, resolveAgentTraceFilePath("job-123", "step-1"));

  const payload = JSON.parse(await readFile(filePath, "utf8")) as {
    jobId: string;
    stepId: string;
    stepTitle: string;
    stepToolId: string;
    agentToolCalls: Array<{ id: string; debugLogs: Array<{ message: string }> }>;
  };

  assert.equal(payload.jobId, "job-123");
  assert.equal(payload.stepId, "step-1");
  assert.equal(payload.stepTitle, "Build audience");
  assert.equal(payload.stepToolId, "audience_builder_agent");
  assert.equal(payload.agentToolCalls[0]?.id, "message_search-1");
  assert.equal(
    payload.agentToolCalls[0]?.debugLogs[0]?.message,
    "Fetched vector message candidates from Atlas.",
  );

  delete process.env.PLAN_STORE_DIR;
  await rm("/tmp/rkmt-agent-trace-test", { recursive: true, force: true });
});
