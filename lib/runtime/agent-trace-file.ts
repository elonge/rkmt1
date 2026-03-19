import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { AgentToolCall } from "../types.ts";
import { resolveRuntimeDir } from "./runtime-paths.ts";

type AgentTraceFileInput = {
  jobId: string;
  stepId: string;
  stepTitle: string;
  stepToolId: string;
  agentToolCalls: AgentToolCall[];
};

function sanitizePathSegment(value: string): string {
  return value.replace(/[^a-zA-Z0-9._-]+/g, "-");
}

export function resolveAgentTraceFilePath(jobId: string, stepId: string): string {
  return join(
    resolveRuntimeDir(),
    "agent-tool-traces",
    sanitizePathSegment(jobId),
    `${sanitizePathSegment(stepId)}.json`,
  );
}

export async function writeAgentToolTraceFile({
  jobId,
  stepId,
  stepTitle,
  stepToolId,
  agentToolCalls,
}: AgentTraceFileInput): Promise<string> {
  const filePath = resolveAgentTraceFilePath(jobId, stepId);
  await mkdir(join(resolveRuntimeDir(), "agent-tool-traces", sanitizePathSegment(jobId)), {
    recursive: true,
  });
  await writeFile(
    filePath,
    JSON.stringify(
      {
        jobId,
        stepId,
        stepTitle,
        stepToolId,
        updatedAt: new Date().toISOString(),
        agentToolCalls,
      },
      null,
      2,
    ) + "\n",
    "utf8",
  );
  return filePath;
}
