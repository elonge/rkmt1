import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync } from "node:fs";
import { readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { Collection } from "mongodb";
import { getToolDefinition } from "./agents/tools";
import { getMongoDb } from "./data/mongo";
import { resolveRuntimeDir } from "./runtime/runtime-paths.ts";
import {
  AgentToolCall,
  clonePlanJob,
  ExecutionArtifact,
  FinalAnswer,
  normalizeDbStatsQueryInput,
  PlanDraft,
  PlanJob,
  PlanJobStatus,
} from "./types";

const STORE_DIR = resolveRuntimeDir();
const STORE_FILE = join(STORE_DIR, "plan-jobs.json");

type PlanJobsById = Record<string, PlanJob>;
type StoredPlanJob = PlanJob & { _id: string };

function shouldUseMongoPlanStore(): boolean {
  return Boolean(process.env.MONGO_URI?.trim());
}

function readPlanJobsCollectionName(): string {
  return (
    process.env.MONGO_PLAN_JOBS_COLLECTION?.trim() ||
    process.env.PLAN_JOBS_COLLECTION?.trim() ||
    "plan_jobs"
  );
}

async function getPlanJobsCollection(): Promise<Collection<StoredPlanJob>> {
  const db = await getMongoDb();
  return db.collection<StoredPlanJob>(readPlanJobsCollectionName());
}

function toStoredPlanJob(job: PlanJob): StoredPlanJob {
  const normalized = normalizeJob(job);
  return {
    _id: normalized.id,
    ...normalized,
  };
}

function fromStoredPlanJob(job: StoredPlanJob | null): PlanJob | null {
  if (!job) {
    return null;
  }

  const { _id: _ignored, ...rest } = job;
  return normalizeJob(rest as PlanJob);
}

function inferToolIdFromOwner(owner: string): string {
  switch (owner) {
    case "planner":
      return "planning_notes";
    case "stats_query":
      return "db_stats_query";
    case "audience_builder":
      return "audience_builder_agent";
    case "narrative_explorer":
      return "narrative_explorer_agent";
    case "synthesizer":
      return "synthesis_agent";
    default:
      return "planning_notes";
  }
}

function normalizeStep(step: Record<string, unknown>): Record<string, unknown> {
  const owner = typeof step.owner === "string" ? step.owner : "planner";
  const toolId =
    typeof step.toolId === "string" && step.toolId.length > 0
      ? step.toolId
      : inferToolIdFromOwner(owner);
  const toolDefinition = getToolDefinition(toolId);
  const rawArgs =
    step.args && typeof step.args === "object" && !Array.isArray(step.args)
      ? (step.args as Record<string, unknown>)
      : {};
  const normalizedArgs =
    toolId === "db_stats_query" ? normalizeDbStatsQueryInput(rawArgs) : rawArgs;

  return {
    ...step,
    owner: toolDefinition?.owner ?? owner,
    toolId,
    tool:
      typeof step.tool === "string" && step.tool.length > 0
        ? step.tool
        : toolDefinition?.label ?? toolId,
    args: normalizedArgs,
    dependsOn: Array.isArray(step.dependsOn)
      ? step.dependsOn.filter((value): value is string => typeof value === "string")
      : [],
    inputBindings:
      step.inputBindings && typeof step.inputBindings === "object" && !Array.isArray(step.inputBindings)
        ? step.inputBindings
        : {},
    agentToolCalls: Array.isArray(step.agentToolCalls)
      ? step.agentToolCalls.map((agentToolCall) =>
          normalizeAgentToolCall(agentToolCall as Record<string, unknown>),
        )
      : [],
  };
}

function normalizeAgentToolCall(agentToolCall: Record<string, unknown>): Record<string, unknown> {
  return {
    ...agentToolCall,
    debugLogs: Array.isArray(agentToolCall.debugLogs) ? agentToolCall.debugLogs : [],
  };
}

function normalizeArtifact(artifact: Record<string, unknown>): Record<string, unknown> {
  const owner = typeof artifact.owner === "string" ? artifact.owner : "planner";
  const toolId =
    typeof artifact.toolId === "string" && artifact.toolId.length > 0
      ? artifact.toolId
      : inferToolIdFromOwner(owner);
  const toolDefinition = getToolDefinition(toolId);

  return {
    ...artifact,
    owner: toolDefinition?.owner ?? owner,
    toolId,
    tool:
      typeof artifact.tool === "string" && artifact.tool.length > 0
        ? artifact.tool
        : toolDefinition?.label ?? toolId,
    agentToolCalls: Array.isArray(artifact.agentToolCalls)
      ? artifact.agentToolCalls.map((agentToolCall) =>
          normalizeAgentToolCall(agentToolCall as Record<string, unknown>),
        )
      : [],
  };
}

function normalizeJob(job: PlanJob): PlanJob {
  return {
    ...job,
    plan: {
      ...job.plan,
      steps: job.plan.steps.map((step) => normalizeStep(step as unknown as Record<string, unknown>) as any),
    },
    artifacts: job.artifacts.map((artifact) =>
      normalizeArtifact(artifact as unknown as Record<string, unknown>) as any,
    ),
  };
}

function nowIso(): string {
  return new Date().toISOString();
}

function resetExecutionState(current: PlanJob, status: PlanJobStatus): PlanJob {
  current.plan.steps = current.plan.steps.map((step) => ({
    ...step,
    status: "pending",
    outputSummary: undefined,
    agentToolCalls: [],
  }));
  current.artifacts = [];
  current.finalAnswer = undefined;
  current.error = undefined;
  current.status = status;
  return current;
}

async function ensureStoreExists(): Promise<void> {
  if (!existsSync(STORE_DIR)) {
    mkdirSync(STORE_DIR, { recursive: true });
  }

  if (!existsSync(STORE_FILE)) {
    await writeFile(STORE_FILE, "{}\n", "utf8");
  }
}

async function readPlanJobsFromFile(): Promise<PlanJobsById> {
  await ensureStoreExists();

  try {
    const raw = await readFile(STORE_FILE, "utf8");
    const parsed = JSON.parse(raw) as unknown;

    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    const jobs = parsed as PlanJobsById;
    for (const [jobId, job] of Object.entries(jobs)) {
      jobs[jobId] = normalizeJob(job);
    }

    return jobs;
  } catch {
    return {};
  }
}

async function writePlanJobsToFile(jobs: PlanJobsById): Promise<void> {
  await ensureStoreExists();

  const tempFile = `${STORE_FILE}.${process.pid}.tmp`;
  const serialized = `${JSON.stringify(jobs, null, 2)}\n`;

  await writeFile(tempFile, serialized, "utf8");
  await rename(tempFile, STORE_FILE);
}

export async function createPlanJob(question: string, plan: PlanDraft): Promise<PlanJob> {
  const now = nowIso();
  const normalizedJob = normalizeJob({
    id: randomUUID(),
    question,
    status: "awaiting_approval",
    createdAt: now,
    updatedAt: now,
    plan,
    revisionNotes: [],
    artifacts: [],
  });

  if (shouldUseMongoPlanStore()) {
    const collection = await getPlanJobsCollection();
    await collection.insertOne(toStoredPlanJob(normalizedJob));
    return clonePlanJob(normalizedJob);
  }

  const jobs = await readPlanJobsFromFile();
  jobs[normalizedJob.id] = normalizedJob;
  await writePlanJobsToFile(jobs);

  return clonePlanJob(normalizedJob);
}

export async function getPlanJob(id: string): Promise<PlanJob | null> {
  if (shouldUseMongoPlanStore()) {
    const collection = await getPlanJobsCollection();
    const job = fromStoredPlanJob(await collection.findOne({ _id: id }));
    return job ? clonePlanJob(job) : null;
  }

  const jobs = await readPlanJobsFromFile();
  const job = jobs[id];
  return job ? clonePlanJob(normalizeJob(job)) : null;
}

export async function updatePlanJob(
  id: string,
  update: (current: PlanJob) => PlanJob,
): Promise<PlanJob | null> {
  const current = await getPlanJob(id);

  if (!current) {
    return null;
  }

  const next = normalizeJob(update(clonePlanJob(current)));
  next.updatedAt = nowIso();

  if (shouldUseMongoPlanStore()) {
    const collection = await getPlanJobsCollection();
    await collection.replaceOne({ _id: id }, toStoredPlanJob(next));
    return clonePlanJob(next);
  }

  const jobs = await readPlanJobsFromFile();
  jobs[id] = next;
  await writePlanJobsToFile(jobs);

  return clonePlanJob(next);
}

export async function setPlanStatus(
  id: string,
  status: PlanJobStatus,
  error?: string,
): Promise<PlanJob | null> {
  return updatePlanJob(id, (current) => {
    current.status = status;
    current.error = error;
    return current;
  });
}

export async function appendRevisionNote(id: string, note: string): Promise<PlanJob | null> {
  return updatePlanJob(id, (current) => {
    current.revisionNotes.push(note);
    return current;
  });
}

export async function replacePlan(id: string, plan: PlanDraft): Promise<PlanJob | null> {
  return updatePlanJob(id, (current) => {
    current.plan = plan;
    return resetExecutionState(current, "awaiting_approval");
  });
}

export async function resetPlanExecution(
  id: string,
  status: PlanJobStatus = "awaiting_approval",
): Promise<PlanJob | null> {
  return updatePlanJob(id, (current) => resetExecutionState(current, status));
}

export async function updateStepStatus(
  id: string,
  stepId: string,
  status: "pending" | "running" | "completed" | "failed",
  outputSummary?: string,
): Promise<PlanJob | null> {
  return updatePlanJob(id, (current) => {
    current.plan.steps = current.plan.steps.map((step) => {
      if (step.id !== stepId) {
        return step;
      }

      return {
        ...step,
        status,
        outputSummary,
      };
    });

    return current;
  });
}

export async function updateStepAgentToolCalls(
  id: string,
  stepId: string,
  agentToolCalls: AgentToolCall[],
): Promise<PlanJob | null> {
  return updatePlanJob(id, (current) => {
    current.plan.steps = current.plan.steps.map((step) => {
      if (step.id !== stepId) {
        return step;
      }

      return {
        ...step,
        agentToolCalls,
      };
    });

    return current;
  });
}

export async function appendArtifact(
  id: string,
  artifact: ExecutionArtifact,
): Promise<PlanJob | null> {
  return updatePlanJob(id, (current) => {
    current.artifacts.push(artifact);
    return current;
  });
}

export async function setFinalAnswer(
  id: string,
  finalAnswer: FinalAnswer,
): Promise<PlanJob | null> {
  return updatePlanJob(id, (current) => {
    current.finalAnswer = finalAnswer;
    return current;
  });
}
