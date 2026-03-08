import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { getToolDefinition } from "./agents/tools";
import {
  clonePlanJob,
  ExecutionArtifact,
  FinalAnswer,
  normalizeStatsMetric,
  PlanDraft,
  PlanJob,
  PlanJobStatus,
} from "./types";

function resolveStoreDir(): string {
  if (process.env.PLAN_STORE_DIR) {
    return process.env.PLAN_STORE_DIR;
  }

  if (process.env.VERCEL || process.env.LAMBDA_TASK_ROOT || process.env.AWS_EXECUTION_ENV) {
    return join(tmpdir(), "rkmt-runtime");
  }

  return join(process.cwd(), ".runtime");
}

const STORE_DIR = resolveStoreDir();
const STORE_FILE = join(STORE_DIR, "plan-jobs.json");

type PlanJobsById = Record<string, PlanJob>;

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
    toolId === "db_stats_query"
      ? {
          ...rawArgs,
          metric: normalizeStatsMetric(rawArgs.metric),
        }
      : rawArgs;

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

function ensureStoreExists(): void {
  if (!existsSync(STORE_DIR)) {
    mkdirSync(STORE_DIR, { recursive: true });
  }

  if (!existsSync(STORE_FILE)) {
    writeFileSync(STORE_FILE, "{}\n", "utf8");
  }
}

function readPlanJobs(): PlanJobsById {
  ensureStoreExists();

  try {
    const raw = readFileSync(STORE_FILE, "utf8");
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

function writePlanJobs(jobs: PlanJobsById): void {
  ensureStoreExists();

  const tempFile = `${STORE_FILE}.${process.pid}.tmp`;
  const serialized = `${JSON.stringify(jobs, null, 2)}\n`;

  writeFileSync(tempFile, serialized, "utf8");
  renameSync(tempFile, STORE_FILE);
}

export function createPlanJob(question: string, plan: PlanDraft): PlanJob {
  const now = nowIso();
  const job: PlanJob = {
    id: randomUUID(),
    question,
    status: "awaiting_approval",
    createdAt: now,
    updatedAt: now,
    plan,
    revisionNotes: [],
    artifacts: [],
  };

  const jobs = readPlanJobs();
  jobs[job.id] = normalizeJob(job);
  writePlanJobs(jobs);

  return clonePlanJob(normalizeJob(job));
}

export function getPlanJob(id: string): PlanJob | null {
  const jobs = readPlanJobs();
  const job = jobs[id];
  return job ? clonePlanJob(normalizeJob(job)) : null;
}

export function updatePlanJob(
  id: string,
  update: (current: PlanJob) => PlanJob,
): PlanJob | null {
  const jobs = readPlanJobs();
  const current = jobs[id];

  if (!current) {
    return null;
  }

  const next = normalizeJob(update(clonePlanJob(current)));
  next.updatedAt = nowIso();

  jobs[id] = next;
  writePlanJobs(jobs);

  return clonePlanJob(next);
}

export function setPlanStatus(
  id: string,
  status: PlanJobStatus,
  error?: string,
): PlanJob | null {
  return updatePlanJob(id, (current) => {
    current.status = status;
    current.error = error;
    return current;
  });
}

export function appendRevisionNote(id: string, note: string): PlanJob | null {
  return updatePlanJob(id, (current) => {
    current.revisionNotes.push(note);
    return current;
  });
}

export function replacePlan(id: string, plan: PlanDraft): PlanJob | null {
  return updatePlanJob(id, (current) => {
    current.plan = plan;
    current.plan.steps = current.plan.steps.map((step) => ({
      ...step,
      status: "pending",
      outputSummary: undefined,
    }));
    current.artifacts = [];
    current.finalAnswer = undefined;
    current.error = undefined;
    current.status = "awaiting_approval";
    return current;
  });
}

export function updateStepStatus(
  id: string,
  stepId: string,
  status: "pending" | "running" | "completed" | "failed",
  outputSummary?: string,
): PlanJob | null {
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

export function appendArtifact(id: string, artifact: ExecutionArtifact): PlanJob | null {
  return updatePlanJob(id, (current) => {
    current.artifacts.push(artifact);
    return current;
  });
}

export function setFinalAnswer(id: string, finalAnswer: FinalAnswer): PlanJob | null {
  return updatePlanJob(id, (current) => {
    current.finalAnswer = finalAnswer;
    return current;
  });
}
