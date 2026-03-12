import { z } from "zod";

export const planStepOwnerSchema = z.enum([
  "planner",
  "audience_builder",
  "narrative_explorer",
  "stats_query",
  "synthesizer",
]);

export const planStepStatusSchema = z.enum([
  "pending",
  "running",
  "completed",
  "failed",
]);

export const planJobStatusSchema = z.enum([
  "awaiting_approval",
  "running",
  "completed",
  "failed",
]);

const jsonPrimitiveSchema = z.union([
  z.string(),
  z.number(),
  z.boolean(),
  z.null(),
]);

export const jsonValueSchema: z.ZodType<
  string | number | boolean | null | Record<string, unknown> | unknown[]
> = z.lazy(() =>
  z.union([
    jsonPrimitiveSchema,
    z.array(jsonValueSchema),
    z.record(jsonValueSchema),
  ]),
);

export const planStepSchema = z.object({
  id: z.string().min(1),
  title: z.string().min(1),
  rationale: z.string().min(1),
  owner: planStepOwnerSchema,
  tool: z.string().min(1),
  toolId: z.string().min(1),
  args: z.record(jsonValueSchema).default({}),
  dependsOn: z.array(z.string().min(1)).default([]),
  inputBindings: z.record(z.string().min(1)).default({}),
  status: planStepStatusSchema.default("pending"),
  outputSummary: z.string().optional(),
});

export const planDraftSchema = z.object({
  objective: z.string().min(1),
  assumptions: z.array(z.string()).default([]),
  steps: z.array(planStepSchema).min(1),
  expectedOutput: z.object({
    format: z.literal("json"),
    sections: z.array(z.string()).min(1),
  }),
});

export const executionArtifactSchema = z.object({
  stepId: z.string().min(1),
  owner: planStepOwnerSchema,
  tool: z.string().min(1),
  toolId: z.string().min(1),
  data: z.record(z.any()),
});

export const finalAnswerSchema = z.object({
  answer: z.string().min(1),
  keyFindings: z.array(z.string()).default([]),
  dataPoints: z
    .array(
      z.object({
        label: z.string(),
        value: z.union([z.string(), z.number(), z.boolean()]),
      }),
    )
    .default([]),
  caveats: z.array(z.string()).default([]),
  recommendedNextQuestions: z.array(z.string()).default([]),
});

export const planJobSchema = z.object({
  id: z.string(),
  question: z.string().min(1),
  status: planJobStatusSchema,
  createdAt: z.string(),
  updatedAt: z.string(),
  plan: planDraftSchema,
  revisionNotes: z.array(z.string()).default([]),
  artifacts: z.array(executionArtifactSchema).default([]),
  finalAnswer: finalAnswerSchema.optional(),
  error: z.string().optional(),
});

export const createQuestionRequestSchema = z.object({
  question: z.string().min(3),
});

export const revisePlanRequestSchema = z.object({
  feedback: z.string().min(3),
});

export const statsMetricSchema = z.enum([
  "political_leaning_distribution",
  "active_messages_last_days",
  "top_groups_by_member_count",
]);

export type StatsMetric = z.infer<typeof statsMetricSchema>;

export const politicalLeaningSchema = z.enum([
  "left",
  "center-left",
  "center",
  "center-right",
  "right",
  "mixed",
]);

export type PoliticalLeaning = z.infer<typeof politicalLeaningSchema>;

const statsMetricAliasMap: Record<string, StatsMetric> = {
  top_groups: "top_groups_by_member_count",
  top_groups_by_member: "top_groups_by_member_count",
  top_groups_by_members: "top_groups_by_member_count",
  top_groups_by_size: "top_groups_by_member_count",
  group_membership_ranking: "top_groups_by_member_count",
  recent_message_volume: "active_messages_last_days",
  message_volume: "active_messages_last_days",
  recent_messages: "active_messages_last_days",
  leaning_distribution: "political_leaning_distribution",
  political_leanings_distribution: "political_leaning_distribution",
};

export function normalizeStatsMetric(metric: unknown): unknown {
  if (typeof metric !== "string") {
    return metric;
  }

  const normalized = metric.trim().toLowerCase();
  return statsMetricAliasMap[normalized] ?? metric;
}

const politicalLeaningAliasMap: Record<string, PoliticalLeaning> = {
  left: "left",
  "left-wing": "left",
  "center-left": "center-left",
  centerleft: "center-left",
  center: "center",
  centrist: "center",
  "center-right": "center-right",
  centerright: "center-right",
  right: "right",
  "right-wing": "right",
  mixed: "mixed",
};

export function normalizePoliticalLeaning(leaning: unknown): unknown {
  if (typeof leaning !== "string") {
    return leaning;
  }

  const normalized = leaning.trim().toLowerCase();
  return politicalLeaningAliasMap[normalized] ?? leaning;
}

export const statsFilterSchema = z
  .object({
    politicalLeaning: politicalLeaningSchema.optional(),
  })
  .strict();

export type StatsFilter = z.infer<typeof statsFilterSchema>;

export const dbStatsQueryToolFilterSchema = z
  .object({
    // OpenAI structured outputs require nested fields to be required, so nullable is the tool-facing adapter.
    politicalLeaning: politicalLeaningSchema.nullable(),
  })
  .strict();

export const dbStatsQueryToolInputSchema = z
  .object({
    metric: statsMetricSchema,
    lastDays: z.number().int().min(1).max(365).nullable().optional(),
    limit: z.number().int().min(1).max(50).nullable().optional(),
    filter: dbStatsQueryToolFilterSchema.nullable().optional(),
  })
  .strict();

export const statsQueryInputSchema = z.object({
  metric: statsMetricSchema,
  lastDays: z.number().int().min(1).max(365).optional(),
  limit: z.number().int().min(1).max(50).optional(),
  filter: statsFilterSchema.optional(),
});

export function normalizeDbStatsQueryInput(
  rawInput: Record<string, unknown>,
): Record<string, unknown> {
  const normalizedMetric = normalizeStatsMetric(rawInput.metric);
  const rawFilter =
    rawInput.filter && typeof rawInput.filter === "object" && !Array.isArray(rawInput.filter)
      ? (rawInput.filter as Record<string, unknown>)
      : null;
  const normalizedPoliticalLeaning = normalizePoliticalLeaning(rawFilter?.politicalLeaning);

  const normalized: Record<string, unknown> = {
    ...rawInput,
    metric: normalizedMetric,
  };

  if (rawInput.lastDays === null) {
    delete normalized.lastDays;
  }

  if (rawInput.limit === null) {
    delete normalized.limit;
  }

  if (normalizedPoliticalLeaning && typeof normalizedPoliticalLeaning === "string") {
    normalized.filter = {
      politicalLeaning: normalizedPoliticalLeaning,
    };
  } else {
    delete normalized.filter;
  }

  return normalized;
}

export function parseDbStatsQueryInput(rawInput: unknown): StatsQueryInput {
  const parsedToolInput = dbStatsQueryToolInputSchema.parse(rawInput);
  const normalized = normalizeDbStatsQueryInput(parsedToolInput as Record<string, unknown>);
  return statsQueryInputSchema.parse(normalized);
}

export function parsePartialDbStatsQueryInput(
  rawInput: unknown,
): Partial<StatsQueryInput> {
  const parsedToolInput = dbStatsQueryToolInputSchema.partial().parse(rawInput);
  const normalized = normalizeDbStatsQueryInput(parsedToolInput as Record<string, unknown>);
  return statsQueryInputSchema.partial().parse(normalized);
}

export type PlanStepOwner = z.infer<typeof planStepOwnerSchema>;
export type PlanStep = z.infer<typeof planStepSchema>;
export type PlanDraft = z.infer<typeof planDraftSchema>;
export type PlanJobStatus = z.infer<typeof planJobStatusSchema>;
export type PlanJob = z.infer<typeof planJobSchema>;
export type ExecutionArtifact = z.infer<typeof executionArtifactSchema>;
export type FinalAnswer = z.infer<typeof finalAnswerSchema>;
export type StatsQueryInput = z.infer<typeof statsQueryInputSchema>;

export function clonePlanJob(job: PlanJob): PlanJob {
  return JSON.parse(JSON.stringify(job)) as PlanJob;
}
