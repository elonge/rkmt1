import { Agent, run, tool } from "@openai/agents";
import { z } from "zod";
import {
  latestNarrativesInTimeframe,
} from "../data/dummy-db";
import { runStatsQuery } from "../data/stats";
import {
  lookupAudienceSegmentsSemantic,
  lookupInfluencersSemantic,
  SemanticConfigError,
  SemanticSearchError,
} from "../data/semantic";
import {
  describeExecutableTools,
  getExecutableToolDefinition,
  getToolDefinition,
  plannerExecutableToolIdValues,
  PlannerExecutableToolId,
  ToolDefinition,
} from "./tools";
import {
  audienceAgentOutputSchema,
  narrativeAgentOutputSchema,
  statsAgentOutputSchema,
  summarizationOutputSchema,
} from "./tool-output-schemas";
import { createAudienceBuilderAgent } from "./audience-builder";
import {
  AgentToolCall,
  dbStatsQueryToolFilterFieldByEntity,
  ExecutionArtifact,
  FinalAnswer,
  normalizeDbStatsQueryInput,
  parseDbStatsQueryInput,
  parsePartialDbStatsQueryInput,
  PlanDraft,
  PlanStep,
  serializeDbStatsQueryInputForToolSchema,
  StatsQueryInput,
} from "../types";
import { plannerReferenceContext } from "./context";
import { runWithToolDebugLogging } from "../runtime/tool-debug.ts";

const MODEL_NAME = process.env.OPENAI_MODEL ?? "gpt-4.1";
const OPENAI_ENABLED = Boolean(process.env.OPENAI_API_KEY);

const llmFinalAnswerSchema = z.object({
  answer: z.string(),
  keyFindings: z.array(z.string()),
  dataPoints: z.array(
    z.object({
      label: z.string(),
      value: z.union([z.string(), z.number(), z.boolean()]),
    }),
  ),
  caveats: z.array(z.string()),
  recommendedNextQuestions: z.array(z.string()),
});

export class PlannerUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PlannerUnavailableError";
  }
}

export class PlannerExecutionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PlannerExecutionError";
  }
}

export class PlanExecutionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PlanExecutionError";
  }
}

const plannerBindingReferenceSchema = z.discriminatedUnion("source", [
  z.object({ source: z.literal("question") }).strict(),
  z
    .object({
      source: z.literal("currentStep"),
      field: z.enum(["title", "rationale"]),
    })
    .strict(),
  z
    .object({
      source: z.literal("stepData"),
      stepId: z.string().min(1),
      path: z.string().min(1),
    })
    .strict(),
  z
    .object({
      source: z.literal("stepArgs"),
      stepId: z.string().min(1),
      path: z.string().min(1),
    })
    .strict(),
  z
    .object({
      source: z.literal("stepOutputSummary"),
      stepId: z.string().min(1),
    })
    .strict(),
]);

type PlannerBindingReference = z.infer<typeof plannerBindingReferenceSchema>;

function transformPlannerArgsSchema(schema: z.ZodTypeAny): z.ZodTypeAny {
  const { schema: unwrapped, optional, nullable } = unwrapSchema(schema);
  let transformed: z.ZodTypeAny;

  if (unwrapped instanceof z.ZodObject) {
    transformed = z
      .object(
        Object.fromEntries(
          Object.entries(unwrapped.shape).map(([key, value]) => [
            key,
            transformPlannerArgsSchema(value as z.ZodTypeAny),
          ]),
        ),
      )
      .strict();
  } else if (unwrapped instanceof z.ZodArray) {
    transformed = z.array(transformPlannerArgsSchema(unwrapped.element));
  } else {
    transformed = unwrapped;
  }

  if (optional || nullable) {
    transformed = transformed.nullable();
  }

  return transformed;
}

function transformPlannerBindingsSchema(schema: z.ZodTypeAny): z.ZodTypeAny {
  const { schema: unwrapped, optional, nullable } = unwrapSchema(schema);

  if (unwrapped instanceof z.ZodObject) {
    let transformed: z.ZodTypeAny = z
      .object(
        Object.fromEntries(
          Object.entries(unwrapped.shape).map(([key, value]) => [
            key,
            transformPlannerBindingsSchema(value as z.ZodTypeAny),
          ]),
        ),
      )
      .strict();

    if (optional || nullable) {
      transformed = transformed.nullable();
    }

    return transformed;
  }

  let transformed: z.ZodTypeAny = plannerBindingReferenceSchema.nullable();
  if (optional || nullable) {
    transformed = transformed.nullable();
  }
  return transformed;
}

function buildPlannerStepSchema(toolId: PlannerExecutableToolId): z.ZodObject<any> {
  const toolDefinition = requireToolDefinition(toolId);
  return z
    .object({
      id: z.string(),
      title: z.string(),
      rationale: z.string(),
      toolId: z.literal(toolId),
      args: transformPlannerArgsSchema(toolDefinition.argsSchema),
      dependsOn: z.array(z.string()),
      inputBindings: transformPlannerBindingsSchema(toolDefinition.argsSchema),
    })
    .strict();
}

function buildStepRepairSchema(toolId: PlannerExecutableToolId): z.ZodObject<any> {
  const plannerStepSchema = plannerStepSchemaByToolId[toolId];
  return z
    .object({
      args: plannerStepSchema.shape.args,
      inputBindings: plannerStepSchema.shape.inputBindings,
    })
    .strict();
}

const plannerStepSchemaByToolId = Object.fromEntries(
  plannerExecutableToolIdValues.map((toolId) => [toolId, buildPlannerStepSchema(toolId)]),
) as Record<PlannerExecutableToolId, z.ZodObject<any>>;

const llmPlanStepSchema = z.discriminatedUnion(
  "toolId",
  plannerExecutableToolIdValues.map((toolId) => plannerStepSchemaByToolId[toolId]) as any,
);

const llmPlanDraftSchema = z.object({
  objective: z.string(),
  assumptions: z.array(z.string()),
  steps: z.array(llmPlanStepSchema).min(1),
  expectedOutput: z.object({
    format: z.literal("json"),
    sections: z.array(z.string()).min(1),
  }),
});

const stepRepairSchemaByToolId = Object.fromEntries(
  plannerExecutableToolIdValues.map((toolId) => [toolId, buildStepRepairSchema(toolId)]),
) as Record<PlannerExecutableToolId, z.ZodObject<any>>;

type LlmPlanStep = z.infer<typeof llmPlanStepSchema>;
type LlmPlanDraft = z.infer<typeof llmPlanDraftSchema>;

function buildNarrativeProbe(question: string) {
  return {
    source: "dummy-narrative-tool",
    narratives: [
      { name: "Security", sentiment: "mixed", volume: "high" },
      { name: "Cost of living", sentiment: "negative", volume: "medium" },
      { name: "Leadership trust", sentiment: "polarized", volume: "medium" },
    ],
    query: question,
  };
}

function requireToolDefinition(toolId: string) {
  const toolDefinition = getToolDefinition(toolId);
  if (!toolDefinition) {
    throw new Error(`Unknown tool definition: ${toolId}`);
  }
  return toolDefinition;
}

function transformStructuredToolParametersSchema(
  schema: z.ZodObject<any>,
): z.ZodObject<any> {
  const transformed = transformPlannerArgsSchema(schema);
  if (!(transformed instanceof z.ZodObject)) {
    throw new Error("Tool parameter schema must resolve to a ZodObject.");
  }
  return transformed.strip();
}

function specialistToolParameters(toolId: string) {
  return transformStructuredToolParametersSchema(requireToolDefinition(toolId).argsSchema);
}

type SpecialistExecutionContext = {
  agentToolCalls: AgentToolCall[];
  onAgentToolCalls?: (agentToolCalls: AgentToolCall[]) => Promise<void> | void;
  rootQuestion?: string;
  failedToolCallEncountered?: boolean;
};

function normalizeTraceValue(value: unknown): AgentToolCall["input"] {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (Array.isArray(value)) {
    return value.map((item) => normalizeTraceValue(item));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .filter(([, entryValue]) => entryValue !== undefined)
        .map(([key, entryValue]) => [key, normalizeTraceValue(entryValue)]),
    );
  }

  return String(value);
}

function cloneAgentToolCalls(agentToolCalls: AgentToolCall[]): AgentToolCall[] {
  return agentToolCalls.map((agentToolCall) => ({
    ...agentToolCall,
    input: normalizeTraceValue(agentToolCall.input),
    output:
      agentToolCall.output === undefined ? undefined : normalizeTraceValue(agentToolCall.output),
    debugLogs: agentToolCall.debugLogs.map((debugLog) => ({
      ...debugLog,
      data: debugLog.data === undefined ? undefined : normalizeTraceValue(debugLog.data),
    })),
  }));
}

function updateAgentToolCall(
  agentToolCalls: AgentToolCall[],
  toolCallId: string,
  update: (agentToolCall: AgentToolCall) => AgentToolCall,
): AgentToolCall[] {
  return agentToolCalls.map((agentToolCall) =>
    agentToolCall.id === toolCallId ? update(agentToolCall) : agentToolCall,
  );
}

async function publishAgentToolCalls(
  traceContext: SpecialistExecutionContext | undefined,
  agentToolCalls: AgentToolCall[],
): Promise<void> {
  if (!traceContext) {
    return;
  }

  traceContext.agentToolCalls = cloneAgentToolCalls(agentToolCalls);
  await traceContext.onAgentToolCalls?.(cloneAgentToolCalls(traceContext.agentToolCalls));
}

function getSpecialistExecutionContext(context: unknown): SpecialistExecutionContext | undefined {
  if (!context || typeof context !== "object" || !("context" in context)) {
    return undefined;
  }

  const nestedContext = (context as { context?: unknown }).context;
  if (
    !nestedContext ||
    typeof nestedContext !== "object" ||
    !Array.isArray((nestedContext as { agentToolCalls?: unknown }).agentToolCalls)
  ) {
    return undefined;
  }

  return nestedContext as SpecialistExecutionContext;
}

async function executeWithAgentToolTrace<T>(
  toolDefinition: ToolDefinition,
  input: unknown,
  context: unknown,
  execute: () => Promise<T>,
): Promise<T> {
  const traceContext = getSpecialistExecutionContext(context);
  if (!traceContext) {
    return execute();
  }

  if (traceContext.failedToolCallEncountered) {
    throw new Error(
      `A previous internal tool call already failed for this step. Additional fallback tool calls are disabled.`,
    );
  }

  const nextToolCall: AgentToolCall = {
    id: `${toolDefinition.toolId}-${traceContext.agentToolCalls.length + 1}`,
    tool: toolDefinition.label,
    toolId: toolDefinition.toolId,
    status: "running",
    input: normalizeTraceValue(input),
    debugLogs: [],
  };

  const startedToolCalls = [...traceContext.agentToolCalls, nextToolCall];
  await publishAgentToolCalls(traceContext, startedToolCalls);

  const appendDebugLog = async (message: string, data?: unknown) => {
    const updatedToolCalls = updateAgentToolCall(
      traceContext.agentToolCalls,
      nextToolCall.id,
      (agentToolCall) => ({
        ...agentToolCall,
        debugLogs: [
          ...agentToolCall.debugLogs,
          {
            message,
            data: data === undefined ? undefined : normalizeTraceValue(data),
          },
        ],
      }),
    );
    await publishAgentToolCalls(traceContext, updatedToolCalls);
  };

  try {
    const output = await runWithToolDebugLogging(appendDebugLog, execute);
    const completedToolCalls = updateAgentToolCall(
      traceContext.agentToolCalls,
      nextToolCall.id,
      (agentToolCall) => ({
        ...agentToolCall,
        status: "completed" as const,
        output: normalizeTraceValue(output),
      }),
    );
    await publishAgentToolCalls(traceContext, completedToolCalls);
    return output;
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown tool execution error";
    traceContext.failedToolCallEncountered = true;
    const failedToolCalls = updateAgentToolCall(
      traceContext.agentToolCalls,
      nextToolCall.id,
      (agentToolCall) => ({
        ...agentToolCall,
        status: "failed" as const,
        error: message,
      }),
    );
    await publishAgentToolCalls(traceContext, failedToolCalls);
    throw error;
  }
}

const summarizationToolDefinition = requireToolDefinition("summarization");
const dbStatsQueryToolDefinition = requireToolDefinition("db_stats_query");
const audienceLookupToolDefinition = requireToolDefinition("audience_lookup");
const influencerLookupToolDefinition = requireToolDefinition("influencer_lookup");
const narrativeProbeToolDefinition = requireToolDefinition("narrative_probe");
const latestNarrativesToolDefinition = requireToolDefinition("find_narratives_in_timeframe");


const summarizationTool = tool({
  name: summarizationToolDefinition.toolId,
  description: summarizationToolDefinition.description,
  parameters: specialistToolParameters(summarizationToolDefinition.toolId),
  async execute(input, context) {
    return executeWithAgentToolTrace(summarizationToolDefinition, input, context, async () =>
      runSummarization({
        text: input.text,
        maxBullets: input.maxBullets,
        focus: typeof input.focus === "string" ? input.focus : undefined,
      }),
    );
  },
});

const dbStatsQueryTool = tool({
  name: dbStatsQueryToolDefinition.toolId,
  description: dbStatsQueryToolDefinition.description,
  parameters: specialistToolParameters(dbStatsQueryToolDefinition.toolId),
  async execute(input, context) {
    return executeWithAgentToolTrace(dbStatsQueryToolDefinition, input, context, async () =>
      runStatsQuery(parseDbStatsQueryInput(input)),
    );
  },
});

const audienceLookupTool = tool({
  name: audienceLookupToolDefinition.toolId,
  description: audienceLookupToolDefinition.description,
  parameters: specialistToolParameters(audienceLookupToolDefinition.toolId),
  async execute(input, context) {
    return executeWithAgentToolTrace(audienceLookupToolDefinition, input, context, async () =>
      runAudienceLookup(input.question),
    );
  },
});

const influencerLookupTool = tool({
  name: influencerLookupToolDefinition.toolId,
  description: influencerLookupToolDefinition.description,
  parameters: specialistToolParameters(influencerLookupToolDefinition.toolId),
  async execute(input, context) {
    return executeWithAgentToolTrace(influencerLookupToolDefinition, input, context, async () =>
      runInfluencerLookup({
        question: typeof input.question === "string" ? input.question : undefined,
        narrative:
          typeof input.narrative === "string" || input.narrative === null
            ? input.narrative
            : null,
      }),
    );
  },
});

const narrativeProbeTool = tool({
  name: narrativeProbeToolDefinition.toolId,
  description: narrativeProbeToolDefinition.description,
  parameters: specialistToolParameters(narrativeProbeToolDefinition.toolId),
  async execute(input, context) {
    return executeWithAgentToolTrace(narrativeProbeToolDefinition, input, context, async () =>
      buildNarrativeProbe(input.question),
    );
  },
});

const latestNarrativesTimeframeTool = tool({
  name: latestNarrativesToolDefinition.toolId,
  description: latestNarrativesToolDefinition.description,
  parameters: specialistToolParameters(latestNarrativesToolDefinition.toolId),
  async execute(input, context) {
    return executeWithAgentToolTrace(latestNarrativesToolDefinition, input, context, async () =>
      latestNarrativesInTimeframe(input.timeframeDays, input.limit),
    );
  },
});

const audienceBuilderAgent = createAudienceBuilderAgent({
  modelName: MODEL_NAME,
  executeWithAgentToolTrace,
  specialistToolParameters,
});

const narrativeExplorerAgent = new Agent({
  name: "NarrativeExplorer",
  model: MODEL_NAME,
  instructions:
    "You are a specialist for narrative, sentiment, and text exploration. For latest narrative/volume requests, use find_narratives_in_timeframe. Return concise JSON.",
  tools: [narrativeProbeTool, latestNarrativesTimeframeTool, summarizationTool],
  outputType: narrativeAgentOutputSchema,
});

const statsQueryAgent = new Agent({
  name: "StatsQuery",
  model: MODEL_NAME,
  instructions:
    "You are a metrics specialist. Pick an appropriate stats query, call db_stats_query, match entity to the correct filter object, and summarize key facts.",
  tools: [dbStatsQueryTool],
  outputType: statsAgentOutputSchema,
});

const managerToolOutputSchemaByName = {
  audience_builder_agent: audienceAgentOutputSchema,
  narrative_explorer_agent: narrativeAgentOutputSchema,
  stats_query_agent: statsAgentOutputSchema,
} as const;

type ManagerToolName = keyof typeof managerToolOutputSchemaByName;
type ManagerToolOutputByName = {
  [K in ManagerToolName]: z.infer<(typeof managerToolOutputSchemaByName)[K]>;
};

const specialistAgentByToolName: Record<ManagerToolName, Agent<any, any>> = {
  audience_builder_agent: audienceBuilderAgent,
  narrative_explorer_agent: narrativeExplorerAgent,
  stats_query_agent: statsQueryAgent,
};

const plannerAgent = new Agent({
  name: "Planner",
  model: MODEL_NAME,
  instructions: [
    "Build an execution plan for the question.",
    "Use only registered toolIds from the provided executable tool registry.",
    "Return executable steps with toolId, args, dependsOn, and inputBindings.",
    "Return args using the selected tool's exact arg object shape.",
    "Set optional arg fields to null when unused.",
    "The runtime dispatches by toolId, not by free-text tool names.",
    "Only use dependsOn references to earlier steps.",
    "Use inputBindings when step args depend on previous outputs.",
    "Return inputBindings using the same object shape as args, but with binding reference objects at leaf fields and null when a field has no binding.",
    "If inputBindings references a prior step, include that step id in dependsOn.",
    "For db_stats_query, match entity to its filter object exactly: messages -> messageFilters, groups -> groupFilters, users -> userFilters, memberships -> membershipFilters.",
    "Do not use announcementOnly or membershipApproval in message stats queries.",
    "Prefer 3-6 steps and keep titles/rationales concrete.",
  ].join(" "),
  outputType: llmPlanDraftSchema,
});

const reviserAgent = new Agent({
  name: "PlanReviser",
  model: MODEL_NAME,
  instructions: [
    "Revise the existing plan according to user feedback.",
    "Preserve executable toolIds and return an executable plan.",
    "Use only registered toolIds from the provided executable tool registry.",
    "Return args using each selected tool's exact arg object shape.",
    "Set optional arg fields to null when unused.",
    "Return inputBindings using the same object shape as args, but with binding reference objects at leaf fields and null when a field has no binding.",
    "If inputBindings references a prior step, include that step id in dependsOn.",
    "For db_stats_query, match entity to its filter object exactly: messages -> messageFilters, groups -> groupFilters, users -> userFilters, memberships -> membershipFilters.",
    "Return complete plan JSON only.",
  ].join(" "),
  outputType: llmPlanDraftSchema,
});

function createStepArgsRepairAgent(toolId: PlannerExecutableToolId) {
  return new Agent({
    name: "StepArgsRepair",
    model: MODEL_NAME,
    instructions: [
      "Regenerate args for a revised plan step when its toolId changed.",
      "Return args and inputBindings only.",
      "Use the selected tool's exact arg object shape.",
      "Set optional arg fields to null when unused.",
      "Return inputBindings using the same object shape as args, but with binding reference objects at leaf fields and null when a field has no binding.",
      "Regenerate args from scratch for the new tool schema instead of carrying over args from the previous tool.",
      "Prefer simple valid args over over-specified args.",
      "If inputBindings references a prior step, include that step id in dependsOn.",
      "For db_stats_query, match entity to its filter object exactly: messages -> messageFilters, groups -> groupFilters, users -> userFilters, memberships -> membershipFilters.",
    ].join(" "),
    outputType: stepRepairSchemaByToolId[toolId],
  });
}

const synthesisAgent = new Agent({
  name: "Synthesizer",
  model: MODEL_NAME,
  instructions:
    "Synthesize specialist outputs into strict JSON for UI. Mention uncertainty/caveats where relevant.",
  outputType: llmFinalAnswerSchema,
});

const summarizerAgent = new Agent({
  name: "Summarizer",
  model: MODEL_NAME,
  instructions: [
    "Summarize the supplied text into strict JSON.",
    "Return fields: source, answer, bullets.",
    "Set source to `llm-summarizer`.",
    "Keep answer to 1-3 concise sentences grounded only in the supplied text.",
    "Return factual bullets only, with no more than the requested maxBullets.",
    "Do not add keys outside the required schema.",
  ].join(" "),
  outputType: summarizationOutputSchema,
});

function normalizePlanDraft(plan: LlmPlanDraft): PlanDraft {
  const seenStepIds = new Set<string>();
  const seenStepToolDefinitions = new Map<string, ToolDefinition>();

  return {
    objective: plan.objective,
    assumptions: plan.assumptions,
    steps: plan.steps.map((step) => {
      const toolDefinition = getExecutableToolDefinition(step.toolId);
      if (!toolDefinition) {
        throw new Error(`Unknown toolId in plan: ${step.toolId}`);
      }

      const normalizedStepId = normalizePlannedStepId(step.id, seenStepIds);
      const normalizedDependsOn = normalizePlannedDependsOn(
        step.dependsOn,
        normalizedStepId,
        seenStepIds,
      );
      const parsedBindings = normalizePlannedInputBindings(
        normalizedStepId,
        toolDefinition,
        step.inputBindings,
      );
      validatePlannedBindingSources(
        normalizedStepId,
        parsedBindings,
        normalizedDependsOn,
        seenStepIds,
        seenStepToolDefinitions,
      );
      const normalizedArgs = validatePlannedStepArgs(
        normalizedStepId,
        toolDefinition,
        compactPlannerArgs(step.args),
      );
      validateRequiredPlannedInputs(
        normalizedStepId,
        toolDefinition,
        normalizedArgs,
        parsedBindings,
      );

      const normalized: PlanStep = {
        id: normalizedStepId,
        title: step.title,
        rationale: step.rationale,
        owner: toolDefinition.owner,
        tool: toolDefinition.label,
        toolId: step.toolId,
        args: normalizedArgs,
        dependsOn: normalizedDependsOn,
        inputBindings: parsedBindings,
        status: "pending",
        agentToolCalls: [],
      };

      seenStepIds.add(normalized.id);
      seenStepToolDefinitions.set(normalized.id, toolDefinition);

      return normalized;
    }),
    expectedOutput: {
      format: "json",
      sections: plan.expectedOutput.sections,
    },
  };
}

function formatZodIssues(error: z.ZodError): string {
  return error.issues
    .map((issue) => {
      const path = issue.path.length > 0 ? issue.path.join(".") : "(root)";
      return `${path}: ${issue.message}`;
    })
    .join("; ");
}

function unwrapSchema(
  schema: z.ZodTypeAny,
): { schema: z.ZodTypeAny; optional: boolean; nullable: boolean } {
  let current = schema;
  let optional = false;
  let nullable = false;

  while (true) {
    if (current instanceof z.ZodOptional) {
      optional = true;
      current = current.unwrap();
      continue;
    }

    if (current instanceof z.ZodNullable) {
      nullable = true;
      current = current.unwrap();
      continue;
    }

    if (current instanceof z.ZodDefault) {
      optional = true;
      current = current.removeDefault();
      continue;
    }

    break;
  }

  return { schema: current, optional, nullable };
}

function normalizePlannedStepId(stepId: string, seenStepIds: Set<string>): string {
  const normalized = stepId.trim();
  if (!normalized) {
    throw new PlannerExecutionError("Planner returned an empty step id.");
  }

  if (seenStepIds.has(normalized)) {
    throw new PlannerExecutionError(`Planner returned duplicate step id "${normalized}".`);
  }

  return normalized;
}

function normalizePlannedDependsOn(
  dependsOn: string[],
  stepId: string,
  seenStepIds: Set<string>,
): string[] {
  if (!Array.isArray(dependsOn) || dependsOn.some((value) => typeof value !== "string")) {
    throw new PlannerExecutionError(`Planner returned invalid dependsOn for ${stepId}.`);
  }

  const normalizedDependencies: string[] = [];
  const seenDependencies = new Set<string>();

  for (const dependency of dependsOn) {
    const normalizedDependency = dependency.trim();
    if (!normalizedDependency) {
      throw new PlannerExecutionError(
        `Planner returned an empty dependency reference for ${stepId}.`,
      );
    }

    if (!seenStepIds.has(normalizedDependency)) {
      throw new PlannerExecutionError(
        `Planner returned unknown or forward dependency "${normalizedDependency}" for ${stepId}. Dependencies must reference earlier steps.`,
      );
    }

    if (seenDependencies.has(normalizedDependency)) {
      throw new PlannerExecutionError(
        `Planner returned duplicate dependency "${normalizedDependency}" for ${stepId}.`,
      );
    }

    normalizedDependencies.push(normalizedDependency);
    seenDependencies.add(normalizedDependency);
  }

  return normalizedDependencies;
}

function getSchemaForArgPath(
  schema: z.ZodTypeAny,
  argPath: string,
): z.ZodTypeAny | null {
  const segments = argPath.split(".").filter(Boolean);
  if (segments.length === 0) {
    return null;
  }

  let current: z.ZodTypeAny = schema;
  for (const segment of segments) {
    const { schema: unwrapped } = unwrapSchema(current);
    if (!(unwrapped instanceof z.ZodObject)) {
      return null;
    }

    const next = unwrapped.shape[segment];
    if (!next) {
      return null;
    }

    current = next;
  }

  return current;
}

function compactNullableStructuredValue(value: unknown): unknown {
  if (value === null || value === undefined) {
    return undefined;
  }

  if (Array.isArray(value)) {
    return value
      .map((entry) => compactNullableStructuredValue(entry))
      .filter((entry): entry is Exclude<typeof entry, undefined> => entry !== undefined);
  }

  if (typeof value === "object") {
    const entries = Object.entries(value).flatMap(([key, entry]) => {
      const compactEntry = compactNullableStructuredValue(entry);
      return compactEntry === undefined ? [] : [[key, compactEntry] as const];
    });

    if (entries.length === 0) {
      return undefined;
    }

    return Object.fromEntries(entries);
  }

  return value;
}

function compactPlannerArgs(rawArgs: unknown): Record<string, unknown> {
  const compact = compactNullableStructuredValue(rawArgs);
  if (compact === undefined) {
    return {};
  }

  if (!compact || typeof compact !== "object" || Array.isArray(compact)) {
    throw new PlannerExecutionError("Planner returned invalid args: expected an object.");
  }

  return compact as Record<string, unknown>;
}

function serializeBindingReference(binding: PlannerBindingReference): string {
  switch (binding.source) {
    case "question":
      return "question";
    case "currentStep":
      return `currentStep.${binding.field}`;
    case "stepData":
      return `steps.${binding.stepId}.data.${binding.path}`;
    case "stepArgs":
      return `steps.${binding.stepId}.args.${binding.path}`;
    case "stepOutputSummary":
      return `steps.${binding.stepId}.outputSummary`;
  }
}

function parseSerializedBindingReference(binding: string): PlannerBindingReference {
  const normalized = binding.startsWith("$") ? binding.slice(1) : binding;

  if (normalized === "question") {
    return { source: "question" };
  }

  if (normalized === "currentStep.title") {
    return { source: "currentStep", field: "title" };
  }

  if (normalized === "currentStep.rationale") {
    return { source: "currentStep", field: "rationale" };
  }

  if (normalized.startsWith("steps.")) {
    const [, stepId, source, ...path] = normalized.split(".");
    if (!stepId || !source) {
      throw new PlannerExecutionError(`Current plan contains malformed binding "${binding}".`);
    }

    if (source === "data") {
      if (path.length === 0) {
        throw new PlannerExecutionError(`Current plan contains malformed binding "${binding}".`);
      }
      return { source: "stepData", stepId, path: path.join(".") };
    }

    if (source === "args") {
      if (path.length === 0) {
        throw new PlannerExecutionError(`Current plan contains malformed binding "${binding}".`);
      }
      return { source: "stepArgs", stepId, path: path.join(".") };
    }

    if (source === "outputSummary" && path.length === 0) {
      return { source: "stepOutputSummary", stepId };
    }
  }

  throw new PlannerExecutionError(
    `Current plan contains unsupported binding "${binding}" that cannot be serialized for planner revision.`,
  );
}

function flattenPlannerInputBindingsInto(
  value: unknown,
  schema: z.ZodTypeAny,
  path: string[],
  target: Record<string, string>,
): void {
  if (value === null || value === undefined) {
    return;
  }

  const { schema: unwrapped } = unwrapSchema(schema);

  if (unwrapped instanceof z.ZodObject) {
    if (typeof value !== "object" || Array.isArray(value)) {
      throw new PlannerExecutionError(
        "Planner returned invalid inputBindings: expected an object mirroring the tool args shape.",
      );
    }

    for (const [key, childSchema] of Object.entries(unwrapped.shape)) {
      flattenPlannerInputBindingsInto(
        (value as Record<string, unknown>)[key],
        childSchema as z.ZodTypeAny,
        [...path, key],
        target,
      );
    }
    return;
  }

  const parsedBinding = plannerBindingReferenceSchema.safeParse(value);
  if (!parsedBinding.success) {
    throw new PlannerExecutionError(
      "Planner returned invalid inputBindings: expected binding reference objects or null at leaf fields.",
    );
  }

  if (parsedBinding.data) {
    if (path.length === 0) {
      throw new PlannerExecutionError(
        "Planner returned invalid inputBindings: root binding must be an object.",
      );
    }
    target[path.join(".")] = serializeBindingReference(parsedBinding.data);
  }
}

function flattenPlannerInputBindings(
  rawInputBindings: unknown,
  argsSchema: z.ZodTypeAny,
): Record<string, string> {
  if (rawInputBindings === null || rawInputBindings === undefined) {
    return {};
  }

  if (!rawInputBindings || typeof rawInputBindings !== "object" || Array.isArray(rawInputBindings)) {
    throw new PlannerExecutionError("Planner returned invalid inputBindings: expected an object.");
  }

  const flattened: Record<string, string> = {};
  flattenPlannerInputBindingsInto(rawInputBindings, argsSchema, [], flattened);
  return flattened;
}

function hasNonNullStructuredValue(value: unknown): boolean {
  if (value === null || value === undefined) {
    return false;
  }

  if (Array.isArray(value)) {
    return value.some((entry) => hasNonNullStructuredValue(entry));
  }

  if (typeof value === "object") {
    return Object.values(value).some((entry) => hasNonNullStructuredValue(entry));
  }

  return true;
}

function serializePlannerArgsValue(schema: z.ZodTypeAny, value: unknown): unknown {
  const { schema: unwrapped, optional, nullable } = unwrapSchema(schema);

  if (unwrapped instanceof z.ZodObject) {
    const objectValue =
      value && typeof value === "object" && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : undefined;
    const serialized = Object.fromEntries(
      Object.entries(unwrapped.shape).map(([key, childSchema]) => [
        key,
        serializePlannerArgsValue(childSchema as z.ZodTypeAny, objectValue?.[key]),
      ]),
    );

    if ((optional || nullable) && !hasNonNullStructuredValue(serialized)) {
      return null;
    }

    return serialized;
  }

  if (value === undefined) {
    return null;
  }

  return value;
}

function serializePlannerBindingsValue(
  schema: z.ZodTypeAny,
  inputBindings: Record<string, string>,
  path: string[] = [],
): unknown {
  const { schema: unwrapped, optional, nullable } = unwrapSchema(schema);

  if (unwrapped instanceof z.ZodObject) {
    const serialized = Object.fromEntries(
      Object.entries(unwrapped.shape).map(([key, childSchema]) => [
        key,
        serializePlannerBindingsValue(childSchema as z.ZodTypeAny, inputBindings, [...path, key]),
      ]),
    );

    if ((optional || nullable) && !hasNonNullStructuredValue(serialized)) {
      return null;
    }

    return serialized;
  }

  const binding = inputBindings[path.join(".")];
  return binding ? parseSerializedBindingReference(binding) : null;
}

function serializeDbStatsQueryBindingsForToolSchema(
  step: PlanStep,
): Record<string, string> {
  const entity = typeof step.args?.entity === "string" ? step.args.entity : undefined;
  const filterField =
    entity && entity in dbStatsQueryToolFilterFieldByEntity
      ? dbStatsQueryToolFilterFieldByEntity[
          entity as keyof typeof dbStatsQueryToolFilterFieldByEntity
        ]
      : null;

  if (!filterField) {
    return step.inputBindings ?? {};
  }

  return Object.fromEntries(
    Object.entries(step.inputBindings ?? {}).map(([path, binding]) => [
      path.startsWith("filters.") ? `${filterField}.${path.slice("filters.".length)}` : path,
      binding,
    ]),
  );
}

function normalizePlannedInputBindings(
  stepId: string,
  toolDefinition: ToolDefinition,
  rawInputBindings: unknown,
): PlanStep["inputBindings"] {
  const inputBindings = flattenPlannerInputBindings(rawInputBindings, toolDefinition.argsSchema);

  for (const argPath of Object.keys(inputBindings ?? {})) {
    if (!getSchemaForArgPath(toolDefinition.argsSchema, argPath)) {
      throw new PlannerExecutionError(
        `Planner returned invalid input binding target "${argPath}" for ${stepId} (${toolDefinition.toolId}).`,
      );
    }
  }

  return inputBindings;
}

function validatePlannedBindingSources(
  stepId: string,
  inputBindings: PlanStep["inputBindings"],
  dependsOn: string[],
  seenStepIds: Set<string>,
  seenStepToolDefinitions: Map<string, ToolDefinition>,
): void {
  const allowedDependencies = new Set(dependsOn);

  for (const [argPath, rawBinding] of Object.entries(inputBindings ?? {})) {
    const binding = rawBinding.startsWith("$") ? rawBinding.slice(1) : rawBinding;
    if (
      binding === "question" ||
      binding === "currentStep.title" ||
      binding === "currentStep.rationale"
    ) {
      continue;
    }

    if (!binding.startsWith("steps.")) {
      throw new PlannerExecutionError(
        `Planner returned invalid binding source "${rawBinding}" for ${stepId}.${argPath}. inputBindings may only contain binding references, not literal values.`,
      );
    }

    const [, dependencyId, source, ...path] = binding.split(".");
    if (!dependencyId || !source) {
      throw new PlannerExecutionError(
        `Planner returned malformed binding source "${rawBinding}" for ${stepId}.${argPath}.`,
      );
    }

    if (!seenStepIds.has(dependencyId)) {
      throw new PlannerExecutionError(
        `Planner returned unknown or forward binding source "${rawBinding}" for ${stepId}.${argPath}.`,
      );
    }

    if (!allowedDependencies.has(dependencyId)) {
      throw new PlannerExecutionError(
        `Planner returned binding source "${rawBinding}" for ${stepId}.${argPath} without listing "${dependencyId}" in dependsOn.`,
      );
    }

    if (source !== "data" && source !== "args" && source !== "outputSummary") {
      throw new PlannerExecutionError(
        `Planner returned invalid binding source "${rawBinding}" for ${stepId}.${argPath}.`,
      );
    }

    const dependencyToolDefinition = seenStepToolDefinitions.get(dependencyId);
    if (!dependencyToolDefinition) {
      throw new PlannerExecutionError(
        `Planner returned binding source "${rawBinding}" for ${stepId}.${argPath}, but no tool definition was found for step "${dependencyId}".`,
      );
    }

    if (source === "outputSummary" && path.length > 0) {
      throw new PlannerExecutionError(
        `Planner returned invalid outputSummary binding "${rawBinding}" for ${stepId}.${argPath}.`,
      );
    }

    if (source === "args" && !getSchemaForArgPath(dependencyToolDefinition.argsSchema, path.join("."))) {
      throw new PlannerExecutionError(
        `Planner returned invalid stepArgs binding "${rawBinding}" for ${stepId}.${argPath}. "${path.join(".")}" is not present in ${dependencyToolDefinition.toolId} args.`,
      );
    }

    if (
      source === "data" &&
      !getSchemaForArgPath(dependencyToolDefinition.outputSchema, path.join("."))
    ) {
      throw new PlannerExecutionError(
        `Planner returned invalid stepData binding "${rawBinding}" for ${stepId}.${argPath}. "${path.join(".")}" is not present in ${dependencyToolDefinition.toolId} output.`,
      );
    }
  }
}

function validateRequiredPlannedInputs(
  stepId: string,
  toolDefinition: ToolDefinition,
  args: PlanStep["args"],
  inputBindings: PlanStep["inputBindings"],
): void {
  const requiredArgRoots = (Object.entries(toolDefinition.argsSchema.shape) as Array<
    [string, z.ZodTypeAny]
  >)
    .filter(([, schema]) => !unwrapSchema(schema).optional)
    .map(([key]) => key);
  const providedArgRoots = new Set([
    ...Object.keys(args ?? {}),
    ...Object.keys(inputBindings ?? {}).map((path) => path.split(".")[0]),
  ]);
  const missingArgRoots = requiredArgRoots.filter((argRoot) => !providedArgRoots.has(argRoot));

  if (missingArgRoots.length > 0) {
    throw new PlannerExecutionError(
      `Planner omitted required args for ${stepId} (${toolDefinition.toolId}): ${missingArgRoots.join(", ")}`,
    );
  }
}

function validatePlannedStepArgs(
  stepId: string,
  toolDefinition: ToolDefinition,
  rawArgs: Record<string, unknown>,
): PlanStep["args"] {
  const normalizedArgs = normalizeToolArgs(toolDefinition.toolId, rawArgs);
  try {
    const parsedArgs =
      toolDefinition.toolId === "db_stats_query"
        ? parsePartialDbStatsQueryInput(normalizedArgs)
        : toolDefinition.argsSchema.partial().parse(normalizedArgs);

    return parsedArgs as PlanStep["args"];
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new PlannerExecutionError(
        `Planner returned invalid args for ${stepId} (${toolDefinition.toolId}): ${formatZodIssues(error)}`,
      );
    }
    throw error;
  }
}

function normalizeToolArgs(
  toolId: PlanStep["toolId"],
  args: Record<string, unknown>,
): Record<string, unknown> {
  if (toolId !== "db_stats_query") {
    return args;
  }

  return normalizeDbStatsQueryInput(args);
}

function serializePlanForPlanner(plan: PlanDraft) {
  return {
    objective: plan.objective,
    assumptions: plan.assumptions,
    steps: plan.steps.map((step) => ({
      id: step.id,
      title: step.title,
      rationale: step.rationale,
      toolId: step.toolId,
      args:
        step.toolId in plannerStepSchemaByToolId
          ? serializePlannerArgsValue(
              plannerStepSchemaByToolId[step.toolId as PlannerExecutableToolId].shape.args,
              step.toolId === "db_stats_query"
                ? serializeDbStatsQueryInputForToolSchema(step.args ?? {})
                : (step.args ?? {}),
            )
          : step.args ?? {},
      dependsOn: step.dependsOn,
      inputBindings:
        step.toolId in plannerStepSchemaByToolId
          ? serializePlannerBindingsValue(
              plannerStepSchemaByToolId[step.toolId as PlannerExecutableToolId].shape.inputBindings,
              step.toolId === "db_stats_query"
                ? serializeDbStatsQueryBindingsForToolSchema(step)
                : (step.inputBindings ?? {}),
            )
          : step.inputBindings ?? {},
    })),
    expectedOutput: plan.expectedOutput,
  };
}

function formatRevisionHistory(revisionHistory: string[]): string {
  const normalizedRevisionHistory = revisionHistory
    .map((note) => note.trim())
    .filter((note) => note.length > 0);

  return normalizedRevisionHistory.length > 0
    ? normalizedRevisionHistory.map((note, index) => `${index + 1}. ${note}`).join("\n")
    : "(none)";
}

async function regenerateChangedStepArgs(
  question: string,
  currentPlan: PlanDraft,
  revisedStep: LlmPlanStep,
  currentStep: PlanStep,
  feedback: string,
  revisionHistory: string[],
): Promise<LlmPlanStep> {
  const serializedCurrentStep =
    serializePlanForPlanner(currentPlan).steps.find((step) => step.id === currentStep.id) ?? {
      id: currentStep.id,
      title: currentStep.title,
      rationale: currentStep.rationale,
      toolId: currentStep.toolId,
      args: currentStep.args ?? {},
      dependsOn: currentStep.dependsOn,
      inputBindings: currentStep.inputBindings ?? {},
    };

  const prompt = [
    `Question:\n${question}`,
    "",
    "Revision history for this plan:",
    formatRevisionHistory(revisionHistory),
    "",
    `Latest revision request:\n${feedback}`,
    "",
    "Executable tool registry:",
    describeExecutableTools(),
    "",
    "Binding syntax:",
    '- Use `dependsOn` with earlier step ids only.',
    '- Return `args` using the selected tool\'s exact arg object shape.',
    '- Set optional arg fields to `null` when unused.',
    '- Return `inputBindings` using the same object shape as `args`.',
    '- Any step referenced by `inputBindings` must also be listed in `dependsOn`.',
    '- Put literal values directly in `args`. `inputBindings` is only for references.',
    '- At leaf fields in `inputBindings`, set a binding object or `null`.',
    '- Allowed binding objects are: `{"source":"question"}`, `{"source":"currentStep","field":"title"}`, `{"source":"currentStep","field":"rationale"}`, `{"source":"stepData","stepId":"step-1","path":"field.name"}`, `{"source":"stepArgs","stepId":"step-1","path":"field.name"}`, or `{"source":"stepOutputSummary","stepId":"step-1"}`.',
    '- Do not put literal strings like `\"7\"`, `\"30\"`, or free text into `inputBindings`.',
    '- For nested args like `messageFilters.lastDays`, set `inputBindings.messageFilters.lastDays = {"source":"stepData","stepId":"step-1","path":"windowDays"}`.',
    '- For `stepData` bindings, only reference fields declared in the producing tool\'s Output schema.',
    '- For `db_stats_query`, use exactly one matching filter object: `messageFilters`, `groupFilters`, `userFilters`, or `membershipFilters`, based on `entity`.',
    "",
    `Current step before revision:\n${JSON.stringify(serializedCurrentStep, null, 2)}`,
    "",
    `Revised step metadata:\n${JSON.stringify(
      {
        id: revisedStep.id,
        title: revisedStep.title,
        rationale: revisedStep.rationale,
        toolId: revisedStep.toolId,
        dependsOn: revisedStep.dependsOn,
      },
      null,
      2,
    )}`,
    "",
    "The toolId changed for this step. Regenerate args and inputBindings from scratch for the revised tool schema. Do not reuse the previous tool args or bindings.",
  ].join("\n");

  const revisedToolId = revisedStep.toolId as PlannerExecutableToolId;
  const repaired = await runStructuredAgentWithRetry(
    createStepArgsRepairAgent(revisedToolId),
    prompt,
    (output) => stepRepairSchemaByToolId[revisedToolId].parse(output),
  );

  return {
    ...revisedStep,
    args: repaired.args,
    inputBindings: repaired.inputBindings,
  };
}

async function regenerateArgsForChangedToolSteps(
  question: string,
  currentPlan: PlanDraft,
  revisedPlan: LlmPlanDraft,
  feedback: string,
  revisionHistory: string[],
): Promise<LlmPlanDraft> {
  const currentStepsById = new Map(currentPlan.steps.map((step) => [step.id, step]));
  const repairedSteps: LlmPlanStep[] = [];

  for (const revisedStep of revisedPlan.steps) {
    const currentStep = currentStepsById.get(revisedStep.id.trim());
    if (!currentStep || currentStep.toolId === revisedStep.toolId) {
      repairedSteps.push(revisedStep);
      continue;
    }

    repairedSteps.push(
      await regenerateChangedStepArgs(
        question,
        currentPlan,
        revisedStep,
        currentStep,
        feedback,
        revisionHistory,
      ),
    );
  }

  return {
    ...revisedPlan,
    steps: repairedSteps,
  };
}

function getObjectPath(source: unknown, path: string[]): unknown {
  let current = source;
  for (const segment of path) {
    if (!current || typeof current !== "object" || Array.isArray(current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return current;
}

function setObjectPath(target: Record<string, unknown>, path: string, value: unknown): void {
  const segments = path.split(".").filter(Boolean);
  if (segments.length === 0) {
    return;
  }

  let cursor: Record<string, unknown> = target;
  for (let index = 0; index < segments.length - 1; index += 1) {
    const segment = segments[index];
    const next = cursor[segment];
    if (!next || typeof next !== "object" || Array.isArray(next)) {
      cursor[segment] = {};
    }
    cursor = cursor[segment] as Record<string, unknown>;
  }

  cursor[segments[segments.length - 1]] = value;
}

function resolveBindingValue(
  argPath: string,
  binding: string,
  question: string,
  step: PlanStep,
  plan: PlanDraft,
  artifacts: ExecutionArtifact[],
): unknown {
  const normalized = binding.startsWith("$") ? binding.slice(1) : binding;

  if (normalized === "question") {
    return question;
  }

  if (normalized === "currentStep.title") {
    return step.title;
  }

  if (normalized === "currentStep.rationale") {
    return step.rationale;
  }

  if (normalized.startsWith("steps.")) {
    const [, stepId, source, ...path] = normalized.split(".");
    const referencedStep = plan.steps.find((candidate) => candidate.id === stepId);
    const artifact = artifacts.find((candidate) => candidate.stepId === stepId);
    let resolvedValue: unknown;

    if (source === "data") {
      resolvedValue = getObjectPath(artifact?.data, path);
    } else if (source === "args") {
      const argsSource =
        referencedStep?.toolId === "db_stats_query" && referencedStep.args
          ? serializeDbStatsQueryInputForToolSchema(referencedStep.args)
          : referencedStep?.args;
      resolvedValue = getObjectPath(argsSource, path);
    } else if (source === "outputSummary") {
      if (typeof referencedStep?.outputSummary === "string") {
        resolvedValue = referencedStep.outputSummary;
      } else if (
        artifact?.data &&
        typeof artifact.data === "object" &&
        !Array.isArray(artifact.data)
      ) {
        resolvedValue = summarizeStepData(artifact.data);
      }
    }

    if (resolvedValue === undefined) {
      throw new PlanExecutionError(
        `Step ${step.id} could not resolve binding "${binding}" for "${argPath}".`,
      );
    }

    return resolvedValue;
  }

  throw new PlanExecutionError(
    `Step ${step.id} has invalid binding "${binding}" for "${argPath}". inputBindings must contain only binding references.`,
  );
}

function resolveStepArgs(
  question: string,
  step: PlanStep,
  plan: PlanDraft,
  artifacts: ExecutionArtifact[],
): Record<string, unknown> {
  const clonedArgs = JSON.parse(JSON.stringify(step.args ?? {})) as Record<string, unknown>;

  for (const [argPath, binding] of Object.entries(step.inputBindings ?? {})) {
    const resolvedValue = resolveBindingValue(argPath, binding, question, step, plan, artifacts);
    setObjectPath(clonedArgs, argPath, resolvedValue);
  }

  return clonedArgs;
}

function plannerUnavailableError() {
  return new PlannerUnavailableError("Planner unavailable: OPENAI_API_KEY is not configured.");
}

function plannerFailureError(prefix: string, error: unknown) {
  const message = error instanceof Error ? error.message : "Unknown planner error";
  return new PlannerExecutionError(`${prefix}: ${message}`);
}

function plannerValidationErrorMessage(error: PlannerExecutionError | z.ZodError): string {
  return error instanceof z.ZodError ? formatZodIssues(error) : error.message;
}

function buildPlannerRetryPrompt(prompt: string, validationMessage: string): string {
  return [
    prompt,
    "",
    "Previous attempt failed validation.",
    `Validation error: ${validationMessage}`,
    "Return corrected JSON only and fix the validation error exactly.",
  ].join("\n");
}

async function runStructuredAgentWithRetry<T>(
  agent: Agent<any, any>,
  prompt: string,
  parseOutput: (output: unknown) => T,
  runContext?: SpecialistExecutionContext,
): Promise<T> {
  let currentPrompt = prompt;
  let validationError: PlannerExecutionError | z.ZodError | null = null;

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const result = await run(
      agent,
      currentPrompt,
      runContext ? { context: runContext } : undefined,
    );

    try {
      return parseOutput(result.finalOutput);
    } catch (error) {
      if (
        attempt === 0 &&
        (error instanceof PlannerExecutionError || error instanceof z.ZodError)
      ) {
        validationError = error;
        currentPrompt = buildPlannerRetryPrompt(
          prompt,
          plannerValidationErrorMessage(validationError),
        );
        continue;
      }

      throw error;
    }
  }

  throw validationError ?? new PlannerExecutionError("Structured agent failed after retry.");
}

function summarizeStepData(data: Record<string, unknown>): string {
  const keys = Object.keys(data);
  if (keys.length === 0) {
    return "No structured output returned.";
  }

  if (typeof data.answer === "string") {
    return data.answer.slice(0, 140);
  }

  return `Returned fields: ${keys.slice(0, 5).join(", ")}${keys.length > 5 ? ", ..." : ""}`;
}

type SummarizationInput = {
  text: string;
  maxBullets: number;
  focus?: string;
};

type InfluencerLookupInput = {
  question?: string;
  narrative?: string | null;
};

function throwIfAgentToolCallsFailed(
  managerToolName: ManagerToolName,
  agentToolCalls: AgentToolCall[],
): void {
  const failedToolCalls = agentToolCalls.filter((agentToolCall) => agentToolCall.status === "failed");
  if (failedToolCalls.length === 0) {
    return;
  }

  const failureSummary = failedToolCalls
    .map((agentToolCall) =>
      `${agentToolCall.toolId}: ${agentToolCall.error ?? "Unknown internal tool failure"}`,
    )
    .join("; ");

  throw new Error(
    `Specialist agent "${managerToolName}" had failed internal tool calls: ${failureSummary}`,
  );
}

async function runSummarization(input: SummarizationInput) {
  if (!OPENAI_ENABLED) {
    throw new PlanExecutionError("Cannot execute summarization without OPENAI_API_KEY.");
  }

  const prompt = [
    "Summarize the supplied text into strict JSON.",
    `Requested max bullets: ${input.maxBullets}`,
    input.focus ? `Focus: ${input.focus}` : "Focus: none",
    "",
    `Text:\n${input.text}`,
  ].join("\n");

  try {
    return await runStructuredAgentWithRetry(summarizerAgent, prompt, (output) =>
      summarizationOutputSchema.parse(output),
    );
  } catch (error) {
    const message =
      error instanceof z.ZodError
        ? formatZodIssues(error)
        : error instanceof Error
          ? error.message
          : "Unknown summarization error";
    throw new PlanExecutionError(`Summarization failed: ${message}`);
  }
}

function semanticLookupErrorMessage(error: unknown): string {
  if (error instanceof SemanticConfigError || error instanceof SemanticSearchError) {
    return error.message;
  }

  return error instanceof Error ? error.message : "Unknown semantic audience error";
}

async function runAudienceLookup(question: string) {
  try {
    return await lookupAudienceSegmentsSemantic(question);
  } catch (error) {
    throw new PlanExecutionError(`Audience lookup failed: ${semanticLookupErrorMessage(error)}`);
  }
}

async function runInfluencerLookup(input: InfluencerLookupInput) {
  try {
    return await lookupInfluencersSemantic(input);
  } catch (error) {
    throw new PlanExecutionError(`Influencer lookup failed: ${semanticLookupErrorMessage(error)}`);
  }
}

function buildSpecialistPrompt(
  question: string,
  step: PlanStep,
  resolvedArgs: Record<string, unknown>,
  artifacts: ExecutionArtifact[],
  instruction: string,
): string {
  return [
    `Question: ${question}`,
    `Step title: ${step.title}`,
    `Step rationale: ${step.rationale}`,
    `Tool id: ${step.toolId}`,
    `Resolved args:\n${JSON.stringify(resolvedArgs, null, 2)}`,
    `Prior artifacts:\n${JSON.stringify(artifacts, null, 2)}`,
    instruction,
  ].join("\n\n");
}

async function runWithManagerTool(
  toolName: "audience_builder_agent",
  prompt: string,
  traceContext?: SpecialistExecutionContext,
): Promise<ManagerToolOutputByName["audience_builder_agent"]>;
async function runWithManagerTool(
  toolName: "narrative_explorer_agent",
  prompt: string,
  traceContext?: SpecialistExecutionContext,
): Promise<ManagerToolOutputByName["narrative_explorer_agent"]>;
async function runWithManagerTool(
  toolName: "stats_query_agent",
  prompt: string,
  traceContext?: SpecialistExecutionContext,
): Promise<ManagerToolOutputByName["stats_query_agent"]>;
async function runWithManagerTool(
  toolName: ManagerToolName,
  prompt: string,
  traceContext?: SpecialistExecutionContext,
): Promise<ManagerToolOutputByName[ManagerToolName]> {
  if (!OPENAI_ENABLED) {
    throw new PlanExecutionError(
      `Cannot execute specialist tool "${toolName}" without OPENAI_API_KEY.`,
    );
  }

  try {
    const output = await runStructuredAgentWithRetry(
      specialistAgentByToolName[toolName],
      prompt,
      (output) => managerToolOutputSchemaByName[toolName].parse(output),
      traceContext,
    );
    throwIfAgentToolCallsFailed(toolName, traceContext?.agentToolCalls ?? []);
    return output;
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown specialist execution error";
    throw new PlanExecutionError(`Specialist tool "${toolName}" failed: ${message}`);
  }
}

async function executeToolById(
  question: string,
  plan: PlanDraft,
  step: PlanStep,
  artifacts: ExecutionArtifact[],
  hooks: Pick<ExecutionHooks, "onAgentToolCalls"> = {},
): Promise<StepExecutionResult> {
  const toolDefinition = getToolDefinition(step.toolId);
  if (!toolDefinition) {
    throw new PlanExecutionError(`Unknown toolId at execution time: ${step.toolId}`);
  }

  const normalizedResolvedArgs = normalizeToolArgs(
    step.toolId,
    resolveStepArgs(question, step, plan, artifacts),
  );
  const resolvedArgs =
    step.toolId === "db_stats_query"
      ? (parseDbStatsQueryInput(normalizedResolvedArgs) as unknown as Record<string, unknown>)
      : (toolDefinition.argsSchema.parse(normalizedResolvedArgs) as Record<string, unknown>);

  switch (step.toolId) {
    case "planning_notes": {
      const data = {
        question: String(resolvedArgs.question ?? question),
        focus: resolvedArgs.focus ?? step.title,
        note: "Question classified and routed according to the executable plan.",
        expectedArtifacts: ["stats", "audience", "narratives"],
      };
      return { data, summary: summarizeStepData(data) };
    }

    case "db_stats_query": {
      const data = await runStatsQuery(resolvedArgs as StatsQueryInput);
      return { data, summary: summarizeStepData(data) };
    }

    case "find_narratives_in_timeframe": {
      const data = await latestNarrativesInTimeframe(
        resolvedArgs.timeframeDays as number,
        resolvedArgs.limit as number,
      );
      return { data, summary: summarizeStepData(data) };
    }

    case "summarization": {
      const data = await runSummarization({
        text: resolvedArgs.text as string,
        maxBullets: resolvedArgs.maxBullets as number,
        focus: typeof resolvedArgs.focus === "string" ? resolvedArgs.focus : undefined,
      });
      return { data, summary: summarizeStepData(data) };
    }

    case "audience_lookup": {
      const data = await runAudienceLookup(resolvedArgs.question as string);
      return { data, summary: summarizeStepData(data) };
    }

    case "influencer_lookup": {
      const data = await runInfluencerLookup({
        question: typeof resolvedArgs.question === "string" ? resolvedArgs.question : undefined,
        narrative:
          typeof resolvedArgs.narrative === "string" || resolvedArgs.narrative === null
            ? resolvedArgs.narrative
            : null,
      });
      return { data, summary: summarizeStepData(data) };
    }

    case "narrative_probe": {
      const data = buildNarrativeProbe(resolvedArgs.question as string);
      return { data, summary: summarizeStepData(data) };
    }

    case "audience_builder_agent": {
      const traceContext: SpecialistExecutionContext = {
        agentToolCalls: [],
        onAgentToolCalls: async (agentToolCalls) => hooks.onAgentToolCalls?.(step, agentToolCalls),
        rootQuestion: typeof resolvedArgs.question === "string" ? resolvedArgs.question : question,
        failedToolCallEncountered: false,
      };
      const prompt = buildSpecialistPrompt(
        question,
        step,
        resolvedArgs,
        artifacts,
        "Return audience insights and influencer leads.",
      );
      const data = await runWithManagerTool("audience_builder_agent", prompt, traceContext);
      return {
        data,
        summary: summarizeStepData(data),
        agentToolCalls: cloneAgentToolCalls(traceContext.agentToolCalls),
      };
    }

    case "narrative_explorer_agent": {
      const traceContext: SpecialistExecutionContext = {
        agentToolCalls: [],
        onAgentToolCalls: async (agentToolCalls) => hooks.onAgentToolCalls?.(step, agentToolCalls),
        rootQuestion: typeof resolvedArgs.question === "string" ? resolvedArgs.question : question,
        failedToolCallEncountered: false,
      };
      const prompt = buildSpecialistPrompt(
        question,
        step,
        resolvedArgs,
        artifacts,
        "Return narrative, retrieval, and sentiment insights.",
      );
      const data = await runWithManagerTool("narrative_explorer_agent", prompt, traceContext);
      return {
        data,
        summary: summarizeStepData(data),
        agentToolCalls: cloneAgentToolCalls(traceContext.agentToolCalls),
      };
    }

    case "synthesis_agent": {
      const finalAnswer = await synthesizeFinalAnswer(question, artifacts);
      const data = {
        answer: finalAnswer.answer,
        keyFindingsCount: finalAnswer.keyFindings.length,
        caveatsCount: finalAnswer.caveats.length,
      };
      return { data, summary: summarizeStepData(data), finalAnswer };
    }

    default: {
      throw new PlanExecutionError(`Unhandled toolId at execution time: ${step.toolId}`);
    }
  }
}

export async function draftPlan(question: string): Promise<PlanDraft> {
  if (!OPENAI_ENABLED) {
    throw plannerUnavailableError();
  }

  const prompt = [
    `User question:\n${question}`,
    "",
    "Executable tool registry:",
    describeExecutableTools(),
    "",
    "Binding syntax:",
    '- Use `dependsOn` with earlier step ids only.',
    '- Return `args` using the selected tool\'s exact arg object shape.',
    '- Set optional arg fields to `null` when unused.',
    '- Return `inputBindings` using the same object shape as `args`.',
    '- Any step referenced by `inputBindings` must also be listed in `dependsOn`.',
    '- Put literal values directly in `args`. `inputBindings` is only for references.',
    '- At leaf fields in `inputBindings`, set a binding object or `null`.',
    '- Allowed binding objects are: `{"source":"question"}`, `{"source":"currentStep","field":"title"}`, `{"source":"currentStep","field":"rationale"}`, `{"source":"stepData","stepId":"step-1","path":"field.name"}`, `{"source":"stepArgs","stepId":"step-1","path":"field.name"}`, or `{"source":"stepOutputSummary","stepId":"step-1"}`.',
    '- Do not put literal strings like `\"7\"`, `\"30\"`, or free text into `inputBindings`.',
    '- For nested args like `messageFilters.lastDays`, set `inputBindings.messageFilters.lastDays = {"source":"stepData","stepId":"step-1","path":"windowDays"}`.',
    '- For `stepData` bindings, only reference fields declared in the producing tool\'s Output schema.',
    '- Do not invent input binding target fields like `narratives`, `segments`, `stats`, or `results` unless they are declared args for that tool.',
    '- For `db_stats_query`, use exactly one matching filter object: `messageFilters`, `groupFilters`, `userFilters`, or `membershipFilters`, based on `entity`.',
    "",
    "Reference context:",
    plannerReferenceContext(),
    "",
    "Build a strict execution plan JSON.",
  ].join("\n");

  try {
    return await runStructuredAgentWithRetry(plannerAgent, prompt, (output) =>
      normalizePlanDraft(llmPlanDraftSchema.parse(output)),
    );
  } catch (error) {
    throw plannerFailureError("Planner failed to draft a plan", error);
  }
}

export async function revisePlan(
  question: string,
  currentPlan: PlanDraft,
  feedback: string,
  revisionHistory: string[] = [feedback],
): Promise<PlanDraft> {
  if (!OPENAI_ENABLED) {
    throw plannerUnavailableError();
  }

  const normalizedRevisionHistory = revisionHistory
    .map((note) => note.trim())
    .filter((note) => note.length > 0);

  const prompt = [
    `Question:\n${question}`,
    "",
    "Revision history for this plan:",
    formatRevisionHistory(normalizedRevisionHistory),
    "",
    `Latest revision request:\n${feedback}`,
    "",
    "Executable tool registry:",
    describeExecutableTools(),
    "",
    "Binding syntax:",
    '- Use `dependsOn` with earlier step ids only.',
    '- Return `args` using the selected tool\'s exact arg object shape.',
    '- Set optional arg fields to `null` when unused.',
    '- Return `inputBindings` using the same object shape as `args`.',
    '- Any step referenced by `inputBindings` must also be listed in `dependsOn`.',
    '- Put literal values directly in `args`. `inputBindings` is only for references.',
    '- At leaf fields in `inputBindings`, set a binding object or `null`.',
    '- Allowed binding objects are: `{"source":"question"}`, `{"source":"currentStep","field":"title"}`, `{"source":"currentStep","field":"rationale"}`, `{"source":"stepData","stepId":"step-1","path":"field.name"}`, `{"source":"stepArgs","stepId":"step-1","path":"field.name"}`, or `{"source":"stepOutputSummary","stepId":"step-1"}`.',
    '- Do not put literal strings like `\"7\"`, `\"30\"`, or free text into `inputBindings`.',
    '- For nested args like `messageFilters.lastDays`, set `inputBindings.messageFilters.lastDays = {"source":"stepData","stepId":"step-1","path":"windowDays"}`.',
    '- For `stepData` bindings, only reference fields declared in the producing tool\'s Output schema.',
    '- Do not invent input binding target fields like `narratives`, `segments`, `stats`, or `results` unless they are declared args for that tool.',
    '- For `db_stats_query`, use exactly one matching filter object: `messageFilters`, `groupFilters`, `userFilters`, or `membershipFilters`, based on `entity`.',
    "- Keep already-accepted revisions unless the latest request explicitly changes them.",
    "- Preserve existing step ids where possible.",
    "- If a step changes toolId, regenerate args and inputBindings for the new tool schema instead of reusing the prior tool's args.",
    "",
    `Current plan JSON:\n${JSON.stringify(serializePlanForPlanner(currentPlan), null, 2)}`,
    "",
    "Return the revised plan JSON.",
  ].join("\n");

  try {
    const parsed = await runStructuredAgentWithRetry(reviserAgent, prompt, (output) =>
      llmPlanDraftSchema.parse(output),
    );
    const repaired = await regenerateArgsForChangedToolSteps(
      question,
      currentPlan,
      parsed,
      feedback,
      normalizedRevisionHistory,
    );
    return normalizePlanDraft(repaired);
  } catch (error) {
    throw plannerFailureError("Planner failed to revise the plan", error);
  }
}

type StepExecutionResult = {
  data: Record<string, unknown>;
  summary: string;
  finalAnswer?: FinalAnswer;
  agentToolCalls?: AgentToolCall[];
};

async function executeSingleStep(
  question: string,
  plan: PlanDraft,
  step: PlanStep,
  artifacts: ExecutionArtifact[],
  hooks: Pick<ExecutionHooks, "onAgentToolCalls"> = {},
): Promise<StepExecutionResult> {
  for (const dependency of step.dependsOn) {
    const satisfied = artifacts.some((artifact) => artifact.stepId === dependency);
    if (!satisfied) {
      throw new PlanExecutionError(
        `Step ${step.id} cannot run before dependency ${dependency} completes.`,
      );
    }
  }

  return executeToolById(question, plan, step, artifacts, hooks);
}

async function synthesizeFinalAnswer(
  question: string,
  artifacts: ExecutionArtifact[],
): Promise<FinalAnswer> {
  if (!OPENAI_ENABLED) {
    throw new PlanExecutionError("Cannot synthesize a final answer without OPENAI_API_KEY.");
  }

  const prompt = [
    `Question: ${question}`,
    "",
    `Artifacts:\n${JSON.stringify(artifacts, null, 2)}`,
    "",
    "Produce strict JSON with fields: answer, keyFindings, dataPoints, caveats, recommendedNextQuestions.",
    "Potential questions are inspirations for follow-ups only.",
    plannerReferenceContext(),
  ].join("\n");

  try {
    const result = await run(synthesisAgent, prompt);
    return llmFinalAnswerSchema.parse(result.finalOutput);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown synthesis error";
    throw new PlanExecutionError(`Synthesis failed: ${message}`);
  }
}

export type ExecutionHooks = {
  onStepStart?: (step: PlanStep) => Promise<void> | void;
  onAgentToolCalls?: (
    step: PlanStep,
    agentToolCalls: AgentToolCall[],
  ) => Promise<void> | void;
  onStepComplete?: (
    step: PlanStep,
    artifact: ExecutionArtifact,
    summary: string,
  ) => Promise<void> | void;
  onStepFailed?: (step: PlanStep, error: string) => Promise<void> | void;
};

export async function executePlan(
  question: string,
  plan: PlanDraft,
  hooks: ExecutionHooks = {},
): Promise<{ artifacts: ExecutionArtifact[]; finalAnswer: FinalAnswer }> {
  const artifacts: ExecutionArtifact[] = [];
  let finalAnswer: FinalAnswer | null = null;

  for (const step of plan.steps) {
    await hooks.onStepStart?.(step);

    try {
      const stepResult = await executeSingleStep(question, plan, step, artifacts, hooks);
      const artifact: ExecutionArtifact = {
        stepId: step.id,
        owner: step.owner,
        tool: step.tool,
        toolId: step.toolId,
        data: stepResult.data,
        agentToolCalls: stepResult.agentToolCalls ?? [],
      };

      artifacts.push(artifact);
      if (stepResult.finalAnswer) {
        finalAnswer = stepResult.finalAnswer;
      }

      await hooks.onStepComplete?.(step, artifact, stepResult.summary);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown step failure";
      await hooks.onStepFailed?.(step, message);
      throw error;
    }
  }

  if (!finalAnswer) {
    finalAnswer = await synthesizeFinalAnswer(question, artifacts);
  }

  return {
    artifacts,
    finalAnswer,
  };
}
