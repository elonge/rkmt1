import { Agent, run, tool } from "@openai/agents";
import { z } from "zod";
import {
  latestNarrativesInTimeframe,
  lookupAudienceSegments,
  lookupInfluencers,
  runStatsQuery,
  searchMessagesByMockVector,
} from "../data/dummy-db";
import {
  describeExecutableTools,
  executableToolIdSchema,
  getExecutableToolDefinition,
  getToolDefinition,
} from "./tools";
import {
  ExecutionArtifact,
  FinalAnswer,
  PlanDraft,
  PlanStep,
  StatsQueryInput,
} from "../types";
import { getPotentialQuestions, plannerReferenceContext } from "./context";

const MODEL_NAME = process.env.OPENAI_MODEL ?? "gpt-4.1";
const OPENAI_ENABLED = Boolean(process.env.OPENAI_API_KEY);

const llmPlanStepSchema = z.object({
  id: z.string(),
  title: z.string(),
  rationale: z.string(),
  toolId: executableToolIdSchema,
  argsJson: z.string(),
  dependsOn: z.array(z.string()),
  inputBindingsJson: z.string(),
});

const llmPlanDraftSchema = z.object({
  objective: z.string(),
  assumptions: z.array(z.string()),
  steps: z.array(llmPlanStepSchema).min(1),
  expectedOutput: z.object({
    format: z.literal("json"),
    sections: z.array(z.string()).min(1),
  }),
});

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

type LlmPlanDraft = z.infer<typeof llmPlanDraftSchema>;

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

function buildSummaryBullets(text: string, maxBullets: number) {
  const compact = text.replace(/\s+/g, " ").trim();
  const snippet = compact.slice(0, 320);
  const bulletCount = Math.max(1, Math.min(maxBullets, 10));
  return Array.from({ length: bulletCount }, (_, idx) =>
    idx === 0 ? snippet : `Dummy bullet ${idx + 1}`,
  );
}

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

function strictToolParameters(toolId: string) {
  return requireToolDefinition(toolId).argsSchema.strict();
}

const vectorSearchToolDefinition = requireToolDefinition("vector_search");
const summarizationToolDefinition = requireToolDefinition("summarization");
const dbStatsQueryToolDefinition = requireToolDefinition("db_stats_query");
const audienceLookupToolDefinition = requireToolDefinition("audience_lookup");
const influencerLookupToolDefinition = requireToolDefinition("influencer_lookup");
const narrativeProbeToolDefinition = requireToolDefinition("narrative_probe");
const latestNarrativesToolDefinition = requireToolDefinition("find_narratives_in_timeframe");
const audienceBuilderAgentToolDefinition = requireToolDefinition("audience_builder_agent");
const narrativeExplorerAgentToolDefinition = requireToolDefinition("narrative_explorer_agent");
const statsQueryAgentToolDefinition = requireToolDefinition("stats_query_agent");

const vectorSearchTool = tool({
  name: vectorSearchToolDefinition.toolId,
  description: vectorSearchToolDefinition.description,
  parameters: strictToolParameters(vectorSearchToolDefinition.toolId),
  async execute(input) {
    return searchMessagesByMockVector(input.query, input.topK);
  },
});

const summarizationTool = tool({
  name: summarizationToolDefinition.toolId,
  description: summarizationToolDefinition.description,
  parameters: strictToolParameters(summarizationToolDefinition.toolId),
  async execute(input) {
    return {
      source: "dummy-summarizer",
      bullets: buildSummaryBullets(input.text, input.maxBullets),
    };
  },
});

const dbStatsQueryTool = tool({
  name: dbStatsQueryToolDefinition.toolId,
  description: dbStatsQueryToolDefinition.description,
  parameters: strictToolParameters(dbStatsQueryToolDefinition.toolId),
  async execute(input) {
    return runStatsQuery({
      metric: input.metric,
      lastDays: input.lastDays ?? undefined,
      limit: input.limit ?? undefined,
    } as StatsQueryInput);
  },
});

const audienceLookupTool = tool({
  name: audienceLookupToolDefinition.toolId,
  description: audienceLookupToolDefinition.description,
  parameters: strictToolParameters(audienceLookupToolDefinition.toolId),
  async execute(input) {
    return lookupAudienceSegments(input.question);
  },
});

const influencerLookupTool = tool({
  name: influencerLookupToolDefinition.toolId,
  description: influencerLookupToolDefinition.description,
  parameters: strictToolParameters(influencerLookupToolDefinition.toolId),
  async execute(input) {
    return lookupInfluencers(input.narrative);
  },
});

const narrativeProbeTool = tool({
  name: narrativeProbeToolDefinition.toolId,
  description: narrativeProbeToolDefinition.description,
  parameters: strictToolParameters(narrativeProbeToolDefinition.toolId),
  async execute(input) {
    return buildNarrativeProbe(input.question);
  },
});

const latestNarrativesTimeframeTool = tool({
  name: latestNarrativesToolDefinition.toolId,
  description: latestNarrativesToolDefinition.description,
  parameters: strictToolParameters(latestNarrativesToolDefinition.toolId),
  async execute(input) {
    return latestNarrativesInTimeframe(input.timeframeDays, input.limit);
  },
});

const audienceAgentOutputSchema = z.object({
  audienceSummary: z.string(),
  segmentCandidates: z.array(z.string()),
  influencerLeads: z.array(z.string()),
});

const narrativeAgentOutputSchema = z.object({
  narrativeSummary: z.string(),
  topNarratives: z.array(z.string()),
  sentimentHighlights: z.array(z.string()),
});

const statsAgentOutputSchema = z.object({
  metricUsed: z.string(),
  highlights: z.array(z.string()),
  raw: z.record(z.any()),
});

const audienceBuilderAgent = new Agent({
  name: "AudienceBuilder",
  model: MODEL_NAME,
  instructions:
    "You are a specialist for users/groups/influencer audience construction. Use tools and return concise JSON.",
  tools: [audienceLookupTool, influencerLookupTool, dbStatsQueryTool],
  outputType: audienceAgentOutputSchema,
});

const narrativeExplorerAgent = new Agent({
  name: "NarrativeExplorer",
  model: MODEL_NAME,
  instructions:
    "You are a specialist for narrative, sentiment, and text exploration. For latest narrative/volume requests, use find_narratives_in_timeframe. Return concise JSON.",
  tools: [narrativeProbeTool, latestNarrativesTimeframeTool, vectorSearchTool, summarizationTool],
  outputType: narrativeAgentOutputSchema,
});

const statsQueryAgent = new Agent({
  name: "StatsQuery",
  model: MODEL_NAME,
  instructions:
    "You are a metrics specialist. Pick an appropriate stats query, call db_stats_query, and summarize key facts.",
  tools: [dbStatsQueryTool],
  outputType: statsAgentOutputSchema,
});

const audienceBuilderAgentTool = audienceBuilderAgent.asTool({
  toolName: audienceBuilderAgentToolDefinition.toolId,
  toolDescription: audienceBuilderAgentToolDefinition.description,
});

const narrativeExplorerAgentTool = narrativeExplorerAgent.asTool({
  toolName: narrativeExplorerAgentToolDefinition.toolId,
  toolDescription: narrativeExplorerAgentToolDefinition.description,
});

const statsQueryAgentTool = statsQueryAgent.asTool({
  toolName: statsQueryAgentToolDefinition.toolId,
  toolDescription: statsQueryAgentToolDefinition.description,
});

const plannerAgent = new Agent({
  name: "Planner",
  model: MODEL_NAME,
  instructions: [
    "Build an execution plan for the question.",
    "Use only registered toolIds from the provided executable tool registry.",
    "Return executable steps with toolId, argsJson, dependsOn, and inputBindingsJson.",
    "argsJson must be a JSON string encoding an object, for example {\"metric\":\"political_leaning_distribution\",\"lastDays\":30}.",
    "inputBindingsJson must be a JSON string encoding an object whose values are strings.",
    "The runtime dispatches by toolId, not by free-text tool names.",
    "Only use dependsOn references to earlier steps.",
    "Use inputBindingsJson when step args depend on previous outputs.",
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
    "Return argsJson as a JSON string encoding an object for each step.",
    "Return inputBindingsJson as a JSON string encoding an object with string values for each step.",
    "Return complete plan JSON only.",
  ].join(" "),
  outputType: llmPlanDraftSchema,
});

const synthesisAgent = new Agent({
  name: "Synthesizer",
  model: MODEL_NAME,
  instructions:
    "Synthesize specialist outputs into strict JSON for UI. Mention uncertainty/caveats where relevant.",
  outputType: llmFinalAnswerSchema,
});

function normalizePlanDraft(plan: LlmPlanDraft): PlanDraft {
  const seenStepIds = new Set<string>();

  return {
    objective: plan.objective,
    assumptions: plan.assumptions,
    steps: plan.steps.map((step, idx) => {
      const toolDefinition = getExecutableToolDefinition(step.toolId);
      if (!toolDefinition) {
        throw new Error(`Unknown toolId in plan: ${step.toolId}`);
      }

      const normalized: PlanStep = {
        id:
          step.id.trim() && !seenStepIds.has(step.id.trim())
            ? step.id.trim()
            : `step-${idx + 1}`,
        title: step.title,
        rationale: step.rationale,
        owner: toolDefinition.owner,
        tool: toolDefinition.label,
        toolId: step.toolId,
        args: parsePlannerArgs(step.argsJson, step.id),
        dependsOn: parsePlannerDependsOn(step.dependsOn, step.id).filter((dependency) =>
          seenStepIds.has(dependency),
        ),
        inputBindings: parsePlannerInputBindings(step.inputBindingsJson, step.id),
        status: "pending",
      };

      seenStepIds.add(normalized.id);

      return normalized;
    }),
    expectedOutput: {
      format: "json",
      sections: plan.expectedOutput.sections,
    },
  };
}

function parsePlannerArgs(argsJson: string, stepId: string): PlanStep["args"] {
  try {
    const parsed = JSON.parse(argsJson) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("argsJson must decode to an object");
    }
    return parsed as PlanStep["args"];
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown JSON parse error";
    throw new PlannerExecutionError(`Planner returned invalid argsJson for ${stepId}: ${message}`);
  }
}

function parsePlannerDependsOn(dependsOn: string[], stepId: string): string[] {
  if (!Array.isArray(dependsOn) || dependsOn.some((value) => typeof value !== "string")) {
    throw new PlannerExecutionError(`Planner returned invalid dependsOn for ${stepId}.`);
  }
  return dependsOn;
}

function parsePlannerInputBindings(
  inputBindingsJson: string,
  stepId: string,
): PlanStep["inputBindings"] {
  try {
    const parsed = JSON.parse(inputBindingsJson) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("inputBindingsJson must decode to an object");
    }

    for (const value of Object.values(parsed)) {
      if (typeof value !== "string") {
        throw new Error("inputBindingsJson values must be strings");
      }
    }

    return parsed as PlanStep["inputBindings"];
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown JSON parse error";
    throw new PlannerExecutionError(
      `Planner returned invalid inputBindingsJson for ${stepId}: ${message}`,
    );
  }
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
      argsJson: JSON.stringify(step.args ?? {}),
      dependsOn: step.dependsOn,
      inputBindingsJson: JSON.stringify(step.inputBindings ?? {}),
    })),
    expectedOutput: plan.expectedOutput,
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

    if (source === "data") {
      return getObjectPath(artifact?.data, path);
    }

    if (source === "args") {
      return getObjectPath(referencedStep?.args, path);
    }

    if (source === "outputSummary") {
      return referencedStep?.outputSummary;
    }
  }

  return binding;
}

function resolveStepArgs(
  question: string,
  step: PlanStep,
  plan: PlanDraft,
  artifacts: ExecutionArtifact[],
): Record<string, unknown> {
  const clonedArgs = JSON.parse(JSON.stringify(step.args ?? {})) as Record<string, unknown>;

  for (const [argPath, binding] of Object.entries(step.inputBindings ?? {})) {
    const resolvedValue = resolveBindingValue(binding, question, step, plan, artifacts);
    if (resolvedValue !== undefined) {
      setObjectPath(clonedArgs, argPath, resolvedValue);
    }
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

function parseRecordOutput(raw: unknown): Record<string, unknown> {
  if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    return raw as Record<string, unknown>;
  }

  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>;
      }
    } catch {
      // No-op; fallback below.
    }
    return { raw };
  }

  return { raw: JSON.stringify(raw) };
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

function pickStatsQuery(question: string): StatsQueryInput {
  const normalized = question.toLowerCase();

  if (normalized.includes("distribution") || normalized.includes("leaning")) {
    return { metric: "political_leaning_distribution", lastDays: 30 };
  }

  if (normalized.includes("influencer") || normalized.includes("group")) {
    return { metric: "top_groups_by_member_count", limit: 5 };
  }

  return { metric: "active_messages_last_days", lastDays: 7 };
}

function fallbackFinalAnswer(question: string, artifacts: ExecutionArtifact[]): FinalAnswer {
  const keyFindings = artifacts
    .slice(0, 4)
    .map((artifact) => `${artifact.owner}: ${summarizeStepData(artifact.data)}`);

  const dataPoints = artifacts.flatMap((artifact) => {
    const candidates: { label: string; value: string | number | boolean }[] = [];
    for (const [key, value] of Object.entries(artifact.data)) {
      if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
        candidates.push({ label: `${artifact.owner}.${key}`, value });
      }
      if (candidates.length >= 2) {
        break;
      }
    }
    return candidates;
  });

  return {
    answer: `Prototype answer for: ${question}`,
    keyFindings,
    dataPoints: dataPoints.slice(0, 8),
    caveats: [
      "This run uses Mongo-backed data from the configured collections.",
      "Audience, narrative extraction, influencer scoring, and vector search still use simplified heuristics.",
    ],
    recommendedNextQuestions: getPotentialQuestions().slice(0, 3),
  };
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

function specialistManager(toolChoice: string): Agent {
  return new Agent({
    name: "SpecialistManager",
    model: MODEL_NAME,
    instructions: [
      "Execute a single specialist step.",
      "Call the selected tool exactly once and return structured JSON.",
      "Do not answer from general knowledge without using the tool.",
    ].join(" "),
    tools: [audienceBuilderAgentTool, narrativeExplorerAgentTool, statsQueryAgentTool],
    toolUseBehavior: "stop_on_first_tool",
    modelSettings: {
      toolChoice,
    },
  });
}

async function runWithManagerTool(
  toolName: "audience_builder_agent" | "narrative_explorer_agent" | "stats_query_agent",
  prompt: string,
): Promise<Record<string, unknown>> {
  if (!OPENAI_ENABLED) {
    if (toolName === "audience_builder_agent") {
      const audience = await lookupAudienceSegments(prompt);
      const influencers = await lookupInfluencers(null);
      return {
        mode: "fallback",
        tool: toolName,
        audience,
        influencers,
      };
    }

    if (toolName === "narrative_explorer_agent") {
      return {
        mode: "fallback",
        tool: toolName,
        latestNarratives: await latestNarrativesInTimeframe(7, 8),
        relatedMessages: await searchMessagesByMockVector(prompt, 5),
      };
    }

    if (toolName === "stats_query_agent") {
      return {
        mode: "fallback",
        tool: toolName,
        stats: await runStatsQuery(pickStatsQuery(prompt)),
      };
    }

    return {
      mode: "fallback",
      tool: toolName,
      note: "OPENAI_API_KEY missing; returned deterministic Mongo-backed fallback output.",
      prompt,
    };
  }

  try {
    const result = await run(specialistManager(toolName), prompt);
    return parseRecordOutput(result.finalOutput);
  } catch (error) {
    return {
      mode: "fallback",
      tool: toolName,
      error: error instanceof Error ? error.message : "Unknown error",
    };
  }
}

async function executeToolById(
  question: string,
  plan: PlanDraft,
  step: PlanStep,
  artifacts: ExecutionArtifact[],
): Promise<StepExecutionResult> {
  const toolDefinition = getExecutableToolDefinition(step.toolId);
  if (!toolDefinition) {
    throw new Error(`Unknown toolId at execution time: ${step.toolId}`);
  }

  const resolvedArgs = toolDefinition.argsSchema.parse(
    resolveStepArgs(question, step, plan, artifacts),
  ) as Record<string, unknown>;

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
      const statsArgs = {
        ...pickStatsQuery(`${question} ${step.title}`),
        ...resolvedArgs,
      } as StatsQueryInput;
      const data = await runStatsQuery(statsArgs);
      return { data, summary: summarizeStepData(data) };
    }

    case "find_narratives_in_timeframe": {
      const timeframeDays =
        typeof resolvedArgs.timeframeDays === "number" ? resolvedArgs.timeframeDays : 7;
      const limit = typeof resolvedArgs.limit === "number" ? resolvedArgs.limit : 8;
      const data = await latestNarrativesInTimeframe(timeframeDays, limit);
      return { data, summary: summarizeStepData(data) };
    }

    case "vector_search": {
      const query =
        typeof resolvedArgs.query === "string" && resolvedArgs.query.trim().length > 0
          ? resolvedArgs.query
          : question;
      const topK = typeof resolvedArgs.topK === "number" ? resolvedArgs.topK : 5;
      const data = await searchMessagesByMockVector(query, topK);
      return { data, summary: summarizeStepData(data) };
    }

    case "summarization": {
      const text = typeof resolvedArgs.text === "string" ? resolvedArgs.text : JSON.stringify(resolvedArgs);
      const maxBullets = typeof resolvedArgs.maxBullets === "number" ? resolvedArgs.maxBullets : 3;
      const data = {
        source: "dummy-summarizer",
        bullets: buildSummaryBullets(text, maxBullets),
      };
      return { data, summary: summarizeStepData(data) };
    }

    case "audience_lookup": {
      const toolQuestion =
        typeof resolvedArgs.question === "string" && resolvedArgs.question.trim().length > 0
          ? resolvedArgs.question
          : question;
      const data = await lookupAudienceSegments(toolQuestion);
      return { data, summary: summarizeStepData(data) };
    }

    case "influencer_lookup": {
      const narrative =
        typeof resolvedArgs.narrative === "string" || resolvedArgs.narrative === null
          ? resolvedArgs.narrative
          : null;
      const data = await lookupInfluencers(narrative);
      return { data, summary: summarizeStepData(data) };
    }

    case "narrative_probe": {
      const probeQuestion =
        typeof resolvedArgs.question === "string" && resolvedArgs.question.trim().length > 0
          ? resolvedArgs.question
          : question;
      const data = buildNarrativeProbe(probeQuestion);
      return { data, summary: summarizeStepData(data) };
    }

    case "audience_builder_agent": {
      const prompt = buildSpecialistPrompt(
        question,
        step,
        resolvedArgs,
        artifacts,
        "Return audience insights and influencer leads.",
      );
      const data = await runWithManagerTool("audience_builder_agent", prompt);
      return { data, summary: summarizeStepData(data) };
    }

    case "narrative_explorer_agent": {
      const prompt = buildSpecialistPrompt(
        question,
        step,
        resolvedArgs,
        artifacts,
        "Return narrative, retrieval, and sentiment insights.",
      );
      const data = await runWithManagerTool("narrative_explorer_agent", prompt);
      return { data, summary: summarizeStepData(data) };
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
      const data = {
        note: `Unknown toolId: ${step.toolId}`,
      };
      return { data, summary: summarizeStepData(data) };
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
    '- Use `inputBindingsJson` to map resolved values into args.',
    '- Binding values may be `question`, `currentStep.title`, `currentStep.rationale`, `steps.<stepId>.data.<field>`, `steps.<stepId>.args.<field>`, or `steps.<stepId>.outputSummary`.',
    '- Set `argsJson` to a JSON string that decodes to an object. Example: `{"metric":"political_leaning_distribution","lastDays":30}`.',
    '- Set `inputBindingsJson` to a JSON string that decodes to an object. Example: `{"metric":"steps.step-2.data.metric"}`.',
    "",
    "Reference context:",
    plannerReferenceContext(),
    "",
    "Build a strict execution plan JSON.",
  ].join("\n");

  try {
    const result = await run(plannerAgent, prompt);
    const parsed = llmPlanDraftSchema.parse(result.finalOutput);
    return normalizePlanDraft(parsed);
  } catch (error) {
    throw plannerFailureError("Planner failed to draft a plan", error);
  }
}

export async function revisePlan(
  question: string,
  currentPlan: PlanDraft,
  feedback: string,
): Promise<PlanDraft> {
  if (!OPENAI_ENABLED) {
    throw plannerUnavailableError();
  }

  const prompt = [
    `Question:\n${question}`,
    "",
    `User feedback:\n${feedback}`,
    "",
    "Executable tool registry:",
    describeExecutableTools(),
    "",
    "Binding syntax:",
    '- Use `dependsOn` with earlier step ids only.',
    '- Use `inputBindingsJson` to map resolved values into args.',
    '- Binding values may be `question`, `currentStep.title`, `currentStep.rationale`, `steps.<stepId>.data.<field>`, `steps.<stepId>.args.<field>`, or `steps.<stepId>.outputSummary`.',
    '- Set `argsJson` to a JSON string that decodes to an object. Example: `{"metric":"political_leaning_distribution","lastDays":30}`.',
    '- Set `inputBindingsJson` to a JSON string that decodes to an object. Example: `{"metric":"steps.step-2.data.metric"}`.',
    "",
    `Current plan JSON:\n${JSON.stringify(serializePlanForPlanner(currentPlan), null, 2)}`,
    "",
    "Return the revised plan JSON.",
  ].join("\n");

  try {
    const result = await run(reviserAgent, prompt);
    const parsed = llmPlanDraftSchema.parse(result.finalOutput);
    return normalizePlanDraft(parsed);
  } catch (error) {
    throw plannerFailureError("Planner failed to revise the plan", error);
  }
}

type StepExecutionResult = {
  data: Record<string, unknown>;
  summary: string;
  finalAnswer?: FinalAnswer;
};

async function executeSingleStep(
  question: string,
  plan: PlanDraft,
  step: PlanStep,
  artifacts: ExecutionArtifact[],
): Promise<StepExecutionResult> {
  for (const dependency of step.dependsOn) {
    const satisfied = artifacts.some((artifact) => artifact.stepId === dependency);
    if (!satisfied) {
      throw new Error(`Step ${step.id} cannot run before dependency ${dependency} completes.`);
    }
  }

  return executeToolById(question, plan, step, artifacts);
}

async function synthesizeFinalAnswer(
  question: string,
  artifacts: ExecutionArtifact[],
): Promise<FinalAnswer> {
  if (!OPENAI_ENABLED) {
    return fallbackFinalAnswer(question, artifacts);
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
  } catch {
    return fallbackFinalAnswer(question, artifacts);
  }
}

export type ExecutionHooks = {
  onStepStart?: (step: PlanStep) => Promise<void> | void;
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
      const stepResult = await executeSingleStep(question, plan, step, artifacts);
      const artifact: ExecutionArtifact = {
        stepId: step.id,
        owner: step.owner,
        tool: step.tool,
        toolId: step.toolId,
        data: stepResult.data,
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
