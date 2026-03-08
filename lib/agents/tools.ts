import { z } from "zod";
import { PlanStepOwner, statsMetricSchema } from "../types";

export const executableToolIdValues = [
  "db_stats_query",
  "find_narratives_in_timeframe",
  "vector_search",
  "summarization",
  "audience_lookup",
  "influencer_lookup",
  "narrative_probe",
  "audience_builder_agent",
  "narrative_explorer_agent",
  "synthesis_agent",
] as const;

export const internalOnlyToolIdValues = ["planning_notes", "stats_query_agent"] as const;

export const toolIdValues = [
  ...executableToolIdValues,
  ...internalOnlyToolIdValues,
] as const;

export const executableToolIdSchema = z.enum(executableToolIdValues);
export const toolIdSchema = z.enum(toolIdValues);

export type ExecutableToolId = z.infer<typeof executableToolIdSchema>;
export type ToolId = z.infer<typeof toolIdSchema>;

export type ToolDefinition = {
  toolId: ToolId;
  owner: PlanStepOwner;
  label: string;
  description: string;
  argsSchema: z.ZodObject<any>;
  plannerVisible: boolean;
  specialistVisible: boolean;
  kind: "leaf" | "agent";
};

const planningNotesArgsSchema = z
  .object({
    question: z.string().optional(),
    focus: z.string().optional(),
  })
  .passthrough();

const dbStatsQueryArgsSchema = z
  .object({
    metric: statsMetricSchema,
    lastDays: z.number().int().min(1).max(365).nullable().optional(),
    limit: z.number().int().min(1).max(50).nullable().optional(),
  })
  .passthrough();

const narrativesArgsSchema = z
  .object({
    timeframeDays: z.number().int().min(1).max(365),
    limit: z.number().int().min(1).max(50),
  })
  .passthrough();

const vectorSearchArgsSchema = z
  .object({
    query: z.string(),
    topK: z.number().int().min(1).max(10),
  })
  .passthrough();

const summarizationArgsSchema = z
  .object({
    text: z.string(),
    maxBullets: z.number().int().min(1),
  })
  .passthrough();

const audienceLookupArgsSchema = z
  .object({
    question: z.string(),
  })
  .passthrough();

const influencerLookupArgsSchema = z
  .object({
    narrative: z.string().nullable(),
  })
  .passthrough();

const narrativeProbeArgsSchema = z
  .object({
    question: z.string(),
  })
  .passthrough();

const audienceBuilderAgentArgsSchema = z
  .object({
    question: z.string().optional(),
    focus: z.string().optional(),
    narrative: z.string().nullable().optional(),
  })
  .passthrough();

const narrativeExplorerAgentArgsSchema = z
  .object({
    question: z.string().optional(),
    focus: z.string().optional(),
    timeframeDays: z.number().int().min(1).max(365).optional(),
    limit: z.number().int().min(1).max(50).optional(),
    query: z.string().optional(),
  })
  .passthrough();

const synthesisAgentArgsSchema = z
  .object({
    sections: z.array(z.string()).optional(),
    emphasis: z.string().optional(),
  })
  .passthrough();

const statsQueryAgentArgsSchema = z
  .object({
    question: z.string().optional(),
    focus: z.string().optional(),
  })
  .passthrough();

export const toolCatalog: Record<ToolId, ToolDefinition> = {
  planning_notes: {
    toolId: "planning_notes",
    owner: "planner",
    label: "Planning Notes",
    description: "Record a planning note or schema checkpoint without querying external data.",
    argsSchema: planningNotesArgsSchema,
    plannerVisible: false,
    specialistVisible: false,
    kind: "leaf",
  },
  db_stats_query: {
    toolId: "db_stats_query",
    owner: "stats_query",
    label: "DB Stats Query",
    description:
      "Run a Mongo-backed stats query for aggregate metrics such as leaning distribution, recent message volume, and top groups.",
    argsSchema: dbStatsQueryArgsSchema,
    plannerVisible: true,
    specialistVisible: true,
    kind: "leaf",
  },
  find_narratives_in_timeframe: {
    toolId: "find_narratives_in_timeframe",
    owner: "narrative_explorer",
    label: "Find Narratives In Timeframe",
    description:
      "Return simple narrative clusters from Mongo messages in a time window using lightweight heuristics.",
    argsSchema: narrativesArgsSchema,
    plannerVisible: true,
    specialistVisible: true,
    kind: "leaf",
  },
  vector_search: {
    toolId: "vector_search",
    owner: "narrative_explorer",
    label: "Mock Vector Search",
    description: "Run mock semantic search over real Mongo messages and return the top matching snippets.",
    argsSchema: vectorSearchArgsSchema,
    plannerVisible: true,
    specialistVisible: true,
    kind: "leaf",
  },
  summarization: {
    toolId: "summarization",
    owner: "synthesizer",
    label: "Summarization",
    description: "Summarize supplied text into short bullets.",
    argsSchema: summarizationArgsSchema,
    plannerVisible: true,
    specialistVisible: true,
    kind: "leaf",
  },
  audience_lookup: {
    toolId: "audience_lookup",
    owner: "audience_builder",
    label: "Audience Lookup",
    description:
      "Build simple audience segments from Mongo groups, tags, and recent activity for a question.",
    argsSchema: audienceLookupArgsSchema,
    plannerVisible: true,
    specialistVisible: true,
    kind: "leaf",
  },
  influencer_lookup: {
    toolId: "influencer_lookup",
    owner: "audience_builder",
    label: "Influencer Lookup",
    description:
      "Rank likely influencers from Mongo message activity, reactions, replies, and group spread.",
    argsSchema: influencerLookupArgsSchema,
    plannerVisible: true,
    specialistVisible: true,
    kind: "leaf",
  },
  narrative_probe: {
    toolId: "narrative_probe",
    owner: "narrative_explorer",
    label: "Narrative Probe",
    description:
      "Return a lightweight narrative and sentiment interpretation for a question.",
    argsSchema: narrativeProbeArgsSchema,
    plannerVisible: true,
    specialistVisible: true,
    kind: "leaf",
  },
  audience_builder_agent: {
    toolId: "audience_builder_agent",
    owner: "audience_builder",
    label: "Audience Builder Agent",
    description:
      "Find exact audience segments based on the question, focus, and narrative using a specialist agent that can orchestrate audience lookup, influencer lookup, and supporting stats internally.",
    argsSchema: audienceBuilderAgentArgsSchema,
    plannerVisible: true,
    specialistVisible: true,
    kind: "agent",
  },
  narrative_explorer_agent: {
    toolId: "narrative_explorer_agent",
    owner: "narrative_explorer",
    label: "Narrative Explorer Agent",
    description:
      "Specialist agent that can orchestrate narrative timeframe lookup, mock vector search, narrative probe, and summarization internally.",
    argsSchema: narrativeExplorerAgentArgsSchema,
    plannerVisible: true,
    specialistVisible: true,
    kind: "agent",
  },
  synthesis_agent: {
    toolId: "synthesis_agent",
    owner: "synthesizer",
    label: "Synthesis Agent",
    description: "Combine prior step outputs into the final strict JSON answer for the UI.",
    argsSchema: synthesisAgentArgsSchema,
    plannerVisible: true,
    specialistVisible: false,
    kind: "agent",
  },
  stats_query_agent: {
    toolId: "stats_query_agent",
    owner: "stats_query",
    label: "Stats Query Agent",
    description: "Specialist agent for counts, distributions, and aggregate metrics.",
    argsSchema: statsQueryAgentArgsSchema,
    plannerVisible: false,
    specialistVisible: true,
    kind: "agent",
  },
};

export const executableToolRegistry = Object.fromEntries(
  executableToolIdValues.map((toolId) => [toolId, toolCatalog[toolId]]),
) as Record<ExecutableToolId, ToolDefinition>;

export function getToolDefinition(toolId: ToolId): ToolDefinition;
export function getToolDefinition(toolId: string): ToolDefinition | null;
export function getToolDefinition(toolId: string): ToolDefinition | null {
  return toolCatalog[toolId as ToolId] ?? null;
}

export function getExecutableToolDefinition(toolId: string): ToolDefinition | null {
  const tool = getToolDefinition(toolId);
  return tool?.plannerVisible ? tool : null;
}

export function describeExecutableTools(): string {
  return executableToolIdValues
    .map((toolId) => {
      const tool = toolCatalog[toolId];
      const argKeys = Object.keys(tool.argsSchema.shape);
      const argSummary = argKeys.length > 0 ? argKeys.join(", ") : "(no explicit args)";
      return `- ${tool.toolId} (owner: ${tool.owner}) — ${tool.description} Args: ${argSummary}`;
    })
    .join("\n");
}
