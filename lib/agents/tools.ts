import { z } from "zod";
import {
  dbStatsQueryToolInputSchema,
  PlanStepOwner,
} from "../types";
import {
  audienceAgentOutputSchema,
  audienceLookupOutputSchema,
  dbStatsQueryOutputSchema,
  dynamicMongoAggregationOutputSchema,
  groupSearchOutputSchema,
  influencerLookupOutputSchema,
  latestNarrativesOutputSchema,
  messageSearchOutputSchema,
  narrativeAgentOutputSchema,
  narrativeProbeOutputSchema,
  planningNotesOutputSchema,
  statsAgentOutputSchema,
  summarizationOutputSchema,
  synthesisAgentOutputSchema,
  userProfileLookupOutputSchema,
} from "./tool-output-schemas";

export const executableToolIdValues = [
  "find_narratives_in_timeframe",
  "summarization",
  "user_profile_lookup",
  "message_search",
  "group_search",
  "audience_lookup",
  "influencer_lookup",
  "narrative_probe",
  "audience_builder_agent",
  "narrative_explorer_agent",
] as const;

export const internalOnlyToolIdValues = [
  "db_stats_query",
  "dynamic_mongo_aggregation",
  "planning_notes",
  "stats_query_agent",
  "synthesis_agent",
] as const;

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
  outputSchema: z.ZodTypeAny;
  plannerVisible: boolean;
  specialistVisible: boolean;
  kind: "leaf" | "agent";
};

const planningNotesArgsSchema = z
  .object({
    question: z.string().optional(),
    focus: z.string().optional(),
  })
  .strict();

const dbStatsQueryArgsSchema = dbStatsQueryToolInputSchema;

const narrativesArgsSchema = z
  .object({
    timeframeDays: z.number().int().min(1).max(365),
    limit: z.number().int().min(1).max(50),
  })
  .strict();

const summarizationArgsSchema = z
  .object({
    text: z.string(),
    maxBullets: z.number().int().min(1).max(10),
    focus: z.string().optional(),
  })
  .strict();

const audienceLookupArgsSchema = z
  .object({
    question: z.string(),
  })
  .strict();

const dynamicMongoAggregationArgsSchema = z
  .object({
    question: z.string(),
  })
  .strict();

const userProfileLookupArgsSchema = z
  .object({
    query: z.string().nullable().optional(),
    userIds: z.array(z.string()).nullable().optional(),
    groupIds: z.array(z.string()).nullable().optional(),
    membershipStatus: z.enum(["JOINED", "LEFT"]).nullable().optional(),
    roles: z.array(z.string()).nullable().optional(),
    limit: z.number().int().min(1).max(50).optional(),
  })
  .strict();

const messageSearchArgsSchema = z
  .object({
    searchMode: z.enum(["regex", "vector"]),
    query: z.string(),
    lastDays: z.number().int().min(1).max(365).nullable().optional(),
    groupIds: z.array(z.string()).nullable().optional(),
    authorIds: z.array(z.string()).nullable().optional(),
    minReplies: z.number().int().min(0).nullable().optional(),
    minReactions: z.number().int().min(0).nullable().optional(),
    limit: z.number().int().min(1).max(50).optional(),
  })
  .strict();

const groupSearchArgsSchema = z
  .object({
    searchMode: z.enum(["regex", "vector"]),
    query: z.string(),
    lastDays: z.number().int().min(1).max(365).nullable().optional(),
    minActivity: z.number().int().min(0).nullable().optional(),
    limit: z.number().int().min(1).max(50).optional(),
  })
  .strict();

const influencerLookupArgsSchema = z
  .object({
    question: z.string().optional(),
    narrative: z.string().nullable(),
  })
  .strict();

const narrativeProbeArgsSchema = z
  .object({
    question: z.string(),
  })
  .strict();

const audienceBuilderAgentArgsSchema = z
  .object({
    question: z.string().optional(),
    focus: z.string().optional(),
    narrative: z.string().nullable().optional(),
  })
  .strict();

const narrativeExplorerAgentArgsSchema = z
  .object({
    question: z.string().optional(),
    focus: z.string().optional(),
    timeframeDays: z.number().int().min(1).max(365).optional(),
    limit: z.number().int().min(1).max(50).optional(),
    query: z.string().optional(),
  })
  .strict();

const synthesisAgentArgsSchema = z
  .object({
    sections: z.array(z.string()).optional(),
    emphasis: z.string().optional(),
  })
  .strict();

const statsQueryAgentArgsSchema = z
  .object({
    question: z.string().optional(),
    focus: z.string().optional(),
  })
  .strict();

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

function describeScalarSchema(schema: z.ZodTypeAny): string {
  const { schema: unwrapped, optional, nullable } = unwrapSchema(schema);
  let description = "unknown";

  if (unwrapped instanceof z.ZodEnum) {
    description = `enum(${unwrapped.options.join(" | ")})`;
  } else if (unwrapped instanceof z.ZodLiteral) {
    description = `literal(${JSON.stringify(unwrapped._def.value)})`;
  } else if (unwrapped instanceof z.ZodString) {
    description = "string";
  } else if (unwrapped instanceof z.ZodNumber) {
    const checks = unwrapped._def.checks ?? [];
    const fragments = ["number"];
    if (checks.some((check) => check.kind === "int")) {
      fragments[0] = "integer";
    }
    const minCheck = checks.find((check) => check.kind === "min");
    const maxCheck = checks.find((check) => check.kind === "max");
    if (minCheck && "value" in minCheck) {
      fragments.push(`min ${minCheck.value}`);
    }
    if (maxCheck && "value" in maxCheck) {
      fragments.push(`max ${maxCheck.value}`);
    }
    description = fragments.join(", ");
  } else if (unwrapped instanceof z.ZodArray) {
    description = `array<${describeScalarSchema(unwrapped.element)}>`;
  } else if (unwrapped instanceof z.ZodObject) {
    description = `object{${describeArgsSchema(unwrapped)}}`;
  } else if (unwrapped instanceof z.ZodRecord) {
    description = `record<${describeScalarSchema(unwrapped.valueSchema)}>`;
  } else if (unwrapped instanceof z.ZodBoolean) {
    description = "boolean";
  } else if (unwrapped instanceof z.ZodAny) {
    description = "any";
  }

  if (nullable) {
    description = `${description}, nullable`;
  }
  if (optional) {
    description = `${description}, optional`;
  }

  return description;
}

function describeArgsSchema(schema: z.ZodObject<any>): string {
  const entries = Object.entries(schema.shape) as Array<[string, z.ZodTypeAny]>;
  if (entries.length === 0) {
    return "(no args)";
  }

  return entries.map(([key, value]) => `${key}: ${describeScalarSchema(value)}`).join("; ");
}

export const toolCatalog: Record<ToolId, ToolDefinition> = {
  planning_notes: {
    toolId: "planning_notes",
    owner: "planner",
    label: "Planning Notes",
    description: "Record a planning note or schema checkpoint without querying external data.",
    argsSchema: planningNotesArgsSchema,
    outputSchema: planningNotesOutputSchema,
    plannerVisible: false,
    specialistVisible: false,
    kind: "leaf",
  },
  db_stats_query: {
    toolId: "db_stats_query",
    owner: "stats_query",
    label: "DB Stats Query",
    description:
      "Internal Mongo-backed analytics tool for specialist agents. Run structured aggregate queries over messages, groups, users, and memberships using entity, aggregation, measure, groupBy, and timeBucket, plus exactly one matching entity-specific filter object. Do not pass raw Mongo filters or collection names.",
    argsSchema: dbStatsQueryArgsSchema,
    outputSchema: dbStatsQueryOutputSchema,
    plannerVisible: false,
    specialistVisible: true,
    kind: "leaf",
  },
  dynamic_mongo_aggregation: {
    toolId: "dynamic_mongo_aggregation",
    owner: "audience_builder",
    label: "Dynamic Mongo Aggregation",
    description:
      "Internal analytics tool that converts a natural-language question into a guarded Mongo aggregation pipeline over groups, messages, and userprofiles, then executes it read-only with execution safeguards.",
    argsSchema: dynamicMongoAggregationArgsSchema,
    outputSchema: dynamicMongoAggregationOutputSchema,
    plannerVisible: false,
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
    outputSchema: latestNarrativesOutputSchema,
    plannerVisible: false,
    specialistVisible: true,
    kind: "leaf",
  },
  summarization: {
    toolId: "summarization",
    owner: "synthesizer",
    label: "Summarization",
    description: "Summarize supplied text into a short answer and concise bullets.",
    argsSchema: summarizationArgsSchema,
    outputSchema: summarizationOutputSchema,
    plannerVisible: true,
    specialistVisible: true,
    kind: "leaf",
  },
  user_profile_lookup: {
    toolId: "user_profile_lookup",
    owner: "audience_builder",
    label: "User Profile Lookup",
    description:
      "Use this tool to fetch concrete, actionable user profiles when you already have specific userIds or groupIds, or when you need to filter users by basic membership status or roles. It returns a strict list of user objects. Do not use this for counting or distributions.",
    argsSchema: userProfileLookupArgsSchema,
    outputSchema: userProfileLookupOutputSchema,
    plannerVisible: false,
    specialistVisible: true,
    kind: "leaf",
  },
  message_search: {
    toolId: "message_search",
    owner: "audience_builder",
    label: "Message Search",
    description:
      "Use this tool to discover users based on their behavior or content. Use it when the request asks for users who talk about specific topics (via regex or vector search) or users who meet engagement thresholds (e.g., minReplies, lastDays). It returns a list of active message authors and message snippets.",
    argsSchema: messageSearchArgsSchema,
    outputSchema: messageSearchOutputSchema,
    plannerVisible: false,
    specialistVisible: true,
    kind: "leaf",
  },
  group_search: {
    toolId: "group_search",
    owner: "audience_builder",
    label: "Group Search",
    description:
      "Use this tool to discover groups based on their subject, description, and tags. Use regex pattern matching for text-based searches or Atlas vector search for embedding-based searches. Optionally filter by recent activity.",
    argsSchema: groupSearchArgsSchema,
    outputSchema: groupSearchOutputSchema,
    plannerVisible: false,
    specialistVisible: true,
    kind: "leaf",
  },
  audience_lookup: {
    toolId: "audience_lookup",
    owner: "audience_builder",
    label: "Audience Lookup",
    description:
      "Use semantic group and message retrieval plus LLM synthesis to build audience segments for a question.",
    argsSchema: audienceLookupArgsSchema,
    outputSchema: audienceLookupOutputSchema,
    plannerVisible: false,
    specialistVisible: true,
    kind: "leaf",
  },
  influencer_lookup: {
    toolId: "influencer_lookup",
    owner: "audience_builder",
    label: "Influencer Lookup",
    description:
      "Rank likely influencers inside a semantically matched message slice using vector retrieval, engagement signals, and LLM synthesis.",
    argsSchema: influencerLookupArgsSchema,
    outputSchema: influencerLookupOutputSchema,
    plannerVisible: false,
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
    outputSchema: narrativeProbeOutputSchema,
    plannerVisible: false,
    specialistVisible: true,
    kind: "leaf",
  },
  audience_builder_agent: {
    toolId: "audience_builder_agent",
    owner: "audience_builder",
    label: "Audience Builder Agent",
    description:
      "Use this agent to identify, filter, or group specific users based on profile fields, group participation, message search, group search, engagement filters, vector embeddings, and internal aggregate stats. It should be the primary tool for requests requiring a list of users, audience segments, or candidate amplifiers.",
    argsSchema: audienceBuilderAgentArgsSchema,
    outputSchema: audienceAgentOutputSchema,
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
    outputSchema: narrativeAgentOutputSchema,
    plannerVisible: false,
    specialistVisible: true,
    kind: "agent",
  },
  synthesis_agent: {
    toolId: "synthesis_agent",
    owner: "synthesizer",
    label: "Synthesis Agent",
    description: "Combine prior step outputs into the final strict JSON answer for the UI.",
    argsSchema: synthesisAgentArgsSchema,
    outputSchema: synthesisAgentOutputSchema,
    plannerVisible: false,
    specialistVisible: false,
    kind: "agent",
  },
  stats_query_agent: {
    toolId: "stats_query_agent",
    owner: "stats_query",
    label: "Stats Query Agent",
    description: "Specialist agent for counts, distributions, and aggregate metrics.",
    argsSchema: statsQueryAgentArgsSchema,
    outputSchema: statsAgentOutputSchema,
    plannerVisible: false,
    specialistVisible: true,
    kind: "agent",
  },
};

export const plannerExecutableToolIdValues = executableToolIdValues.filter(
  (toolId): toolId is ExecutableToolId => toolCatalog[toolId].plannerVisible,
);

export const plannerExecutableToolIdSchema = z.enum(
  plannerExecutableToolIdValues as [ExecutableToolId, ...ExecutableToolId[]],
);

export type PlannerExecutableToolId = z.infer<typeof plannerExecutableToolIdSchema>;

export const executableToolRegistry = Object.fromEntries(
  plannerExecutableToolIdValues.map((toolId) => [toolId, toolCatalog[toolId]]),
) as Record<PlannerExecutableToolId, ToolDefinition>;

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
  return plannerExecutableToolIdValues
    .map((toolId) => {
      const tool = toolCatalog[toolId];
      return `- ${tool.toolId} (owner: ${tool.owner}) — ${tool.description} Args: ${describeArgsSchema(tool.argsSchema)} Output: ${describeScalarSchema(tool.outputSchema)}`;
    })
    .join("\n");
}
