import { Agent, tool } from "@openai/agents";
import { z } from "zod";
import {
  runAudienceGroupSearch,
  runAudienceMessageSearch,
  searchUserProfiles,
} from "../data/audience";
import { runDynamicMongoAggregation } from "../data/dynamic-mongo-aggregation.ts";
import { audienceAgentOutputSchema } from "./tool-output-schemas";
import { getToolDefinition, ToolDefinition } from "./tools";

type AgentToolTraceExecutor = <T>(
  toolDefinition: ToolDefinition,
  input: unknown,
  context: unknown,
  execute: () => Promise<T>,
) => Promise<T>;

type AudienceBuilderDependencies = {
  modelName: string;
  executeWithAgentToolTrace: AgentToolTraceExecutor;
  specialistToolParameters: (toolId: string) => z.ZodObject<any>;
};

function readRootQuestionFromContext(context: unknown): string | null {
  if (!context || typeof context !== "object" || !("context" in context)) {
    return null;
  }

  const nestedContext = (context as { context?: unknown }).context;
  if (!nestedContext || typeof nestedContext !== "object") {
    return null;
  }

  return typeof (nestedContext as { rootQuestion?: unknown }).rootQuestion === "string"
    ? ((nestedContext as { rootQuestion?: string }).rootQuestion ?? null)
    : null;
}

function requireToolDefinition(toolId: string): ToolDefinition {
  const toolDefinition = getToolDefinition(toolId);
  if (!toolDefinition) {
    throw new Error(`Unknown tool definition: ${toolId}`);
  }
  return toolDefinition;
}

export function createAudienceBuilderAgent({
  modelName,
  executeWithAgentToolTrace,
  specialistToolParameters,
}: AudienceBuilderDependencies) {
  const userProfileLookupToolDefinition = requireToolDefinition("user_profile_lookup");
  const messageSearchToolDefinition = requireToolDefinition("message_search");
  const groupSearchToolDefinition = requireToolDefinition("group_search");
  const dynamicMongoAggregationToolDefinition = requireToolDefinition("dynamic_mongo_aggregation");

  const userProfileLookupTool = tool({
    name: userProfileLookupToolDefinition.toolId,
    description: userProfileLookupToolDefinition.description,
    parameters: specialistToolParameters(userProfileLookupToolDefinition.toolId),
    async execute(input, context) {
      return executeWithAgentToolTrace(userProfileLookupToolDefinition, input, context, async () =>
        searchUserProfiles({
          query: typeof input.query === "string" ? input.query : null,
          userIds: Array.isArray(input.userIds) ? input.userIds : null,
          groupIds: Array.isArray(input.groupIds) ? input.groupIds : null,
          membershipStatus:
            typeof input.membershipStatus === "string" ? input.membershipStatus : null,
          roles: Array.isArray(input.roles) ? input.roles : null,
          limit: typeof input.limit === "number" ? input.limit : null,
        }),
      );
    },
  });

  const messageSearchTool = tool({
    name: messageSearchToolDefinition.toolId,
    description: messageSearchToolDefinition.description,
    parameters: specialistToolParameters(messageSearchToolDefinition.toolId),
    async execute(input, context) {
      return executeWithAgentToolTrace(
        messageSearchToolDefinition,
        input,
        context,
        async () =>
          runAudienceMessageSearch({
            searchMode: typeof input.searchMode === "string" ? input.searchMode : null,
            query: input.query,
            lastDays: typeof input.lastDays === "number" ? input.lastDays : null,
            groupIds: Array.isArray(input.groupIds) ? input.groupIds : null,
            authorIds: Array.isArray(input.authorIds) ? input.authorIds : null,
            minReplies: typeof input.minReplies === "number" ? input.minReplies : null,
            minReactions: typeof input.minReactions === "number" ? input.minReactions : null,
            limit: typeof input.limit === "number" ? input.limit : null,
          }),
      );
    },
  });

  const groupSearchTool = tool({
    name: groupSearchToolDefinition.toolId,
    description: groupSearchToolDefinition.description,
    parameters: specialistToolParameters(groupSearchToolDefinition.toolId),
    async execute(input, context) {
      return executeWithAgentToolTrace(
        groupSearchToolDefinition,
        input,
        context,
        async () =>
          runAudienceGroupSearch({
            searchMode: typeof input.searchMode === "string" ? input.searchMode : null,
            query: input.query,
            lastDays: typeof input.lastDays === "number" ? input.lastDays : null,
            minActivity: typeof input.minActivity === "number" ? input.minActivity : null,
            limit: typeof input.limit === "number" ? input.limit : null,
          }),
      );
    },
  });

  const dynamicMongoAggregationTool = tool({
    name: dynamicMongoAggregationToolDefinition.toolId,
    description: dynamicMongoAggregationToolDefinition.description,
    parameters: specialistToolParameters(dynamicMongoAggregationToolDefinition.toolId),
    async execute(input, context) {
      const rootQuestion = readRootQuestionFromContext(context);
      const requestedQuestion = typeof input.question === "string" ? input.question.trim() : "";
      if (!requestedQuestion) {
        throw new Error("dynamic_mongo_aggregation requires a non-empty question.");
      }
      if (rootQuestion && requestedQuestion !== rootQuestion.trim()) {
        throw new Error(
          "dynamic_mongo_aggregation must use the original step question exactly. Rewritten or fallback questions are not allowed.",
        );
      }

      return executeWithAgentToolTrace(
        dynamicMongoAggregationToolDefinition,
        input,
        context,
        async () => runDynamicMongoAggregation(requestedQuestion),
      );
    },
  });

  return new Agent({
    name: "AudienceBuilder",
    model: modelName,
    instructions: [
      "You are a specialist for collection-backed audience construction.",
      "If the user asks for a specific list of people (e.g., 'Identify users...', 'Find influencers...'), use message_search, group_search, and user_profile_lookup to gather their IDs.",
      "If the user asks for distributions, overlaps, or statistics involving multiple collections (e.g., 'What is the distribution of...', 'How many users...'), immediately use dynamic_mongo_aggregation and pass the step question exactly as received.",
      "Do not paraphrase, narrow, broaden, or substitute fallback questions. Do not change explicit time windows, requested cohorts, or requested metrics.",
      "If an internal tool fails, do not attempt an alternate semantic fallback that changes the user's request.",
      "Use message_search for message retrieval. Set searchMode=regex for pattern matching and searchMode=vector for embedding search.",
      "Use group_search to find communities by topic or tag.",
      "Return candidateUsers as concrete user ids whenever the retrieval results support them.",
      "Return concise JSON.",
    ].join(" "),
    tools: [
      groupSearchTool,
      messageSearchTool,
      userProfileLookupTool,
      dynamicMongoAggregationTool,
    ],
    outputType: audienceAgentOutputSchema,
  });
}
