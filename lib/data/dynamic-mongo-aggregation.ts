import { Agent, run } from "@openai/agents";
import { ObjectId } from "mongodb";
import type { Db, Document } from "mongodb";
import { z } from "zod";
import { dynamicMongoAggregationOutputSchema } from "../agents/tool-output-schemas.ts";
import { jsonValueSchema } from "../types.ts";
import { getMongoCollectionNames, getMongoDb } from "./mongo.ts";
import { logToolDebug } from "../runtime/tool-debug.ts";

const MODEL_NAME = process.env.OPENAI_MODEL ?? "gpt-4.1";
const EXECUTION_TIMEOUT_MS = 5_000;
const FORCED_RESULT_LIMIT = 1_000;
const OUTPUT_RESULT_LIMIT = 100;
const MAX_PIPELINE_STAGES = 40;

const rootCollectionSchema = z.enum(["groups", "messages", "userprofiles"]);
const dynamicAggregationQuestionTypeSchema = z.enum([
  "distribution",
  "count",
  "overlap",
  "ranking",
  "list",
  "timeseries",
]);
const dynamicAggregationJoinAliasSchema = z.enum(["group", "author"]);
const dynamicAggregationJoinRelationshipSchema = z.enum([
  "messages_to_groups_by_groupId",
  "messages_to_userprofiles_by_authorId",
  "userprofiles_to_groups_by_groupObjectId",
]);
const dynamicAggregationFieldRefSchema = z
  .string()
  .regex(
    /^(root|group|author)\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$/,
    "Field references must use alias.path format such as root.timestamp or group.tags.politicalLeaning.tagValue.",
  );
const dynamicAggregationOutputFieldNameSchema = z
  .string()
  .regex(
    /^[A-Za-z_][A-Za-z0-9_]*$/,
    "Output field names must be simple identifiers such as totalActiveUsers or distribution.",
  );
const dynamicAggregationFilterSchema = z
  .object({
    fieldRef: dynamicAggregationFieldRefSchema,
    operator: z.enum(["eq", "in", "exists"]),
    value: jsonValueSchema.nullable(),
    values: z.array(jsonValueSchema).min(1).max(20).nullable(),
    exists: z.boolean().nullable(),
  })
  .strict();
const dynamicAggregationDimensionSchema = z
  .object({
    label: z.string().min(1).max(80),
    fieldRef: dynamicAggregationFieldRefSchema,
    nullBucketLabel: z.string().min(1).max(80).nullable(),
  })
  .strict();
const dynamicAggregationMeasureSchema = z
  .object({
    aggregation: z.enum(["count", "count_distinct", "sum", "avg", "min", "max"]),
    fieldRef: dynamicAggregationFieldRefSchema.nullable(),
    outputFieldName: dynamicAggregationOutputFieldNameSchema,
  })
  .strict();
const dynamicAggregationOutputSchema = z
  .object({
    includeTotals: z.boolean(),
    totalFieldName: dynamicAggregationOutputFieldNameSchema.nullable(),
    includePercentages: z.boolean(),
    distributionFieldName: dynamicAggregationOutputFieldNameSchema.nullable(),
  })
  .strict();
const dynamicAggregationSortSchema = z
  .object({
    by: z.enum(["measure", "dimension"]),
    direction: z.enum(["asc", "desc"]),
    dimensionLabel: z.string().min(1).max(80).nullable(),
  })
  .strict();
const dynamicAggregationIntentOutputSchema = z
  .object({
    rootCollection: rootCollectionSchema,
    questionType: dynamicAggregationQuestionTypeSchema,
    summary: z.string().min(1).max(280),
    populationDescription: z.string().min(1).max(400),
    joins: z
      .array(
        z
          .object({
            alias: dynamicAggregationJoinAliasSchema,
            relationship: dynamicAggregationJoinRelationshipSchema,
          })
          .strict(),
      )
      .max(3),
    timeframe: z
      .object({
        fieldRef: dynamicAggregationFieldRefSchema,
        lastDays: z.number().int().min(1).max(3650),
      })
      .strict()
      .nullable(),
    filters: z.array(dynamicAggregationFilterSchema).max(8),
    dimensions: z.array(dynamicAggregationDimensionSchema).max(3),
    measure: dynamicAggregationMeasureSchema,
    output: dynamicAggregationOutputSchema,
    sort: z.array(dynamicAggregationSortSchema).max(3),
    assumptions: z.array(z.string().min(1).max(200)).max(5),
  })
  .strict();

const validatedDynamicAggregationIntentSchema = dynamicAggregationIntentOutputSchema.superRefine(
  (intent, ctx) => {
    if (intent.questionType === "distribution" && intent.dimensions.length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["dimensions"],
        message: 'questionType "distribution" requires at least one dimension.',
      });
    }

    if (
      ["count_distinct", "sum", "avg", "min", "max"].includes(intent.measure.aggregation) &&
      !intent.measure.fieldRef
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["measure", "fieldRef"],
        message: `measure.fieldRef is required for aggregation "${intent.measure.aggregation}".`,
      });
    }

    if (intent.output.includePercentages && intent.dimensions.length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["output", "includePercentages"],
        message: "Percentages require at least one grouping dimension.",
      });
    }

    const aliases = new Set<string>();
    intent.joins.forEach((join, index) => {
      if (aliases.has(join.alias)) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["joins", index, "alias"],
          message: `Join alias "${join.alias}" can only be used once.`,
        });
      }
      aliases.add(join.alias);
    });

    intent.filters.forEach((filter, index) => {
      if (filter.operator === "eq" && filter.value === null) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["filters", index, "value"],
          message: 'filters[].operator "eq" requires value.',
        });
      }
      if (filter.operator === "in" && (!filter.values || filter.values.length === 0)) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["filters", index, "values"],
          message: 'filters[].operator "in" requires values.',
        });
      }
      if (filter.operator === "exists" && typeof filter.exists !== "boolean") {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["filters", index, "exists"],
          message: 'filters[].operator "exists" requires exists=true|false.',
        });
      }
    });

    intent.sort.forEach((sort, index) => {
      if (sort.by === "dimension" && !sort.dimensionLabel) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["sort", index, "dimensionLabel"],
          message: 'sort entries with by="dimension" require dimensionLabel.',
        });
      }
    });

    if (intent.output.includeTotals && !intent.output.totalFieldName) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["output", "totalFieldName"],
        message: "includeTotals=true requires totalFieldName.",
      });
    }

    if (intent.output.includePercentages && !intent.output.distributionFieldName) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["output", "distributionFieldName"],
        message: "includePercentages=true requires distributionFieldName.",
      });
    }
  },
);
const allowedStageOperators = [
  "$match",
  "$project",
  "$addFields",
  "$set",
  "$unset",
  "$group",
  "$sort",
  "$limit",
  "$skip",
  "$unwind",
  "$lookup",
  "$facet",
  "$count",
  "$sortByCount",
  "$bucket",
  "$bucketAuto",
  "$replaceRoot",
  "$replaceWith",
] as const;
const generatedPipelineStageSchema = z
  .object({
    operator: z.enum(allowedStageOperators),
    value: jsonValueSchema,
  })
  .strict();

const dynamicMongoAggregationPipelineSchema = z
  .object({
    summary: z.string().min(1).max(280),
    pipeline: z.array(generatedPipelineStageSchema).min(1).max(MAX_PIPELINE_STAGES),
  })
  .strict();

type RootCollection = z.infer<typeof rootCollectionSchema>;
type DynamicMongoAggregationIntent = z.infer<typeof dynamicAggregationIntentOutputSchema>;
type DynamicMongoAggregationPipelineResponse = z.infer<typeof dynamicMongoAggregationPipelineSchema>;
type GeneratedPipelineStage = z.infer<typeof generatedPipelineStageSchema>;
type DynamicAggregationJoinRelationship = z.infer<typeof dynamicAggregationJoinRelationshipSchema>;
type NormalizedDynamicAggregationJoin = {
  alias: z.infer<typeof dynamicAggregationJoinAliasSchema>;
  relationship: DynamicAggregationJoinRelationship;
  targetCollection: RootCollection;
  localField: string;
  foreignField: string;
};
type NormalizedDynamicAggregationIntent = Omit<
  DynamicMongoAggregationIntent,
  "joins" | "timeframe"
> & {
  joins: NormalizedDynamicAggregationJoin[];
  timeframe:
    | (NonNullable<DynamicMongoAggregationIntent["timeframe"]> & {
        cutoffTimestampMs: number;
      })
    | null;
};
type DynamicMongoAggregationPlan = {
  rootCollection: RootCollection;
  summary: string;
  pipeline: Array<Record<string, unknown>>;
  forcedLimitApplied: boolean;
  intent: NormalizedDynamicAggregationIntent;
};
type MongoCollectionNames = ReturnType<typeof getMongoCollectionNames>;

const ALLOWED_STAGE_OPERATORS = new Set<string>(allowedStageOperators);

const BANNED_OPERATORS = new Set([
  "$out",
  "$merge",
  "$where",
  "$function",
  "$accumulator",
]);

const AGGREGATED_STAGE_OPERATORS = new Set([
  "$group",
  "$count",
  "$sortByCount",
  "$bucket",
  "$bucketAuto",
  "$facet",
]);

const dynamicMongoAggregationIntentAgent = new Agent({
  name: "DynamicMongoAggregationPlanner",
  model: MODEL_NAME,
  instructions: [
    "You turn natural-language analytics questions into a normalized execution intent before any MongoDB pipeline is written.",
    "Return strict JSON only.",
    "Do not write MongoDB stages in this step.",
    "Use field references in alias.path format where alias is root or one declared join alias.",
    "Available join relationships are: messages_to_groups_by_groupId with alias group; messages_to_userprofiles_by_authorId with alias author; userprofiles_to_groups_by_groupObjectId with alias group.",
    "Political leaning exists on groups.tags.politicalLeaning.tagValue, not on userprofiles.",
    "If the question asks about active users in a recent time window, rootCollection should usually be messages and the measure should count distinct root.authorId.",
    "Prefer the smallest reasonable assumption and list it in assumptions.",
  ].join(" "),
  outputType: dynamicAggregationIntentOutputSchema,
});

const dynamicMongoAggregationPipelineAgent = new Agent({
  name: "DynamicMongoAggregationPipelineGenerator",
  model: MODEL_NAME,
  instructions: [
    "You generate guarded MongoDB aggregation pipelines from a normalized analytics intent.",
    "Return strict JSON only.",
    "Do not reinterpret the question. Implement the supplied normalized intent exactly.",
    "Use only valid MongoDB aggregation stages and only fields present in the supplied schema summary.",
    "Do not use write stages such as $out or $merge.",
    "Do not use $$NOW or other relative runtime time expressions. Use any provided cutoffTimestampMs literal directly.",
    "Prefer placing $match stages before $lookup when possible.",
    "Use $lookup only for joins defined in the normalized intent.",
    'Return pipeline as an array of stage wrappers shaped like {"operator":"$match","value":{...}}.',
    "Do not stringify pipeline.",
    "For nested pipelines inside $lookup.pipeline and $facet, use the same operator/value wrapper shape recursively.",
  ].join(" "),
  outputType: dynamicMongoAggregationPipelineSchema,
});

export class DynamicMongoAggregationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DynamicMongoAggregationError";
  }
}

const DAY_IN_MS = 24 * 60 * 60 * 1000;

const ALLOWED_FIELD_PATHS_BY_COLLECTION: Record<RootCollection, Set<string>> = {
  groups: new Set([
    "_id",
    "groupId",
    "subject",
    "description",
    "memberCount",
    "lastActivityTimestamp",
    "creationTimestamp",
    "announcementOnly",
    "memberAddMode",
    "membershipApproval",
    "avgMessagesPerDay30d",
    "avgAuthorsPerDay30d",
    "avgReactionsPerMessage30d",
    "avgRepliesPerMessage30d",
    "activeMemberPercentage30d",
    "activeDays30d",
    "engagementScore",
    "normalizedEngagementScore",
    "tags.politicalLeaning.tagValue",
    "tags.topic.tagValue",
    "tags.region.tagValue",
    "tags.organization.tagValue",
    "tags.organization.organizationType",
    "tags.demographic.tagValue",
    "tags.demographic.age",
    "tags.demographic.gender",
    "tags.lifeEvent.tagValue",
    "tags.strategicMarkets.tagValue",
  ]),
  messages: new Set([
    "_id",
    "messageId",
    "groupId",
    "authorId",
    "timestamp",
    "body",
    "forwardingScore",
    "quotedMessageId",
    "messageReplies",
    "messageReactions",
    "messageMedia",
    "reactionCount",
    "replyCount",
    "hasMedia",
    "hasQuote",
  ]),
  userprofiles: new Set([
    "_id",
    "userId",
    "name",
    "status",
    "groups.group",
    "groups.role",
    "groups.status",
    "groups.joinedAt",
    "groups.leftAt",
  ]),
};

const ALLOWED_TIMEFRAME_FIELD_PATHS_BY_COLLECTION: Record<RootCollection, Set<string>> = {
  groups: new Set(["lastActivityTimestamp", "creationTimestamp"]),
  messages: new Set(["timestamp"]),
  userprofiles: new Set(),
};

const JOIN_RELATIONSHIP_DEFINITIONS: Record<
  DynamicAggregationJoinRelationship,
  {
    rootCollection: RootCollection;
    alias: z.infer<typeof dynamicAggregationJoinAliasSchema>;
    targetCollection: RootCollection;
    localField: string;
    foreignField: string;
  }
> = {
  messages_to_groups_by_groupId: {
    rootCollection: "messages",
    alias: "group",
    targetCollection: "groups",
    localField: "groupId",
    foreignField: "groupId",
  },
  messages_to_userprofiles_by_authorId: {
    rootCollection: "messages",
    alias: "author",
    targetCollection: "userprofiles",
    localField: "authorId",
    foreignField: "userId",
  },
  userprofiles_to_groups_by_groupObjectId: {
    rootCollection: "userprofiles",
    alias: "group",
    targetCollection: "groups",
    localField: "groups.group",
    foreignField: "_id",
  },
};

function requireOpenAIKey(): string {
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) {
    throw new DynamicMongoAggregationError(
      "Missing OPENAI_API_KEY for dynamic Mongo aggregation generation.",
    );
  }
  return apiKey;
}

function buildSchemaPrompt(names: MongoCollectionNames): string {
  return [
    "Available collections and exact field paths:",
    `1. Root collection "groups" maps to Mongo collection "${names.groupsCollection}".`,
    "Fields: _id:ObjectId, groupId:string, subject:string, description:string, memberCount:number, lastActivityTimestamp:number, creationTimestamp:number, announcementOnly:boolean, memberAddMode:string, membershipApproval:boolean, avgMessagesPerDay30d:number, avgAuthorsPerDay30d:number, avgReactionsPerMessage30d:number, avgRepliesPerMessage30d:number, activeMemberPercentage30d:number, activeDays30d:number, engagementScore:number, normalizedEngagementScore:number, tags.politicalLeaning.tagValue:string, tags.topic.tagValue:string, tags.region.tagValue:string, tags.organization.tagValue:string, tags.organization.organizationType:string, tags.demographic.tagValue:string, tags.demographic.age:string, tags.demographic.gender:string, tags.lifeEvent.tagValue:string, tags.strategicMarkets.tagValue:string.",
    `2. Root collection "messages" maps to Mongo collection "${names.messagesCollection}".`,
    "Fields: _id:ObjectId, messageId:string, groupId:string, authorId:string, timestamp:number, body:string, forwardingScore:number, quotedMessageId:string, messageReplies:array<string>, messageReactions:array<object>, messageMedia:object.",
    "Useful derived values for messages: reactionCount = {$size: {$ifNull: ['$messageReactions', []]}}, replyCount = {$size: {$ifNull: ['$messageReplies', []]}}, hasMedia = {$ne: ['$messageMedia', null]}, hasQuote = {$ne: ['$quotedMessageId', null]}.",
    `3. Root collection "userprofiles" maps to Mongo collection "${names.usersCollection}".`,
    "Fields: _id:ObjectId, userId:string, name:string, status:string, groups:array<{group:ObjectId, role:string, status:string, joinedAt:Date, leftAt:Date}>.",
    "Exact relationships:",
    `- messages.groupId joins groups.groupId via $lookup.from = "${names.groupsCollection}".`,
    `- messages.authorId joins userprofiles.userId via $lookup.from = "${names.usersCollection}".`,
    `- userprofiles.groups.group joins groups._id via $lookup.from = "${names.groupsCollection}".`,
    'Important analytical note: political leaning is a group-level field at tags.politicalLeaning.tagValue on groups. If you need user activity by political leaning, you usually join messages -> groups and count distinct authorId per leaning.',
    "Return the rootCollection as one of the logical names groups, messages, or userprofiles. Inside $lookup.from, use the actual Mongo collection names shown above.",
  ].join("\n");
}

function buildIntentPrompt(question: string, names: MongoCollectionNames): string {
  return [
    `User question:\n${question}`,
    "",
    buildSchemaPrompt(names),
    "",
    "Return JSON with:",
    '- rootCollection: "groups" | "messages" | "userprofiles"',
    '- questionType: "distribution" | "count" | "overlap" | "ranking" | "list" | "timeseries"',
    "- summary: one sentence explaining what will be computed",
    "- populationDescription: one sentence describing the population being measured",
    '- joins: array of { alias, relationship } using only the documented join relationships',
    "- timeframe: optional { fieldRef, lastDays } using alias.path references such as root.timestamp",
    '- filters: optional array using operators "eq", "in", or "exists"',
    "- dimensions: optional grouping dimensions using alias.path references",
    "- measure: { aggregation, fieldRef?, outputFieldName }",
    "- output: { includeTotals, totalFieldName?, includePercentages, distributionFieldName? }",
    '- sort: optional array with by="measure" or by="dimension"',
    "- assumptions: optional array of brief assumptions",
  ].join("\n");
}

function buildPipelinePrompt(
  question: string,
  names: MongoCollectionNames,
  intent: NormalizedDynamicAggregationIntent,
): string {
  return [
    `Original user question:\n${question}`,
    "",
    "Normalized analytics intent:",
    JSON.stringify(intent, null, 2),
    "",
    buildSchemaPrompt(names),
    "",
    "Generate the MongoDB aggregation pipeline that implements the normalized analytics intent exactly.",
    "Do not change the root collection, grouping dimensions, measure, or joins.",
    "If timeframe.cutoffTimestampMs is present, use that exact numeric literal in the pipeline. Do not use $$NOW.",
    "Return JSON with:",
    "- summary: one sentence describing the executed aggregation",
    '- pipeline: array of stage wrappers like {"operator":"$match","value":{...}}',
    "- use the same wrapper shape recursively for nested pipelines",
  ].join("\n");
}

function formatGenerationError(error: unknown): string {
  if (error instanceof z.ZodError) {
    return error.issues
      .map((issue) => `${issue.path.join(".") || "(root)"}: ${issue.message}`)
      .join("; ");
  }

  return error instanceof Error ? error.message : "Unknown generation error";
}

function parseFieldRef(fieldRef: string): { alias: "root" | "group" | "author"; path: string } {
  const [alias, ...pathParts] = fieldRef.split(".");
  const path = pathParts.join(".");
  if ((alias !== "root" && alias !== "group" && alias !== "author") || path.length === 0) {
    throw new DynamicMongoAggregationError(`Invalid field reference "${fieldRef}".`);
  }

  return { alias, path };
}

function resolveFieldRefCollection(
  fieldRef: string,
  rootCollection: RootCollection,
  joins: NormalizedDynamicAggregationJoin[],
): RootCollection {
  const { alias } = parseFieldRef(fieldRef);
  if (alias === "root") {
    return rootCollection;
  }

  const join = joins.find((candidate) => candidate.alias === alias);
  if (!join) {
    throw new DynamicMongoAggregationError(
      `Field reference "${fieldRef}" uses alias "${alias}" without declaring the required join.`,
    );
  }

  return join.targetCollection;
}

function assertAllowedFieldRef(
  fieldRef: string,
  rootCollection: RootCollection,
  joins: NormalizedDynamicAggregationJoin[],
  mode: "general" | "timeframe" = "general",
): void {
  const { path } = parseFieldRef(fieldRef);
  const collection = resolveFieldRefCollection(fieldRef, rootCollection, joins);
  const allowedFields =
    mode === "timeframe"
      ? ALLOWED_TIMEFRAME_FIELD_PATHS_BY_COLLECTION[collection]
      : ALLOWED_FIELD_PATHS_BY_COLLECTION[collection];

  if (!allowedFields.has(path)) {
    throw new DynamicMongoAggregationError(
      `Field reference "${fieldRef}" is not supported for collection "${collection}"${mode === "timeframe" ? " as a timeframe field" : ""}.`,
    );
  }
}

function normalizeDynamicAggregationIntentJoins(
  rootCollection: RootCollection,
  joins: DynamicMongoAggregationIntent["joins"],
): NormalizedDynamicAggregationJoin[] {
  return joins.map((join) => {
    const definition = JOIN_RELATIONSHIP_DEFINITIONS[join.relationship];
    if (definition.rootCollection !== rootCollection) {
      throw new DynamicMongoAggregationError(
        `Join relationship "${join.relationship}" is not valid for rootCollection "${rootCollection}".`,
      );
    }
    if (definition.alias !== join.alias) {
      throw new DynamicMongoAggregationError(
        `Join relationship "${join.relationship}" must use alias "${definition.alias}", not "${join.alias}".`,
      );
    }
    return {
      ...join,
      targetCollection: definition.targetCollection,
      localField: definition.localField,
      foreignField: definition.foreignField,
    };
  });
}

export function normalizeDynamicAggregationIntentPlan(
  intent: DynamicMongoAggregationIntent,
  nowMs: number = Date.now(),
): NormalizedDynamicAggregationIntent {
  const joins = normalizeDynamicAggregationIntentJoins(intent.rootCollection, intent.joins);

  if (intent.timeframe) {
    assertAllowedFieldRef(intent.timeframe.fieldRef, intent.rootCollection, joins, "timeframe");
  }

  intent.filters.forEach((filter) => {
    assertAllowedFieldRef(filter.fieldRef, intent.rootCollection, joins);
  });

  intent.dimensions.forEach((dimension) => {
    assertAllowedFieldRef(dimension.fieldRef, intent.rootCollection, joins);
  });

  if (intent.measure.fieldRef) {
    assertAllowedFieldRef(intent.measure.fieldRef, intent.rootCollection, joins);
  }

  const sortDimensionLabels = new Set(intent.dimensions.map((dimension) => dimension.label));
  intent.sort.forEach((sort) => {
    if (sort.by === "dimension" && sort.dimensionLabel && !sortDimensionLabels.has(sort.dimensionLabel)) {
      throw new DynamicMongoAggregationError(
        `sort.dimensionLabel "${sort.dimensionLabel}" does not match any declared dimension label.`,
      );
    }
  });

  return {
    ...intent,
    joins,
    timeframe: intent.timeframe
      ? {
          ...intent.timeframe,
          cutoffTimestampMs: nowMs - intent.timeframe.lastDays * DAY_IN_MS,
        }
      : null,
  };
}

function normalizeLookupCollectionName(
  from: string,
  names: MongoCollectionNames,
): string {
  const normalized = from.trim();
  if (normalized === "groups") {
    return names.groupsCollection;
  }
  if (normalized === "messages") {
    return names.messagesCollection;
  }
  if (normalized === "userprofiles" || normalized === "users") {
    return names.usersCollection;
  }
  return normalized;
}

function cloneJsonValue(value: unknown): unknown {
  if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value instanceof ObjectId) {
    return value.toString();
  }

  if (Array.isArray(value)) {
    return value.map((entry) => cloneJsonValue(entry));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
        key,
        cloneJsonValue(entry),
      ]),
    );
  }

  return String(value);
}

function assertNoBannedOperators(value: unknown, path = "pipeline"): void {
  if (value === "$$NOW") {
    throw new DynamicMongoAggregationError(
      `Generated pipeline uses disallowed runtime variable "$$NOW" at ${path}. Use a literal cutoff timestamp instead.`,
    );
  }

  if (Array.isArray(value)) {
    value.forEach((entry, index) => assertNoBannedOperators(entry, `${path}[${index}]`));
    return;
  }

  if (!value || typeof value !== "object") {
    return;
  }

  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (BANNED_OPERATORS.has(key)) {
      throw new DynamicMongoAggregationError(
        `Generated pipeline uses banned operator "${key}" at ${path}.${key}.`,
      );
    }
    assertNoBannedOperators(entry, `${path}.${key}`);
  }
}

function normalizeAndValidateStage(
  stage: Record<string, unknown>,
  names: MongoCollectionNames,
  path: string,
): Record<string, unknown> {
  const stageKeys = Object.keys(stage);
  if (stageKeys.length !== 1) {
    throw new DynamicMongoAggregationError(
      `Each aggregation stage must have exactly one top-level operator. Invalid stage at ${path}.`,
    );
  }

  const stageOperator = stageKeys[0];
  if (!ALLOWED_STAGE_OPERATORS.has(stageOperator)) {
    throw new DynamicMongoAggregationError(
      `Generated pipeline uses unsupported stage "${stageOperator}" at ${path}.`,
    );
  }

  const stageValue = stage[stageOperator];
  assertNoBannedOperators(stageValue, `${path}.${stageOperator}`);

  if (stageOperator === "$lookup") {
    if (!stageValue || typeof stageValue !== "object" || Array.isArray(stageValue)) {
      throw new DynamicMongoAggregationError(`$lookup must be an object at ${path}.`);
    }

    const lookupStage = { ...(stageValue as Record<string, unknown>) };
    if (typeof lookupStage.from !== "string") {
      throw new DynamicMongoAggregationError(`$lookup.from must be a string at ${path}.`);
    }

    const normalizedLookupFrom = normalizeLookupCollectionName(lookupStage.from, names);
    lookupStage.from = normalizedLookupFrom;
    const allowedLookupTargets = new Set([
      names.groupsCollection,
      names.messagesCollection,
      names.usersCollection,
    ]);
    if (!allowedLookupTargets.has(normalizedLookupFrom)) {
      throw new DynamicMongoAggregationError(
        `$lookup.from must target one of the configured collections at ${path}.`,
      );
    }

    if ("pipeline" in lookupStage) {
      if (!Array.isArray(lookupStage.pipeline)) {
        throw new DynamicMongoAggregationError(`$lookup.pipeline must be an array at ${path}.`);
      }
      lookupStage.pipeline = lookupStage.pipeline.map((entry, index) => {
        if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
          throw new DynamicMongoAggregationError(
            `Nested $lookup pipeline stage must be an object at ${path}.pipeline[${index}].`,
          );
        }
        return normalizeAndValidateStage(
          entry as Record<string, unknown>,
          names,
          `${path}.pipeline[${index}]`,
        );
      });
    }

    return { $lookup: lookupStage };
  }

  if (stageOperator === "$facet") {
    if (!stageValue || typeof stageValue !== "object" || Array.isArray(stageValue)) {
      throw new DynamicMongoAggregationError(`$facet must be an object at ${path}.`);
    }

    const normalizedFacet = Object.fromEntries(
      Object.entries(stageValue as Record<string, unknown>).map(([facetName, facetPipeline]) => {
        if (!Array.isArray(facetPipeline)) {
          throw new DynamicMongoAggregationError(
            `Facet "${facetName}" must be an array at ${path}.`,
          );
        }
        return [
          facetName,
          facetPipeline.map((entry, index) => {
            if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
              throw new DynamicMongoAggregationError(
                `Facet stage must be an object at ${path}.${facetName}[${index}].`,
              );
            }
            return normalizeAndValidateStage(
              entry as Record<string, unknown>,
              names,
              `${path}.${facetName}[${index}]`,
            );
          }),
        ];
      }),
    );

    return { $facet: normalizedFacet };
  }

  return { [stageOperator]: stageValue };
}

function isGroupedStatisticalPipeline(pipeline: Array<Record<string, unknown>>): boolean {
  return pipeline.some((stage) => AGGREGATED_STAGE_OPERATORS.has(Object.keys(stage)[0] ?? ""));
}

function hasTerminalLimitAtOrBelowThreshold(pipeline: Array<Record<string, unknown>>): boolean {
  const finalStage = pipeline[pipeline.length - 1];
  if (!finalStage || !("$limit" in finalStage)) {
    return false;
  }

  return typeof finalStage.$limit === "number" && finalStage.$limit <= FORCED_RESULT_LIMIT;
}

export function prepareDynamicAggregationPipeline(
  pipeline: Array<Record<string, unknown>>,
  names: MongoCollectionNames,
): { pipeline: Array<Record<string, unknown>>; forcedLimitApplied: boolean } {
  if (!Array.isArray(pipeline) || pipeline.length === 0) {
    throw new DynamicMongoAggregationError("Generated pipeline must be a non-empty array.");
  }

  const normalizedPipeline = pipeline.map((stage, index) => {
    if (!stage || typeof stage !== "object" || Array.isArray(stage)) {
      throw new DynamicMongoAggregationError(
        `Generated pipeline stage ${index + 1} must be an object.`,
      );
    }

    return normalizeAndValidateStage(stage, names, `pipeline[${index}]`);
  });

  const groupedStatisticalOutput = isGroupedStatisticalPipeline(normalizedPipeline);
  if (groupedStatisticalOutput || hasTerminalLimitAtOrBelowThreshold(normalizedPipeline)) {
    return { pipeline: normalizedPipeline, forcedLimitApplied: false };
  }

  return {
    pipeline: [...normalizedPipeline, { $limit: FORCED_RESULT_LIMIT }],
    forcedLimitApplied: true,
  };
}

function isGeneratedPipelineStage(value: unknown): value is GeneratedPipelineStage {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.operator === "string" &&
    "value" in candidate &&
    Object.keys(candidate).length === 2
  );
}

function materializeGeneratedStageValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    if (value.every((entry) => isGeneratedPipelineStage(entry))) {
      return value.map((entry) => ({
        [entry.operator]: materializeGeneratedStageValue(entry.value),
      }));
    }

    return value.map((entry) => materializeGeneratedStageValue(entry));
  }

  if (!value || typeof value !== "object") {
    return value;
  }

  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
      key,
      materializeGeneratedStageValue(entry),
    ]),
  );
}

export function materializeGeneratedPipeline(
  generatedPipeline: GeneratedPipelineStage[],
): Array<Record<string, unknown>> {
  return generatedPipeline.map((stage) => ({
    [stage.operator]: materializeGeneratedStageValue(stage.value),
  }));
}

function resolveRootCollectionName(
  rootCollection: RootCollection,
  names: MongoCollectionNames,
): string {
  switch (rootCollection) {
    case "groups":
      return names.groupsCollection;
    case "messages":
      return names.messagesCollection;
    case "userprofiles":
      return names.usersCollection;
  }
}

function resolveRootCollection(
  db: Db,
  rootCollection: RootCollection,
  names: MongoCollectionNames,
) {
  return db.collection(resolveRootCollectionName(rootCollection, names));
}

async function generateDynamicAggregationPlan(
  question: string,
  names: MongoCollectionNames,
): Promise<DynamicMongoAggregationPlan> {
  requireOpenAIKey();

  const intentBasePrompt = buildIntentPrompt(question, names);
  let intentPrompt = intentBasePrompt;
  let lastIntentError: unknown = null;

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const intentResult = await run(dynamicMongoAggregationIntentAgent, intentPrompt);

    try {
      const parsedIntent = validatedDynamicAggregationIntentSchema.parse(
        intentResult.finalOutput,
      ) as DynamicMongoAggregationIntent;
      const normalizedIntent = normalizeDynamicAggregationIntentPlan(parsedIntent);

      const pipelineBasePrompt = buildPipelinePrompt(question, names, normalizedIntent);
      let pipelinePrompt = pipelineBasePrompt;
      let lastPipelineError: unknown = null;

      for (let pipelineAttempt = 0; pipelineAttempt < 2; pipelineAttempt += 1) {
        const pipelineResult = await run(dynamicMongoAggregationPipelineAgent, pipelinePrompt);

        try {
          const parsedPipelineResponse = dynamicMongoAggregationPipelineSchema.parse(
            pipelineResult.finalOutput,
          ) as DynamicMongoAggregationPipelineResponse;
          const parsedPipeline = materializeGeneratedPipeline(parsedPipelineResponse.pipeline);
          const guarded = prepareDynamicAggregationPipeline(parsedPipeline, names);
          return {
            rootCollection: normalizedIntent.rootCollection,
            summary: parsedPipelineResponse.summary,
            pipeline: guarded.pipeline,
            forcedLimitApplied: guarded.forcedLimitApplied,
            intent: normalizedIntent,
          };
        } catch (error) {
          lastPipelineError = error;
          pipelinePrompt = [
            pipelineBasePrompt,
            "",
            "Previous attempt was invalid.",
            `Validation error: ${formatGenerationError(error)}`,
            "Return corrected JSON only.",
          ].join("\n");
        }
      }

      throw lastPipelineError ?? new DynamicMongoAggregationError("Pipeline generation failed.");
    } catch (error) {
      lastIntentError = error;
      intentPrompt = [
        intentBasePrompt,
        "",
        "Previous attempt was invalid.",
        `Validation error: ${formatGenerationError(error)}`,
        "Return corrected JSON only.",
      ].join("\n");
    }
  }

  throw new DynamicMongoAggregationError(
    `Failed to generate a valid aggregation plan: ${formatGenerationError(lastIntentError)}`,
  );
}

function buildExecutionSummary(
  plan: DynamicMongoAggregationPlan,
  rowCount: number,
  forcedLimitApplied: boolean,
): string {
  const rowSummary = `${rowCount} row${rowCount === 1 ? "" : "s"} returned`;
  const limitSummary = forcedLimitApplied ? ` Safety limit ${FORCED_RESULT_LIMIT} applied.` : "";
  return `${plan.summary} Executed against ${plan.rootCollection}; ${rowSummary}.${limitSummary}`;
}

export async function runDynamicMongoAggregation(question: string) {
  const trimmedQuestion = question.trim();
  if (!trimmedQuestion) {
    throw new DynamicMongoAggregationError("dynamic_mongo_aggregation requires a non-empty question.");
  }

  const names = getMongoCollectionNames();
  await logToolDebug("Dynamic aggregation prompt assembled.", {
    question: trimmedQuestion,
    collections: {
      groups: names.groupsCollection,
      messages: names.messagesCollection,
      userprofiles: names.usersCollection,
    },
  });

  const plan = await generateDynamicAggregationPlan(trimmedQuestion, names);
  await logToolDebug("Dynamic aggregation intent normalized.", {
    rootCollection: plan.intent.rootCollection,
    questionType: plan.intent.questionType,
    populationDescription: plan.intent.populationDescription,
    joins: plan.intent.joins.map((join) => ({
      alias: join.alias,
      relationship: join.relationship,
      targetCollection: join.targetCollection,
    })),
    timeframe: plan.intent.timeframe
      ? {
          fieldRef: plan.intent.timeframe.fieldRef,
          lastDays: plan.intent.timeframe.lastDays,
          cutoffTimestampMs: plan.intent.timeframe.cutoffTimestampMs,
        }
      : null,
    dimensions: plan.intent.dimensions.map((dimension) => ({
      label: dimension.label,
      fieldRef: dimension.fieldRef,
      nullBucketLabel: dimension.nullBucketLabel ?? null,
    })),
    measure: plan.intent.measure,
    output: plan.intent.output,
    assumptions: plan.intent.assumptions,
  });
  await logToolDebug("Dynamic aggregation pipeline generated.", {
    rootCollection: plan.rootCollection,
    stageCount: plan.pipeline.length,
    forcedLimitApplied: plan.forcedLimitApplied,
    summary: plan.summary,
  });

  const serializedPipeline = plan.pipeline.map(
    (stage) => cloneJsonValue(stage) as Record<string, unknown>,
  );
  await logToolDebug("Dynamic aggregation pipeline preview.", {
    stageCount: serializedPipeline.length,
    pipeline: serializedPipeline,
  });

  const db = await getMongoDb();
  const rootCollection = resolveRootCollection(db, plan.rootCollection, names);
  let rawResults: Document[];
  try {
    rawResults = await rootCollection
      .aggregate(plan.pipeline as Document[], {
        maxTimeMS: EXECUTION_TIMEOUT_MS,
      })
      .toArray();
  } catch (error) {
    await logToolDebug("Dynamic aggregation execution failed.", {
      error: error instanceof Error ? error.message : "Unknown aggregation error",
      rootCollection: plan.rootCollection,
      executedCollection: resolveRootCollectionName(plan.rootCollection, names),
      timeoutMs: EXECUTION_TIMEOUT_MS,
      stageCount: serializedPipeline.length,
    });
    throw error;
  }

  const serializedResults = rawResults
    .slice(0, OUTPUT_RESULT_LIMIT)
    .map((entry) => cloneJsonValue(entry) as Record<string, unknown>);

  await logToolDebug("Dynamic aggregation result preview.", {
    rowCount: rawResults.length,
    previewRows: serializedResults.length,
    preview: serializedResults.slice(0, 5),
  });

  await logToolDebug("Dynamic aggregation completed.", {
    rootCollection: plan.rootCollection,
    executedCollection: resolveRootCollectionName(plan.rootCollection, names),
    rowCount: rawResults.length,
    returnedRows: serializedResults.length,
    timeoutMs: EXECUTION_TIMEOUT_MS,
    forcedLimitApplied: plan.forcedLimitApplied,
  });

  return dynamicMongoAggregationOutputSchema.parse({
    source: "dynamic-mongo-aggregation",
    question: trimmedQuestion,
    summary: buildExecutionSummary(plan, rawResults.length, plan.forcedLimitApplied),
    pipelineUsed: serializedPipeline,
    results: serializedResults,
  });
}
