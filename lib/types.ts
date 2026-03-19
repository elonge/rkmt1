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

export const agentToolCallStatusSchema = z.enum([
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

export const agentToolCallSchema = z.object({
  id: z.string().min(1),
  tool: z.string().min(1),
  toolId: z.string().min(1),
  status: agentToolCallStatusSchema,
  input: jsonValueSchema,
  output: jsonValueSchema.optional(),
  error: z.string().optional(),
  debugLogs: z.array(
    z.object({
      message: z.string().min(1),
      data: jsonValueSchema.optional(),
    }),
  ).default([]),
});

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
  agentToolCalls: z.array(agentToolCallSchema).default([]),
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
  agentToolCalls: z.array(agentToolCallSchema).default([]),
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

export const politicalLeaningSchema = z.enum([
  "left",
  "center-left",
  "center",
  "center-right",
  "right",
  "mixed",
]);

export type PoliticalLeaning = z.infer<typeof politicalLeaningSchema>;

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

export const statsQueryEntitySchema = z.enum([
  "messages",
  "groups",
  "users",
  "memberships",
]);

export const statsQueryAggregationSchema = z.enum([
  "count",
  "sum",
  "avg",
  "min",
  "max",
  "distinct_count",
  "distribution",
  "top_values",
  "time_series",
]);

export const statsQueryMeasureSchema = z.enum([
  "records",
  "distinct_authors",
  "distinct_groups",
  "distinct_users",
  "reaction_count",
  "reply_count",
  "forwarding_score",
  "member_count",
  "engagement_score",
  "normalized_engagement_score",
  "avg_messages_per_day_30d",
  "avg_authors_per_day_30d",
  "avg_reactions_per_message_30d",
  "avg_replies_per_message_30d",
  "active_member_percentage_30d",
  "active_days_30d",
]);

export const statsQueryDimensionSchema = z.enum([
  "group",
  "author",
  "political_leaning",
  "topic",
  "region",
  "organization",
  "organization_type",
  "demographic",
  "age",
  "gender",
  "membership_role",
  "membership_status",
  "user_status",
  "announcement_only",
  "membership_approval",
  "member_add_mode",
  "has_media",
  "has_quote",
]);

export const statsTimeBucketSchema = z.enum(["day", "week", "month"]);

export const statsSortBySchema = z.enum(["value", "label"]);
export const statsSortDirectionSchema = z.enum(["asc", "desc"]);

export const membershipRoleSchema = z.enum(["ADMIN", "MEMBER", "SUPER_ADMIN"]);
export const membershipStatusSchema = z.enum(["JOINED", "LEFT"]);

export const statsQueryFilterSchema = z
  .object({
    lastDays: z.number().int().min(1).max(365).optional(),
    groupIds: z.array(z.string().min(1)).max(200).optional(),
    userIds: z.array(z.string().min(1)).max(200).optional(),
    politicalLeanings: z.array(politicalLeaningSchema).max(10).optional(),
    topics: z.array(z.string().min(1)).max(100).optional(),
    regions: z.array(z.string().min(1)).max(100).optional(),
    organizations: z.array(z.string().min(1)).max(100).optional(),
    organizationTypes: z.array(z.string().min(1)).max(100).optional(),
    demographics: z.array(z.string().min(1)).max(100).optional(),
    ages: z.array(z.string().min(1)).max(100).optional(),
    genders: z.array(z.string().min(1)).max(50).optional(),
    membershipRoles: z.array(membershipRoleSchema).max(10).optional(),
    membershipStatuses: z.array(membershipStatusSchema).max(10).optional(),
    userStatuses: z.array(z.string().min(1)).max(100).optional(),
    minMemberCount: z.number().int().min(0).optional(),
    maxMemberCount: z.number().int().min(0).optional(),
    minReplies: z.number().int().min(0).optional(),
    minReactions: z.number().int().min(0).optional(),
    hasMedia: z.boolean().optional(),
    hasQuote: z.boolean().optional(),
    announcementOnly: z.boolean().optional(),
    membershipApproval: z.boolean().optional(),
    memberAddModes: z.array(z.string().min(1)).max(20).optional(),
  })
  .strict();

export const dbStatsQueryToolFilterSchema = z
  .object({
    lastDays: z.number().int().min(1).max(365).nullable().optional(),
    groupIds: z.array(z.string().min(1)).max(200).nullable().optional(),
    userIds: z.array(z.string().min(1)).max(200).nullable().optional(),
    politicalLeanings: z.array(politicalLeaningSchema).max(10).nullable().optional(),
    topics: z.array(z.string().min(1)).max(100).nullable().optional(),
    regions: z.array(z.string().min(1)).max(100).nullable().optional(),
    organizations: z.array(z.string().min(1)).max(100).nullable().optional(),
    organizationTypes: z.array(z.string().min(1)).max(100).nullable().optional(),
    demographics: z.array(z.string().min(1)).max(100).nullable().optional(),
    ages: z.array(z.string().min(1)).max(100).nullable().optional(),
    genders: z.array(z.string().min(1)).max(50).nullable().optional(),
    membershipRoles: z.array(membershipRoleSchema).max(10).nullable().optional(),
    membershipStatuses: z.array(membershipStatusSchema).max(10).nullable().optional(),
    userStatuses: z.array(z.string().min(1)).max(100).nullable().optional(),
    minMemberCount: z.number().int().min(0).nullable().optional(),
    maxMemberCount: z.number().int().min(0).nullable().optional(),
    minReplies: z.number().int().min(0).nullable().optional(),
    minReactions: z.number().int().min(0).nullable().optional(),
    hasMedia: z.boolean().nullable().optional(),
    hasQuote: z.boolean().nullable().optional(),
    announcementOnly: z.boolean().nullable().optional(),
    membershipApproval: z.boolean().nullable().optional(),
    memberAddModes: z.array(z.string().min(1)).max(20).nullable().optional(),
  })
  .strict();

export const messageStatsQueryToolFilterSchema = dbStatsQueryToolFilterSchema
  .pick({
    lastDays: true,
    groupIds: true,
    userIds: true,
    politicalLeanings: true,
    topics: true,
    regions: true,
    organizations: true,
    organizationTypes: true,
    demographics: true,
    ages: true,
    genders: true,
    minReplies: true,
    minReactions: true,
    hasMedia: true,
    hasQuote: true,
  })
  .strict();

export const groupStatsQueryToolFilterSchema = dbStatsQueryToolFilterSchema
  .pick({
    lastDays: true,
    groupIds: true,
    politicalLeanings: true,
    topics: true,
    regions: true,
    organizations: true,
    organizationTypes: true,
    demographics: true,
    ages: true,
    genders: true,
    minMemberCount: true,
    maxMemberCount: true,
    announcementOnly: true,
    membershipApproval: true,
    memberAddModes: true,
  })
  .strict();

export const userStatsQueryToolFilterSchema = dbStatsQueryToolFilterSchema
  .pick({
    userIds: true,
    userStatuses: true,
  })
  .strict();

export const membershipStatsQueryToolFilterSchema = dbStatsQueryToolFilterSchema
  .pick({
    groupIds: true,
    userIds: true,
    politicalLeanings: true,
    topics: true,
    regions: true,
    organizations: true,
    organizationTypes: true,
    demographics: true,
    ages: true,
    genders: true,
    membershipRoles: true,
    membershipStatuses: true,
    userStatuses: true,
  })
  .strict();

export const dbStatsQueryToolFilterFieldByEntity = {
  messages: "messageFilters",
  groups: "groupFilters",
  users: "userFilters",
  memberships: "membershipFilters",
} as const;

export const dbStatsQueryToolSortSchema = z
  .object({
    by: statsSortBySchema,
    direction: statsSortDirectionSchema,
  })
  .strict();

export const dbStatsQueryToolInputSchema = z
  .object({
    entity: statsQueryEntitySchema,
    aggregation: statsQueryAggregationSchema,
    measure: statsQueryMeasureSchema,
    groupBy: z.array(statsQueryDimensionSchema).max(2).nullable().optional(),
    messageFilters: messageStatsQueryToolFilterSchema.nullable().optional(),
    groupFilters: groupStatsQueryToolFilterSchema.nullable().optional(),
    userFilters: userStatsQueryToolFilterSchema.nullable().optional(),
    membershipFilters: membershipStatsQueryToolFilterSchema.nullable().optional(),
    timeBucket: statsTimeBucketSchema.nullable().optional(),
    sort: dbStatsQueryToolSortSchema.nullable().optional(),
    limit: z.number().int().min(1).max(100).nullable().optional(),
  })
  .strict();

export const statsQuerySortSchema = z
  .object({
    by: statsSortBySchema,
    direction: statsSortDirectionSchema,
  })
  .strict();

export const statsQueryInputSchema = z
  .object({
    entity: statsQueryEntitySchema,
    aggregation: statsQueryAggregationSchema,
    measure: statsQueryMeasureSchema,
    groupBy: z.array(statsQueryDimensionSchema).max(2).default([]),
    filters: statsQueryFilterSchema.optional(),
    timeBucket: statsTimeBucketSchema.optional(),
    sort: statsQuerySortSchema.optional(),
    limit: z.number().int().min(1).max(100).optional(),
  })
  .strict();

const supportedStatsQueryDimensionsByEntity: Record<
  z.infer<typeof statsQueryEntitySchema>,
  Set<z.infer<typeof statsQueryDimensionSchema>>
> = {
  messages: new Set([
    "group",
    "author",
    "political_leaning",
    "topic",
    "region",
    "organization",
    "organization_type",
    "demographic",
    "age",
    "gender",
    "has_media",
    "has_quote",
  ]),
  groups: new Set([
    "group",
    "political_leaning",
    "topic",
    "region",
    "organization",
    "organization_type",
    "demographic",
    "age",
    "gender",
    "announcement_only",
    "membership_approval",
    "member_add_mode",
  ]),
  users: new Set(["user_status"]),
  memberships: new Set([
    "group",
    "political_leaning",
    "topic",
    "region",
    "organization",
    "organization_type",
    "demographic",
    "age",
    "gender",
    "membership_role",
    "membership_status",
    "user_status",
  ]),
};

const supportedStatsQueryMeasuresByEntity: Record<
  z.infer<typeof statsQueryEntitySchema>,
  Set<z.infer<typeof statsQueryMeasureSchema>>
> = {
  messages: new Set([
    "records",
    "distinct_authors",
    "distinct_groups",
    "reaction_count",
    "reply_count",
    "forwarding_score",
  ]),
  groups: new Set([
    "records",
    "member_count",
    "engagement_score",
    "normalized_engagement_score",
    "avg_messages_per_day_30d",
    "avg_authors_per_day_30d",
    "avg_reactions_per_message_30d",
    "avg_replies_per_message_30d",
    "active_member_percentage_30d",
    "active_days_30d",
  ]),
  users: new Set(["records", "distinct_users"]),
  memberships: new Set(["records", "distinct_users", "distinct_groups"]),
};

const supportedStatsQueryFiltersByEntity: Record<
  z.infer<typeof statsQueryEntitySchema>,
  Set<keyof z.infer<typeof statsQueryFilterSchema>>
> = {
  messages: new Set([
    "lastDays",
    "groupIds",
    "userIds",
    "politicalLeanings",
    "topics",
    "regions",
    "organizations",
    "organizationTypes",
    "demographics",
    "ages",
    "genders",
    "minReplies",
    "minReactions",
    "hasMedia",
    "hasQuote",
  ]),
  groups: new Set([
    "lastDays",
    "groupIds",
    "politicalLeanings",
    "topics",
    "regions",
    "organizations",
    "organizationTypes",
    "demographics",
    "ages",
    "genders",
    "minMemberCount",
    "maxMemberCount",
    "announcementOnly",
    "membershipApproval",
    "memberAddModes",
  ]),
  users: new Set(["userIds", "userStatuses"]),
  memberships: new Set([
    "groupIds",
    "userIds",
    "politicalLeanings",
    "topics",
    "regions",
    "organizations",
    "organizationTypes",
    "demographics",
    "ages",
    "genders",
    "membershipRoles",
    "membershipStatuses",
    "userStatuses",
  ]),
};

function addStatsQueryCompatibilityIssues(
  input: Partial<z.infer<typeof statsQueryInputSchema>>,
  ctx: z.RefinementCtx,
) {
  if (!input.entity) {
    return;
  }

  const supportedDimensions = supportedStatsQueryDimensionsByEntity[input.entity];
  for (const [index, dimension] of (input.groupBy ?? []).entries()) {
    if (!supportedDimensions.has(dimension)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["groupBy", index],
        message: `groupBy "${dimension}" is not supported for entity "${input.entity}".`,
      });
    }
  }

  if (input.measure) {
    const supportedMeasures = supportedStatsQueryMeasuresByEntity[input.entity];
    if (!supportedMeasures.has(input.measure)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["measure"],
        message: `measure "${input.measure}" is not supported for entity "${input.entity}".`,
      });
    }
  }

  const filters = input.filters ?? {};
  const supportedFilters = supportedStatsQueryFiltersByEntity[input.entity];
  for (const key of Object.keys(filters) as Array<keyof z.infer<typeof statsQueryFilterSchema>>) {
    if (!supportedFilters.has(key)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["filters", key],
        message: `filter "${key}" is not supported for entity "${input.entity}".`,
      });
    }
  }

  if (input.aggregation === "count" && input.measure && input.measure !== "records") {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ["measure"],
      message: 'aggregation "count" requires measure "records".',
    });
  }

  if (
    input.aggregation === "distinct_count" &&
    input.measure &&
    !["distinct_authors", "distinct_groups", "distinct_users"].includes(input.measure)
  ) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ["measure"],
      message: 'aggregation "distinct_count" requires a distinct_* measure.',
    });
  }

  if (
    (input.aggregation === "distribution" || input.aggregation === "top_values") &&
    (input.groupBy?.length ?? 0) === 0
  ) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ["groupBy"],
      message: `aggregation "${input.aggregation}" requires at least one groupBy dimension.`,
    });
  }

  if (input.aggregation === "time_series") {
    if (!input.timeBucket) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["timeBucket"],
        message: 'aggregation "time_series" requires timeBucket.',
      });
    }
    if ((input.groupBy?.length ?? 0) > 1) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["groupBy"],
        message: 'aggregation "time_series" supports at most one groupBy dimension.',
      });
    }
    if (input.entity === "users" || input.entity === "memberships") {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["entity"],
        message: `aggregation "time_series" is not supported for entity "${input.entity}".`,
      });
    }
    if (
      input.measure &&
      ["distinct_authors", "distinct_groups", "distinct_users"].includes(input.measure)
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["measure"],
        message: 'aggregation "time_series" does not support distinct_* measures.',
      });
    }
  }

  if (
    input.aggregation &&
    ["sum", "avg", "min", "max"].includes(input.aggregation) &&
    input.measure &&
    ["records", "distinct_authors", "distinct_groups", "distinct_users"].includes(input.measure)
  ) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ["measure"],
      message: `aggregation "${input.aggregation}" requires a numeric measure.`,
    });
  }
}

const validatedStatsQueryInputSchema = statsQueryInputSchema.superRefine(
  addStatsQueryCompatibilityIssues,
);
const validatedPartialStatsQueryInputSchema = statsQueryInputSchema
  .partial()
  .superRefine(addStatsQueryCompatibilityIssues);

const legacyStatsMetricAliasMap = {
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
} as const;

type LegacyStatsMetric = keyof typeof legacyStatsMetricAliasMap | "political_leaning_distribution" | "active_messages_last_days" | "top_groups_by_member_count";

const statsEntityAliasMap: Record<string, z.infer<typeof statsQueryEntitySchema>> = {
  message: "messages",
  messages: "messages",
  group: "groups",
  groups: "groups",
  user: "users",
  users: "users",
  userprofile: "users",
  userprofiles: "users",
  membership: "memberships",
  memberships: "memberships",
};

const statsAggregationAliasMap: Record<string, z.infer<typeof statsQueryAggregationSchema>> = {
  count: "count",
  sum: "sum",
  avg: "avg",
  average: "avg",
  min: "min",
  max: "max",
  distinct: "distinct_count",
  distinct_count: "distinct_count",
  distribution: "distribution",
  breakdown: "distribution",
  top: "top_values",
  top_values: "top_values",
  ranking: "top_values",
  time_series: "time_series",
  timeseries: "time_series",
};

const statsMeasureAliasMap: Record<string, z.infer<typeof statsQueryMeasureSchema>> = {
  records: "records",
  count: "records",
  distinct_authors: "distinct_authors",
  authors: "distinct_authors",
  distinct_groups: "distinct_groups",
  groups: "distinct_groups",
  distinct_users: "distinct_users",
  users: "distinct_users",
  reaction_count: "reaction_count",
  reactions: "reaction_count",
  reply_count: "reply_count",
  replies: "reply_count",
  forwarding_score: "forwarding_score",
  forwarding: "forwarding_score",
  member_count: "member_count",
  members: "member_count",
  engagement_score: "engagement_score",
  normalized_engagement_score: "normalized_engagement_score",
  avg_messages_per_day_30d: "avg_messages_per_day_30d",
  avg_authors_per_day_30d: "avg_authors_per_day_30d",
  avg_reactions_per_message_30d: "avg_reactions_per_message_30d",
  avg_replies_per_message_30d: "avg_replies_per_message_30d",
  active_member_percentage_30d: "active_member_percentage_30d",
  active_days_30d: "active_days_30d",
};

const statsDimensionAliasMap: Record<string, z.infer<typeof statsQueryDimensionSchema>> = {
  group: "group",
  author: "author",
  political_leaning: "political_leaning",
  politicalleaning: "political_leaning",
  topic: "topic",
  region: "region",
  organization: "organization",
  organization_type: "organization_type",
  organizationtype: "organization_type",
  demographic: "demographic",
  age: "age",
  gender: "gender",
  membership_role: "membership_role",
  membershiprole: "membership_role",
  membership_status: "membership_status",
  membershipstatus: "membership_status",
  user_status: "user_status",
  userstatus: "user_status",
  announcement_only: "announcement_only",
  announcementonly: "announcement_only",
  membership_approval: "membership_approval",
  membershipapproval: "membership_approval",
  member_add_mode: "member_add_mode",
  memberaddmode: "member_add_mode",
  has_media: "has_media",
  hasmedia: "has_media",
  has_quote: "has_quote",
  hasquote: "has_quote",
};

function normalizeStatsEntity(value: unknown): unknown {
  if (typeof value !== "string") {
    return value;
  }

  return statsEntityAliasMap[value.trim().toLowerCase()] ?? value;
}

function normalizeStatsAggregation(value: unknown): unknown {
  if (typeof value !== "string") {
    return value;
  }

  return statsAggregationAliasMap[value.trim().toLowerCase()] ?? value;
}

function normalizeStatsMeasure(value: unknown): unknown {
  if (typeof value !== "string") {
    return value;
  }

  return statsMeasureAliasMap[value.trim().toLowerCase()] ?? value;
}

function normalizeStatsDimension(value: unknown): unknown {
  if (typeof value !== "string") {
    return value;
  }

  const normalized = value.trim().replace(/[\s-]+/g, "_").toLowerCase();
  return statsDimensionAliasMap[normalized] ?? value;
}

function normalizeStringArray(
  value: unknown,
  itemNormalizer?: (item: unknown) => unknown,
): string[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }

  const normalized = value
    .map((item) => (itemNormalizer ? itemNormalizer(item) : item))
    .filter((item): item is string => typeof item === "string" && item.trim().length > 0)
    .map((item) => item.trim());

  return normalized.length > 0 ? [...new Set(normalized)] : undefined;
}

function normalizeLegacyMetric(metric: unknown): LegacyStatsMetric | null {
  if (typeof metric !== "string") {
    return null;
  }

  const normalized = metric.trim().toLowerCase();
  return (legacyStatsMetricAliasMap[normalized as keyof typeof legacyStatsMetricAliasMap] ??
    (normalized as LegacyStatsMetric)) as LegacyStatsMetric;
}

function mapLegacyStatsInput(rawInput: Record<string, unknown>): Record<string, unknown> | null {
  const metric = normalizeLegacyMetric(rawInput.metric);
  if (!metric) {
    return null;
  }

  const rawFilter =
    rawInput.filter && typeof rawInput.filter === "object" && !Array.isArray(rawInput.filter)
      ? (rawInput.filter as Record<string, unknown>)
      : null;
  const normalizedPoliticalLeaning = normalizePoliticalLeaning(rawFilter?.politicalLeaning);
  const politicalLeanings =
    typeof normalizedPoliticalLeaning === "string" ? [normalizedPoliticalLeaning] : undefined;
  const lastDays =
    typeof rawInput.lastDays === "number" ? rawInput.lastDays : undefined;
  const limit = typeof rawInput.limit === "number" ? rawInput.limit : undefined;

  if (metric === "political_leaning_distribution") {
    return {
      entity: "messages",
      aggregation: "distinct_count",
      measure: "distinct_authors",
      groupBy: ["political_leaning"],
      filters: {
        ...(lastDays ? { lastDays } : {}),
        ...(politicalLeanings ? { politicalLeanings } : {}),
      },
      sort: { by: "value", direction: "desc" },
      ...(limit ? { limit } : {}),
    };
  }

  if (metric === "active_messages_last_days") {
    return {
      entity: "messages",
      aggregation: "count",
      measure: "records",
      filters: {
        ...(lastDays ? { lastDays } : {}),
        ...(politicalLeanings ? { politicalLeanings } : {}),
      },
    };
  }

  return {
    entity: "groups",
    aggregation: "top_values",
    measure: "member_count",
    groupBy: ["group"],
    filters: {
      ...(politicalLeanings ? { politicalLeanings } : {}),
    },
    sort: { by: "value", direction: "desc" },
    ...(limit ? { limit } : {}),
  };
}

export function normalizeDbStatsQueryInput(
  rawInput: Record<string, unknown>,
): Record<string, unknown> {
  const legacyNormalized = mapLegacyStatsInput(rawInput);
  const source = legacyNormalized ?? rawInput;
  const rawFilters = [
    source.filters,
    source.messageFilters,
    source.groupFilters,
    source.userFilters,
    source.membershipFilters,
  ].reduce<Record<string, unknown>>((merged, candidate) => {
    if (candidate && typeof candidate === "object" && !Array.isArray(candidate)) {
      Object.assign(merged, candidate as Record<string, unknown>);
    }
    return merged;
  }, {});

  const filters: Record<string, unknown> = {
    ...(typeof rawFilters.lastDays === "number" ? { lastDays: rawFilters.lastDays } : {}),
    ...(normalizeStringArray(rawFilters.groupIds) ? { groupIds: normalizeStringArray(rawFilters.groupIds) } : {}),
    ...(normalizeStringArray(rawFilters.userIds) ? { userIds: normalizeStringArray(rawFilters.userIds) } : {}),
    ...(normalizeStringArray(rawFilters.politicalLeanings, normalizePoliticalLeaning)
      ? {
          politicalLeanings: normalizeStringArray(
            rawFilters.politicalLeanings,
            normalizePoliticalLeaning,
          ),
        }
      : {}),
    ...(normalizeStringArray(rawFilters.topics) ? { topics: normalizeStringArray(rawFilters.topics) } : {}),
    ...(normalizeStringArray(rawFilters.regions) ? { regions: normalizeStringArray(rawFilters.regions) } : {}),
    ...(normalizeStringArray(rawFilters.organizations)
      ? { organizations: normalizeStringArray(rawFilters.organizations) }
      : {}),
    ...(normalizeStringArray(rawFilters.organizationTypes)
      ? { organizationTypes: normalizeStringArray(rawFilters.organizationTypes) }
      : {}),
    ...(normalizeStringArray(rawFilters.demographics)
      ? { demographics: normalizeStringArray(rawFilters.demographics) }
      : {}),
    ...(normalizeStringArray(rawFilters.ages) ? { ages: normalizeStringArray(rawFilters.ages) } : {}),
    ...(normalizeStringArray(rawFilters.genders) ? { genders: normalizeStringArray(rawFilters.genders) } : {}),
    ...(normalizeStringArray(rawFilters.membershipRoles)
      ? { membershipRoles: normalizeStringArray(rawFilters.membershipRoles) }
      : {}),
    ...(normalizeStringArray(rawFilters.membershipStatuses)
      ? { membershipStatuses: normalizeStringArray(rawFilters.membershipStatuses) }
      : {}),
    ...(normalizeStringArray(rawFilters.userStatuses)
      ? { userStatuses: normalizeStringArray(rawFilters.userStatuses) }
      : {}),
    ...(typeof rawFilters.minMemberCount === "number"
      ? { minMemberCount: rawFilters.minMemberCount }
      : {}),
    ...(typeof rawFilters.maxMemberCount === "number"
      ? { maxMemberCount: rawFilters.maxMemberCount }
      : {}),
    ...(typeof rawFilters.minReplies === "number" ? { minReplies: rawFilters.minReplies } : {}),
    ...(typeof rawFilters.minReactions === "number"
      ? { minReactions: rawFilters.minReactions }
      : {}),
    ...(typeof rawFilters.hasMedia === "boolean" ? { hasMedia: rawFilters.hasMedia } : {}),
    ...(typeof rawFilters.hasQuote === "boolean" ? { hasQuote: rawFilters.hasQuote } : {}),
    ...(typeof rawFilters.announcementOnly === "boolean"
      ? { announcementOnly: rawFilters.announcementOnly }
      : {}),
    ...(typeof rawFilters.membershipApproval === "boolean"
      ? { membershipApproval: rawFilters.membershipApproval }
      : {}),
    ...(normalizeStringArray(rawFilters.memberAddModes)
      ? { memberAddModes: normalizeStringArray(rawFilters.memberAddModes) }
      : {}),
  };

  const normalized: Record<string, unknown> = {
    entity: normalizeStatsEntity(source.entity),
    aggregation: normalizeStatsAggregation(source.aggregation),
    measure: normalizeStatsMeasure(source.measure),
  };

  const normalizedGroupBy = Array.isArray(source.groupBy)
    ? source.groupBy.map((value) => normalizeStatsDimension(value))
    : undefined;
  if (normalizedGroupBy) {
    normalized.groupBy = normalizedGroupBy;
  }

  if (Object.keys(filters).length > 0) {
    normalized.filters = filters;
  }

  if (typeof source.timeBucket === "string") {
    normalized.timeBucket = source.timeBucket.trim().toLowerCase();
  }

  const rawSort =
    source.sort && typeof source.sort === "object" && !Array.isArray(source.sort)
      ? (source.sort as Record<string, unknown>)
      : null;
  if (rawSort?.by && rawSort?.direction) {
    normalized.sort = {
      by: rawSort.by,
      direction: rawSort.direction,
    };
  }

  if (typeof source.limit === "number") {
    normalized.limit = source.limit;
  }

  return Object.fromEntries(
    Object.entries(normalized).filter(([, value]) => value !== undefined && value !== null),
  );
}

export function serializeDbStatsQueryInputForToolSchema(
  rawInput: Record<string, unknown>,
): Record<string, unknown> {
  const normalized = normalizeDbStatsQueryInput(rawInput);
  const serialized: Record<string, unknown> = {
    entity: normalized.entity,
    aggregation: normalized.aggregation,
    measure: normalized.measure,
  };

  if (Array.isArray(normalized.groupBy)) {
    serialized.groupBy = normalized.groupBy;
  }

  if (typeof normalized.timeBucket === "string") {
    serialized.timeBucket = normalized.timeBucket;
  }

  if (normalized.sort && typeof normalized.sort === "object" && !Array.isArray(normalized.sort)) {
    serialized.sort = normalized.sort;
  }

  if (typeof normalized.limit === "number") {
    serialized.limit = normalized.limit;
  }

  if (
    normalized.entity &&
    typeof normalized.entity === "string" &&
    normalized.filters &&
    typeof normalized.filters === "object" &&
    !Array.isArray(normalized.filters)
  ) {
    const filterField =
      dbStatsQueryToolFilterFieldByEntity[
        normalized.entity as keyof typeof dbStatsQueryToolFilterFieldByEntity
      ];
    if (filterField) {
      serialized[filterField] = normalized.filters;
    }
  }

  return Object.fromEntries(
    Object.entries(serialized).filter(([, value]) => value !== undefined && value !== null),
  );
}

export function parseDbStatsQueryInput(rawInput: unknown): StatsQueryInput {
  const normalized = normalizeDbStatsQueryInput(
    rawInput && typeof rawInput === "object" && !Array.isArray(rawInput)
      ? (rawInput as Record<string, unknown>)
      : {},
  );
  return validatedStatsQueryInputSchema.parse(normalized);
}

export function parsePartialDbStatsQueryInput(
  rawInput: unknown,
): Partial<StatsQueryInput> {
  const normalized = normalizeDbStatsQueryInput(
    rawInput && typeof rawInput === "object" && !Array.isArray(rawInput)
      ? (rawInput as Record<string, unknown>)
      : {},
  );
  return validatedPartialStatsQueryInputSchema.parse(normalized);
}

export type PlanStepOwner = z.infer<typeof planStepOwnerSchema>;
export type PlanStep = z.infer<typeof planStepSchema>;
export type PlanDraft = z.infer<typeof planDraftSchema>;
export type PlanJobStatus = z.infer<typeof planJobStatusSchema>;
export type PlanJob = z.infer<typeof planJobSchema>;
export type ExecutionArtifact = z.infer<typeof executionArtifactSchema>;
export type FinalAnswer = z.infer<typeof finalAnswerSchema>;
export type StatsQueryInput = z.infer<typeof statsQueryInputSchema>;
export type StatsQueryFilter = z.infer<typeof statsQueryFilterSchema>;
export type StatsQueryEntity = z.infer<typeof statsQueryEntitySchema>;
export type StatsQueryAggregation = z.infer<typeof statsQueryAggregationSchema>;
export type StatsQueryMeasure = z.infer<typeof statsQueryMeasureSchema>;
export type StatsQueryDimension = z.infer<typeof statsQueryDimensionSchema>;
export type StatsQueryTimeBucket = z.infer<typeof statsTimeBucketSchema>;
export type AgentToolCall = z.infer<typeof agentToolCallSchema>;
export type AgentToolDebugLog = AgentToolCall["debugLogs"][number];

export function clonePlanJob(job: PlanJob): PlanJob {
  return JSON.parse(JSON.stringify(job)) as PlanJob;
}
