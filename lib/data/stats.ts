import type { Collection, Document } from "mongodb";
import {
  dummyDatasetSummary,
  getMongoCollectionNames,
  getMongoDb,
} from "./mongo";
import { logToolDebug } from "../runtime/tool-debug.ts";
import {
  parseDbStatsQueryInput,
  StatsQueryAggregation,
  StatsQueryDimension,
  StatsQueryEntity,
  StatsQueryFilter,
  StatsQueryInput,
  StatsQueryMeasure,
  StatsQueryTimeBucket,
} from "../types";

type TagField = {
  tagValue?: string | null;
  age?: string | null;
  gender?: string | null;
  organizationType?: string | null;
};

type GroupDoc = {
  groupId?: string;
  subject?: string;
  description?: string;
  memberCount?: number;
  lastActivityTimestamp?: number;
  announcementOnly?: boolean;
  membershipApproval?: boolean;
  memberAddMode?: string;
  engagementScore?: number;
  normalizedEngagementScore?: number;
  avgMessagesPerDay30d?: number;
  avgAuthorsPerDay30d?: number;
  avgReactionsPerMessage30d?: number;
  avgRepliesPerMessage30d?: number;
  activeMemberPercentage30d?: number;
  activeDays30d?: number;
  tags?: {
    politicalLeaning?: TagField;
    topic?: TagField;
    region?: TagField;
    organization?: TagField;
    demographic?: TagField;
  };
};

type MessageDoc = {
  messageId?: string;
  timestamp?: number;
  groupId?: string;
  authorId?: string;
  body?: string;
  messageReactions?: Array<unknown>;
  messageReplies?: Array<unknown>;
  quotedMessageId?: string;
  forwardingScore?: number;
  messageMedia?: unknown;
};

type UserProfileDoc = {
  userId?: string;
  status?: string;
  groups?: Array<{
    group?: unknown;
    role?: string;
    status?: string;
    joinedAt?: Date;
    leftAt?: Date;
  }>;
};

type DimensionValue = {
  value: string;
  label: string;
};

type StatsRow = {
  rank: number;
  dimensions: Record<string, DimensionValue>;
  value: number;
  share?: number;
};

type StatsSeriesPoint = {
  bucket: string;
  startTimestamp?: number;
  endTimestamp?: number;
  dimensions?: Record<string, DimensionValue>;
  value: number;
};

type DimensionDefinition = {
  valueExpr: Document | string;
  labelExpr: Document | string;
};

type MeasureDefinition =
  | {
      kind: "records";
      label: string;
    }
  | {
      kind: "numeric";
      label: string;
      expr: Document | string;
    }
  | {
      kind: "distinct";
      label: string;
      expr: Document | string;
    };

type StatsPlan = {
  collection: Collection<any>;
  entity: StatsQueryEntity;
  basePipeline: Document[];
  timeField: string | null;
  dimensions: Partial<Record<StatsQueryDimension, DimensionDefinition>>;
  measures: Partial<Record<StatsQueryMeasure, MeasureDefinition>>;
  notes: string[];
};

const DAY_IN_MS = 24 * 60 * 60 * 1000;

const SUPPORTED_DIMENSIONS_BY_ENTITY: Record<StatsQueryEntity, Set<StatsQueryDimension>> = {
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

const SUPPORTED_MEASURES_BY_ENTITY: Record<StatsQueryEntity, Set<StatsQueryMeasure>> = {
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

const SUPPORTED_FILTERS_BY_ENTITY: Record<StatsQueryEntity, Set<keyof StatsQueryFilter>> = {
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

function normalizeWhitespace(value: string | null | undefined): string {
  return (value ?? "").replace(/\s+/g, " ").trim();
}

function numberOrZero(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function formatDimensionValue(value: unknown, fallback = "unknown"): string {
  if (typeof value === "string") {
    const normalized = normalizeWhitespace(value);
    return normalized || fallback;
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }

  return fallback;
}

function ensureValidCombination(input: StatsQueryInput) {
  const supportedDimensions = SUPPORTED_DIMENSIONS_BY_ENTITY[input.entity];
  for (const dimension of input.groupBy) {
    if (!supportedDimensions.has(dimension)) {
      throw new Error(
        `db_stats_query does not support groupBy="${dimension}" for entity="${input.entity}".`,
      );
    }
  }

  const supportedMeasures = SUPPORTED_MEASURES_BY_ENTITY[input.entity];
  if (!supportedMeasures.has(input.measure)) {
    throw new Error(
      `db_stats_query does not support measure="${input.measure}" for entity="${input.entity}".`,
    );
  }

  const filters = input.filters ?? {};
  const supportedFilters = SUPPORTED_FILTERS_BY_ENTITY[input.entity];
  for (const key of Object.keys(filters) as Array<keyof StatsQueryFilter>) {
    if (!supportedFilters.has(key)) {
      throw new Error(
        `db_stats_query does not support filter="${key}" for entity="${input.entity}".`,
      );
    }
  }

  if (input.aggregation === "count" && input.measure !== "records") {
    throw new Error('db_stats_query with aggregation="count" requires measure="records".');
  }

  if (
    input.aggregation === "distinct_count" &&
    !["distinct_authors", "distinct_groups", "distinct_users"].includes(input.measure)
  ) {
    throw new Error(
      'db_stats_query with aggregation="distinct_count" requires a distinct_* measure.',
    );
  }

  if (
    (input.aggregation === "distribution" || input.aggregation === "top_values") &&
    input.groupBy.length === 0
  ) {
    throw new Error(
      `db_stats_query with aggregation="${input.aggregation}" requires at least one groupBy dimension.`,
    );
  }

  if (input.aggregation === "time_series") {
    if (!input.timeBucket) {
      throw new Error('db_stats_query with aggregation="time_series" requires timeBucket.');
    }
    if (input.groupBy.length > 1) {
      throw new Error('db_stats_query with aggregation="time_series" supports at most one groupBy dimension.');
    }
    if (input.entity === "users" || input.entity === "memberships") {
      throw new Error(
        `db_stats_query with aggregation="time_series" is not supported for entity="${input.entity}".`,
      );
    }
    if (["distinct_authors", "distinct_groups", "distinct_users"].includes(input.measure)) {
      throw new Error(
        'db_stats_query with aggregation="time_series" does not support distinct_* measures.',
      );
    }
  }

  if (
    ["sum", "avg", "min", "max"].includes(input.aggregation) &&
    ["records", "distinct_authors", "distinct_groups", "distinct_users"].includes(input.measure)
  ) {
    throw new Error(
      `db_stats_query with aggregation="${input.aggregation}" requires a numeric measure.`,
    );
  }
}

async function getCollections(): Promise<{
  groups: Collection<GroupDoc>;
  messages: Collection<MessageDoc>;
  users: Collection<UserProfileDoc>;
}> {
  const db = await getMongoDb();
  const names = getMongoCollectionNames();

  return {
    groups: db.collection<GroupDoc>(names.groupsCollection),
    messages: db.collection<MessageDoc>(names.messagesCollection),
    users: db.collection<UserProfileDoc>(names.usersCollection),
  };
}

async function getReferenceTimestamp(
  collection: Collection<any>,
  field: string,
): Promise<number> {
  const projection = { [field]: 1 } as Record<string, 1>;
  const sort = { [field]: -1 } as Record<string, -1>;
  const latest = await collection
    .find({ [field]: { $exists: true, $ne: null } }, { projection })
    .sort(sort)
    .limit(1)
    .next();

  const value = latest?.[field];
  return typeof value === "number" && Number.isFinite(value) ? value : Date.now();
}

async function getCutoffTimestamp(
  collection: Collection<any>,
  field: string,
  lastDays: number,
): Promise<number> {
  return (await getReferenceTimestamp(collection, field)) - lastDays * DAY_IN_MS;
}

function booleanDimensionExpr(fieldPath: string): Document {
  return {
    $cond: [{ $eq: [fieldPath, true] }, "true", "false"],
  };
}

function stringDimensionExpr(fieldPath: string, fallback = "unknown"): Document {
  return {
    $ifNull: [fieldPath, fallback],
  };
}

function lookupGroupStages(groupsCollectionName: string): Document[] {
  return [
    {
      $lookup: {
        from: groupsCollectionName,
        localField: "groupId",
        foreignField: "groupId",
        as: "groupDoc",
      },
    },
    {
      $unwind: {
        path: "$groupDoc",
        preserveNullAndEmptyArrays: true,
      },
    },
  ];
}

function lookupMembershipGroupStages(groupsCollectionName: string): Document[] {
  return [
    {
      $lookup: {
        from: groupsCollectionName,
        localField: "groups.group",
        foreignField: "_id",
        as: "groupDoc",
      },
    },
    {
      $unwind: {
        path: "$groupDoc",
        preserveNullAndEmptyArrays: true,
      },
    },
  ];
}

function hasAnyGroupBackedFilter(filters: StatsQueryFilter | undefined): boolean {
  return Boolean(
    filters?.politicalLeanings ||
      filters?.topics ||
      filters?.regions ||
      filters?.organizations ||
      filters?.organizationTypes ||
      filters?.demographics ||
      filters?.ages ||
      filters?.genders,
  );
}

async function buildMessagesPlan(
  input: StatsQueryInput,
  collections: Awaited<ReturnType<typeof getCollections>>,
): Promise<StatsPlan> {
  const names = getMongoCollectionNames();
  const needsGroupLookup =
    input.groupBy.some((dimension) =>
      [
        "group",
        "political_leaning",
        "topic",
        "region",
        "organization",
        "organization_type",
        "demographic",
        "age",
        "gender",
      ].includes(dimension),
    ) || hasAnyGroupBackedFilter(input.filters);

  const basePipeline: Document[] = [
    {
      $addFields: {
        reactionCount: { $size: { $ifNull: ["$messageReactions", []] } },
        replyCount: { $size: { $ifNull: ["$messageReplies", []] } },
        forwardingScoreValue: { $ifNull: ["$forwardingScore", 0] },
        hasMediaValue: { $ne: [{ $ifNull: ["$messageMedia", null] }, null] },
        hasQuoteValue: {
          $gt: [{ $strLenCP: { $ifNull: ["$quotedMessageId", ""] } }, 0],
        },
      },
    },
  ];

  if (input.filters?.lastDays) {
    const cutoff = await getCutoffTimestamp(collections.messages, "timestamp", input.filters.lastDays);
    basePipeline.push({ $match: { timestamp: { $gte: cutoff } } });
  }
  if (input.filters?.groupIds?.length) {
    basePipeline.push({ $match: { groupId: { $in: input.filters.groupIds } } });
  }
  if (input.filters?.userIds?.length) {
    basePipeline.push({ $match: { authorId: { $in: input.filters.userIds } } });
  }
  if (typeof input.filters?.minReplies === "number") {
    basePipeline.push({ $match: { replyCount: { $gte: input.filters.minReplies } } });
  }
  if (typeof input.filters?.minReactions === "number") {
    basePipeline.push({ $match: { reactionCount: { $gte: input.filters.minReactions } } });
  }
  if (typeof input.filters?.hasMedia === "boolean") {
    basePipeline.push({ $match: { hasMediaValue: input.filters.hasMedia } });
  }
  if (typeof input.filters?.hasQuote === "boolean") {
    basePipeline.push({ $match: { hasQuoteValue: input.filters.hasQuote } });
  }

  if (needsGroupLookup) {
    basePipeline.push(...lookupGroupStages(names.groupsCollection));
    if (input.filters?.politicalLeanings?.length) {
      basePipeline.push({
        $match: {
          "groupDoc.tags.politicalLeaning.tagValue": { $in: input.filters.politicalLeanings },
        },
      });
    }
    if (input.filters?.topics?.length) {
      basePipeline.push({
        $match: {
          "groupDoc.tags.topic.tagValue": { $in: input.filters.topics },
        },
      });
    }
    if (input.filters?.regions?.length) {
      basePipeline.push({
        $match: {
          "groupDoc.tags.region.tagValue": { $in: input.filters.regions },
        },
      });
    }
    if (input.filters?.organizations?.length) {
      basePipeline.push({
        $match: {
          "groupDoc.tags.organization.tagValue": { $in: input.filters.organizations },
        },
      });
    }
    if (input.filters?.organizationTypes?.length) {
      basePipeline.push({
        $match: {
          "groupDoc.tags.organization.organizationType": {
            $in: input.filters.organizationTypes,
          },
        },
      });
    }
    if (input.filters?.demographics?.length) {
      basePipeline.push({
        $match: {
          "groupDoc.tags.demographic.tagValue": { $in: input.filters.demographics },
        },
      });
    }
    if (input.filters?.ages?.length) {
      basePipeline.push({
        $match: {
          "groupDoc.tags.demographic.age": { $in: input.filters.ages },
        },
      });
    }
    if (input.filters?.genders?.length) {
      basePipeline.push({
        $match: {
          "groupDoc.tags.demographic.gender": { $in: input.filters.genders },
        },
      });
    }
  }

  return {
    collection: collections.messages,
    entity: "messages",
    basePipeline,
    timeField: "timestamp",
    notes: needsGroupLookup
      ? [`Joined ${names.groupsCollection} to expose group tag dimensions for messages.`]
      : [],
    dimensions: {
      group: {
        valueExpr: stringDimensionExpr("$groupId"),
        labelExpr: {
          $ifNull: ["$groupDoc.subject", stringDimensionExpr("$groupId")],
        },
      },
      author: {
        valueExpr: stringDimensionExpr("$authorId"),
        labelExpr: stringDimensionExpr("$authorId"),
      },
      political_leaning: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.politicalLeaning.tagValue"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.politicalLeaning.tagValue"),
      },
      topic: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.topic.tagValue"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.topic.tagValue"),
      },
      region: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.region.tagValue"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.region.tagValue"),
      },
      organization: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.organization.tagValue"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.organization.tagValue"),
      },
      organization_type: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.organization.organizationType"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.organization.organizationType"),
      },
      demographic: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.demographic.tagValue"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.demographic.tagValue"),
      },
      age: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.demographic.age"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.demographic.age"),
      },
      gender: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.demographic.gender"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.demographic.gender"),
      },
      has_media: {
        valueExpr: booleanDimensionExpr("$hasMediaValue"),
        labelExpr: booleanDimensionExpr("$hasMediaValue"),
      },
      has_quote: {
        valueExpr: booleanDimensionExpr("$hasQuoteValue"),
        labelExpr: booleanDimensionExpr("$hasQuoteValue"),
      },
    },
    measures: {
      records: { kind: "records", label: "message count" },
      distinct_authors: {
        kind: "distinct",
        label: "distinct author count",
        expr: stringDimensionExpr("$authorId", ""),
      },
      distinct_groups: {
        kind: "distinct",
        label: "distinct group count",
        expr: stringDimensionExpr("$groupId", ""),
      },
      reaction_count: {
        kind: "numeric",
        label: "reaction count",
        expr: "$reactionCount",
      },
      reply_count: {
        kind: "numeric",
        label: "reply count",
        expr: "$replyCount",
      },
      forwarding_score: {
        kind: "numeric",
        label: "forwarding score",
        expr: "$forwardingScoreValue",
      },
    },
  };
}

async function buildGroupsPlan(
  input: StatsQueryInput,
  collections: Awaited<ReturnType<typeof getCollections>>,
): Promise<StatsPlan> {
  const basePipeline: Document[] = [];

  if (input.filters?.lastDays) {
    const cutoff = await getCutoffTimestamp(
      collections.groups,
      "lastActivityTimestamp",
      input.filters.lastDays,
    );
    basePipeline.push({ $match: { lastActivityTimestamp: { $gte: cutoff } } });
  }
  if (input.filters?.groupIds?.length) {
    basePipeline.push({ $match: { groupId: { $in: input.filters.groupIds } } });
  }
  if (input.filters?.politicalLeanings?.length) {
    basePipeline.push({
      $match: {
        "tags.politicalLeaning.tagValue": { $in: input.filters.politicalLeanings },
      },
    });
  }
  if (input.filters?.topics?.length) {
    basePipeline.push({ $match: { "tags.topic.tagValue": { $in: input.filters.topics } } });
  }
  if (input.filters?.regions?.length) {
    basePipeline.push({ $match: { "tags.region.tagValue": { $in: input.filters.regions } } });
  }
  if (input.filters?.organizations?.length) {
    basePipeline.push({
      $match: { "tags.organization.tagValue": { $in: input.filters.organizations } },
    });
  }
  if (input.filters?.organizationTypes?.length) {
    basePipeline.push({
      $match: {
        "tags.organization.organizationType": { $in: input.filters.organizationTypes },
      },
    });
  }
  if (input.filters?.demographics?.length) {
    basePipeline.push({
      $match: { "tags.demographic.tagValue": { $in: input.filters.demographics } },
    });
  }
  if (input.filters?.ages?.length) {
    basePipeline.push({ $match: { "tags.demographic.age": { $in: input.filters.ages } } });
  }
  if (input.filters?.genders?.length) {
    basePipeline.push({
      $match: { "tags.demographic.gender": { $in: input.filters.genders } },
    });
  }
  if (typeof input.filters?.minMemberCount === "number") {
    basePipeline.push({ $match: { memberCount: { $gte: input.filters.minMemberCount } } });
  }
  if (typeof input.filters?.maxMemberCount === "number") {
    basePipeline.push({ $match: { memberCount: { $lte: input.filters.maxMemberCount } } });
  }
  if (typeof input.filters?.announcementOnly === "boolean") {
    basePipeline.push({ $match: { announcementOnly: input.filters.announcementOnly } });
  }
  if (typeof input.filters?.membershipApproval === "boolean") {
    basePipeline.push({ $match: { membershipApproval: input.filters.membershipApproval } });
  }
  if (input.filters?.memberAddModes?.length) {
    basePipeline.push({ $match: { memberAddMode: { $in: input.filters.memberAddModes } } });
  }

  return {
    collection: collections.groups,
    entity: "groups",
    basePipeline,
    timeField: "lastActivityTimestamp",
    notes: [],
    dimensions: {
      group: {
        valueExpr: stringDimensionExpr("$groupId"),
        labelExpr: {
          $ifNull: ["$subject", stringDimensionExpr("$groupId")],
        },
      },
      political_leaning: {
        valueExpr: stringDimensionExpr("$tags.politicalLeaning.tagValue"),
        labelExpr: stringDimensionExpr("$tags.politicalLeaning.tagValue"),
      },
      topic: {
        valueExpr: stringDimensionExpr("$tags.topic.tagValue"),
        labelExpr: stringDimensionExpr("$tags.topic.tagValue"),
      },
      region: {
        valueExpr: stringDimensionExpr("$tags.region.tagValue"),
        labelExpr: stringDimensionExpr("$tags.region.tagValue"),
      },
      organization: {
        valueExpr: stringDimensionExpr("$tags.organization.tagValue"),
        labelExpr: stringDimensionExpr("$tags.organization.tagValue"),
      },
      organization_type: {
        valueExpr: stringDimensionExpr("$tags.organization.organizationType"),
        labelExpr: stringDimensionExpr("$tags.organization.organizationType"),
      },
      demographic: {
        valueExpr: stringDimensionExpr("$tags.demographic.tagValue"),
        labelExpr: stringDimensionExpr("$tags.demographic.tagValue"),
      },
      age: {
        valueExpr: stringDimensionExpr("$tags.demographic.age"),
        labelExpr: stringDimensionExpr("$tags.demographic.age"),
      },
      gender: {
        valueExpr: stringDimensionExpr("$tags.demographic.gender"),
        labelExpr: stringDimensionExpr("$tags.demographic.gender"),
      },
      announcement_only: {
        valueExpr: booleanDimensionExpr("$announcementOnly"),
        labelExpr: booleanDimensionExpr("$announcementOnly"),
      },
      membership_approval: {
        valueExpr: booleanDimensionExpr("$membershipApproval"),
        labelExpr: booleanDimensionExpr("$membershipApproval"),
      },
      member_add_mode: {
        valueExpr: stringDimensionExpr("$memberAddMode"),
        labelExpr: stringDimensionExpr("$memberAddMode"),
      },
    },
    measures: {
      records: { kind: "records", label: "group count" },
      member_count: { kind: "numeric", label: "member count", expr: { $ifNull: ["$memberCount", 0] } },
      engagement_score: {
        kind: "numeric",
        label: "engagement score",
        expr: { $ifNull: ["$engagementScore", 0] },
      },
      normalized_engagement_score: {
        kind: "numeric",
        label: "normalized engagement score",
        expr: { $ifNull: ["$normalizedEngagementScore", 0] },
      },
      avg_messages_per_day_30d: {
        kind: "numeric",
        label: "average messages per day over 30d",
        expr: { $ifNull: ["$avgMessagesPerDay30d", 0] },
      },
      avg_authors_per_day_30d: {
        kind: "numeric",
        label: "average authors per day over 30d",
        expr: { $ifNull: ["$avgAuthorsPerDay30d", 0] },
      },
      avg_reactions_per_message_30d: {
        kind: "numeric",
        label: "average reactions per message over 30d",
        expr: { $ifNull: ["$avgReactionsPerMessage30d", 0] },
      },
      avg_replies_per_message_30d: {
        kind: "numeric",
        label: "average replies per message over 30d",
        expr: { $ifNull: ["$avgRepliesPerMessage30d", 0] },
      },
      active_member_percentage_30d: {
        kind: "numeric",
        label: "active member percentage over 30d",
        expr: { $ifNull: ["$activeMemberPercentage30d", 0] },
      },
      active_days_30d: {
        kind: "numeric",
        label: "active days over 30d",
        expr: { $ifNull: ["$activeDays30d", 0] },
      },
    },
  };
}

async function buildUsersPlan(
  input: StatsQueryInput,
  collections: Awaited<ReturnType<typeof getCollections>>,
): Promise<StatsPlan> {
  const basePipeline: Document[] = [];

  if (input.filters?.userIds?.length) {
    basePipeline.push({ $match: { userId: { $in: input.filters.userIds } } });
  }
  if (input.filters?.userStatuses?.length) {
    basePipeline.push({ $match: { status: { $in: input.filters.userStatuses } } });
  }

  return {
    collection: collections.users,
    entity: "users",
    basePipeline,
    timeField: null,
    notes: [],
    dimensions: {
      user_status: {
        valueExpr: stringDimensionExpr("$status"),
        labelExpr: stringDimensionExpr("$status"),
      },
    },
    measures: {
      records: { kind: "records", label: "user count" },
      distinct_users: {
        kind: "distinct",
        label: "distinct user count",
        expr: stringDimensionExpr("$userId", ""),
      },
    },
  };
}

async function buildMembershipsPlan(
  input: StatsQueryInput,
  collections: Awaited<ReturnType<typeof getCollections>>,
): Promise<StatsPlan> {
  const names = getMongoCollectionNames();
  const basePipeline: Document[] = [
    {
      $unwind: {
        path: "$groups",
        preserveNullAndEmptyArrays: false,
      },
    },
    ...lookupMembershipGroupStages(names.groupsCollection),
  ];

  if (input.filters?.userIds?.length) {
    basePipeline.push({ $match: { userId: { $in: input.filters.userIds } } });
  }
  if (input.filters?.groupIds?.length) {
    basePipeline.push({ $match: { "groupDoc.groupId": { $in: input.filters.groupIds } } });
  }
  if (input.filters?.membershipRoles?.length) {
    basePipeline.push({ $match: { "groups.role": { $in: input.filters.membershipRoles } } });
  }
  if (input.filters?.membershipStatuses?.length) {
    basePipeline.push({
      $match: { "groups.status": { $in: input.filters.membershipStatuses } },
    });
  }
  if (input.filters?.userStatuses?.length) {
    basePipeline.push({ $match: { status: { $in: input.filters.userStatuses } } });
  }
  if (input.filters?.politicalLeanings?.length) {
    basePipeline.push({
      $match: {
        "groupDoc.tags.politicalLeaning.tagValue": { $in: input.filters.politicalLeanings },
      },
    });
  }
  if (input.filters?.topics?.length) {
    basePipeline.push({
      $match: { "groupDoc.tags.topic.tagValue": { $in: input.filters.topics } },
    });
  }
  if (input.filters?.regions?.length) {
    basePipeline.push({
      $match: { "groupDoc.tags.region.tagValue": { $in: input.filters.regions } },
    });
  }
  if (input.filters?.organizations?.length) {
    basePipeline.push({
      $match: { "groupDoc.tags.organization.tagValue": { $in: input.filters.organizations } },
    });
  }
  if (input.filters?.organizationTypes?.length) {
    basePipeline.push({
      $match: {
        "groupDoc.tags.organization.organizationType": {
          $in: input.filters.organizationTypes,
        },
      },
    });
  }
  if (input.filters?.demographics?.length) {
    basePipeline.push({
      $match: {
        "groupDoc.tags.demographic.tagValue": { $in: input.filters.demographics },
      },
    });
  }
  if (input.filters?.ages?.length) {
    basePipeline.push({
      $match: { "groupDoc.tags.demographic.age": { $in: input.filters.ages } },
    });
  }
  if (input.filters?.genders?.length) {
    basePipeline.push({
      $match: { "groupDoc.tags.demographic.gender": { $in: input.filters.genders } },
    });
  }

  return {
    collection: collections.users,
    entity: "memberships",
    basePipeline,
    timeField: null,
    notes: [`Unwound memberships from ${names.usersCollection} and joined ${names.groupsCollection}.`],
    dimensions: {
      group: {
        valueExpr: {
          $ifNull: ["$groupDoc.groupId", { $toString: "$groups.group" }],
        },
        labelExpr: {
          $ifNull: ["$groupDoc.subject", { $ifNull: ["$groupDoc.groupId", "unknown"] }],
        },
      },
      political_leaning: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.politicalLeaning.tagValue"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.politicalLeaning.tagValue"),
      },
      topic: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.topic.tagValue"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.topic.tagValue"),
      },
      region: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.region.tagValue"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.region.tagValue"),
      },
      organization: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.organization.tagValue"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.organization.tagValue"),
      },
      organization_type: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.organization.organizationType"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.organization.organizationType"),
      },
      demographic: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.demographic.tagValue"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.demographic.tagValue"),
      },
      age: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.demographic.age"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.demographic.age"),
      },
      gender: {
        valueExpr: stringDimensionExpr("$groupDoc.tags.demographic.gender"),
        labelExpr: stringDimensionExpr("$groupDoc.tags.demographic.gender"),
      },
      membership_role: {
        valueExpr: stringDimensionExpr("$groups.role"),
        labelExpr: stringDimensionExpr("$groups.role"),
      },
      membership_status: {
        valueExpr: stringDimensionExpr("$groups.status"),
        labelExpr: stringDimensionExpr("$groups.status"),
      },
      user_status: {
        valueExpr: stringDimensionExpr("$status"),
        labelExpr: stringDimensionExpr("$status"),
      },
    },
    measures: {
      records: { kind: "records", label: "membership count" },
      distinct_users: {
        kind: "distinct",
        label: "distinct user count",
        expr: stringDimensionExpr("$userId", ""),
      },
      distinct_groups: {
        kind: "distinct",
        label: "distinct group count",
        expr: {
          $ifNull: ["$groupDoc.groupId", ""],
        },
      },
    },
  };
}

async function buildStatsPlan(
  input: StatsQueryInput,
  collections: Awaited<ReturnType<typeof getCollections>>,
): Promise<StatsPlan> {
  switch (input.entity) {
    case "messages":
      return buildMessagesPlan(input, collections);
    case "groups":
      return buildGroupsPlan(input, collections);
    case "users":
      return buildUsersPlan(input, collections);
    case "memberships":
      return buildMembershipsPlan(input, collections);
  }
}

function buildDimensionAddFields(
  groupBy: StatsQueryDimension[],
  plan: StatsPlan,
): Document {
  const fields: Record<string, Document | string> = {};
  for (const dimension of groupBy) {
    const definition = plan.dimensions[dimension];
    if (!definition) {
      continue;
    }
    fields[`_dim_${dimension}_value`] = definition.valueExpr;
    fields[`_dim_${dimension}_label`] = definition.labelExpr;
  }
  return fields;
}

function buildGroupId(groupBy: StatsQueryDimension[]): Document | null {
  if (groupBy.length === 0) {
    return null;
  }

  return Object.fromEntries(
    groupBy.flatMap((dimension) => [
      [`_dim_${dimension}_value`, `$_dim_${dimension}_value`],
      [`_dim_${dimension}_label`, `$_dim_${dimension}_label`],
    ]),
  );
}

function buildSecondStageGroupId(
  groupBy: StatsQueryDimension[],
  includeBucket: boolean,
): Document | null {
  const entries: Array<[string, string]> = [];
  if (includeBucket) {
    entries.push(["_bucket_date", "$_id._bucket_date"]);
  }
  for (const dimension of groupBy) {
    entries.push([`_dim_${dimension}_value`, `$_id._dim_${dimension}_value`]);
    entries.push([`_dim_${dimension}_label`, `$_id._dim_${dimension}_label`]);
  }

  return entries.length > 0 ? Object.fromEntries(entries) : null;
}

function applyGroupedValueSort(
  rows: StatsRow[],
  sort: StatsQueryInput["sort"],
): StatsRow[] {
  const sorted = [...rows];
  const direction = sort?.direction === "asc" ? 1 : -1;
  const by = sort?.by ?? "value";

  sorted.sort((left, right) => {
    if (by === "label") {
      const leftLabel = Object.values(left.dimensions)[0]?.label ?? "";
      const rightLabel = Object.values(right.dimensions)[0]?.label ?? "";
      return direction * leftLabel.localeCompare(rightLabel || "");
    }

    return direction * (left.value - right.value);
  });

  return sorted;
}

function buildSummary(input: StatsQueryInput): string {
  const dimensions = input.groupBy.length > 0 ? ` grouped by ${input.groupBy.join(", ")}` : "";
  const window =
    typeof input.filters?.lastDays === "number" ? ` over the last ${input.filters.lastDays} days` : "";
  return `${input.aggregation} of ${input.measure} for ${input.entity}${dimensions}${window}.`;
}

function buildCoverage(input: StatsQueryInput): string {
  const filterParts: string[] = [];
  const filters = input.filters ?? {};
  for (const [key, value] of Object.entries(filters)) {
    if (Array.isArray(value)) {
      if (value.length > 0) {
        filterParts.push(`${key}=${value.join(",")}`);
      }
      continue;
    }

    if (value !== undefined && value !== null) {
      filterParts.push(`${key}=${String(value)}`);
    }
  }

  return filterParts.length > 0
    ? `Mongo analytics over ${input.entity} with filters: ${filterParts.join("; ")}.`
    : `Mongo analytics over ${input.entity} without additional filters.`;
}

function computeBucketEndTimestamp(startTimestamp: number, bucket: StatsQueryTimeBucket): number {
  const date = new Date(startTimestamp);
  if (bucket === "day") {
    date.setUTCDate(date.getUTCDate() + 1);
  } else if (bucket === "week") {
    date.setUTCDate(date.getUTCDate() + 7);
  } else {
    date.setUTCMonth(date.getUTCMonth() + 1);
  }
  return date.getTime();
}

async function aggregateRows(
  input: StatsQueryInput,
  plan: StatsPlan,
  measureDefinition: MeasureDefinition,
): Promise<{ rows: StatsRow[]; series?: StatsSeriesPoint[] }> {
  const pipeline = [...plan.basePipeline];
  const addFields: Record<string, Document | string> = buildDimensionAddFields(input.groupBy, plan);

  if (measureDefinition.kind === "numeric") {
    addFields._measure_value = measureDefinition.expr;
  }
  if (measureDefinition.kind === "distinct") {
    addFields._distinct_value = measureDefinition.expr;
  }
  if (input.aggregation === "time_series" && plan.timeField && input.timeBucket) {
    addFields._bucket_date = {
      $dateTrunc: {
        date: { $toDate: `$${plan.timeField}` },
        unit: input.timeBucket,
      },
    };
  }
  if (Object.keys(addFields).length > 0) {
    pipeline.push({ $addFields: addFields });
  }

  const includeBucket = input.aggregation === "time_series";
  const groupId = buildGroupId(input.groupBy);

  if (measureDefinition.kind === "distinct") {
    pipeline.push({
      $match: {
        _distinct_value: { $nin: [null, ""] },
      },
    });

    const firstGroupId = {
      ...(includeBucket ? { _bucket_date: "$_bucket_date" } : {}),
      ...(groupId ?? {}),
      _distinct_value: "$_distinct_value",
    };
    pipeline.push({ $group: { _id: firstGroupId } });
    pipeline.push({
      $group: {
        _id: buildSecondStageGroupId(input.groupBy, includeBucket),
        value: { $sum: 1 },
      },
    });
  } else {
    const valueAccumulator =
      measureDefinition.kind === "records"
        ? { $sum: 1 }
        : input.aggregation === "avg"
          ? { $avg: "$_measure_value" }
          : input.aggregation === "min"
            ? { $min: "$_measure_value" }
            : input.aggregation === "max"
              ? { $max: "$_measure_value" }
              : { $sum: "$_measure_value" };

    pipeline.push({
      $group: {
        _id: includeBucket
          ? {
              ...(groupId ?? {}),
              _bucket_date: "$_bucket_date",
            }
          : groupId,
        value: valueAccumulator,
      },
    });
  }

  await logToolDebug("Running Mongo stats aggregation.", {
    entity: input.entity,
    aggregation: input.aggregation,
    measure: input.measure,
    groupBy: input.groupBy,
    pipelineStages: pipeline.length,
  });

  const rawRows = await plan.collection.aggregate<Document>(pipeline).toArray();

  if (input.aggregation === "time_series") {
    const series = rawRows
      .flatMap((row) => {
        const id = (row._id ?? {}) as Record<string, unknown>;
        const rawBucket = id._bucket_date;
        const startTimestamp =
          rawBucket instanceof Date
            ? rawBucket.getTime()
            : rawBucket
              ? new Date(String(rawBucket)).getTime()
              : undefined;
        if (typeof startTimestamp !== "number" || Number.isNaN(startTimestamp)) {
          return [];
        }

        const dimensions =
          input.groupBy.length > 0
            ? Object.fromEntries(
                input.groupBy.map((dimension) => [
                  dimension,
                  {
                    value: formatDimensionValue(id[`_dim_${dimension}_value`]),
                    label: formatDimensionValue(id[`_dim_${dimension}_label`]),
                  },
                ]),
              )
            : undefined;

        return [{
          bucket: new Date(startTimestamp).toISOString(),
          startTimestamp,
          endTimestamp: computeBucketEndTimestamp(startTimestamp, input.timeBucket!),
          ...(dimensions ? { dimensions } : {}),
          value: numberOrZero(row.value),
        }];
      })
      .sort((left, right) => numberOrZero(left.startTimestamp) - numberOrZero(right.startTimestamp));

    return {
      rows: [],
      series,
    };
  }

  const rows = rawRows.map((row, index) => {
    const id = (row._id ?? {}) as Record<string, unknown>;
    const dimensions = Object.fromEntries(
      input.groupBy.map((dimension) => [
        dimension,
        {
          value: formatDimensionValue(id[`_dim_${dimension}_value`]),
          label: formatDimensionValue(id[`_dim_${dimension}_label`]),
        },
      ]),
    );

    return {
      rank: index + 1,
      dimensions,
      value: numberOrZero(row.value),
    } satisfies StatsRow;
  });

  const sortedRows = applyGroupedValueSort(rows, input.sort);
  const limitedRows =
    typeof input.limit === "number" ? sortedRows.slice(0, input.limit) : sortedRows;
  const totalValue = limitedRows.reduce((sum, row) => sum + row.value, 0);

  const withRankAndShare = limitedRows.map((row, index) => ({
    ...row,
    rank: index + 1,
    ...(input.aggregation === "distribution" && totalValue > 0
      ? { share: Number((row.value / totalValue).toFixed(4)) }
      : {}),
  }));

  return { rows: withRankAndShare };
}

export async function runStatsQuery(rawInput: StatsQueryInput): Promise<Record<string, unknown>> {
  const input = parseDbStatsQueryInput(rawInput);
  ensureValidCombination(input);

  await logToolDebug("db_stats_query request parsed.", {
    entity: input.entity,
    aggregation: input.aggregation,
    measure: input.measure,
    groupBy: input.groupBy,
    filters: input.filters ?? {},
    timeBucket: input.timeBucket ?? null,
    limit: input.limit ?? null,
  });

  const collections = await getCollections();
  const plan = await buildStatsPlan(input, collections);
  const measureDefinition = plan.measures[input.measure];

  if (!measureDefinition) {
    throw new Error(
      `db_stats_query does not have a measure definition for "${input.measure}" on entity "${input.entity}".`,
    );
  }

  await logToolDebug("db_stats_query execution plan resolved.", {
    baseStages: plan.basePipeline.length,
    notes: plan.notes,
  });

  const { rows, series } = await aggregateRows(input, plan, measureDefinition);
  const totalValue =
    input.aggregation === "time_series"
      ? (series ?? []).reduce((sum, point) => sum + point.value, 0)
      : rows.length > 0
        ? rows.reduce((sum, row) => sum + row.value, 0)
        : 0;
  const rowCount = input.aggregation === "time_series" ? series?.length ?? 0 : rows.length;
  const notes = [...plan.notes];

  if (
    input.groupBy.length > 0 &&
    ["distinct_count", "distribution", "top_values"].includes(input.aggregation) &&
    ["distinct_authors", "distinct_groups", "distinct_users"].includes(input.measure)
  ) {
    notes.push("Grouped distinct counts may not sum to a global distinct total.");
  }

  await logToolDebug("db_stats_query completed.", {
    rows: rows.length,
    seriesPoints: series?.length ?? 0,
    totalValue,
  });

  return {
    source: "mongo-db-stats-query",
    entity: input.entity,
    aggregation: input.aggregation,
    measure: input.measure,
    groupBy: input.groupBy,
    summary: buildSummary(input),
    coverage: `${buildCoverage(input)} ${dummyDatasetSummary()}`,
    appliedFilters: input.filters ?? {},
    totals: {
      value: totalValue,
      rowCount,
    },
    rows,
    ...(series && series.length > 0 ? { series } : {}),
    ...(notes.length > 0 ? { notes } : {}),
  };
}
