import { z } from "zod";
import {
  finalAnswerSchema,
  statsQueryAggregationSchema,
  statsQueryDimensionSchema,
  statsQueryEntitySchema,
  statsQueryMeasureSchema,
} from "../types.ts";

export const planningNotesOutputSchema = z
  .object({
    question: z.string(),
    focus: z.string(),
    note: z.string(),
    expectedArtifacts: z.array(z.string()),
  })
  .strict();

const statsDimensionValueSchema = z
  .object({
    value: z.string(),
    label: z.string(),
  })
  .strict();

const statsQueryRowSchema = z
  .object({
    rank: z.number().int().min(1),
    dimensions: z.record(statsDimensionValueSchema),
    value: z.number(),
    share: z.number().min(0).max(1).optional(),
  })
  .strict();

const statsQuerySeriesPointSchema = z
  .object({
    bucket: z.string(),
    startTimestamp: z.number().int().optional(),
    endTimestamp: z.number().int().optional(),
    dimensions: z.record(statsDimensionValueSchema).optional(),
    value: z.number(),
  })
  .strict();

export const dbStatsQueryOutputSchema = z
  .object({
    source: z.literal("mongo-db-stats-query"),
    entity: statsQueryEntitySchema,
    aggregation: statsQueryAggregationSchema,
    measure: statsQueryMeasureSchema,
    groupBy: z.array(statsQueryDimensionSchema).max(2),
    summary: z.string(),
    coverage: z.string(),
    appliedFilters: z.record(z.any()),
    totals: z
      .object({
        value: z.number(),
        rowCount: z.number().int().min(0),
      })
      .strict(),
    rows: z.array(statsQueryRowSchema).max(100),
    series: z.array(statsQuerySeriesPointSchema).max(500).optional(),
    notes: z.array(z.string()).optional(),
  })
  .strict();

export const latestNarrativesOutputSchema = z
  .object({
    tool: z.literal("find_narratives_in_timeframe"),
    timeframeDays: z.number().int().min(1),
    coverage: z.string(),
    narratives: z.array(
      z
        .object({
          narrative: z.string(),
          volume: z.number().int().min(0),
        })
        .strict(),
    ),
  })
  .strict();

export const summarizationOutputSchema = z
  .object({
    source: z.literal("llm-summarizer"),
    answer: z.string(),
    bullets: z.array(z.string()).min(1).max(10),
  })
  .strict();

const audienceCandidateUserSchema = z
  .object({
    userId: z.string(),
    name: z.string(),
    status: z.string(),
    activeMembershipCount: z.number().int().min(0),
    matchingGroupIds: z.array(z.string()).max(10),
    matchingGroups: z.array(z.string()).max(10),
    roles: z.array(z.string()).max(10),
  })
  .strict();

const audienceMessageAuthorSchema = z
  .object({
    userId: z.string(),
    messageCount: z.number().int().min(0),
    replyCount: z.number().int().min(0),
    reactionCount: z.number().int().min(0),
    groupCount: z.number().int().min(0),
  })
  .strict();

const audienceMessageHitSchema = z
  .object({
    messageId: z.string(),
    authorId: z.string(),
    groupId: z.string(),
    groupSubject: z.string(),
    timestamp: z.number().int().min(0),
    replyCount: z.number().int().min(0),
    reactionCount: z.number().int().min(0),
    bodyPreview: z.string(),
  })
  .strict();

const audienceMessageSearchHitSchema = audienceMessageHitSchema
  .extend({
    vectorScore: z.number().min(0).optional(),
  })
  .strict();

const audienceGroupSearchHitSchema = z
  .object({
    groupId: z.string(),
    subject: z.string(),
    topic: z.string(),
    region: z.string(),
    politicalLeaning: z.string(),
    memberCount: z.number().int().min(0),
    activityCount: z.number().int().min(0),
    vectorScore: z.number().min(0).optional(),
  })
  .strict();

export const userProfileLookupOutputSchema = z
  .object({
    source: z.literal("mongo-user-profile-lookup"),
    query: z.string(),
    matchedUsers: z.number().int().min(0),
    users: z.array(audienceCandidateUserSchema).max(25),
  })
  .strict();

export const messageSearchOutputSchema = z
  .object({
    source: z.literal("mongo-message-search"),
    searchMode: z.enum(["regex", "vector"]),
    query: z.string(),
    lastDays: z.number().int().min(1).nullable(),
    matchedMessages: z.number().int().min(0),
    uniqueAuthors: z.number().int().min(0),
    authors: z.array(audienceMessageAuthorSchema).max(10),
    messages: z.array(audienceMessageSearchHitSchema).max(20),
  })
  .strict();

export const groupSearchOutputSchema = z
  .object({
    source: z.literal("mongo-group-search"),
    searchMode: z.enum(["regex", "vector"]),
    query: z.string(),
    lastDays: z.number().int().min(1),
    matchedGroups: z.number().int().min(0),
    groups: z.array(audienceGroupSearchHitSchema).max(20),
  })
  .strict();

export const audienceLookupOutputSchema = z
  .object({
    source: z.literal("semantic-audience-lookup"),
    query: z.string(),
    audienceSummary: z.string(),
    matchedGroups: z.number().int().min(0),
    segments: z.array(z.string()).min(1).max(6),
    sampleGroups: z.array(z.string()).max(5),
    evidence: z
      .array(
        z
          .object({
            groupId: z.string(),
            subject: z.string(),
            reason: z.string(),
            memberCount: z.number().int().min(0),
            activity30d: z.number().int().min(0),
            sampleMessages: z.array(z.string()).max(2),
          })
          .strict(),
      )
      .max(5),
  })
  .strict();

export const influencerLookupOutputSchema = z
  .object({
    source: z.literal("semantic-influencer-lookup"),
    narrative: z.string(),
    query: z.string(),
    summary: z.string(),
    influencers: z
      .array(
        z
          .object({
            maskedUserId: z.string(),
            displayName: z.string(),
            rationale: z.string(),
            influenceScore: z.number().min(0),
            messageCount: z.number().int().min(0),
            reactionCount: z.number().int().min(0),
            replyCount: z.number().int().min(0),
            groupCount: z.number().int().min(0),
          })
          .strict(),
      )
      .max(5),
  })
  .strict();

export const narrativeProbeOutputSchema = z
  .object({
    source: z.literal("dummy-narrative-tool"),
    narratives: z.array(
      z
        .object({
          name: z.string(),
          sentiment: z.string(),
          volume: z.string(),
        })
        .strict(),
    ),
    query: z.string(),
  })
  .strict();

export const audienceAgentOutputSchema = z
  .object({
    audienceSummary: z.string(),
    candidateUsers: z.array(z.string()),
    candidateAudienceSegments: z.array(z.string()),
    influencerLeads: z.array(z.string()),
  })
  .strict();

export const narrativeAgentOutputSchema = z
  .object({
    narrativeSummary: z.string(),
    topNarratives: z.array(z.string()),
    sentimentHighlights: z.array(z.string()),
  })
  .strict();

export const statsAgentOutputSchema = z
  .object({
    metricUsed: z.string(),
    highlights: z.array(z.string()),
    raw: z.record(z.any()),
  })
  .strict();

export const synthesisAgentOutputSchema = finalAnswerSchema;

export const dynamicMongoAggregationOutputSchema = z
  .object({
    source: z.literal("dynamic-mongo-aggregation"),
    question: z.string(),
    summary: z.string().describe("A brief natural language summary of the results"),
    pipelineUsed: z.array(z.record(z.any())).optional().describe("The Mongo pipeline executed (for logging/debugging)"),
    results: z.array(z.record(z.any())).max(100).describe("The raw aggregation results limited to 100 rows"),
  })
  .strict();
  