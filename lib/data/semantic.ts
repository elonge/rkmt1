import { createHash } from "node:crypto";
import OpenAI from "openai";
import { Agent, run } from "@openai/agents";
import { ObjectId } from "mongodb";
import type { Collection, Document, Filter } from "mongodb";
import { z } from "zod";
import {
  audienceLookupOutputSchema,
  influencerLookupOutputSchema,
} from "../agents/tool-output-schemas.ts";
import { getMongoCollectionNames, getMongoDb } from "./mongo.ts";
import { logToolDebug } from "../runtime/tool-debug.ts";

const DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small";
const DEFAULT_EMBEDDING_VERSION = "v1";
const DEFAULT_EMBEDDING_BATCH_SIZE = 64;
const DEFAULT_INCREMENTAL_GROUP_LIMIT = 200;
const DEFAULT_INCREMENTAL_MESSAGE_LIMIT = 500;
const DEFAULT_GROUP_SEARCH_LIMIT = 8;
const DEFAULT_GROUP_EVIDENCE_LIMIT = 16;
const DEFAULT_MESSAGE_SEARCH_LIMIT = 40;
const DEFAULT_QUERY_CANDIDATE_MULTIPLIER = 20;

export const semanticSyncModeSchema = z.enum([
  "groups_backfill",
  "messages_backfill",
  "incremental",
]);

export type SemanticSyncMode = z.infer<typeof semanticSyncModeSchema>;

export type SemanticSyncOptions = {
  limit?: number;
  log?: (message: string) => void;
};

type TagField = {
  tagValue?: string | null;
};

type GroupDoc = {
  _id?: ObjectId;
  groupId?: string;
  subject?: string;
  description?: string;
  memberCount?: number;
  lastActivityTimestamp?: number;
  lastTaggedTimestamp?: number;
  joinedAt?: number;
  creationTimestamp?: number;
  tags?: {
    politicalLeaning?: TagField;
    demographic?: TagField & {
      age?: string | null;
      gender?: string | null;
    };
    topic?: TagField;
    region?: TagField;
    organization?: TagField & {
      organizationType?: string | null;
    };
    lifeEvent?: TagField;
    strategicMarkets?: TagField;
  };
};

type UserProfileDoc = {
  userId?: string;
  name?: string;
};

type MessageDoc = {
  _id?: ObjectId;
  timestamp?: number;
  messageId?: string;
  groupId?: string;
  authorId?: string;
  body?: string;
  forwardingScore?: number;
  messageReactions?: Array<{
    senderId?: string;
    reaction?: string;
    timestamp?: number;
  }>;
  messageReplies?: string[];
};

export type GroupSemanticDoc = {
  _id?: ObjectId;
  groupId: string;
  searchText: string;
  embedding: number[];
  embeddingModel: string;
  embeddingVersion: string;
  contentHash: string;
  indexedAt: Date;
  sourceUpdatedAt: Date;
  subject: string;
  description: string;
  topic: string;
  region: string;
  demographic: string;
  politicalLeaning: string;
  organization: string;
  lifeEvent: string;
  strategicMarkets: string;
  memberCount: number;
  lastActivityTimestamp: number;
  lastTaggedTimestamp: number;
};

export type MessageSemanticDoc = {
  _id?: ObjectId;
  messageId: string;
  groupId: string;
  authorId: string;
  searchText: string;
  embedding: number[];
  embeddingModel: string;
  embeddingVersion: string;
  contentHash: string;
  indexedAt: Date;
  sourceUpdatedAt: Date;
  timestamp: number;
  reactionCount: number;
  replyCount: number;
  forwardingScore: number;
  bodyPreview: string;
  groupSubject: string;
  groupTopic: string;
  groupRegion: string;
};

type SyncCollections = {
  groups: Collection<GroupDoc>;
  users: Collection<UserProfileDoc>;
  messages: Collection<MessageDoc>;
  groupSemantic: Collection<GroupSemanticDoc>;
  messageSemantic: Collection<MessageSemanticDoc>;
};

type GroupSemanticSourceRecord = {
  groupId: string;
  searchText: string;
  contentHash: string;
  sourceUpdatedAtMs: number;
  subject: string;
  description: string;
  topic: string;
  region: string;
  demographic: string;
  politicalLeaning: string;
  organization: string;
  lifeEvent: string;
  strategicMarkets: string;
  memberCount: number;
  lastActivityTimestamp: number;
  lastTaggedTimestamp: number;
};

type MessageSemanticSourceRecord = {
  messageId: string;
  groupId: string;
  authorId: string;
  searchText: string;
  contentHash: string;
  sourceUpdatedAtMs: number;
  timestamp: number;
  reactionCount: number;
  replyCount: number;
  forwardingScore: number;
  bodyPreview: string;
  groupSubject: string;
  groupTopic: string;
  groupRegion: string;
};

type ExistingGroupSemanticState = {
  groupId: string;
  contentHash?: string;
  embeddingVersion?: string;
};

type ExistingMessageSemanticState = {
  messageId: string;
  contentHash?: string;
  embeddingVersion?: string;
};

export type GroupAudienceEvidence = {
  groupId: string;
  subject: string;
  memberCount: number;
  lastActivityTimestamp: number;
  topic: string;
  region: string;
  demographic: string;
  politicalLeaning: string;
  score: number;
  activity30d: number;
  sampleMessages: string[];
};

export type InfluencerAggregate = {
  authorId: string;
  rawScore: number;
  messageCount: number;
  reactionCount: number;
  replyCount: number;
  groupCount: number;
  displayName: string;
};

export type SemanticSyncResult = {
  mode: SemanticSyncMode;
  embeddingModel: string;
  embeddingVersion: string;
  groupStats: {
    stale: number;
    processed: number;
    upserted: number;
  };
  messageStats: {
    stale: number;
    processed: number;
    upserted: number;
  };
  startedAt: string;
  finishedAt: string;
};

export class SemanticConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SemanticConfigError";
  }
}

export class SemanticSearchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SemanticSearchError";
  }
}

export function resolveSemanticSyncLimit(
  mode: SemanticSyncMode,
  incrementalLimit: number,
  limit?: number | null,
): number | null {
  if (limit == null) {
    return mode === "incremental" ? incrementalLimit : null;
  }

  if (!Number.isInteger(limit) || limit <= 0) {
    throw new SemanticConfigError(
      `Semantic sync limit must be a positive integer, received "${limit}".`,
    );
  }

  if (mode === "incremental") {
    throw new SemanticConfigError(
      "Semantic sync limit overrides are only supported for groups_backfill and messages_backfill.",
    );
  }

  return limit;
}

function normalizeWhitespace(value: string | null | undefined): string {
  return (value ?? "").replace(/\s+/g, " ").trim();
}

function truncateText(value: string, maxLength: number): string {
  return value.length > maxLength ? `${value.slice(0, maxLength - 1).trimEnd()}…` : value;
}

function lowerCase(value: string | null | undefined): string {
  return normalizeWhitespace(value).toLowerCase();
}

function toDisplayLabel(value: string | null | undefined): string {
  return normalizeWhitespace(value) || "unknown";
}

function joinSearchSections(sections: Array<string | null | undefined>): string {
  return sections.map((section) => normalizeWhitespace(section)).filter(Boolean).join("\n");
}

function hashContent(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function parsePositiveInteger(value: string | undefined, fallback: number): number {
  const normalized = value?.trim();
  if (!normalized) {
    return fallback;
  }
  const parsed = Number(normalized);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new SemanticConfigError(`Expected positive integer env value, received "${value}".`);
  }
  return parsed;
}

function inferEmbeddingDimensions(model: string): number {
  switch (model) {
    case "text-embedding-3-small":
      return 1536;
    case "text-embedding-3-large":
      return 3072;
    case "text-embedding-ada-002":
      return 1536;
    default: {
      const configured = process.env.OPENAI_EMBEDDING_DIMENSIONS?.trim();
      if (!configured) {
        throw new SemanticConfigError(
          `Unknown embedding dimensions for model "${model}". Set OPENAI_EMBEDDING_DIMENSIONS.`,
        );
      }
      const parsed = Number(configured);
      if (!Number.isInteger(parsed) || parsed <= 0) {
        throw new SemanticConfigError(
          `OPENAI_EMBEDDING_DIMENSIONS must be a positive integer, received "${configured}".`,
        );
      }
      return parsed;
    }
  }
}

function readSemanticConfig() {
  const embeddingModel = process.env.OPENAI_EMBEDDING_MODEL?.trim() || DEFAULT_EMBEDDING_MODEL;
  const embeddingVersion =
    process.env.SEMANTIC_EMBEDDING_VERSION?.trim() || DEFAULT_EMBEDDING_VERSION;

  return {
    embeddingModel,
    embeddingVersion,
    embeddingDimensions: inferEmbeddingDimensions(embeddingModel),
    embeddingBatchSize: parsePositiveInteger(
      process.env.SEMANTIC_EMBEDDING_BATCH_SIZE,
      DEFAULT_EMBEDDING_BATCH_SIZE,
    ),
    incrementalGroupLimit: parsePositiveInteger(
      process.env.SEMANTIC_INCREMENTAL_GROUP_LIMIT,
      DEFAULT_INCREMENTAL_GROUP_LIMIT,
    ),
    incrementalMessageLimit: parsePositiveInteger(
      process.env.SEMANTIC_INCREMENTAL_MESSAGE_LIMIT,
      DEFAULT_INCREMENTAL_MESSAGE_LIMIT,
    ),
    groupVectorIndexName:
      process.env.MONGO_GROUP_SEMANTIC_VECTOR_INDEX?.trim() || "group_semantic_vector_index",
    messageVectorIndexName:
      process.env.MONGO_MESSAGE_SEMANTIC_VECTOR_INDEX?.trim() || "message_semantic_vector_index",
  };
}

function requireOpenAIKey(): string {
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) {
    throw new SemanticConfigError("Missing OPENAI_API_KEY for semantic indexing and synthesis.");
  }
  return apiKey;
}

let openAiClient: OpenAI | null = null;

function getOpenAIClient(): OpenAI {
  if (!openAiClient) {
    openAiClient = new OpenAI({ apiKey: requireOpenAIKey() });
  }
  return openAiClient;
}

async function getCollections(): Promise<SyncCollections> {
  const db = await getMongoDb();
  const names = getMongoCollectionNames();

  return {
    groups: db.collection<GroupDoc>(names.groupsCollection),
    users: db.collection<UserProfileDoc>(names.usersCollection),
    messages: db.collection<MessageDoc>(names.messagesCollection),
    groupSemantic: db.collection<GroupSemanticDoc>(names.groupSemanticCollection),
    messageSemantic: db.collection<MessageSemanticDoc>(names.messageSemanticCollection),
  };
}

function readGroupSourceUpdatedAt(group: GroupDoc): number {
  return Math.max(
    typeof group.lastActivityTimestamp === "number" ? group.lastActivityTimestamp : 0,
    typeof group.lastTaggedTimestamp === "number" ? group.lastTaggedTimestamp : 0,
    typeof group.joinedAt === "number" ? group.joinedAt : 0,
    typeof group.creationTimestamp === "number" ? group.creationTimestamp : 0,
  );
}

export function buildGroupSearchText(group: GroupDoc): string {
  return joinSearchSections([
    `Subject: ${normalizeWhitespace(group.subject) || "unknown"}`,
    `Description: ${normalizeWhitespace(group.description) || "unknown"}`,
    `Topic: ${toDisplayLabel(group.tags?.topic?.tagValue)}`,
    `Region: ${toDisplayLabel(group.tags?.region?.tagValue)}`,
    `Demographic: ${toDisplayLabel(group.tags?.demographic?.tagValue)}`,
    `Political leaning: ${toDisplayLabel(group.tags?.politicalLeaning?.tagValue)}`,
    `Organization: ${toDisplayLabel(group.tags?.organization?.tagValue)}`,
    `Life event: ${toDisplayLabel(group.tags?.lifeEvent?.tagValue)}`,
    `Strategic markets: ${toDisplayLabel(group.tags?.strategicMarkets?.tagValue)}`,
  ]);
}

export function buildMessageSearchText(
  message: MessageDoc,
  groupContext?: { subject?: string; topic?: string; region?: string },
): string {
  return joinSearchSections([
    `Message: ${normalizeWhitespace(message.body) || "unknown"}`,
    `Group subject: ${normalizeWhitespace(groupContext?.subject) || "unknown"}`,
    `Group topic: ${normalizeWhitespace(groupContext?.topic) || "unknown"}`,
    `Group region: ${normalizeWhitespace(groupContext?.region) || "unknown"}`,
  ]);
}

export function buildGroupSemanticSourceRecord(group: GroupDoc): GroupSemanticSourceRecord | null {
  const groupId = normalizeWhitespace(group.groupId);
  if (!groupId) {
    return null;
  }

  const subject = normalizeWhitespace(group.subject) || "(No subject)";
  const description = normalizeWhitespace(group.description);
  const topic = toDisplayLabel(group.tags?.topic?.tagValue);
  const region = toDisplayLabel(group.tags?.region?.tagValue);
  const demographic = toDisplayLabel(group.tags?.demographic?.tagValue);
  const politicalLeaning = toDisplayLabel(group.tags?.politicalLeaning?.tagValue);
  const organization = toDisplayLabel(group.tags?.organization?.tagValue);
  const lifeEvent = toDisplayLabel(group.tags?.lifeEvent?.tagValue);
  const strategicMarkets = toDisplayLabel(group.tags?.strategicMarkets?.tagValue);
  const memberCount = typeof group.memberCount === "number" ? Math.max(0, group.memberCount) : 0;
  const lastActivityTimestamp =
    typeof group.lastActivityTimestamp === "number" ? group.lastActivityTimestamp : 0;
  const lastTaggedTimestamp =
    typeof group.lastTaggedTimestamp === "number" ? group.lastTaggedTimestamp : 0;

  const searchText = buildGroupSearchText(group);
  const contentHash = hashContent({
    searchText,
    subject,
    description,
    topic,
    region,
    demographic,
    politicalLeaning,
    organization,
    lifeEvent,
    strategicMarkets,
    memberCount,
    lastActivityTimestamp,
    lastTaggedTimestamp,
  });

  return {
    groupId,
    searchText,
    contentHash,
    sourceUpdatedAtMs: readGroupSourceUpdatedAt(group),
    subject,
    description,
    topic,
    region,
    demographic,
    politicalLeaning,
    organization,
    lifeEvent,
    strategicMarkets,
    memberCount,
    lastActivityTimestamp,
    lastTaggedTimestamp,
  };
}

export function buildMessageSemanticSourceRecord(
  message: MessageDoc,
  groupContext?: { subject?: string; topic?: string; region?: string },
): MessageSemanticSourceRecord | null {
  const messageId = normalizeWhitespace(message.messageId);
  const groupId = normalizeWhitespace(message.groupId);
  const authorId = normalizeWhitespace(message.authorId);
  const body = normalizeWhitespace(message.body);
  const timestamp = typeof message.timestamp === "number" ? message.timestamp : 0;

  if (!messageId || !groupId || !authorId || !body) {
    return null;
  }

  const groupSubject = normalizeWhitespace(groupContext?.subject) || "unknown";
  const groupTopic = normalizeWhitespace(groupContext?.topic) || "unknown";
  const groupRegion = normalizeWhitespace(groupContext?.region) || "unknown";
  const reactionCount = Array.isArray(message.messageReactions) ? message.messageReactions.length : 0;
  const replyCount = Array.isArray(message.messageReplies) ? message.messageReplies.length : 0;
  const forwardingScore =
    typeof message.forwardingScore === "number" ? Math.max(0, message.forwardingScore) : 0;
  const searchText = buildMessageSearchText(message, groupContext);
  const bodyPreview = truncateText(body, 220);
  const contentHash = hashContent({
    searchText,
    messageId,
    groupId,
    authorId,
    timestamp,
    reactionCount,
    replyCount,
    forwardingScore,
  });

  return {
    messageId,
    groupId,
    authorId,
    searchText,
    contentHash,
    sourceUpdatedAtMs: timestamp,
    timestamp,
    reactionCount,
    replyCount,
    forwardingScore,
    bodyPreview,
    groupSubject,
    groupTopic,
    groupRegion,
  };
}

export function isSemanticDocumentStale(
  existing:
    | ExistingGroupSemanticState
    | ExistingMessageSemanticState
    | null
    | undefined,
  next: { contentHash: string },
  embeddingVersion: string,
): boolean {
  if (!existing) {
    return true;
  }

  return (
    normalizeWhitespace(existing.contentHash) !== next.contentHash ||
    normalizeWhitespace(existing.embeddingVersion) !== embeddingVersion
  );
}

export function mergeAudienceEvidence(
  groups: GroupAudienceEvidence[],
  groupMessageEvidence: Map<string, MessageSemanticDoc[]>,
  referenceNowMs = Date.now(),
): GroupAudienceEvidence[] {
  return groups
    .map((group) => {
      const evidence = groupMessageEvidence.get(group.groupId) ?? [];
      const activity30d = evidence.filter((item) => {
        const ageMs = referenceNowMs - item.timestamp;
        return ageMs <= 30 * 24 * 60 * 60 * 1000;
      }).length;
      const sampleMessages = evidence
        .slice(0, 2)
        .map((item) => truncateText(item.bodyPreview, 160));

      return {
        ...group,
        activity30d,
        sampleMessages,
      };
    })
    .sort((left, right) => right.score - left.score || right.memberCount - left.memberCount);
}

export function rankInfluencerAggregates(aggregates: InfluencerAggregate[]): InfluencerAggregate[] {
  return [...aggregates].sort(
    (left, right) =>
      right.rawScore - left.rawScore ||
      right.groupCount - left.groupCount ||
      right.reactionCount - left.reactionCount,
  );
}

function chunkArray<T>(values: T[], chunkSize: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }
  return chunks;
}

function logSemanticSync(options: SemanticSyncOptions, message: string): void {
  options.log?.(message);
}

async function embedTexts(texts: string[], config = readSemanticConfig()): Promise<number[][]> {
  if (texts.length === 0) {
    return [];
  }

  const client = getOpenAIClient();
  const batches = chunkArray(texts, config.embeddingBatchSize);
  const embeddings: number[][] = [];

  for (const batch of batches) {
    const response = await client.embeddings.create({
      model: config.embeddingModel,
      input: batch,
    });

    embeddings.push(...response.data.map((item) => item.embedding));
  }

  return embeddings;
}

async function ensureCollectionIndexes(
  collections: SyncCollections,
  config = readSemanticConfig(),
): Promise<void> {
  await collections.groupSemantic.createIndexes([
    { key: { groupId: 1 }, name: "groupId_unique", unique: true },
    { key: { contentHash: 1, embeddingVersion: 1 }, name: "contentHash_embeddingVersion" },
    { key: { sourceUpdatedAt: -1 }, name: "sourceUpdatedAt_desc" },
  ]);
  await collections.messageSemantic.createIndexes([
    { key: { messageId: 1 }, name: "messageId_unique", unique: true },
    { key: { groupId: 1, timestamp: -1 }, name: "groupId_timestamp_desc" },
    { key: { authorId: 1, timestamp: -1 }, name: "authorId_timestamp_desc" },
    { key: { contentHash: 1, embeddingVersion: 1 }, name: "contentHash_embeddingVersion" },
    { key: { sourceUpdatedAt: -1 }, name: "sourceUpdatedAt_desc" },
  ]);

  await ensureVectorSearchIndex(
    collections.groupSemantic.collectionName,
    config.groupVectorIndexName,
    config.embeddingDimensions,
    ["groupId", "topic", "region", "politicalLeaning"],
  );
  await ensureVectorSearchIndex(
    collections.messageSemantic.collectionName,
    config.messageVectorIndexName,
    config.embeddingDimensions,
    ["groupId", "authorId"],
  );
}

async function ensureVectorSearchIndex(
  collectionName: string,
  indexName: string,
  numDimensions: number,
  filterPaths: string[],
): Promise<void> {
  const db = await getMongoDb();

  try {
    await db.command({
      createSearchIndexes: collectionName,
      indexes: [
        {
          name: indexName,
          type: "vectorSearch",
          definition: {
            fields: [
              {
                type: "vector",
                path: "embedding",
                numDimensions,
                similarity: "cosine",
              },
              ...filterPaths.map((path) => ({
                type: "filter",
                path,
              })),
            ],
          },
        },
      ],
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const normalized = message.toLowerCase();
    if (
      normalized.includes("already exists") ||
      normalized.includes("duplicate") ||
      normalized.includes("index with the same name already exists")
    ) {
      return;
    }

    throw new SemanticSearchError(
      `Failed to create Atlas vector index "${indexName}" on "${collectionName}". ${message}`,
    );
  }
}

async function fetchExistingGroupSemanticState(
  collection: Collection<GroupSemanticDoc>,
  groupIds: string[],
): Promise<Map<string, ExistingGroupSemanticState>> {
  if (groupIds.length === 0) {
    return new Map();
  }

  const docs = await collection
    .find(
      { groupId: { $in: groupIds } },
      { projection: { groupId: 1, contentHash: 1, embeddingVersion: 1 } },
    )
    .toArray();

  return new Map(
    docs.map((doc) => [
      doc.groupId,
      {
        groupId: doc.groupId,
        contentHash: doc.contentHash,
        embeddingVersion: doc.embeddingVersion,
      },
    ]),
  );
}

async function fetchExistingMessageSemanticState(
  collection: Collection<MessageSemanticDoc>,
  messageIds: string[],
): Promise<Map<string, ExistingMessageSemanticState>> {
  if (messageIds.length === 0) {
    return new Map();
  }

  const docs = await collection
    .find(
      { messageId: { $in: messageIds } },
      { projection: { messageId: 1, contentHash: 1, embeddingVersion: 1 } },
    )
    .toArray();

  return new Map(
    docs.map((doc) => [
      doc.messageId,
      {
        messageId: doc.messageId,
        contentHash: doc.contentHash,
        embeddingVersion: doc.embeddingVersion,
      },
    ]),
  );
}

function buildGroupSemanticUpsertDocument(
  source: GroupSemanticSourceRecord,
  embedding: number[],
  config = readSemanticConfig(),
): GroupSemanticDoc {
  return {
    groupId: source.groupId,
    searchText: source.searchText,
    embedding,
    embeddingModel: config.embeddingModel,
    embeddingVersion: config.embeddingVersion,
    contentHash: source.contentHash,
    indexedAt: new Date(),
    sourceUpdatedAt: new Date(source.sourceUpdatedAtMs || Date.now()),
    subject: source.subject,
    description: source.description,
    topic: source.topic,
    region: source.region,
    demographic: source.demographic,
    politicalLeaning: source.politicalLeaning,
    organization: source.organization,
    lifeEvent: source.lifeEvent,
    strategicMarkets: source.strategicMarkets,
    memberCount: source.memberCount,
    lastActivityTimestamp: source.lastActivityTimestamp,
    lastTaggedTimestamp: source.lastTaggedTimestamp,
  };
}

function buildMessageSemanticUpsertDocument(
  source: MessageSemanticSourceRecord,
  embedding: number[],
  config = readSemanticConfig(),
): MessageSemanticDoc {
  return {
    messageId: source.messageId,
    groupId: source.groupId,
    authorId: source.authorId,
    searchText: source.searchText,
    embedding,
    embeddingModel: config.embeddingModel,
    embeddingVersion: config.embeddingVersion,
    contentHash: source.contentHash,
    indexedAt: new Date(),
    sourceUpdatedAt: new Date(source.sourceUpdatedAtMs || Date.now()),
    timestamp: source.timestamp,
    reactionCount: source.reactionCount,
    replyCount: source.replyCount,
    forwardingScore: source.forwardingScore,
    bodyPreview: source.bodyPreview,
    groupSubject: source.groupSubject,
    groupTopic: source.groupTopic,
    groupRegion: source.groupRegion,
  };
}

async function syncGroups(
  collections: SyncCollections,
  mode: SemanticSyncMode,
  options: SemanticSyncOptions = {},
  config = readSemanticConfig(),
): Promise<{ stale: number; processed: number; upserted: number }> {
  const cursor = collections.groups.find(
    {},
    {
      projection: {
        groupId: 1,
        subject: 1,
        description: 1,
        memberCount: 1,
        lastActivityTimestamp: 1,
        lastTaggedTimestamp: 1,
        joinedAt: 1,
        creationTimestamp: 1,
        tags: 1,
      },
    },
  );

  const limit = resolveSemanticSyncLimit(mode, config.incrementalGroupLimit, options.limit);
  if (mode === "incremental" || limit !== null) {
    cursor.sort({ lastActivityTimestamp: -1, lastTaggedTimestamp: -1 });
  }
  if (limit !== null) {
    cursor.limit(limit);
  }

  const groups = await cursor.toArray();
  const sources = groups
    .map((group) => buildGroupSemanticSourceRecord(group))
    .filter((group): group is GroupSemanticSourceRecord => Boolean(group));
  const existingByGroupId = await fetchExistingGroupSemanticState(
    collections.groupSemantic,
    sources.map((item) => item.groupId),
  );
  const staleSources = sources.filter((source) =>
    isSemanticDocumentStale(
      existingByGroupId.get(source.groupId),
      { contentHash: source.contentHash },
      config.embeddingVersion,
    ),
  );

  if (staleSources.length === 0) {
    logSemanticSync(options, "[groups] no stale groups to sync.");
    return {
      stale: 0,
      processed: 0,
      upserted: 0,
    };
  }

  const staleChunks = chunkArray(staleSources, config.embeddingBatchSize);
  let processed = 0;
  let upserted = 0;

  logSemanticSync(
    options,
    `[groups] syncing ${staleSources.length} stale groups in ${staleChunks.length} chunk(s) of up to ${config.embeddingBatchSize}.`,
  );

  for (const [chunkIndex, chunk] of staleChunks.entries()) {
    const embeddings = await embedTexts(
      chunk.map((source) => source.searchText),
      config,
    );
    const operations = chunk.map((source, index) => ({
      updateOne: {
        filter: { groupId: source.groupId },
        update: {
          $set: buildGroupSemanticUpsertDocument(source, embeddings[index], config),
        },
        upsert: true,
      },
    }));

    const result = await collections.groupSemantic.bulkWrite(operations, { ordered: false });
    processed += chunk.length;
    upserted += result.upsertedCount + result.modifiedCount;

    logSemanticSync(
      options,
      `[groups] chunk ${chunkIndex + 1}/${staleChunks.length}: processed ${processed}/${staleSources.length}, upserted ${upserted}.`,
    );
  }

  return {
    stale: staleSources.length,
    processed,
    upserted,
  };
}

async function loadGroupContextMap(collections: SyncCollections): Promise<
  Map<
    string,
    {
      subject: string;
      topic: string;
      region: string;
    }
  >
> {
  const groups = await collections.groups
    .find({}, { projection: { groupId: 1, subject: 1, tags: 1 } })
    .toArray();

  return new Map(
    groups
      .map((group) => {
        const groupId = normalizeWhitespace(group.groupId);
        if (!groupId) {
          return null;
        }

        return [
          groupId,
          {
            subject: normalizeWhitespace(group.subject) || "unknown",
            topic: toDisplayLabel(group.tags?.topic?.tagValue),
            region: toDisplayLabel(group.tags?.region?.tagValue),
          },
        ] as const;
      })
      .filter(
        (
          entry,
        ): entry is readonly [
          string,
          {
            subject: string;
            topic: string;
            region: string;
          },
        ] => Boolean(entry),
      ),
  );
}

async function syncMessages(
  collections: SyncCollections,
  mode: SemanticSyncMode,
  options: SemanticSyncOptions = {},
  config = readSemanticConfig(),
): Promise<{ stale: number; processed: number; upserted: number }> {
  const groupContextById = await loadGroupContextMap(collections);
  const cursor = collections.messages.find(
    { body: { $type: "string" } } as Filter<MessageDoc>,
    {
      projection: {
        messageId: 1,
        groupId: 1,
        authorId: 1,
        body: 1,
        timestamp: 1,
        forwardingScore: 1,
        messageReactions: 1,
        messageReplies: 1,
      },
    },
  );

  const limit = resolveSemanticSyncLimit(mode, config.incrementalMessageLimit, options.limit);
  if (mode === "incremental" || limit !== null) {
    cursor.sort({ timestamp: -1 });
  }
  if (limit !== null) {
    cursor.limit(limit);
  }

  const messages = await cursor.toArray();
  const sources = messages
    .map((message) =>
      buildMessageSemanticSourceRecord(
        message,
        message.groupId ? groupContextById.get(message.groupId) : undefined,
      ),
    )
    .filter((message): message is MessageSemanticSourceRecord => Boolean(message));
  const existingByMessageId = await fetchExistingMessageSemanticState(
    collections.messageSemantic,
    sources.map((item) => item.messageId),
  );
  const staleSources = sources.filter((source) =>
    isSemanticDocumentStale(
      existingByMessageId.get(source.messageId),
      { contentHash: source.contentHash },
      config.embeddingVersion,
    ),
  );

  if (staleSources.length === 0) {
    logSemanticSync(options, "[messages] no stale messages to sync.");
    return {
      stale: 0,
      processed: 0,
      upserted: 0,
    };
  }

  const staleChunks = chunkArray(staleSources, config.embeddingBatchSize);
  let processed = 0;
  let upserted = 0;

  logSemanticSync(
    options,
    `[messages] syncing ${staleSources.length} stale messages in ${staleChunks.length} chunk(s) of up to ${config.embeddingBatchSize}.`,
  );

  for (const [chunkIndex, chunk] of staleChunks.entries()) {
    const embeddings = await embedTexts(
      chunk.map((source) => source.searchText),
      config,
    );
    const operations = chunk.map((source, index) => ({
      updateOne: {
        filter: { messageId: source.messageId },
        update: {
          $set: buildMessageSemanticUpsertDocument(source, embeddings[index], config),
        },
        upsert: true,
      },
    }));

    const result = await collections.messageSemantic.bulkWrite(operations, { ordered: false });
    processed += chunk.length;
    upserted += result.upsertedCount + result.modifiedCount;

    logSemanticSync(
      options,
      `[messages] chunk ${chunkIndex + 1}/${staleChunks.length}: processed ${processed}/${staleSources.length}, upserted ${upserted}.`,
    );
  }

  return {
    stale: staleSources.length,
    processed,
    upserted,
  };
}

export async function runSemanticSync(
  mode: SemanticSyncMode,
  options: SemanticSyncOptions = {},
): Promise<SemanticSyncResult> {
  const config = readSemanticConfig();
  requireOpenAIKey();
  const collections = await getCollections();
  await ensureCollectionIndexes(collections, config);

  const startedAt = new Date();
  let groupStats = { stale: 0, processed: 0, upserted: 0 };
  let messageStats = { stale: 0, processed: 0, upserted: 0 };

  if (mode === "groups_backfill" || mode === "incremental") {
    groupStats = await syncGroups(collections, mode, options, config);
  }

  if (mode === "messages_backfill" || mode === "incremental") {
    messageStats = await syncMessages(collections, mode, options, config);
  }

  const finishedAt = new Date();

  return {
    mode,
    embeddingModel: config.embeddingModel,
    embeddingVersion: config.embeddingVersion,
    groupStats,
    messageStats,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
  };
}

async function embedQuery(query: string, config = readSemanticConfig()): Promise<number[]> {
  const normalizedQuery = normalizeWhitespace(query);
  if (!normalizedQuery) {
    throw new SemanticSearchError("Semantic search query cannot be empty.");
  }

  const [embedding] = await embedTexts([normalizedQuery], config);
  return embedding;
}

function semanticSearchFailure(error: unknown, collectionName: string): never {
  const message = error instanceof Error ? error.message : String(error);
  throw new SemanticSearchError(
    `Atlas vector search failed for "${collectionName}". Confirm Atlas vector indexes exist and the deployment supports $vectorSearch. ${message}`,
  );
}

async function vectorSearchGroupSemantic(
  collection: Collection<GroupSemanticDoc>,
  query: string,
  limit = DEFAULT_GROUP_SEARCH_LIMIT,
  config = readSemanticConfig(),
): Promise<Array<GroupSemanticDoc & { score: number }>> {
  const queryVector = await embedQuery(query, config);

  try {
    return await collection
      .aggregate<GroupSemanticDoc & { score: number }>([
        {
          $vectorSearch: {
            index: config.groupVectorIndexName,
            path: "embedding",
            queryVector,
            numCandidates: Math.max(limit * DEFAULT_QUERY_CANDIDATE_MULTIPLIER, 100),
            limit,
          },
        },
        {
          $project: {
            groupId: 1,
            searchText: 1,
            embeddingModel: 1,
            embeddingVersion: 1,
            contentHash: 1,
            indexedAt: 1,
            sourceUpdatedAt: 1,
            subject: 1,
            description: 1,
            topic: 1,
            region: 1,
            demographic: 1,
            politicalLeaning: 1,
            organization: 1,
            lifeEvent: 1,
            strategicMarkets: 1,
            memberCount: 1,
            lastActivityTimestamp: 1,
            lastTaggedTimestamp: 1,
            score: { $meta: "vectorSearchScore" },
          },
        },
      ])
      .toArray();
  } catch (error) {
    semanticSearchFailure(error, collection.collectionName);
  }
}

async function vectorSearchMessageSemantic(
  collection: Collection<MessageSemanticDoc>,
  query: string,
  limit = DEFAULT_MESSAGE_SEARCH_LIMIT,
  filter?: Document,
  config = readSemanticConfig(),
): Promise<Array<MessageSemanticDoc & { score: number }>> {
  const queryVector = await embedQuery(query, config);

  try {
    return await collection
      .aggregate<MessageSemanticDoc & { score: number }>([
        {
          $vectorSearch: {
            index: config.messageVectorIndexName,
            path: "embedding",
            queryVector,
            numCandidates: Math.max(limit * DEFAULT_QUERY_CANDIDATE_MULTIPLIER, 120),
            limit,
            ...(filter ? { filter } : {}),
          },
        },
        {
          $project: {
            messageId: 1,
            groupId: 1,
            authorId: 1,
            searchText: 1,
            embeddingModel: 1,
            embeddingVersion: 1,
            contentHash: 1,
            indexedAt: 1,
            sourceUpdatedAt: 1,
            timestamp: 1,
            reactionCount: 1,
            replyCount: 1,
            forwardingScore: 1,
            bodyPreview: 1,
            groupSubject: 1,
            groupTopic: 1,
            groupRegion: 1,
            score: { $meta: "vectorSearchScore" },
          },
        },
      ])
      .toArray();
  } catch (error) {
    semanticSearchFailure(error, collection.collectionName);
  }
}

export async function searchGroupsBySemanticQuery(
  query: string,
  limit = DEFAULT_GROUP_SEARCH_LIMIT,
): Promise<Array<GroupSemanticDoc & { score: number }>> {
  const collections = await getCollections();
  return vectorSearchGroupSemantic(collections.groupSemantic, query, limit);
}

export async function searchMessagesBySemanticQuery(input: {
  query: string;
  limit?: number;
  groupIds?: string[] | null;
  authorIds?: string[] | null;
}): Promise<Array<MessageSemanticDoc & { score: number }>> {
  const collections = await getCollections();
  const normalizedGroupIds = (input.groupIds ?? [])
    .map((groupId) => normalizeWhitespace(groupId))
    .filter(Boolean);
  const normalizedAuthorIds = (input.authorIds ?? [])
    .map((authorId) => normalizeWhitespace(authorId))
    .filter(Boolean);
  const filter: Document = {};

  if (normalizedGroupIds.length > 0) {
    filter.groupId = { $in: normalizedGroupIds };
  }

  if (normalizedAuthorIds.length > 0) {
    filter.authorId = { $in: normalizedAuthorIds };
  }

  return vectorSearchMessageSemantic(
    collections.messageSemantic,
    input.query,
    input.limit ?? DEFAULT_MESSAGE_SEARCH_LIMIT,
    Object.keys(filter).length > 0 ? filter : undefined,
  );
}

async function fetchGroupActivityCounts(
  messages: Collection<MessageDoc>,
  groupIds: string[],
): Promise<Map<string, number>> {
  if (groupIds.length === 0) {
    return new Map();
  }

  const cutoff = Date.now() - 30 * 24 * 60 * 60 * 1000;
  const rows = await messages
    .aggregate<{ _id: string; count: number }>([
      {
        $match: {
          groupId: { $in: groupIds },
          timestamp: { $gte: cutoff },
        },
      },
      {
        $group: {
          _id: "$groupId",
          count: { $sum: 1 },
        },
      },
    ])
    .toArray();

  return new Map(rows.map((row) => [row._id, row.count]));
}

async function synthesizeStructuredOutput<T>(
  prompt: string,
  agent: Agent<any, any>,
  schema: z.ZodSchema<T>,
): Promise<T> {
  const result = await run(agent, prompt);
  return schema.parse(result.finalOutput);
}

const audienceLookupSynthesisAgent = new Agent({
  name: "AudienceLookupSynthesizer",
  model: process.env.OPENAI_MODEL ?? "gpt-4.1",
  instructions: [
    "You synthesize semantic audience search results into strict JSON.",
    "Ground every statement in the provided evidence only.",
    "Use concise concrete language.",
    "Populate segments with 2-5 actionable audience cuts.",
    "Populate evidence reasons using the matched metadata and supporting messages.",
  ].join(" "),
  outputType: audienceLookupOutputSchema,
});

const influencerLookupSynthesisAgent = new Agent({
  name: "InfluencerLookupSynthesizer",
  model: process.env.OPENAI_MODEL ?? "gpt-4.1",
  instructions: [
    "You synthesize semantic influencer evidence into strict JSON.",
    "Ground every statement in the provided evidence only.",
    "Each influencer rationale should cite why the user ranks highly in the matched slice.",
    "Do not mention unprovided metrics.",
  ].join(" "),
  outputType: influencerLookupOutputSchema,
});

function buildAudienceLookupPrompt(
  query: string,
  evidence: GroupAudienceEvidence[],
): string {
  return [
    `Query: ${query}`,
    "",
    "Matched audience evidence JSON:",
    JSON.stringify(evidence, null, 2),
    "",
    "Return strict JSON only.",
  ].join("\n");
}

function buildInfluencerLookupPrompt(
  query: string,
  narrative: string | null | undefined,
  influencers: InfluencerAggregate[],
): string {
  return [
    `Query: ${query}`,
    `Narrative: ${normalizeWhitespace(narrative) || "general"}`,
    "",
    "Influencer evidence JSON:",
    JSON.stringify(influencers, null, 2),
    "",
    "Return strict JSON only.",
  ].join("\n");
}

export async function lookupAudienceSegmentsSemantic(
  question: string,
): Promise<Record<string, unknown>> {
  requireOpenAIKey();
  const collections = await getCollections();
  const matchedGroups = await vectorSearchGroupSemantic(collections.groupSemantic, question);
  await logToolDebug("Semantic group search completed.", {
    matchedGroups: matchedGroups.length,
    topGroups: matchedGroups.slice(0, 3).map((group) => ({
      groupId: group.groupId,
      subject: group.subject,
      score: group.score,
    })),
  });
  if (matchedGroups.length === 0) {
    await logToolDebug("No groups matched the semantic audience query.");
    return {
      source: "semantic-audience-lookup",
      query: question,
      audienceSummary: "No semantically matched groups were found for this audience request.",
      matchedGroups: 0,
      segments: ["No matching audience segments found in the indexed semantic corpus."],
      sampleGroups: [],
      evidence: [],
    };
  }

  const groupIds = matchedGroups.map((group) => group.groupId);
  const [messageEvidence, activityByGroupId] = await Promise.all([
    vectorSearchMessageSemantic(
      collections.messageSemantic,
      question,
      DEFAULT_GROUP_EVIDENCE_LIMIT,
      { groupId: { $in: groupIds } },
    ),
    fetchGroupActivityCounts(collections.messages, groupIds),
  ]);
  await logToolDebug("Fetched supporting message and activity evidence.", {
    evidenceMessages: messageEvidence.length,
    activityGroups: activityByGroupId.size,
  });

  const messagesByGroupId = new Map<string, MessageSemanticDoc[]>();
  for (const message of messageEvidence) {
    const current = messagesByGroupId.get(message.groupId) ?? [];
    current.push(message);
    messagesByGroupId.set(message.groupId, current);
  }

  const mergedEvidence = mergeAudienceEvidence(
    matchedGroups.map((group) => ({
      groupId: group.groupId,
      subject: group.subject,
      memberCount: group.memberCount,
      lastActivityTimestamp: group.lastActivityTimestamp,
      topic: group.topic,
      region: group.region,
      demographic: group.demographic,
      politicalLeaning: group.politicalLeaning,
      score: group.score,
      activity30d: activityByGroupId.get(group.groupId) ?? 0,
      sampleMessages: [],
    })),
    messagesByGroupId,
  ).map((group) => ({
    ...group,
    activity30d: activityByGroupId.get(group.groupId) ?? group.activity30d,
  }));
  await logToolDebug("Merged audience evidence for synthesis.", {
    mergedGroups: mergedEvidence.length,
    preview: mergedEvidence.slice(0, 3).map((group) => ({
      groupId: group.groupId,
      subject: group.subject,
      activity30d: group.activity30d,
      sampleMessages: group.sampleMessages.length,
    })),
  });

  const synthesized = await synthesizeStructuredOutput(
    buildAudienceLookupPrompt(question, mergedEvidence.slice(0, 5)),
    audienceLookupSynthesisAgent,
    audienceLookupOutputSchema,
  );
  await logToolDebug("Audience synthesis completed.", {
    matchedGroups: synthesized.matchedGroups,
    segments: synthesized.segments.length,
    evidence: synthesized.evidence.length,
  });

  return synthesized;
}

export async function lookupInfluencersSemantic(input: {
  question?: string | null;
  narrative?: string | null;
}): Promise<Record<string, unknown>> {
  requireOpenAIKey();
  const query = joinSearchSections([
    normalizeWhitespace(input.question),
    input.narrative ? `Narrative focus: ${normalizeWhitespace(input.narrative)}` : "",
  ]);
  const narrative = normalizeWhitespace(input.narrative) || "general";
  if (!query) {
    throw new SemanticSearchError(
      "influencer_lookup requires a question or narrative so the semantic slice can be defined.",
    );
  }

  const collections = await getCollections();
  const matchedMessages = await vectorSearchMessageSemantic(
    collections.messageSemantic,
    query,
    DEFAULT_MESSAGE_SEARCH_LIMIT,
  );
  await logToolDebug("Semantic message search completed.", {
    matchedMessages: matchedMessages.length,
    narrative,
  });
  if (matchedMessages.length === 0) {
    await logToolDebug("No messages matched the influencer query.");
    return {
      source: "semantic-influencer-lookup",
      narrative,
      query,
      summary: "No semantically matched messages were found for this influencer lookup.",
      influencers: [],
    };
  }

  const authorStats = new Map<
    string,
    {
      rawScore: number;
      messageCount: number;
      reactionCount: number;
      replyCount: number;
      groupIds: Set<string>;
    }
  >();

  for (const message of matchedMessages) {
    const current =
      authorStats.get(message.authorId) ??
      {
        rawScore: 0,
        messageCount: 0,
        reactionCount: 0,
        replyCount: 0,
        groupIds: new Set<string>(),
      };

    current.messageCount += 1;
    current.reactionCount += message.reactionCount;
    current.replyCount += message.replyCount;
    current.groupIds.add(message.groupId);
    current.rawScore +=
      message.score * 4 +
      current.messageCount +
      message.reactionCount * 1.5 +
      message.replyCount * 2 +
      message.forwardingScore * 0.5;

    authorStats.set(message.authorId, current);
  }
  await logToolDebug("Aggregated matched messages by author.", {
    candidateAuthors: authorStats.size,
  });

  const topUserIds = [...authorStats.keys()];
  const userDocs =
    topUserIds.length > 0
      ? await collections.users
          .find({ userId: { $in: topUserIds } }, { projection: { userId: 1, name: 1 } })
          .toArray()
      : [];
  const nameByUserId = new Map(
    userDocs
      .map((user) => {
        const userId = normalizeWhitespace(user.userId);
        if (!userId) {
          return null;
        }

        return [userId, normalizeWhitespace(user.name) || userId] as const;
      })
      .filter((entry): entry is readonly [string, string] => Boolean(entry)),
  );

  const ranked = rankInfluencerAggregates(
    [...authorStats.entries()].map(([authorId, value]) => ({
      authorId,
      rawScore: Number(value.rawScore.toFixed(3)),
      messageCount: value.messageCount,
      reactionCount: value.reactionCount,
      replyCount: value.replyCount,
      groupCount: value.groupIds.size,
      displayName: nameByUserId.get(authorId) || authorId,
    })),
  ).slice(0, 5);
  await logToolDebug("Ranked influencer candidates.", {
    rankedUsers: ranked.length,
    preview: ranked.slice(0, 3).map((item) => ({
      authorId: item.authorId,
      displayName: item.displayName,
      rawScore: item.rawScore,
      messageCount: item.messageCount,
    })),
  });

  const maxScore = ranked[0]?.rawScore ?? 1;
  const normalized = ranked.map((item) => ({
    ...item,
    rawScore: Number((item.rawScore / maxScore).toFixed(3)),
  }));

  const synthesized = await synthesizeStructuredOutput(
    buildInfluencerLookupPrompt(query, narrative, normalized),
    influencerLookupSynthesisAgent,
    influencerLookupOutputSchema,
  );
  await logToolDebug("Influencer synthesis completed.", {
    influencers: synthesized.influencers.length,
  });

  return synthesized;
}
