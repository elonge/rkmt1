import { Collection, ObjectId } from "mongodb";
import { StatsQueryInput, statsQueryInputSchema } from "../types";
import { dummyDatasetSummary, getMongoCollectionNames, getMongoDb } from "./mongo";

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
  };
};

type UserProfileDoc = {
  userId?: string;
  name?: string;
  groups?: Array<{
    group?: ObjectId;
    role?: string;
    status?: string;
  }>;
};

type MessageDoc = {
  _id?: ObjectId;
  timestamp?: number;
  messageId?: string;
  groupId?: string;
  authorId?: string;
  body?: string;
  messageReactions?: Array<{
    senderId?: string;
    reaction?: string;
    timestamp?: number;
  }>;
  messageReplies?: string[];
  quotedMessageId?: string;
};

type GroupSnapshot = {
  groupId: string;
  subject: string;
  memberCount: number;
  politicalLeaning: string;
};

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const STOP_WORDS = new Set([
  "a",
  "about",
  "across",
  "after",
  "all",
  "and",
  "are",
  "as",
  "at",
  "be",
  "by",
  "for",
  "from",
  "has",
  "have",
  "in",
  "into",
  "is",
  "it",
  "item",
  "of",
  "on",
  "or",
  "our",
  "sharing",
  "that",
  "the",
  "their",
  "this",
  "to",
  "up",
  "was",
  "with",
]);

function stableHash(value: string): number {
  let hash = 2166136261;
  for (const char of value) {
    hash ^= char.charCodeAt(0);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function normalizeWhitespace(value: string | null | undefined): string {
  return (value ?? "").replace(/\s+/g, " ").trim();
}

function lowerCase(value: string | null | undefined): string {
  return normalizeWhitespace(value).toLowerCase();
}

function tokenize(value: string | null | undefined): string[] {
  return (
    lowerCase(value)
      .match(/[\p{L}\p{N}]+/gu)
      ?.filter((token) => token.length > 1 && !STOP_WORDS.has(token)) ?? []
  );
}

function incrementCounter(counter: Map<string, number>, rawLabel: string | null | undefined) {
  const label = normalizeWhitespace(rawLabel) || "unknown";
  counter.set(label, (counter.get(label) ?? 0) + 1);
}

function topCountEntry(counter: Map<string, number>): { label: string; count: number } | null {
  let best: { label: string; count: number } | null = null;
  for (const [label, count] of counter.entries()) {
    if (!best || count > best.count) {
      best = { label, count };
    }
  }
  return best;
}

function extractNarrativeFromBody(body: string | undefined): string {
  const normalized = normalizeWhitespace(body);
  if (!normalized) {
    return "General discussion";
  }

  const leadingClause = normalizeWhitespace(normalized.split(":")[0]);
  if (leadingClause.length >= 3 && leadingClause.length <= 80) {
    return leadingClause;
  }

  const tokens = tokenize(normalized).slice(0, 2);
  return tokens.length > 0 ? tokens.join(" ") : "General discussion";
}

function deriveNarrativeLabel(groupTopic: string | null | undefined, body: string | undefined): string {
  const topic = normalizeWhitespace(groupTopic);
  if (topic) {
    return topic;
  }

  return extractNarrativeFromBody(body);
}

function buildGroupSnapshot(group: GroupDoc): GroupSnapshot {
  return {
    groupId: normalizeWhitespace(group.groupId) || "unknown_group",
    subject: normalizeWhitespace(group.subject) || "(No subject)",
    memberCount: typeof group.memberCount === "number" ? group.memberCount : 0,
    politicalLeaning: lowerCase(group.tags?.politicalLeaning?.tagValue) || "unknown",
  };
}

async function getCollections(): Promise<{
  groups: Collection<GroupDoc>;
  users: Collection<UserProfileDoc>;
  messages: Collection<MessageDoc>;
}> {
  const db = await getMongoDb();
  const names = getMongoCollectionNames();

  return {
    groups: db.collection<GroupDoc>(names.groupsCollection),
    users: db.collection<UserProfileDoc>(names.usersCollection),
    messages: db.collection<MessageDoc>(names.messagesCollection),
  };
}

async function getMaxMessageTimestamp(messages: Collection<MessageDoc>): Promise<number> {
  const latest = await messages
    .find({ timestamp: { $exists: true } }, { projection: { timestamp: 1 } })
    .sort({ timestamp: -1 })
    .limit(1)
    .next();

  return typeof latest?.timestamp === "number" ? latest.timestamp : Date.now();
}

async function getCutoffTimestamp(
  messages: Collection<MessageDoc>,
  lastDays: number,
): Promise<number> {
  const maxTimestamp = await getMaxMessageTimestamp(messages);
  return maxTimestamp - lastDays * DAY_IN_MS;
}

async function politicalLeaningDistribution(lastDays = 30): Promise<{
  metric: string;
  coverage: string;
  counts: Record<string, number>;
}> {
  const { groups, users, messages } = await getCollections();
  const cutoff = await getCutoffTimestamp(messages, lastDays);
  const activeUserIds = (await messages.distinct("authorId", {
    timestamp: { $gte: cutoff },
  })) as string[];
  const uniqueActiveUserIds = [...new Set(activeUserIds.filter((value) => typeof value === "string" && value))];

  const userDocs = await users
    .find({ userId: { $in: uniqueActiveUserIds } }, { projection: { userId: 1, groups: 1 } })
    .toArray();

  const groupIdStrings = new Set<string>();
  for (const user of userDocs) {
    for (const membership of user.groups ?? []) {
      if (!membership.group || membership.status === "LEFT") {
        continue;
      }
      groupIdStrings.add(String(membership.group));
    }
  }

  const groupObjectIds = [...groupIdStrings].map((value) => new ObjectId(value));
  const groupDocs =
    groupObjectIds.length > 0
      ? await groups.find({ _id: { $in: groupObjectIds } }, { projection: { tags: 1 } }).toArray()
      : [];
  const leaningByGroupId = new Map(
    groupDocs.map((group) => [
      String(group._id),
      lowerCase(group.tags?.politicalLeaning?.tagValue) || "unknown",
    ]),
  );

  const userById = new Map(userDocs.map((user) => [user.userId, user]));
  const counts: Record<string, number> = {};
  for (const activeUserId of uniqueActiveUserIds) {
    const user = userById.get(activeUserId);
    const leaning =
      user?.groups
        ?.filter((membership) => membership.status !== "LEFT")
        .map((membership) => leaningByGroupId.get(String(membership.group)))
        .find((value) => Boolean(value)) ?? "unknown";

    counts[leaning] = (counts[leaning] ?? 0) + 1;
  }

  return {
    metric: "political_leaning_distribution",
    coverage: `Active message authors in the last ${lastDays} days, joined with group political-leaning tags from Mongo.`,
    counts,
  };
}

async function activeMessagesLastDays(lastDays = 7): Promise<{
  metric: string;
  coverage: string;
  count: number;
}> {
  const { messages } = await getCollections();
  const cutoff = await getCutoffTimestamp(messages, lastDays);
  const count = await messages.countDocuments({ timestamp: { $gte: cutoff } });

  return {
    metric: "active_messages_last_days",
    coverage: `Messages in the last ${lastDays} days from Mongo.`,
    count,
  };
}

async function topGroupsByMemberCount(limit = 5): Promise<{
  metric: string;
  coverage: string;
  groups: GroupSnapshot[];
}> {
  const { groups } = await getCollections();
  const top = (await groups
    .find(
      {},
      {
        projection: {
          groupId: 1,
          subject: 1,
          memberCount: 1,
          tags: 1,
          lastActivityTimestamp: 1,
        },
      },
    )
    .sort({ memberCount: -1, lastActivityTimestamp: -1 })
    .limit(limit)
    .toArray()).map(buildGroupSnapshot);

  return {
    metric: "top_groups_by_member_count",
    coverage: `Top ${limit} groups by member count from Mongo.`,
    groups: top,
  };
}

export async function latestNarrativesInTimeframe(lastDays = 7, limit = 8): Promise<{
  tool: string;
  timeframeDays: number;
  coverage: string;
  narratives: Array<{ narrative: string; volume: number }>;
}> {
  const { groups, messages } = await getCollections();
  const cutoff = await getCutoffTimestamp(messages, lastDays);
  const recentMessages = await messages
    .find({ timestamp: { $gte: cutoff } }, { projection: { groupId: 1, body: 1, timestamp: 1 } })
    .toArray();

  const groupIds = [...new Set(recentMessages.map((message) => message.groupId).filter(Boolean))] as string[];
  const groupDocs =
    groupIds.length > 0
      ? await groups
          .find(
            { groupId: { $in: groupIds } },
            { projection: { groupId: 1, subject: 1, tags: 1 } },
          )
          .toArray()
      : [];
  const topicByGroupId = new Map(
    groupDocs.map((group) => [
      group.groupId ?? "",
      deriveNarrativeLabel(group.tags?.topic?.tagValue, group.subject),
    ]),
  );

  const stats = new Map<string, { volume: number; lastTimestamp: number }>();
  for (const message of recentMessages) {
    if (typeof message.timestamp !== "number") {
      continue;
    }

    const narrative = deriveNarrativeLabel(
      message.groupId ? topicByGroupId.get(message.groupId) : null,
      message.body,
    );
    const current = stats.get(narrative);
    if (!current) {
      stats.set(narrative, { volume: 1, lastTimestamp: message.timestamp });
      continue;
    }

    current.volume += 1;
    if (message.timestamp > current.lastTimestamp) {
      current.lastTimestamp = message.timestamp;
    }
  }

  const narratives = [...stats.entries()]
    .map(([narrative, value]) => ({
      narrative,
      volume: value.volume,
      lastTimestamp: value.lastTimestamp,
    }))
    .sort((left, right) => right.lastTimestamp - left.lastTimestamp || right.volume - left.volume)
    .slice(0, limit)
    .map((item) => ({
      narrative: item.narrative,
      volume: item.volume,
    }));

  return {
    tool: "find_narratives_in_timeframe",
    timeframeDays: lastDays,
    coverage: `Narrative volume aggregated from Mongo messages in the last ${lastDays} days using group topic tags and simple body heuristics.`,
    narratives,
  };
}

export async function lookupAudienceSegments(question: string): Promise<Record<string, unknown>> {
  const { groups, messages } = await getCollections();
  const queryTokens = tokenize(question);
  const candidateGroups = await groups
    .find(
      {},
      {
        projection: {
          groupId: 1,
          subject: 1,
          description: 1,
          memberCount: 1,
          lastActivityTimestamp: 1,
          tags: 1,
        },
      },
    )
    .sort({ memberCount: -1, lastActivityTimestamp: -1 })
    .limit(250)
    .toArray();

  const scoredGroups = candidateGroups
    .map((group) => {
      const haystack = lowerCase(
        [
          group.subject,
          group.description,
          group.tags?.topic?.tagValue,
          group.tags?.region?.tagValue,
          group.tags?.demographic?.tagValue,
          group.tags?.politicalLeaning?.tagValue,
        ].join(" "),
      );
      const queryScore = queryTokens.reduce(
        (score, token) => score + (haystack.includes(token) ? 3 : 0),
        0,
      );
      const memberWeight = Math.min(2, (group.memberCount ?? 0) / 500);
      return { group, score: queryScore + memberWeight };
    })
    .sort((left, right) => right.score - left.score);

  const hasDirectQueryMatch = scoredGroups.some((entry) => entry.score >= 3);
  const selectedGroups = scoredGroups
    .filter((entry) => !hasDirectQueryMatch || entry.score >= 3)
    .slice(0, 25)
    .map((entry) => entry.group);

  const demographicCounts = new Map<string, number>();
  const regionCounts = new Map<string, number>();
  const leaningCounts = new Map<string, number>();
  const topicCounts = new Map<string, number>();

  for (const group of selectedGroups) {
    incrementCounter(demographicCounts, group.tags?.demographic?.tagValue);
    incrementCounter(regionCounts, group.tags?.region?.tagValue);
    incrementCounter(leaningCounts, group.tags?.politicalLeaning?.tagValue);
    incrementCounter(topicCounts, group.tags?.topic?.tagValue);
  }

  const recentCutoff = await getCutoffTimestamp(messages, 30);
  const selectedGroupIds = selectedGroups
    .map((group) => group.groupId)
    .filter((value): value is string => Boolean(value));
  const recentMessages =
    selectedGroupIds.length > 0
      ? await messages
          .find(
            {
              groupId: { $in: selectedGroupIds },
              timestamp: { $gte: recentCutoff },
            },
            { projection: { groupId: 1, authorId: 1 } },
          )
          .toArray()
      : [];
  const activeAuthors = new Set(
    recentMessages
      .map((message) => message.authorId)
      .filter((value): value is string => Boolean(value)),
  );

  const topDemographic = topCountEntry(demographicCounts);
  const topRegion = topCountEntry(regionCounts);
  const topLeaning = topCountEntry(leaningCounts);
  const topTopic = topCountEntry(topicCounts);

  return {
    source: "mongo-audience-lookup",
    matchedGroups: selectedGroups.length,
    segments: [
      topDemographic
        ? `Top demographic cluster: ${topDemographic.label} (${topDemographic.count} matching groups).`
        : "No demographic tag cluster found in the matching groups.",
      topRegion
        ? `Strongest regional concentration: ${topRegion.label} (${topRegion.count} matching groups, ${activeAuthors.size} active authors in the last 30 days).`
        : `No regional concentration found; ${activeAuthors.size} active authors appeared across the matching groups in the last 30 days.`,
      topLeaning && topTopic
        ? `Dominant audience angle: ${topLeaning.label} leaning around ${topTopic.label} (${topTopic.count} groups).`
        : "No stable leaning/topic combination found in the matching groups.",
    ],
    sampleGroups: selectedGroups
      .slice(0, 5)
      .map((group) => normalizeWhitespace(group.subject) || normalizeWhitespace(group.groupId)),
  };
}

export async function lookupInfluencers(
  narrative?: string | null,
): Promise<Record<string, unknown>> {
  const { groups, users, messages } = await getCollections();
  const cutoff = await getCutoffTimestamp(messages, 30);
  const recentMessages = await messages
    .find(
      { timestamp: { $gte: cutoff } },
      {
        projection: {
          authorId: 1,
          body: 1,
          groupId: 1,
          timestamp: 1,
          messageReactions: 1,
          messageReplies: 1,
        },
      },
    )
    .toArray();

  const groupDocs = await groups
    .find({}, { projection: { groupId: 1, subject: 1, tags: 1 } })
    .toArray();
  const topicByGroupId = new Map(
    groupDocs.map((group) => [
      group.groupId ?? "",
      lowerCase(group.tags?.topic?.tagValue) || lowerCase(group.subject),
    ]),
  );
  const narrativeTokens = tokenize(narrative);
  const filteredMessages =
    narrativeTokens.length === 0
      ? recentMessages
      : recentMessages.filter((message) => {
          const haystack = lowerCase(
            `${message.body ?? ""} ${message.groupId ? topicByGroupId.get(message.groupId) ?? "" : ""}`,
          );
          return narrativeTokens.some((token) => haystack.includes(token));
        });

  const stats = new Map<
    string,
    {
      messageCount: number;
      reactionsReceived: number;
      repliesReceived: number;
      groups: Set<string>;
      lastTimestamp: number;
    }
  >();

  for (const message of filteredMessages) {
    const authorId = normalizeWhitespace(message.authorId);
    if (!authorId) {
      continue;
    }

    const current =
      stats.get(authorId) ??
      {
        messageCount: 0,
        reactionsReceived: 0,
        repliesReceived: 0,
        groups: new Set<string>(),
        lastTimestamp: 0,
      };

    current.messageCount += 1;
    current.reactionsReceived += Array.isArray(message.messageReactions)
      ? message.messageReactions.length
      : 0;
    current.repliesReceived += Array.isArray(message.messageReplies)
      ? message.messageReplies.length
      : 0;
    if (message.groupId) {
      current.groups.add(message.groupId);
    }
    if (typeof message.timestamp === "number" && message.timestamp > current.lastTimestamp) {
      current.lastTimestamp = message.timestamp;
    }

    stats.set(authorId, current);
  }

  const scoredInfluencers = [...stats.entries()]
    .map(([authorId, value]) => ({
      authorId,
      rawScore:
        value.messageCount +
        value.reactionsReceived * 1.5 +
        value.repliesReceived * 2 +
        value.groups.size * 3,
      messageCount: value.messageCount,
      reactionCount: value.reactionsReceived,
      replyCount: value.repliesReceived,
      groupCount: value.groups.size,
      lastTimestamp: value.lastTimestamp,
    }))
    .sort((left, right) => right.rawScore - left.rawScore || right.lastTimestamp - left.lastTimestamp)
    .slice(0, 5);

  const maxScore = scoredInfluencers[0]?.rawScore ?? 1;
  const topUserIds = scoredInfluencers.map((item) => item.authorId);
  const userDocs =
    topUserIds.length > 0
      ? await users.find({ userId: { $in: topUserIds } }, { projection: { userId: 1, name: 1 } }).toArray()
      : [];
  const nameByUserId = new Map(userDocs.map((user) => [user.userId, user.name]));

  return {
    source: "mongo-influencer-lookup",
    influencers: scoredInfluencers.map((item) => ({
      maskedUserId: item.authorId,
      displayName: normalizeWhitespace(nameByUserId.get(item.authorId)) || item.authorId,
      influenceScore: Number((item.rawScore / maxScore).toFixed(2)),
      narrative: narrative ?? "general",
      messageCount: item.messageCount,
      reactionCount: item.reactionCount,
      replyCount: item.replyCount,
      groupCount: item.groupCount,
    })),
  };
}

export async function searchMessagesByMockVector(
  query: string,
  topK: number,
): Promise<Record<string, unknown>> {
  const { messages } = await getCollections();
  const queryTokens = tokenize(query);
  const normalizedQuery = lowerCase(query);
  const candidates = await messages
    .find(
      { body: { $type: "string" } },
      {
        projection: {
          body: 1,
          messageId: 1,
          groupId: 1,
          authorId: 1,
          timestamp: 1,
        },
      },
    )
    .sort({ timestamp: -1 })
    .limit(500)
    .toArray();

  const snippets = candidates
    .map((message) => {
      const text = normalizeWhitespace(message.body);
      const textLower = lowerCase(text);
      const textTokens = new Set(tokenize(text));
      const overlap = queryTokens.reduce(
        (count, token) => count + (textTokens.has(token) ? 1 : 0),
        0,
      );
      const phraseBonus = normalizedQuery && textLower.includes(normalizedQuery) ? 0.35 : 0;
      const coverage = queryTokens.length > 0 ? overlap / queryTokens.length : 0;
      const jitter = (stableHash(`${message.messageId ?? text}:${normalizedQuery}`) % 100) / 1000;
      const score = Number(Math.min(0.99, coverage * 0.7 + phraseBonus + jitter).toFixed(2));

      return {
        id: normalizeWhitespace(message.messageId) || String(message._id ?? ""),
        score,
        overlap,
        timestamp: typeof message.timestamp === "number" ? message.timestamp : 0,
        text: text.slice(0, 320),
        groupId: message.groupId,
        authorId: message.authorId,
      };
    })
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      if (right.overlap !== left.overlap) {
        return right.overlap - left.overlap;
      }
      return right.timestamp - left.timestamp;
    })
    .slice(0, topK)
    .map(({ overlap: _overlap, ...snippet }) => snippet);

  return {
    source: "mongo-message-search-mock-vector",
    query,
    snippets,
  };
}

export async function runStatsQuery(rawInput: StatsQueryInput): Promise<Record<string, unknown>> {
  const input = statsQueryInputSchema.parse(rawInput);

  switch (input.metric) {
    case "political_leaning_distribution":
      return politicalLeaningDistribution(input.lastDays ?? 30);
    case "active_messages_last_days":
      return activeMessagesLastDays(input.lastDays ?? 7);
    case "top_groups_by_member_count":
      return topGroupsByMemberCount(input.limit ?? 5);
    default:
      return {
        metric: input.metric,
        coverage: "No implementation available",
      };
  }
}

export { dummyDatasetSummary };
