import { Filter, ObjectId } from "mongodb";
import type { Collection } from "mongodb";
import { getMongoCollectionNames, getMongoDb } from "./mongo";
import { logToolDebug } from "../runtime/tool-debug.ts";
import { searchGroupsBySemanticQuery, searchMessagesBySemanticQuery } from "./semantic";

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
    demographic?: TagField;
    topic?: TagField;
    region?: TagField;
    organization?: TagField;
  };
};

type UserProfileDoc = {
  _id?: ObjectId;
  userId?: string;
  name?: string;
  status?: string;
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
};

type MessageCandidate = {
  messageId: string;
  authorId: string;
  groupId: string;
  groupSubject: string;
  timestamp: number;
  replyCount: number;
  reactionCount: number;
  bodyPreview: string;
};

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const DEFAULT_LIMIT = 12;
const STOP_WORDS = new Set([
  "a",
  "about",
  "after",
  "all",
  "an",
  "and",
  "are",
  "as",
  "at",
  "be",
  "by",
  "for",
  "from",
  "in",
  "into",
  "is",
  "it",
  "last",
  "of",
  "on",
  "or",
  "show",
  "that",
  "the",
  "these",
  "this",
  "those",
  "to",
  "users",
  "who",
  "with",
]);

function normalizeWhitespace(value: string | null | undefined): string {
  return (value ?? "").replace(/\s+/g, " ").trim();
}

function lowerCase(value: string | null | undefined): string {
  return normalizeWhitespace(value).toLowerCase();
}

function truncateText(value: string | null | undefined, maxLength = 180): string {
  const normalized = normalizeWhitespace(value);
  if (normalized.length <= maxLength) {
    return normalized;
  }

  return `${normalized.slice(0, maxLength - 3)}...`;
}

function tokenize(value: string | null | undefined): string[] {
  return (
    lowerCase(value)
      .match(/[\p{L}\p{N}]+/gu)
      ?.filter((token) => token.length > 1 && !STOP_WORDS.has(token)) ?? []
  );
}

function uniqueStrings(values: Array<string | null | undefined>): string[] {
  const normalized = values.map((value) => normalizeWhitespace(value)).filter(Boolean);
  return [...new Set(normalized)];
}

function computeTokenScore(haystack: string, queryTokens: string[]): number {
  if (queryTokens.length === 0) {
    return 0;
  }

  return queryTokens.reduce((score, token) => score + (haystack.includes(token) ? 1 : 0), 0);
}

function roundScore(value: number): number {
  return Number(value.toFixed(3));
}

function clampLimit(limit: number | null | undefined, fallback = DEFAULT_LIMIT): number {
  if (!Number.isInteger(limit) || !limit || limit < 1) {
    return fallback;
  }

  return Math.min(limit, 50);
}

function buildCaseInsensitiveRegex(pattern: string): RegExp {
  try {
    return new RegExp(pattern, "iu");
  } catch {
    throw new Error(`Invalid regex pattern: ${pattern}`);
  }
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

async function getReferenceTimestamp(messages: Collection<MessageDoc>): Promise<number> {
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
  return (await getReferenceTimestamp(messages)) - lastDays * DAY_IN_MS;
}

function buildGroupText(group: {
  subject?: string | null;
  description?: string | null;
  topic?: string | null;
  region?: string | null;
  politicalLeaning?: string | null;
}): string {
  return lowerCase(
    [
      group.subject,
      group.description,
      group.topic,
      group.region,
      group.politicalLeaning,
    ].join(" "),
  );
}

async function fetchGroupsByGroupIds(
  groups: Collection<GroupDoc>,
  groupIds: string[],
): Promise<GroupDoc[]> {
  if (groupIds.length === 0) {
    return [];
  }

  return groups
    .find(
      { groupId: { $in: groupIds } },
      {
        projection: {
          _id: 1,
          groupId: 1,
          subject: 1,
          description: 1,
          memberCount: 1,
          tags: 1,
          lastActivityTimestamp: 1,
        },
      },
    )
    .toArray();
}

async function fetchGroupActivityCounts(
  messages: Collection<MessageDoc>,
  groupIds: string[],
  cutoffTimestamp: number,
): Promise<Map<string, number>> {
  if (groupIds.length === 0) {
    return new Map();
  }

  const rows = await messages
    .aggregate<{ _id: string; count: number }>([
      {
        $match: {
          groupId: { $in: groupIds },
          timestamp: { $gte: cutoffTimestamp },
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

  return new Map(
    rows
      .map((row) => [normalizeWhitespace(row._id), row.count] as const)
      .filter((entry): entry is readonly [string, number] => Boolean(entry[0])),
  );
}

function summarizeAuthors(messages: MessageCandidate[]): Array<{
  userId: string;
  messageCount: number;
  replyCount: number;
  reactionCount: number;
  groupCount: number;
}> {
  const byAuthor = new Map<
    string,
    { messageCount: number; replyCount: number; reactionCount: number; groups: Set<string> }
  >();

  for (const message of messages) {
    const current =
      byAuthor.get(message.authorId) ??
      {
        messageCount: 0,
        replyCount: 0,
        reactionCount: 0,
        groups: new Set<string>(),
      };
    current.messageCount += 1;
    current.replyCount += message.replyCount;
    current.reactionCount += message.reactionCount;
    current.groups.add(message.groupId);
    byAuthor.set(message.authorId, current);
  }

  return [...byAuthor.entries()]
    .map(([userId, value]) => ({
      userId,
      messageCount: value.messageCount,
      replyCount: value.replyCount,
      reactionCount: value.reactionCount,
      groupCount: value.groups.size,
    }))
    .sort(
      (left, right) =>
        right.messageCount - left.messageCount ||
        right.replyCount - left.replyCount ||
        right.reactionCount - left.reactionCount,
    )
    .slice(0, 10);
}

export async function searchUserProfiles(input: {
  query?: string | null;
  userIds?: string[] | null;
  groupIds?: string[] | null;
  membershipStatus?: string | null;
  roles?: string[] | null;
  limit?: number | null;
}): Promise<Record<string, unknown>> {
  const query = normalizeWhitespace(input.query);
  const userIds = uniqueStrings(input.userIds ?? []);
  const groupIds = uniqueStrings(input.groupIds ?? []);
  const membershipStatus = normalizeWhitespace(input.membershipStatus).toUpperCase();
  const roles = uniqueStrings(input.roles ?? []).map((role) => role.toUpperCase());
  const limit = clampLimit(input.limit, 12);

  if (
    userIds.length === 0 &&
    groupIds.length === 0 &&
    !query &&
    !membershipStatus &&
    roles.length === 0
  ) {
    throw new Error("user_profile_lookup requires at least one filter.");
  }

  const { groups, users } = await getCollections();
  const selectedGroups = await fetchGroupsByGroupIds(groups, groupIds);
  const groupObjectIds = selectedGroups
    .map((group) => group._id)
    .filter((groupId): groupId is ObjectId => Boolean(groupId));
  const groupIdByObjectId = new Map(
    selectedGroups
      .map((group) => {
        if (!group._id) {
          return null;
        }

        const groupId = normalizeWhitespace(group.groupId);
        if (!groupId) {
          return null;
        }

        return [String(group._id), groupId] as const;
      })
      .filter((entry): entry is readonly [string, string] => Boolean(entry)),
  );
  const groupSubjectByGroupId = new Map(
    selectedGroups
      .map((group) => {
        const groupId = normalizeWhitespace(group.groupId);
        if (!groupId) {
          return null;
        }

        return [groupId, normalizeWhitespace(group.subject) || groupId] as const;
      })
      .filter((entry): entry is readonly [string, string] => Boolean(entry)),
  );

  const userFilter: Filter<UserProfileDoc> = {};
  if (userIds.length > 0) {
    userFilter.userId = { $in: userIds };
  }

  const membershipFilter: Record<string, unknown> = {};
  if (groupObjectIds.length > 0) {
    membershipFilter.group = { $in: groupObjectIds };
  }
  if (membershipStatus) {
    membershipFilter.status = membershipStatus;
  }
  if (roles.length > 0) {
    membershipFilter.role = { $in: roles };
  }
  if (Object.keys(membershipFilter).length > 0) {
    userFilter.groups = { $elemMatch: membershipFilter };
  }

  await logToolDebug("User profile lookup filters resolved.", {
    query,
    userIds: userIds.length,
    groupIds: groupIds.length,
    resolvedGroups: selectedGroups.length,
    membershipStatus: membershipStatus || null,
    roles,
  });

  const queryTokens = tokenize(query);
  const rawUsers = await users
    .find(userFilter, {
      projection: {
        userId: 1,
        name: 1,
        status: 1,
        groups: 1,
      },
    })
    .limit(Math.max(limit * 6, 120))
    .toArray();

  await logToolDebug("Fetched candidate user profiles.", {
    candidates: rawUsers.length,
  });

  const matchedUsers = rawUsers
    .map((user) => {
      const userId = normalizeWhitespace(user.userId);
      if (!userId) {
        return null;
      }

      const memberships = user.groups ?? [];
      const activeMembershipCount = memberships.filter(
        (membership) => normalizeWhitespace(membership.status).toUpperCase() !== "LEFT",
      ).length;
      const matchingMemberships = memberships.filter((membership) => {
        const resolvedGroupId = membership.group
          ? groupIdByObjectId.get(String(membership.group))
          : undefined;
        if (groupIds.length > 0 && !resolvedGroupId) {
          return false;
        }
        if (
          membershipStatus &&
          normalizeWhitespace(membership.status).toUpperCase() !== membershipStatus
        ) {
          return false;
        }
        if (
          roles.length > 0 &&
          !roles.includes(normalizeWhitespace(membership.role).toUpperCase())
        ) {
          return false;
        }
        return true;
      });
      if (groupIds.length > 0 && matchingMemberships.length === 0) {
        return null;
      }

      const matchingGroupIds = uniqueStrings(
        matchingMemberships.map((membership) =>
          membership.group ? groupIdByObjectId.get(String(membership.group)) : null,
        ),
      ).slice(0, 10);
      const matchingGroups = matchingGroupIds
        .map((groupId) => groupSubjectByGroupId.get(groupId) || groupId)
        .slice(0, 10);
      const roleValues = uniqueStrings(
        (matchingMemberships.length > 0 ? matchingMemberships : memberships)
          .map((membership) => normalizeWhitespace(membership.role)),
      )
        .map((role) => role.toUpperCase())
        .slice(0, 10);
      const queryScore = computeTokenScore(
        buildGroupText({
          subject: user.name,
          description: user.status,
          topic: matchingGroups.join(" "),
        }),
        queryTokens,
      );
      if (queryTokens.length > 0 && queryScore === 0) {
        return null;
      }

      return {
        userId,
        name: normalizeWhitespace(user.name) || userId,
        status: normalizeWhitespace(user.status),
        activeMembershipCount,
        matchingGroupIds,
        matchingGroups,
        roles: roleValues,
        queryScore,
      };
    })
    .filter(
      (
        user,
      ): user is {
        userId: string;
        name: string;
        status: string;
        activeMembershipCount: number;
        matchingGroupIds: string[];
        matchingGroups: string[];
        roles: string[];
        queryScore: number;
      } => Boolean(user),
    )
    .sort(
      (left, right) =>
        right.queryScore - left.queryScore ||
        right.matchingGroupIds.length - left.matchingGroupIds.length ||
        right.activeMembershipCount - left.activeMembershipCount ||
        left.userId.localeCompare(right.userId),
    );

  await logToolDebug("Filtered user profiles down to final matches.", {
    matchedUsers: matchedUsers.length,
    preview: matchedUsers.slice(0, 5).map((user) => user.userId),
  });

  return {
    source: "mongo-user-profile-lookup",
    query,
    matchedUsers: matchedUsers.length,
    users: matchedUsers.slice(0, limit).map(({ queryScore: _queryScore, ...user }) => user),
  };
}

async function runRegexMessageSearch(input: {
  query: string;
  lastDays?: number | null;
  groupIds?: string[] | null;
  authorIds?: string[] | null;
  minReplies?: number | null;
  minReactions?: number | null;
  limit?: number | null;
}): Promise<Record<string, unknown>> {
  const query = normalizeWhitespace(input.query);
  if (!query) {
    throw new Error("message_search with searchMode=regex requires a non-empty query.");
  }

  const searchPattern = buildCaseInsensitiveRegex(query);
  const lastDays = Math.min(Math.max(input.lastDays ?? 30, 1), 365);
  const groupIds = uniqueStrings(input.groupIds ?? []);
  const authorIds = uniqueStrings(input.authorIds ?? []);
  const minReplies = Math.max(input.minReplies ?? 0, 0);
  const minReactions = Math.max(input.minReactions ?? 0, 0);
  const limit = clampLimit(input.limit, 12);
  const { groups, messages } = await getCollections();
  const cutoffTimestamp = await getCutoffTimestamp(messages, lastDays);
  const filter: Filter<MessageDoc> = {
    timestamp: { $gte: cutoffTimestamp },
    body: { $regex: query, $options: "i" },
  };

  if (groupIds.length > 0) {
    filter.groupId = { $in: groupIds };
  }
  if (authorIds.length > 0) {
    filter.authorId = { $in: authorIds };
  }

  await logToolDebug("Running regex message search.", {
    query,
    lastDays,
    groupIds: groupIds.length,
    authorIds: authorIds.length,
    minReplies,
    minReactions,
  });

  const rawMessages = await messages
    .find(filter, {
      projection: {
        messageId: 1,
        authorId: 1,
        groupId: 1,
        body: 1,
        timestamp: 1,
        messageReactions: 1,
        messageReplies: 1,
      },
    })
    .sort({ timestamp: -1 })
    .limit(Math.max(limit * 20, 400))
    .toArray();

  await logToolDebug("Fetched regex message candidates from Mongo.", {
    candidates: rawMessages.length,
  });

  const matchedGroupDocs = await fetchGroupsByGroupIds(
    groups,
    uniqueStrings(rawMessages.map((message) => message.groupId)),
  );
  const groupSubjectById = new Map(
    matchedGroupDocs
      .map((group) => {
        const groupId = normalizeWhitespace(group.groupId);
        if (!groupId) {
          return null;
        }

        return [groupId, normalizeWhitespace(group.subject) || groupId] as const;
      })
      .filter((entry): entry is readonly [string, string] => Boolean(entry)),
  );

  const matchedMessages = rawMessages
    .map((message) => {
      const messageId = normalizeWhitespace(message.messageId);
      const authorId = normalizeWhitespace(message.authorId);
      const groupId = normalizeWhitespace(message.groupId);
      const timestamp = typeof message.timestamp === "number" ? message.timestamp : 0;

      if (!messageId || !authorId || !groupId || timestamp <= 0) {
        return null;
      }

      const replyCount = Array.isArray(message.messageReplies) ? message.messageReplies.length : 0;
      const reactionCount = Array.isArray(message.messageReactions)
        ? message.messageReactions.length
        : 0;
      if (replyCount < minReplies || reactionCount < minReactions) {
        return null;
      }

      const haystack = [message.body, groupSubjectById.get(groupId), groupId].join(" ");
      if (!searchPattern.test(haystack)) {
        return null;
      }

      return {
        messageId,
        authorId,
        groupId,
        groupSubject: groupSubjectById.get(groupId) || groupId,
        timestamp,
        replyCount,
        reactionCount,
        bodyPreview: truncateText(message.body),
      };
    })
    .filter((message): message is MessageCandidate => Boolean(message))
    .sort(
      (left, right) =>
        right.replyCount - left.replyCount ||
        right.reactionCount - left.reactionCount ||
        right.timestamp - left.timestamp,
    );

  await logToolDebug("Regex message search filtered candidates.", {
    matchedMessages: matchedMessages.length,
    preview: matchedMessages.slice(0, 5).map((message) => ({
      messageId: message.messageId,
      authorId: message.authorId,
      groupId: message.groupId,
    })),
  });

  return {
    source: "mongo-message-search",
    searchMode: "regex",
    query,
    lastDays,
    matchedMessages: matchedMessages.length,
    uniqueAuthors: new Set(matchedMessages.map((message) => message.authorId)).size,
    authors: summarizeAuthors(matchedMessages).slice(0, 10),
    messages: matchedMessages.slice(0, limit),
  };
}

async function runVectorMessageSearch(input: {
  query: string;
  lastDays?: number | null;
  groupIds?: string[] | null;
  authorIds?: string[] | null;
  minReplies?: number | null;
  minReactions?: number | null;
  limit?: number | null;
}): Promise<Record<string, unknown>> {
  const query = normalizeWhitespace(input.query);
  if (!query) {
    throw new Error("message_search with searchMode=vector requires a non-empty query.");
  }

  const lastDays = input.lastDays ? Math.min(Math.max(input.lastDays, 1), 365) : null;
  const groupIds = uniqueStrings(input.groupIds ?? []);
  const authorIds = uniqueStrings(input.authorIds ?? []);
  const minReplies = Math.max(input.minReplies ?? 0, 0);
  const minReactions = Math.max(input.minReactions ?? 0, 0);
  const limit = clampLimit(input.limit, 12);
  const { messages } = await getCollections();
  const cutoffTimestamp = lastDays ? await getCutoffTimestamp(messages, lastDays) : null;

  await logToolDebug("Running vector message search.", {
    query,
    lastDays,
    groupIds: groupIds.length,
    authorIds: authorIds.length,
    minReplies,
    minReactions,
  });

  const rawMessages = await searchMessagesBySemanticQuery({
    query,
    groupIds,
    authorIds,
    limit: Math.max(limit * 8, 80),
  });

  await logToolDebug("Fetched vector message candidates from Atlas.", {
    candidates: rawMessages.length,
    cutoffTimestamp,
    minReplies: minReplies,
    minReactions: minReactions,
  });

  const matchedMessages = rawMessages
    .filter((message) => (cutoffTimestamp ? message.timestamp >= cutoffTimestamp : true))
    .filter(
      (message) =>
        message.replyCount >= minReplies && message.reactionCount >= minReactions,
    )
    .map((message) => ({
      messageId: message.messageId,
      authorId: message.authorId,
      groupId: message.groupId,
      groupSubject: normalizeWhitespace(message.groupSubject) || message.groupId,
      timestamp: message.timestamp,
      replyCount: message.replyCount,
      reactionCount: message.reactionCount,
      bodyPreview: truncateText(message.bodyPreview),
      vectorScore: roundScore(message.score),
    }))
    .sort(
      (left, right) =>
        right.vectorScore - left.vectorScore ||
        right.replyCount - left.replyCount ||
        right.reactionCount - left.reactionCount ||
        right.timestamp - left.timestamp,
    );

  await logToolDebug("Vector message search filtered candidates.", {
    matchedMessages: matchedMessages.length,
    preview: matchedMessages.slice(0, 5).map((message) => ({
      messageId: message.messageId,
      authorId: message.authorId,
      groupId: message.groupId,
      vectorScore: message.vectorScore,
    })),
  });

  return {
    source: "mongo-message-search",
    searchMode: "vector",
    query,
    lastDays,
    matchedMessages: matchedMessages.length,
    uniqueAuthors: new Set(matchedMessages.map((message) => message.authorId)).size,
    authors: summarizeAuthors(matchedMessages).slice(0, 10),
    messages: matchedMessages.slice(0, limit),
  };
}

async function runRegexGroupSearch(input: {
  query: string;
  lastDays?: number | null;
  minActivity?: number | null;
  limit?: number | null;
}): Promise<Record<string, unknown>> {
  const query = normalizeWhitespace(input.query);
  if (!query) {
    throw new Error("group_search with searchMode=regex requires a non-empty query.");
  }

  const searchPattern = buildCaseInsensitiveRegex(query);
  const lastDays = Math.min(Math.max(input.lastDays ?? 30, 1), 365);
  const minActivity = Math.max(input.minActivity ?? 0, 0);
  const limit = clampLimit(input.limit, 10);
  const { groups, messages } = await getCollections();
  const cutoffTimestamp = await getCutoffTimestamp(messages, lastDays);
  const filter: Filter<GroupDoc> = {
    $or: [
      { subject: { $regex: query, $options: "i" } },
      { description: { $regex: query, $options: "i" } },
      { "tags.topic.tagValue": { $regex: query, $options: "i" } },
      { "tags.region.tagValue": { $regex: query, $options: "i" } },
      { "tags.politicalLeaning.tagValue": { $regex: query, $options: "i" } },
      { "tags.demographic.tagValue": { $regex: query, $options: "i" } },
      { "tags.organization.tagValue": { $regex: query, $options: "i" } },
    ],
  };

  await logToolDebug("Running regex group search.", {
    query,
    lastDays,
    minActivity,
  });

  const rawGroups = await groups
    .find(filter, {
      projection: {
        _id: 1,
        groupId: 1,
        subject: 1,
        description: 1,
        memberCount: 1,
        tags: 1,
        lastActivityTimestamp: 1,
      },
    })
    .limit(Math.max(limit * 12, 180))
    .toArray();

  await logToolDebug("Fetched regex group candidates from Mongo.", {
    candidates: rawGroups.length,
  });

  const activityByGroupId = await fetchGroupActivityCounts(
    messages,
    uniqueStrings(rawGroups.map((group) => group.groupId)),
    cutoffTimestamp,
  );

  const matchedGroups = rawGroups
    .map((group) => {
      const groupId = normalizeWhitespace(group.groupId);
      if (!groupId) {
        return null;
      }

      const activityCount = activityByGroupId.get(groupId) ?? 0;
      if (activityCount < minActivity) {
        return null;
      }

      const haystack = [
        group.subject,
        group.description,
        group.tags?.topic?.tagValue,
        group.tags?.region?.tagValue,
        group.tags?.politicalLeaning?.tagValue,
        group.tags?.demographic?.tagValue,
        group.tags?.organization?.tagValue,
      ].join(" ");
      if (!searchPattern.test(haystack)) {
        return null;
      }

      return {
        groupId,
        subject: normalizeWhitespace(group.subject) || groupId,
        topic: normalizeWhitespace(group.tags?.topic?.tagValue),
        region: normalizeWhitespace(group.tags?.region?.tagValue),
        politicalLeaning: normalizeWhitespace(group.tags?.politicalLeaning?.tagValue),
        memberCount: typeof group.memberCount === "number" ? Math.max(0, group.memberCount) : 0,
        activityCount,
      };
    })
    .filter(
      (
        group,
      ): group is {
        groupId: string;
        subject: string;
        topic: string;
        region: string;
        politicalLeaning: string;
        memberCount: number;
        activityCount: number;
      } => Boolean(group),
    )
    .sort(
      (left, right) =>
        right.activityCount - left.activityCount ||
        right.memberCount - left.memberCount ||
        left.subject.localeCompare(right.subject),
    );

  await logToolDebug("Regex groups ranked.", {
    matchedGroups: matchedGroups.length,
    preview: matchedGroups.slice(0, 5).map((group) => ({
      groupId: group.groupId,
      activityCount: group.activityCount,
    })),
  });

  return {
    source: "mongo-group-search",
    searchMode: "regex",
    query,
    lastDays,
    matchedGroups: matchedGroups.length,
    groups: matchedGroups.slice(0, limit),
  };
}

async function runVectorGroupSearch(input: {
  query: string;
  lastDays?: number | null;
  minActivity?: number | null;
  limit?: number | null;
}): Promise<Record<string, unknown>> {
  const query = normalizeWhitespace(input.query);
  if (!query) {
    throw new Error("group_search with searchMode=vector requires a non-empty query.");
  }

  const lastDays = Math.min(Math.max(input.lastDays ?? 30, 1), 365);
  const minActivity = Math.max(input.minActivity ?? 0, 0);
  const limit = clampLimit(input.limit, 10);
  const { messages } = await getCollections();
  const cutoffTimestamp = await getCutoffTimestamp(messages, lastDays);

  await logToolDebug("Running vector group search.", {
    query,
    lastDays,
    minActivity,
  });

  const rawGroups = await searchGroupsBySemanticQuery(query, Math.max(limit * 6, 24));

  await logToolDebug("Fetched vector group candidates from Atlas.", {
    candidates: rawGroups.length,
  });

  const activityByGroupId = await fetchGroupActivityCounts(
    messages,
    rawGroups.map((group) => group.groupId),
    cutoffTimestamp,
  );

  const matchedGroups = rawGroups
    .map((group) => {
      const activityCount = activityByGroupId.get(group.groupId) ?? 0;
      if (activityCount < minActivity) {
        return null;
      }

      return {
        groupId: group.groupId,
        subject: normalizeWhitespace(group.subject) || group.groupId,
        topic: normalizeWhitespace(group.topic),
        region: normalizeWhitespace(group.region),
        politicalLeaning: normalizeWhitespace(group.politicalLeaning),
        memberCount: group.memberCount,
        activityCount,
        vectorScore: roundScore(group.score),
      };
    })
    .filter(
      (
        group,
      ): group is {
        groupId: string;
        subject: string;
        topic: string;
        region: string;
        politicalLeaning: string;
        memberCount: number;
        activityCount: number;
        vectorScore: number;
      } => Boolean(group),
    )
    .sort(
      (left, right) =>
        right.vectorScore - left.vectorScore ||
        right.activityCount - left.activityCount ||
        right.memberCount - left.memberCount,
    );

  await logToolDebug("Vector groups ranked.", {
    matchedGroups: matchedGroups.length,
    preview: matchedGroups.slice(0, 5).map((group) => ({
      groupId: group.groupId,
      activityCount: group.activityCount,
      vectorScore: group.vectorScore,
    })),
  });

  return {
    source: "mongo-group-search",
    searchMode: "vector",
    query,
    lastDays,
    matchedGroups: matchedGroups.length,
    groups: matchedGroups.slice(0, limit),
  };
}

export async function runAudienceMessageSearch(input: {
  searchMode?: string | null;
  query?: string | null;
  lastDays?: number | null;
  groupIds?: string[] | null;
  authorIds?: string[] | null;
  minReplies?: number | null;
  minReactions?: number | null;
  limit?: number | null;
}): Promise<Record<string, unknown>> {
  const searchMode = input.searchMode === "regex" ? "regex" : "vector";
  const query = normalizeWhitespace(input.query);

  await logToolDebug("Audience message search requested.", {
    searchMode,
    query,
    lastDays: input.lastDays ?? null,
    groupIds: Array.isArray(input.groupIds) ? input.groupIds.length : 0,
    authorIds: Array.isArray(input.authorIds) ? input.authorIds.length : 0,
    minReplies: input.minReplies ?? null,
    minReactions: input.minReactions ?? null,
  });

  return searchMode === "regex"
    ? runRegexMessageSearch({
        query,
        lastDays: input.lastDays ?? null,
        groupIds: input.groupIds ?? null,
        authorIds: input.authorIds ?? null,
        minReplies: input.minReplies ?? null,
        minReactions: input.minReactions ?? null,
        limit: input.limit ?? null,
      })
    : runVectorMessageSearch({
        query,
        lastDays: input.lastDays ?? null,
        groupIds: input.groupIds ?? null,
        authorIds: input.authorIds ?? null,
        minReplies: input.minReplies ?? null,
        minReactions: input.minReactions ?? null,
        limit: input.limit ?? null,
      });
}

export async function runAudienceGroupSearch(input: {
  searchMode?: string | null;
  query?: string | null;
  lastDays?: number | null;
  minActivity?: number | null;
  limit?: number | null;
}): Promise<Record<string, unknown>> {
  const searchMode = input.searchMode === "regex" ? "regex" : "vector";
  const query = normalizeWhitespace(input.query);

  await logToolDebug("Audience group search requested.", {
    searchMode,
    query,
    lastDays: input.lastDays ?? null,
    minActivity: input.minActivity ?? null,
  });

  return searchMode === "regex"
    ? runRegexGroupSearch({
        query,
        lastDays: input.lastDays ?? null,
        minActivity: input.minActivity ?? null,
        limit: input.limit ?? null,
      })
    : runVectorGroupSearch({
        query,
        lastDays: input.lastDays ?? null,
        minActivity: input.minActivity ?? null,
        limit: input.limit ?? null,
      });
}
