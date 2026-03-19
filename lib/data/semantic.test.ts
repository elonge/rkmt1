import test from "node:test";
import assert from "node:assert/strict";
import {
  buildGroupSearchText,
  buildGroupSemanticSourceRecord,
  buildMessageSearchText,
  buildMessageSemanticSourceRecord,
  isSemanticDocumentStale,
  mergeAudienceEvidence,
  rankInfluencerAggregates,
  resolveSemanticSyncLimit,
  type GroupAudienceEvidence,
  type MessageSemanticDoc,
} from "./semantic.ts";

test("buildGroupSearchText includes canonical audience fields", () => {
  const text = buildGroupSearchText({
    subject: "Beer Sheva Security Forum",
    description: "Volunteer coordination and local updates",
    tags: {
      topic: { tagValue: "Security updates" },
      region: { tagValue: "Beer Sheva" },
      demographic: { tagValue: "parents" },
      politicalLeaning: { tagValue: "center-right" },
      organization: { tagValue: "municipality" },
      lifeEvent: { tagValue: "reserve duty" },
      strategicMarkets: { tagValue: "community leaders" },
    },
  });

  assert.match(text, /Subject: Beer Sheva Security Forum/);
  assert.match(text, /Region: Beer Sheva/);
  assert.match(text, /Strategic markets: community leaders/);
});

test("buildGroupSemanticSourceRecord produces stable semantic metadata", () => {
  const record = buildGroupSemanticSourceRecord({
    groupId: "group_0001@g.us",
    subject: "Jerusalem Education Forum",
    description: "Organizing around education reform",
    memberCount: 420,
    lastActivityTimestamp: 1_710_000_000_000,
    lastTaggedTimestamp: 1_710_100_000_000,
    tags: {
      topic: { tagValue: "Education reform" },
      region: { tagValue: "Jerusalem" },
      demographic: { tagValue: "students" },
      politicalLeaning: { tagValue: "center-left" },
      organization: { tagValue: "ngo" },
      lifeEvent: { tagValue: "student elections" },
      strategicMarkets: { tagValue: "students" },
    },
  });

  assert.ok(record);
  assert.equal(record?.groupId, "group_0001@g.us");
  assert.equal(record?.memberCount, 420);
  assert.equal(record?.region, "Jerusalem");
  assert.equal(record?.topic, "Education reform");
  assert.ok(record?.contentHash);
});

test("buildMessageSearchText and source record include group context", () => {
  const text = buildMessageSearchText(
    {
      body: "Need volunteers near the university tonight",
    },
    {
      subject: "Beer Sheva Security Forum",
      topic: "Security updates",
      region: "Beer Sheva",
    },
  );

  assert.match(text, /Group topic: Security updates/);

  const record = buildMessageSemanticSourceRecord(
    {
      messageId: "msg_1",
      groupId: "group_1@g.us",
      authorId: "user_1@c.us",
      body: "Need volunteers near the university tonight",
      timestamp: 1_710_000_000_000,
      forwardingScore: 2,
      messageReactions: [{ reaction: "👍" }, { reaction: "✅" }],
      messageReplies: ["m2"],
    },
    {
      subject: "Beer Sheva Security Forum",
      topic: "Security updates",
      region: "Beer Sheva",
    },
  );

  assert.ok(record);
  assert.equal(record?.reactionCount, 2);
  assert.equal(record?.replyCount, 1);
  assert.equal(record?.groupRegion, "Beer Sheva");
  assert.match(record?.searchText ?? "", /Group subject: Beer Sheva Security Forum/);
});

test("isSemanticDocumentStale reacts to hash or version changes", () => {
  assert.equal(
    isSemanticDocumentStale(
      {
        groupId: "group_1@g.us",
        contentHash: "same",
        embeddingVersion: "v1",
      },
      { contentHash: "same" },
      "v1",
    ),
    false,
  );

  assert.equal(
    isSemanticDocumentStale(
      {
        groupId: "group_1@g.us",
        contentHash: "same",
        embeddingVersion: "v1",
      },
      { contentHash: "changed" },
      "v1",
    ),
    true,
  );

  assert.equal(
    isSemanticDocumentStale(
      {
        messageId: "msg_1",
        contentHash: "same",
        embeddingVersion: "v1",
      },
      { contentHash: "same" },
      "v2",
    ),
    true,
  );
});

test("resolveSemanticSyncLimit uses incremental defaults and backfill overrides", () => {
  assert.equal(resolveSemanticSyncLimit("incremental", 200), 200);
  assert.equal(resolveSemanticSyncLimit("groups_backfill", 200), null);
  assert.equal(resolveSemanticSyncLimit("groups_backfill", 200, 50), 50);
  assert.equal(resolveSemanticSyncLimit("messages_backfill", 500, 75), 75);
});

test("resolveSemanticSyncLimit rejects invalid overrides", () => {
  assert.throws(() => resolveSemanticSyncLimit("messages_backfill", 500, 0), {
    name: "SemanticConfigError",
    message: /positive integer/,
  });
  assert.throws(() => resolveSemanticSyncLimit("incremental", 500, 10), {
    name: "SemanticConfigError",
    message: /only supported for groups_backfill and messages_backfill/,
  });
});

test("mergeAudienceEvidence keeps ranking and derives recent message evidence", () => {
  const nowMs = 1_710_200_000_000;
  const groups: GroupAudienceEvidence[] = [
    {
      groupId: "group_1@g.us",
      subject: "Group One",
      memberCount: 120,
      lastActivityTimestamp: nowMs - 1_000,
      topic: "Security updates",
      region: "Beer Sheva",
      demographic: "parents",
      politicalLeaning: "center-right",
      score: 0.91,
      activity30d: 0,
      sampleMessages: [],
    },
    {
      groupId: "group_2@g.us",
      subject: "Group Two",
      memberCount: 500,
      lastActivityTimestamp: nowMs - 2_000,
      topic: "Education reform",
      region: "Jerusalem",
      demographic: "students",
      politicalLeaning: "center-left",
      score: 0.54,
      activity30d: 0,
      sampleMessages: [],
    },
  ];

  const evidence = new Map<string, MessageSemanticDoc[]>([
    [
      "group_1@g.us",
      [
        {
          messageId: "msg_1",
          groupId: "group_1@g.us",
          authorId: "user_1@c.us",
          searchText: "",
          embedding: [],
          embeddingModel: "text-embedding-3-small",
          embeddingVersion: "v1",
          contentHash: "a",
          indexedAt: new Date(nowMs),
          sourceUpdatedAt: new Date(nowMs),
          timestamp: nowMs - 1_000,
          reactionCount: 1,
          replyCount: 0,
          forwardingScore: 0,
          bodyPreview: "Recent matched message",
          groupSubject: "Group One",
          groupTopic: "Security updates",
          groupRegion: "Beer Sheva",
        },
        {
          messageId: "msg_2",
          groupId: "group_1@g.us",
          authorId: "user_2@c.us",
          searchText: "",
          embedding: [],
          embeddingModel: "text-embedding-3-small",
          embeddingVersion: "v1",
          contentHash: "b",
          indexedAt: new Date(nowMs),
          sourceUpdatedAt: new Date(nowMs),
          timestamp: nowMs - 40 * 24 * 60 * 60 * 1000,
          reactionCount: 0,
          replyCount: 0,
          forwardingScore: 0,
          bodyPreview: "Old matched message",
          groupSubject: "Group One",
          groupTopic: "Security updates",
          groupRegion: "Beer Sheva",
        },
      ],
    ],
  ]);

  const merged = mergeAudienceEvidence(groups, evidence, nowMs);

  assert.equal(merged[0]?.groupId, "group_1@g.us");
  assert.equal(merged[0]?.activity30d, 1);
  assert.deepEqual(merged[0]?.sampleMessages, ["Recent matched message", "Old matched message"]);
});

test("rankInfluencerAggregates sorts by score, breadth, then reactions", () => {
  const ranked = rankInfluencerAggregates([
    {
      authorId: "user_2",
      rawScore: 0.8,
      messageCount: 3,
      reactionCount: 4,
      replyCount: 1,
      groupCount: 2,
      displayName: "User Two",
    },
    {
      authorId: "user_1",
      rawScore: 0.8,
      messageCount: 2,
      reactionCount: 10,
      replyCount: 0,
      groupCount: 1,
      displayName: "User One",
    },
    {
      authorId: "user_3",
      rawScore: 0.9,
      messageCount: 1,
      reactionCount: 1,
      replyCount: 0,
      groupCount: 1,
      displayName: "User Three",
    },
  ]);

  assert.deepEqual(
    ranked.map((item) => item.authorId),
    ["user_3", "user_2", "user_1"],
  );
});
