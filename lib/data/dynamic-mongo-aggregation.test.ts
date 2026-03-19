import test from "node:test";
import assert from "node:assert/strict";
import {
  DynamicMongoAggregationError,
  materializeGeneratedPipeline,
  normalizeDynamicAggregationIntentPlan,
  prepareDynamicAggregationPipeline,
} from "./dynamic-mongo-aggregation.ts";

const collectionNames = {
  groupsCollection: "groups",
  usersCollection: "userprofiles",
  messagesCollection: "messages",
  groupSemanticCollection: "group_semantic",
  messageSemanticCollection: "message_semantic",
};

test("prepareDynamicAggregationPipeline appends a safety limit to non-aggregated pipelines", () => {
  const prepared = prepareDynamicAggregationPipeline(
    [
      { $match: { status: "active" } },
      { $project: { userId: 1, _id: 0 } },
    ],
    collectionNames,
  );

  assert.equal(prepared.forcedLimitApplied, true);
  assert.deepEqual(prepared.pipeline.at(-1), { $limit: 1000 });
});

test("prepareDynamicAggregationPipeline does not append a limit to grouped statistical pipelines", () => {
  const prepared = prepareDynamicAggregationPipeline(
    [
      { $group: { _id: "$status", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
    ],
    collectionNames,
  );

  assert.equal(prepared.forcedLimitApplied, false);
  assert.notDeepEqual(prepared.pipeline.at(-1), { $limit: 1000 });
});

test("prepareDynamicAggregationPipeline rejects write stages", () => {
  assert.throws(
    () =>
      prepareDynamicAggregationPipeline(
        [{ $merge: { into: "other_collection" } }],
        collectionNames,
      ),
    (error) => {
      assert.ok(error instanceof DynamicMongoAggregationError);
      assert.match(error.message, /\$merge/);
      return true;
    },
  );
});

test('prepareDynamicAggregationPipeline rejects "$$NOW" runtime expressions', () => {
  assert.throws(
    () =>
      prepareDynamicAggregationPipeline(
        [
          {
            $match: {
              timestamp: {
                $gte: {
                  $subtract: ["$$NOW", 2592000000],
                },
              },
            },
          },
        ],
        collectionNames,
      ),
    (error) => {
      assert.ok(error instanceof DynamicMongoAggregationError);
      assert.match(error.message, /\$\$NOW/);
      return true;
    },
  );
});

test("prepareDynamicAggregationPipeline normalizes logical lookup aliases to configured collections", () => {
  const prepared = prepareDynamicAggregationPipeline(
    [
      {
        $lookup: {
          from: "groups",
          localField: "groupId",
          foreignField: "groupId",
          as: "groupDoc",
        },
      },
    ],
    {
      ...collectionNames,
      groupsCollection: "custom_groups",
    },
  );

  assert.deepEqual(prepared.pipeline[0], {
    $lookup: {
      from: "custom_groups",
      localField: "groupId",
      foreignField: "groupId",
      as: "groupDoc",
    },
  });
});

test("materializeGeneratedPipeline converts wrapped stages into a Mongo pipeline", () => {
  const parsed = materializeGeneratedPipeline([
    { operator: "$match", value: { status: "active" } },
    { operator: "$limit", value: 10 },
  ]);

  assert.deepEqual(parsed, [
    { $match: { status: "active" } },
    { $limit: 10 },
  ]);
});

test("materializeGeneratedPipeline recursively converts nested wrapped pipelines", () => {
  const parsed = materializeGeneratedPipeline([
    {
      operator: "$lookup",
      value: {
        from: "groups",
        let: { currentGroupId: "$groupId" },
        pipeline: [
          {
            operator: "$match",
            value: {
              $expr: {
                $eq: ["$groupId", "$$currentGroupId"],
              },
            },
          },
          {
            operator: "$project",
            value: { subject: 1, _id: 0 },
          },
        ],
        as: "groupDoc",
      },
    },
  ]);

  assert.deepEqual(parsed, [
    {
      $lookup: {
        from: "groups",
        let: { currentGroupId: "$groupId" },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: ["$groupId", "$$currentGroupId"],
              },
            },
          },
          {
            $project: { subject: 1, _id: 0 },
          },
        ],
        as: "groupDoc",
      },
    },
  ]);
});

test("normalizeDynamicAggregationIntentPlan enriches joins and timeframe cutoff", () => {
  const normalized = normalizeDynamicAggregationIntentPlan(
    {
      rootCollection: "messages",
      questionType: "distribution",
      summary: "Count distinct active authors by group political leaning.",
      populationDescription: "Distinct message authors active in the last 30 days.",
      joins: [
        {
          alias: "group",
          relationship: "messages_to_groups_by_groupId",
        },
      ],
      timeframe: {
        fieldRef: "root.timestamp",
        lastDays: 30,
      },
      filters: [],
      dimensions: [
        {
          label: "politicalLeaning",
          fieldRef: "group.tags.politicalLeaning.tagValue",
          nullBucketLabel: "Unknown",
        },
      ],
      measure: {
        aggregation: "count_distinct",
        fieldRef: "root.authorId",
        outputFieldName: "activeUsers",
      },
      output: {
        includeTotals: true,
        totalFieldName: "totalActiveUsers",
        includePercentages: true,
        distributionFieldName: "distribution",
      },
      sort: [{ by: "measure", direction: "desc", dimensionLabel: null }],
      assumptions: [],
    },
    1_700_000_000_000,
  );

  assert.equal(normalized.joins[0]?.targetCollection, "groups");
  assert.equal(normalized.joins[0]?.localField, "groupId");
  assert.equal(normalized.timeframe?.cutoffTimestampMs, 1_697_408_000_000);
});

test("normalizeDynamicAggregationIntentPlan rejects field refs that require an undeclared join", () => {
  assert.throws(
    () =>
      normalizeDynamicAggregationIntentPlan({
        rootCollection: "messages",
        questionType: "distribution",
        summary: "Count distinct active authors by group political leaning.",
        populationDescription: "Distinct message authors active in the last 30 days.",
        joins: [],
        timeframe: {
          fieldRef: "root.timestamp",
          lastDays: 30,
        },
        filters: [],
        dimensions: [
          {
            label: "politicalLeaning",
            fieldRef: "group.tags.politicalLeaning.tagValue",
            nullBucketLabel: "Unknown",
          },
        ],
        measure: {
          aggregation: "count_distinct",
          fieldRef: "root.authorId",
          outputFieldName: "activeUsers",
        },
        output: {
          includeTotals: true,
          totalFieldName: "totalActiveUsers",
          includePercentages: true,
          distributionFieldName: "distribution",
        },
        sort: [{ by: "measure", direction: "desc", dimensionLabel: null }],
        assumptions: [],
      }),
    (error) => {
      assert.ok(error instanceof DynamicMongoAggregationError);
      assert.match(error.message, /without declaring the required join/);
      return true;
    },
  );
});

test("normalizeDynamicAggregationIntentPlan accepts nullables in the agent-facing intent schema", () => {
  const normalized = normalizeDynamicAggregationIntentPlan({
    rootCollection: "messages",
    questionType: "count",
    summary: "Count active messages in the last 30 days.",
    populationDescription: "Messages active in the last 30 days.",
    joins: [],
    timeframe: {
      fieldRef: "root.timestamp",
      lastDays: 30,
    },
    filters: [
      {
        fieldRef: "root.messageMedia",
        operator: "exists",
        value: null,
        values: null,
        exists: true,
      },
    ],
    dimensions: [],
    measure: {
      aggregation: "count",
      fieldRef: null,
      outputFieldName: "messageCount",
    },
    output: {
      includeTotals: true,
      totalFieldName: "totalMessages",
      includePercentages: false,
      distributionFieldName: null,
    },
    sort: [
      {
        by: "measure",
        direction: "desc",
        dimensionLabel: null,
      },
    ],
    assumptions: [],
  });

  assert.equal(normalized.measure.fieldRef, null);
  assert.equal(normalized.filters[0]?.value, null);
  assert.equal(normalized.sort[0]?.dimensionLabel, null);
  assert.ok(normalized.timeframe);
});
