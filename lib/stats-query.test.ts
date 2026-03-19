import test from "node:test";
import assert from "node:assert/strict";
import { ZodError } from "zod";
import {
  parseDbStatsQueryInput,
  serializeDbStatsQueryInputForToolSchema,
} from "./types.ts";

test("parseDbStatsQueryInput maps legacy metric requests to the new analytics DSL", () => {
  const parsed = parseDbStatsQueryInput({
    metric: "active_messages_last_days",
    lastDays: 14,
    filter: {
      politicalLeaning: "right",
    },
  });

  assert.deepEqual(parsed, {
    entity: "messages",
    aggregation: "count",
    measure: "records",
    groupBy: [],
    filters: {
      lastDays: 14,
      politicalLeanings: ["right"],
    },
  });
});

test("parseDbStatsQueryInput normalizes the structured analytics DSL", () => {
  const parsed = parseDbStatsQueryInput({
    entity: "message",
    aggregation: "breakdown",
    measure: "reactions",
    groupBy: ["politicalLeaning", "region"],
    filters: {
      lastDays: 30,
      groupIds: ["g1", "g2", "g1"],
      hasMedia: true,
    },
    sort: {
      by: "value",
      direction: "desc",
    },
    limit: 25,
  });

  assert.deepEqual(parsed, {
    entity: "messages",
    aggregation: "distribution",
    measure: "reaction_count",
    groupBy: ["political_leaning", "region"],
    filters: {
      lastDays: 30,
      groupIds: ["g1", "g2"],
      hasMedia: true,
    },
    sort: {
      by: "value",
      direction: "desc",
    },
    limit: 25,
  });
});

test("parseDbStatsQueryInput rejects group-only filters for message stats queries", () => {
  assert.throws(
    () =>
      parseDbStatsQueryInput({
        entity: "messages",
        aggregation: "count",
        measure: "records",
        filters: {
          announcementOnly: true,
        },
      }),
    (error) => {
      assert.ok(error instanceof ZodError);
      assert.match(error.issues[0]?.message ?? "", /announcementOnly/);
      return true;
    },
  );
});

test("parseDbStatsQueryInput accepts planner-facing messageFilters", () => {
  const parsed = parseDbStatsQueryInput({
    entity: "messages",
    aggregation: "count",
    measure: "records",
    messageFilters: {
      lastDays: 10,
      minReplies: 3,
      hasMedia: true,
    },
  });

  assert.deepEqual(parsed, {
    entity: "messages",
    aggregation: "count",
    measure: "records",
    groupBy: [],
    filters: {
      lastDays: 10,
      minReplies: 3,
      hasMedia: true,
    },
  });
});

test("serializeDbStatsQueryInputForToolSchema maps canonical filters back to entity-specific planner fields", () => {
  const serialized = serializeDbStatsQueryInputForToolSchema({
    entity: "groups",
    aggregation: "top_values",
    measure: "member_count",
    groupBy: ["group"],
    filters: {
      announcementOnly: true,
      membershipApproval: false,
    },
    limit: 5,
  });

  assert.deepEqual(serialized, {
    entity: "groups",
    aggregation: "top_values",
    measure: "member_count",
    groupBy: ["group"],
    groupFilters: {
      announcementOnly: true,
      membershipApproval: false,
    },
    limit: 5,
  });
});
