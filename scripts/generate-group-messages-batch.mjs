#!/usr/bin/env node

import { createReadStream, existsSync, mkdirSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { MongoClient, ObjectId } from "mongodb";
import OpenAI from "openai";

const DEFAULT_MESSAGES_PER_GROUP = 300;
const DEFAULT_CHUNK_SIZE = 50;
const DEFAULT_MAX_AUTHORS = 60;
const DEFAULT_POLL_INTERVAL_MS = 30_000;
const DEFAULT_MAX_WAIT_MS = 6 * 60 * 60 * 1000;
const DEFAULT_LOOKBACK_DAYS = 30;
const DEFAULT_MODEL = process.env.OPENAI_MODEL_FOR_DUMMY_MESSAGES?.trim() || "gpt-4.1-mini";

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const MINUTE_IN_MS = 60 * 1000;
const REACTION_POOL = ["👍", "🔥", "👏", "❤️", "🤝", "✅", "👀", "🎯"];
const MEDIA_MIME_TYPES = ["none", "image/jpeg", "image/png", "video/mp4", "application/pdf"];

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

loadEnvFileIfPresent(path.join(repoRoot, ".env.local"));
loadEnvFileIfPresent(path.join(repoRoot, ".env"));

function loadEnvFileIfPresent(filePath) {
  if (!existsSync(filePath) || typeof process.loadEnvFile !== "function") {
    return;
  }

  process.loadEnvFile(filePath);
}

function printHelp() {
  console.log(`Usage:
  npm run generate:group-messages -- [options]
  node scripts/generate-group-messages-batch.mjs [options]

Options:
  --limit <n>              Limit the number of groups to process.
  --skip <n>               Skip the first N groups.
  --messages-per-group <n> Number of synthetic messages to generate per group. Default: ${DEFAULT_MESSAGES_PER_GROUP}
  --chunk-size <n>         Messages per OpenAI batch request. Default: ${DEFAULT_CHUNK_SIZE}
  --max-authors <n>        Max existing user profiles to expose per group. Default: ${DEFAULT_MAX_AUTHORS}
  --model <id>             OpenAI model to use. Default: ${DEFAULT_MODEL}
  --poll-interval-ms <n>   Batch polling interval. Default: ${DEFAULT_POLL_INTERVAL_MS}
  --max-wait-ms <n>        Max time to wait for batch completion. Default: ${DEFAULT_MAX_WAIT_MS}
  --lookback-days <n>      Synthetic timestamp window per group. Default: ${DEFAULT_LOOKBACK_DAYS}
  --groups-collection <s>  Override groups collection name.
  --users-collection <s>   Override userprofiles collection name.
  --messages-collection <s> Override messages collection name.
  --recover-run <id|path>  Re-import an existing run from .runtime/synthetic-message-batches without creating a new batch.
  --submit-only            Create the OpenAI batch and exit before downloading/importing results.
  --help, -h               Show this help.

Environment:
  OPENAI_API_KEY
  OPENAI_MODEL_FOR_DUMMY_MESSAGES
  MONGO_URI
  MONGO_DB_NAME
  MONGO_GROUPS_COLLECTION
  MONGO_USERS_COLLECTION
  MONGO_MESSAGES_COLLECTION

Notes:
  - The inserted document shape intentionally mirrors schemas/message.model.ts.
  - body_embedding is omitted.
  - Each run writes its input/output artifacts under .runtime/synthetic-message-batches/<runId>/.
`);
}

function parseArgs(argv) {
  const parsed = {
    skip: 0,
    messagesPerGroup: DEFAULT_MESSAGES_PER_GROUP,
    chunkSize: DEFAULT_CHUNK_SIZE,
    maxAuthors: DEFAULT_MAX_AUTHORS,
    model: DEFAULT_MODEL,
    pollIntervalMs: DEFAULT_POLL_INTERVAL_MS,
    maxWaitMs: DEFAULT_MAX_WAIT_MS,
    lookbackDays: DEFAULT_LOOKBACK_DAYS,
    submitOnly: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--help" || arg === "-h") {
      parsed.help = true;
      continue;
    }

    if (arg === "--submit-only") {
      parsed.submitOnly = true;
      continue;
    }

    if (!arg.startsWith("--")) {
      throw new Error(`Unexpected argument: ${arg}`);
    }

    const key = arg.slice(2);
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`Missing value for ${arg}`);
    }

    parsed[key] = value;
    index += 1;
  }

  parsed.limit = parseOptionalPositiveInteger(parsed.limit, "--limit");
  parsed.skip = parseNonNegativeInteger(parsed.skip, "--skip");
  parsed.messagesPerGroup = parsePositiveInteger(parsed.messagesPerGroup, "--messages-per-group");
  parsed.chunkSize = parsePositiveInteger(parsed.chunkSize, "--chunk-size");
  parsed.maxAuthors = parsePositiveInteger(parsed.maxAuthors, "--max-authors");
  parsed.pollIntervalMs = parsePositiveInteger(parsed.pollIntervalMs, "--poll-interval-ms");
  parsed.maxWaitMs = parsePositiveInteger(parsed.maxWaitMs, "--max-wait-ms");
  parsed.lookbackDays = parsePositiveInteger(parsed.lookbackDays, "--lookback-days");

  if (parsed.chunkSize > parsed.messagesPerGroup) {
    parsed.chunkSize = parsed.messagesPerGroup;
  }

  return parsed;
}

function parsePositiveInteger(value, label) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive integer.`);
  }
  return parsed;
}

function parseOptionalPositiveInteger(value, label) {
  if (value === undefined) {
    return undefined;
  }
  return parsePositiveInteger(value, label);
}

function parseNonNegativeInteger(value, label) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${label} must be a non-negative integer.`);
  }
  return parsed;
}

function inferDbNameFromUri(uri) {
  const match = uri.match(/^mongodb(?:\+srv)?:\/\/(?:[^@/]+@)?[^/]+\/([^?]+)/i);
  if (!match) {
    return null;
  }

  const candidate = decodeURIComponent(match[1]).trim();
  return candidate.length > 0 ? candidate : null;
}

function readMongoConfig(overrides) {
  const uri = process.env.MONGO_URI?.trim();
  const dbName =
    process.env.MONGO_DB_NAME?.trim() ||
    process.env.DB_NAME?.trim() ||
    process.env.db_name?.trim() ||
    (uri ? inferDbNameFromUri(uri) : null);

  if (!uri) {
    throw new Error("Missing MONGO_URI in environment.");
  }

  if (!dbName) {
    throw new Error(
      "Missing Mongo DB name. Set MONGO_DB_NAME, DB_NAME, db_name, or include it in MONGO_URI.",
    );
  }

  return {
    uri,
    dbName,
    groupsCollection:
      overrides.groupsCollection ||
      process.env.MONGO_GROUPS_COLLECTION?.trim() ||
      "groups",
    usersCollection:
      overrides.usersCollection ||
      process.env.MONGO_USERS_COLLECTION?.trim() ||
      "userprofiles",
    messagesCollection:
      overrides.messagesCollection ||
      process.env.MONGO_MESSAGES_COLLECTION?.trim() ||
      "messages3",
  };
}

function ensureOpenAiConfigured() {
  if (!process.env.OPENAI_API_KEY?.trim()) {
    throw new Error("Missing OPENAI_API_KEY in environment.");
  }
}

function hash(value) {
  let acc = 2166136261;
  for (const char of String(value)) {
    acc ^= char.charCodeAt(0);
    acc = Math.imul(acc, 16777619);
  }
  return acc >>> 0;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function toObjectIdString(value) {
  if (value instanceof ObjectId) {
    return value.toHexString();
  }

  if (value && typeof value === "object" && "$oid" in value && typeof value.$oid === "string") {
    return value.$oid;
  }

  return String(value);
}

function readMembershipGroupId(membership) {
  if (!membership || typeof membership !== "object") {
    return null;
  }

  if (membership.group === undefined || membership.group === null) {
    return null;
  }

  return toObjectIdString(membership.group);
}

function getMembershipForGroup(user, groupObjectIdString) {
  if (!Array.isArray(user.groups)) {
    return null;
  }

  for (const membership of user.groups) {
    if (readMembershipGroupId(membership) === groupObjectIdString) {
      return membership;
    }
  }

  return null;
}

function slugify(value) {
  return String(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 32);
}

function buildSchemaName(groupId, chunkIndex) {
  return `synthetic_msgs_${slugify(groupId)}_${String(chunkIndex + 1).padStart(2, "0")}`;
}

function sampleUsersForGroup(users, group, maxAuthors) {
  const groupObjectIdString = toObjectIdString(group._id);
  const withMembership = users
    .map((user) => ({
      user,
      membership: getMembershipForGroup(user, groupObjectIdString),
    }))
    .filter((entry) => entry.membership);

  const joined = withMembership.filter(
    (entry) => String(entry.membership.status || "").toUpperCase() !== "LEFT",
  );
  const pool = joined.length > 0 ? joined : withMembership;

  pool.sort((left, right) => {
    const leftScore = hash(`${group.groupId}:${left.user.userId}`);
    const rightScore = hash(`${group.groupId}:${right.user.userId}`);
    if (leftScore !== rightScore) {
      return leftScore - rightScore;
    }
    return String(left.user.userId).localeCompare(String(right.user.userId));
  });

  return pool.slice(0, maxAuthors).map((entry) => ({
    userId: String(entry.user.userId),
    name: typeof entry.user.name === "string" ? entry.user.name : "",
    status: typeof entry.user.status === "string" ? entry.user.status : "",
    role: typeof entry.membership.role === "string" ? entry.membership.role : "MEMBER",
    membershipStatus:
      typeof entry.membership.status === "string" ? entry.membership.status : "JOINED",
  }));
}

function describeGroupForPrompt(group) {
  const tags = group.tags && typeof group.tags === "object" ? group.tags : {};
  return {
    groupId: group.groupId,
    subject: group.subject ?? "",
    description: group.description ?? "",
    politicalLeaning: tags?.politicalLeaning?.tagValue ?? "",
    demographic: tags?.demographic?.tagValue ?? "",
    topic: tags?.topic?.tagValue ?? "",
    region: tags?.region?.tagValue ?? "",
    organization: tags?.organization?.tagValue ?? "",
    lifeEvent: tags?.lifeEvent?.tagValue ?? "",
    strategicMarkets: tags?.strategicMarkets?.tagValue ?? "",
    memberCount: group.memberCount ?? null,
    announcementOnly: group.announcementOnly ?? null,
    avgMessagesPerDay30d: group.avgMessagesPerDay30d ?? null,
    avgRepliesPerMessage30d: group.avgRepliesPerMessage30d ?? null,
    avgReactionsPerMessage30d: group.avgReactionsPerMessage30d ?? null,
    activeMemberPercentage30d: group.activeMemberPercentage30d ?? null,
  };
}

function buildChunkSchema(authorIds, chunkSize) {
  return {
    type: "object",
    additionalProperties: false,
    required: ["messages"],
    properties: {
      messages: {
        type: "array",
        minItems: chunkSize,
        maxItems: chunkSize,
        items: {
          type: "object",
          additionalProperties: false,
          required: ["authorId", "body", "replyToIndex", "forwardingScore", "reactions", "mediaMimeType"],
          properties: {
            authorId: {
              type: "string",
              enum: authorIds,
            },
            body: {
              type: "string",
              minLength: 1,
              maxLength: 700,
            },
            replyToIndex: {
              type: "integer",
              minimum: -1,
              maximum: chunkSize - 1,
            },
            forwardingScore: {
              type: "integer",
              minimum: 0,
              maximum: 6,
            },
            reactions: {
              type: "array",
              maxItems: 3,
              items: {
                type: "object",
                additionalProperties: false,
                required: ["senderId", "reaction"],
                properties: {
                  senderId: {
                    type: "string",
                    enum: authorIds,
                  },
                  reaction: {
                    type: "string",
                    enum: REACTION_POOL,
                  },
                },
              },
            },
            mediaMimeType: {
              type: "string",
              enum: MEDIA_MIME_TYPES,
            },
          },
        },
      },
    },
  };
}

function buildBatchInstructions() {
  return [
    "You are generating synthetic WhatsApp group messages for data seeding.",
    "Return strict JSON only.",
    "Generate realistic chat content that is tightly relevant to the provided group context.",
    "Use only the provided authorIds.",
    "Messages must read like chat messages, not reports or summaries.",
    "Mix coordination, opinions, questions, reactions, short updates, and follow-ups.",
    "Keep most messages short to medium length.",
    "If a message is not replying to an earlier message in the same chunk, set replyToIndex to -1.",
    "Only reference earlier messages from the same chunk in replyToIndex.",
    'Set mediaMimeType to "none" when there is no media.',
    "Keep forwardingScore conservative unless the content feels obviously forwarded.",
    "Do not invent URLs, phone numbers, or external facts unless naturally implied by the group context.",
    "Do not mention that the data is synthetic.",
  ].join(" ");
}

function buildChunkPrompt(group, authors, chunkSize, chunkIndex, totalChunks, messagesPerGroup) {
  const rosterLines = authors.map((author) => {
    const fragments = [author.userId];
    if (author.name) {
      fragments.push(`name=${author.name}`);
    }
    if (author.status) {
      fragments.push(`status=${author.status}`);
    }
    fragments.push(`role=${author.role}`);
    return `- ${fragments.join(" | ")}`;
  });

  return [
    `Generate chunk ${chunkIndex + 1} of ${totalChunks} for this group chat.`,
    `This chunk must contain exactly ${chunkSize} messages out of a total target of ${messagesPerGroup} messages for the group.`,
    "",
    "Group context:",
    JSON.stringify(describeGroupForPrompt(group), null, 2),
    "",
    "Allowed authors:",
    rosterLines.join("\n"),
    "",
    "Requirements:",
    "- Output messages in chronological order within this chunk.",
    "- Make the conversation feel like a real group chat for this audience and topic.",
    "- Use a range of authors from the allowed list.",
    "- Reactions should be sparse and realistic.",
    "- Replies should point only to earlier messages in the same chunk.",
  ].join("\n");
}

function buildRequestLine(context, options) {
  const authorIds = context.authors.map((author) => author.userId);
  const schema = buildChunkSchema(authorIds, context.chunkSize);

  return {
    custom_id: context.customId,
    method: "POST",
    url: "/v1/responses",
    body: {
      model: options.model,
      store: false,
      temperature: 0.8,
      max_output_tokens: 12_000,
      instructions: buildBatchInstructions(),
      input: buildChunkPrompt(
        context.group,
        context.authors,
        context.chunkSize,
        context.chunkIndex,
        context.totalChunks,
        options.messagesPerGroup,
      ),
      metadata: {
        runId: context.runId,
        groupId: context.group.groupId,
        chunkIndex: String(context.chunkIndex),
      },
      text: {
        format: {
          type: "json_schema",
          name: buildSchemaName(context.group.groupId, context.chunkIndex),
          strict: true,
          schema,
        },
      },
    },
  };
}

function resolveRunDirectory(runRef) {
  if (!runRef || typeof runRef !== "string") {
    throw new Error("Missing --recover-run value.");
  }

  const directPath = path.resolve(process.cwd(), runRef);
  if (existsSync(directPath)) {
    return directPath.endsWith("manifest.json") ? path.dirname(directPath) : directPath;
  }

  const runtimePath = path.join(repoRoot, ".runtime", "synthetic-message-batches", runRef);
  if (existsSync(runtimePath)) {
    return runtimePath;
  }

  throw new Error(`Could not find run artifacts for "${runRef}".`);
}

function splitIntoChunkSizes(total, chunkSize) {
  const sizes = [];
  let remaining = total;
  while (remaining > 0) {
    const nextSize = Math.min(remaining, chunkSize);
    sizes.push(nextSize);
    remaining -= nextSize;
  }
  return sizes;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForBatchCompletion(client, batchId, pollIntervalMs, maxWaitMs) {
  const startedAt = Date.now();

  while (true) {
    const batch = await client.batches.retrieve(batchId);
    if (batch.status === "completed") {
      return batch;
    }

    if (
      batch.status === "failed" ||
      batch.status === "expired" ||
      batch.status === "cancelled"
    ) {
      const errorSummary = batch.errors?.data
        ?.map((issue) => issue.message || issue.code || "Unknown batch error")
        .join("; ");
      throw new Error(
        `Batch ${batch.id} ended with status ${batch.status}${errorSummary ? `: ${errorSummary}` : ""}`,
      );
    }

    if (Date.now() - startedAt > maxWaitMs) {
      throw new Error(`Timed out waiting for batch ${batch.id} after ${maxWaitMs}ms.`);
    }

    console.log(
      `Batch ${batch.id} status=${batch.status} completed=${batch.request_counts?.completed ?? 0}/${batch.request_counts?.total ?? "?"}`,
    );
    await sleep(pollIntervalMs);
  }
}

async function downloadFileText(client, fileId, destinationPath) {
  const response = await client.files.content(fileId);
  const text = await response.text();
  await writeFile(destinationPath, text, "utf8");
  return text;
}

function parseJsonLines(rawText) {
  return rawText
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line));
}

function extractResponseOutputText(responseBody) {
  if (responseBody && typeof responseBody.output_text === "string" && responseBody.output_text.length > 0) {
    return responseBody.output_text;
  }

  const output = Array.isArray(responseBody?.output) ? responseBody.output : [];
  for (const item of output) {
    const content = Array.isArray(item?.content) ? item.content : [];
    for (const part of content) {
      if (typeof part?.text === "string" && part.text.length > 0) {
        return part.text;
      }
    }
  }

  throw new Error("Batch response body did not include output_text.");
}

function recoverMessagesFromTruncatedChunkText(rawText) {
  const messagesKeyIndex = rawText.indexOf('"messages"');
  if (messagesKeyIndex < 0) {
    throw new Error("Chunk output did not include a messages array.");
  }

  const arrayStartIndex = rawText.indexOf("[", messagesKeyIndex);
  if (arrayStartIndex < 0) {
    throw new Error("Chunk output did not include a messages array start.");
  }

  const recoveredMessages = [];
  let objectStartIndex = -1;
  let objectDepth = 0;
  let inString = false;
  let isEscaped = false;

  for (let index = arrayStartIndex + 1; index < rawText.length; index += 1) {
    const char = rawText[index];

    if (objectStartIndex < 0) {
      if (char === "]") {
        break;
      }

      if (char === "{") {
        objectStartIndex = index;
        objectDepth = 1;
        inString = false;
        isEscaped = false;
      }
      continue;
    }

    if (inString) {
      if (isEscaped) {
        isEscaped = false;
        continue;
      }

      if (char === "\\") {
        isEscaped = true;
        continue;
      }

      if (char === '"') {
        inString = false;
      }
      continue;
    }

    if (char === '"') {
      inString = true;
      continue;
    }

    if (char === "{") {
      objectDepth += 1;
      continue;
    }

    if (char !== "}") {
      continue;
    }

    objectDepth -= 1;
    if (objectDepth !== 0) {
      continue;
    }

    const objectText = rawText.slice(objectStartIndex, index + 1);
    recoveredMessages.push(JSON.parse(objectText));
    objectStartIndex = -1;
  }

  if (recoveredMessages.length === 0) {
    throw new Error("Unable to recover any complete messages from truncated chunk output.");
  }

  return {
    messages: recoveredMessages,
  };
}

function validateGeneratedChunk(rawChunk, context, options = {}) {
  const allowPartial = options.allowPartial === true;
  if (!rawChunk || typeof rawChunk !== "object" || Array.isArray(rawChunk)) {
    throw new Error(`Chunk ${context.customId} returned invalid JSON root.`);
  }

  if (!Array.isArray(rawChunk.messages)) {
    throw new Error(`Chunk ${context.customId} did not return a messages array.`);
  }

  if (allowPartial) {
    if (rawChunk.messages.length < 1 || rawChunk.messages.length > context.chunkSize) {
      throw new Error(
        `Chunk ${context.customId} returned ${rawChunk.messages.length} recoverable messages; expected between 1 and ${context.chunkSize}.`,
      );
    }
  } else if (rawChunk.messages.length !== context.chunkSize) {
    throw new Error(
      `Chunk ${context.customId} returned ${Array.isArray(rawChunk.messages) ? rawChunk.messages.length : "invalid"} messages; expected ${context.chunkSize}.`,
    );
  }

  const allowedAuthors = new Set(context.authors.map((author) => author.userId));

  return rawChunk.messages.map((message, messageIndex) => {
    if (!message || typeof message !== "object" || Array.isArray(message)) {
      throw new Error(`Chunk ${context.customId} message ${messageIndex} is not an object.`);
    }

    const authorId = String(message.authorId || "").trim();
    const body = String(message.body || "").trim();
    const rawReplyToIndex = Number(message.replyToIndex);
    const forwardingScoreNumber = Number(message.forwardingScore);
    const mediaMimeType = String(message.mediaMimeType || "none").trim();
    const rawReactions = Array.isArray(message.reactions) ? message.reactions : [];

    if (!allowedAuthors.has(authorId)) {
      throw new Error(`Chunk ${context.customId} message ${messageIndex} used unknown authorId "${authorId}".`);
    }

    if (body.length === 0) {
      throw new Error(`Chunk ${context.customId} message ${messageIndex} has an empty body.`);
    }

    let replyToIndex = rawReplyToIndex;
    if (
      !Number.isInteger(replyToIndex) ||
      replyToIndex < -1 ||
      replyToIndex >= context.chunkSize ||
      replyToIndex >= messageIndex
    ) {
      replyToIndex = -1;
    }

    if (!Number.isInteger(forwardingScoreNumber) || forwardingScoreNumber < 0 || forwardingScoreNumber > 6) {
      throw new Error(
        `Chunk ${context.customId} message ${messageIndex} has invalid forwardingScore ${message.forwardingScore}.`,
      );
    }

    if (!MEDIA_MIME_TYPES.includes(mediaMimeType)) {
      throw new Error(
        `Chunk ${context.customId} message ${messageIndex} has invalid mediaMimeType "${mediaMimeType}".`,
      );
    }

    const seenReactions = new Set();
    const reactions = [];
    for (const reaction of rawReactions.slice(0, 3)) {
      const senderId = String(reaction?.senderId || "").trim();
      const reactionValue = String(reaction?.reaction || "").trim();

      if (!allowedAuthors.has(senderId)) {
        throw new Error(
          `Chunk ${context.customId} message ${messageIndex} has reaction sender "${senderId}" outside the allowed roster.`,
        );
      }

      if (!REACTION_POOL.includes(reactionValue)) {
        throw new Error(
          `Chunk ${context.customId} message ${messageIndex} has invalid reaction "${reactionValue}".`,
        );
      }

      const dedupeKey = `${senderId}:${reactionValue}`;
      if (senderId === authorId || seenReactions.has(dedupeKey)) {
        continue;
      }

      seenReactions.add(dedupeKey);
      reactions.push({
        senderId,
        reaction: reactionValue,
      });
    }

    return {
      authorId,
      body: body.slice(0, 700),
      replyToIndex,
      forwardingScore: forwardingScoreNumber,
      reactions,
      mediaMimeType,
    };
  });
}

function parseGeneratedChunkResponse(responseBody, context) {
  const outputText = extractResponseOutputText(responseBody);

  try {
    const outputPayload = JSON.parse(outputText);
    return {
      messages: validateGeneratedChunk(outputPayload, context),
      mode: "full",
      incompleteReason: responseBody?.incomplete_details?.reason ?? null,
    };
  } catch (parseError) {
    const recoveredPayload = recoverMessagesFromTruncatedChunkText(outputText);
    return {
      messages: validateGeneratedChunk(recoveredPayload, context, { allowPartial: true }),
      mode: "partial",
      incompleteReason: responseBody?.incomplete_details?.reason ?? null,
      recoveryError: parseError instanceof Error ? parseError.message : String(parseError),
    };
  }
}

function buildTimestampSeries(group, count, lookbackDays) {
  const now = Date.now();
  const desiredEnd =
    typeof group.lastActivityTimestamp === "number" && Number.isFinite(group.lastActivityTimestamp)
      ? Math.min(group.lastActivityTimestamp, now)
      : now;
  const minStartFromCreation =
    typeof group.creationTimestamp === "number" && Number.isFinite(group.creationTimestamp)
      ? group.creationTimestamp
      : desiredEnd - lookbackDays * DAY_IN_MS;
  const start = Math.max(minStartFromCreation, desiredEnd - lookbackDays * DAY_IN_MS);
  const range = Math.max(desiredEnd - start, count * MINUTE_IN_MS);

  let previousTimestamp = start;
  const timestamps = [];

  for (let index = 0; index < count; index += 1) {
    const base = start + Math.floor(((index + 1) / (count + 1)) * range);
    const jitter = (hash(`${group.groupId}:timestamp:${index}`) % (40 * MINUTE_IN_MS)) - 20 * MINUTE_IN_MS;
    const candidate = Math.max(previousTimestamp + MINUTE_IN_MS, base + jitter);
    const bounded = Math.min(candidate, desiredEnd + index * MINUTE_IN_MS);
    timestamps.push(bounded);
    previousTimestamp = bounded;
  }

  return timestamps;
}

function buildMessageDocuments(groupContext, chunks, runId, lookbackDays) {
  const plannedChunkOffsets = new Map();
  let plannedCursor = 0;
  for (const chunk of [...groupContext.chunks].sort((left, right) => left.chunkIndex - right.chunkIndex)) {
    plannedChunkOffsets.set(chunk.chunkIndex, plannedCursor);
    plannedCursor += chunk.chunkSize;
  }

  const timestamps = buildTimestampSeries(groupContext.group, plannedCursor, lookbackDays);
  const documents = [];

  for (const chunk of chunks) {
    for (let localIndex = 0; localIndex < chunk.messages.length; localIndex += 1) {
      const message = chunk.messages[localIndex];
      const plannedOffset = plannedChunkOffsets.get(chunk.chunkIndex) ?? 0;
      const globalIndex = plannedOffset + localIndex;
      const timestamp = timestamps[globalIndex];
      const firstImported = new Date(timestamp + MINUTE_IN_MS);
      const lastImported = new Date(timestamp + 5 * MINUTE_IN_MS);
      // This document shape intentionally mirrors schemas/message.model.ts.
      const messageId = `synthetic:${runId}:${slugify(groupContext.group.groupId)}:${String(globalIndex + 1).padStart(4, "0")}`;
      const document = {
        _id: new ObjectId(),
        timestamp,
        messageId,
        groupId: groupContext.group.groupId,
        authorId: message.authorId,
        body: message.body,
        messageReactions: message.reactions.map((reaction, reactionIndex) => ({
          timestamp: timestamp + (reactionIndex + 1) * 2 * MINUTE_IN_MS,
          senderId: reaction.senderId,
          reaction: reaction.reaction,
        })),
        firstImported,
        lastImported,
        lastMessageObjectID: new ObjectId(),
        messageReplies: [],
        forwardingScore: message.forwardingScore,
        _chunkIndex: chunk.chunkIndex,
        _localIndex: localIndex,
        _replyToIndex: message.replyToIndex,
      };

      if (message.mediaMimeType !== "none") {
        document.messageMedia = {
          fileSize: 50_000 + (hash(`${messageId}:media`) % 5_000_000),
          mimeType: message.mediaMimeType,
          mediaHash: `synthetic_${hash(`${messageId}:hash`).toString(16).padStart(10, "0")}`,
        };
        document.lastMediaObjectID = new ObjectId();
      }

      if (document.messageReactions.length > 0) {
        document.lastReactionObjectID = new ObjectId();
      }

      documents.push(document);
    }
  }

  const documentsByGlobalIndex = new Map(
    documents.map((document) => {
      const plannedOffset = plannedChunkOffsets.get(document._chunkIndex) ?? 0;
      return [plannedOffset + document._localIndex, document];
    }),
  );

  for (const document of documents) {
    if (document._replyToIndex < 0) {
      continue;
    }

    if (document._replyToIndex >= document._localIndex) {
      continue;
    }

    const chunkOffset = plannedChunkOffsets.get(document._chunkIndex) ?? 0;
    const targetIndex = chunkOffset + document._replyToIndex;
    const target = documentsByGlobalIndex.get(targetIndex);
    if (!target) {
      continue;
    }

    document.quotedMessageId = target.messageId;
    target.messageReplies.push(document.messageId);
  }

  return documents.map(({ _chunkIndex, _localIndex, _replyToIndex, ...document }) => document);
}

async function fetchGroupsAndUsers(db, config, options) {
  const groupsCursor = db
    .collection(config.groupsCollection)
    .find({})
    .sort({ _id: 1 })
    .skip(options.skip);

  if (options.limit !== undefined) {
    groupsCursor.limit(options.limit);
  }

  const groups = await groupsCursor.toArray();
  if (groups.length === 0) {
    return { groups: [], usersByGroupId: new Map() };
  }

  const groupIds = groups.map((group) => group._id);
  const users = await db
    .collection(config.usersCollection)
    .find({
      groups: {
        $elemMatch: {
          group: { $in: groupIds },
        },
      },
    })
    .toArray();

  const usersByGroupId = new Map();
  for (const group of groups) {
    usersByGroupId.set(toObjectIdString(group._id), []);
  }

  for (const user of users) {
    if (!Array.isArray(user.groups)) {
      continue;
    }

    for (const membership of user.groups) {
      const groupObjectIdString = readMembershipGroupId(membership);
      if (!groupObjectIdString || !usersByGroupId.has(groupObjectIdString)) {
        continue;
      }

      usersByGroupId.get(groupObjectIdString).push(user);
    }
  }

  return { groups, usersByGroupId };
}

async function buildGenerationPlan(db, config, options, runId) {
  const { groups, usersByGroupId } = await fetchGroupsAndUsers(db, config, options);
  const groupContexts = [];
  const requestLines = [];

  for (const group of groups) {
    const groupObjectIdString = toObjectIdString(group._id);
    const authors = sampleUsersForGroup(
      usersByGroupId.get(groupObjectIdString) ?? [],
      group,
      options.maxAuthors,
    );

    if (authors.length === 0) {
      console.warn(`Skipping group ${group.groupId}: no matching user profiles found.`);
      continue;
    }

    const chunkSizes = splitIntoChunkSizes(options.messagesPerGroup, options.chunkSize);
    const chunkContexts = chunkSizes.map((chunkSize, chunkIndex) => ({
      runId,
      customId: `${runId}:${slugify(group.groupId)}:chunk-${String(chunkIndex + 1).padStart(2, "0")}`,
      group,
      authors,
      chunkIndex,
      chunkSize,
      totalChunks: chunkSizes.length,
    }));

    groupContexts.push({
      group,
      authors,
      chunks: chunkContexts,
    });

    for (const chunkContext of chunkContexts) {
      requestLines.push(JSON.stringify(buildRequestLine(chunkContext, options)));
    }
  }

  return {
    groupContexts,
    requestJsonl: `${requestLines.join("\n")}\n`,
  };
}

function parseBatchOutputRecords(outputText, contextsByCustomId) {
  const records = parseJsonLines(outputText);
  const parsedChunksByGroupId = new Map();
  const seenCustomIds = new Set();
  const summary = {
    totalRequests: contextsByCustomId.size,
    fullChunks: 0,
    partialChunks: 0,
    importedMessages: 0,
    missingMessages: 0,
    failedChunks: [],
  };

  for (const record of records) {
    const customId = typeof record.custom_id === "string" ? record.custom_id : null;
    if (!customId || !contextsByCustomId.has(customId)) {
      continue;
    }

    seenCustomIds.add(customId);
    const context = contextsByCustomId.get(customId);

    if (record.error) {
      summary.failedChunks.push({
        customId,
        error: `Batch request failed: ${JSON.stringify(record.error)}`,
      });
      continue;
    }

    const statusCode = record.response?.status_code;
    if (typeof statusCode === "number" && statusCode >= 400) {
      summary.failedChunks.push({
        customId,
        error: `Batch request failed with status ${statusCode}: ${JSON.stringify(record.response?.body ?? {})}`,
      });
      continue;
    }

    const responseBody = record.response?.body ?? record.body;
    try {
      const parsedChunk = parseGeneratedChunkResponse(responseBody, context);
      const messages = parsedChunk.messages;
      const missingMessages = context.chunkSize - messages.length;
      if (parsedChunk.mode === "partial") {
        summary.partialChunks += 1;
        summary.missingMessages += missingMessages;
        console.warn(
          `Recovered ${messages.length}/${context.chunkSize} messages from truncated chunk ${customId}${parsedChunk.incompleteReason ? ` (${parsedChunk.incompleteReason})` : ""}.`,
        );
      } else {
        summary.fullChunks += 1;
      }
      summary.importedMessages += messages.length;

      const groupKey = context.group.groupId;
      if (!parsedChunksByGroupId.has(groupKey)) {
        parsedChunksByGroupId.set(groupKey, []);
      }

      parsedChunksByGroupId.get(groupKey).push({
        chunkIndex: context.chunkIndex,
        messages,
      });
    } catch (error) {
      summary.failedChunks.push({
        customId,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  for (const customId of contextsByCustomId.keys()) {
    if (!seenCustomIds.has(customId)) {
      summary.failedChunks.push({
        customId,
        error: `Missing batch result for request ${customId}.`,
      });
    }
  }

  return {
    parsedChunksByGroupId,
    summary,
  };
}

async function insertGeneratedMessages(db, collectionName, groupContexts, parsedChunksByGroupId, runId, lookbackDays) {
  const collection = db.collection(collectionName);
  let totalUpserted = 0;

  for (const groupContext of groupContexts) {
    const groupId = groupContext.group.groupId;
    const parsedChunks = parsedChunksByGroupId.get(groupId) ?? [];

    if (parsedChunks.length === 0) {
      console.warn(`Skipping group ${groupId}: no recoverable chunks were parsed.`);
      continue;
    }

    parsedChunks.sort((left, right) => left.chunkIndex - right.chunkIndex);

    const documents = buildMessageDocuments(groupContext, parsedChunks, runId, lookbackDays);
    const expectedCount = groupContext.chunks.reduce((acc, chunk) => acc + chunk.chunkSize, 0);

    const operations = documents.map((document) => {
      const { _id, ...replacement } = document;
      return {
        updateOne: {
          filter: { messageId: document.messageId },
          update: {
            $set: replacement,
            $setOnInsert: { _id },
          },
          upsert: true,
        },
      };
    });

    const result = await collection.bulkWrite(operations, { ordered: false });
    totalUpserted += (result.upsertedCount ?? 0) + (result.modifiedCount ?? 0);

    console.log(
      `Inserted/upserted ${documents.length}/${expectedCount} synthetic messages for group ${groupId}.`,
    );
  }

  return totalUpserted;
}

async function loadRecoveryPlanFromArtifacts(db, config, runDirectory, runId) {
  const requestFilePath = path.join(runDirectory, "input.jsonl");
  const outputFilePath = path.join(runDirectory, "output.jsonl");
  const requestText = await readFile(requestFilePath, "utf8");
  const outputText = await readFile(outputFilePath, "utf8");
  const requestRecords = parseJsonLines(requestText);
  const requestedGroupIds = [...new Set(
    requestRecords
      .map((record) => record.body?.metadata?.groupId)
      .filter((groupId) => typeof groupId === "string" && groupId.length > 0),
  )];

  const groups = await db
    .collection(config.groupsCollection)
    .find({ groupId: { $in: requestedGroupIds } })
    .toArray();
  const groupsByGroupId = new Map(groups.map((group) => [String(group.groupId), group]));
  const groupContextsByGroupId = new Map();
  const contextsByCustomId = new Map();

  for (const record of requestRecords) {
    const customId = typeof record.custom_id === "string" ? record.custom_id : null;
    const groupId = record.body?.metadata?.groupId;
    const chunkIndex = Number(record.body?.metadata?.chunkIndex);
    const chunkSize = Number(record.body?.text?.format?.schema?.properties?.messages?.minItems);
    const authorIds =
      record.body?.text?.format?.schema?.properties?.messages?.items?.properties?.authorId?.enum;

    if (!customId || typeof groupId !== "string" || !Number.isInteger(chunkIndex) || !Number.isInteger(chunkSize)) {
      console.warn(`Skipping malformed request artifact for custom_id ${customId ?? "<unknown>"}.`);
      continue;
    }

    const group = groupsByGroupId.get(groupId);
    if (!group) {
      console.warn(`Skipping ${customId}: group ${groupId} no longer exists in ${config.groupsCollection}.`);
      continue;
    }

    const authors = Array.isArray(authorIds)
      ? authorIds.map((authorId) => ({ userId: String(authorId) }))
      : [];
    const context = {
      runId,
      customId,
      group,
      authors,
      chunkIndex,
      chunkSize,
      totalChunks: 0,
    };
    contextsByCustomId.set(customId, context);

    if (!groupContextsByGroupId.has(groupId)) {
      groupContextsByGroupId.set(groupId, {
        group,
        authors,
        chunks: [],
      });
    }

    const groupContext = groupContextsByGroupId.get(groupId);
    groupContext.chunks.push({
      customId,
      chunkIndex,
      chunkSize,
    });
  }

  const groupContexts = [...groupContextsByGroupId.values()];
  for (const groupContext of groupContexts) {
    groupContext.chunks.sort((left, right) => left.chunkIndex - right.chunkIndex);
    const totalChunks = groupContext.chunks.length;
    for (const chunk of groupContext.chunks) {
      const context = contextsByCustomId.get(chunk.customId);
      if (context) {
        context.totalChunks = totalChunks;
      }
    }
  }

  return {
    outputText,
    groupContexts,
    contextsByCustomId,
  };
}

async function writeUpdatedManifest(manifestPath, patch) {
  const currentManifest = JSON.parse(await readFile(manifestPath, "utf8"));
  await writeFile(
    manifestPath,
    `${JSON.stringify(
      {
        ...currentManifest,
        ...patch,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printHelp();
    return;
  }

  const recoveryRunRef = typeof args["recover-run"] === "string" ? args["recover-run"] : null;
  const recoveryRunDirectory = recoveryRunRef ? resolveRunDirectory(recoveryRunRef) : null;
  const recoveryManifestPath = recoveryRunDirectory
    ? path.join(recoveryRunDirectory, "manifest.json")
    : null;
  const recoveryManifest =
    recoveryManifestPath && existsSync(recoveryManifestPath)
      ? JSON.parse(await readFile(recoveryManifestPath, "utf8"))
      : null;

  const runId = recoveryManifest?.runId || new Date().toISOString().replace(/[:.]/g, "-");
  const runtimeDir =
    recoveryRunDirectory ||
    path.join(repoRoot, ".runtime", "synthetic-message-batches", runId);
  mkdirSync(runtimeDir, { recursive: true });

  const mongoConfig = readMongoConfig({
    groupsCollection: args["groups-collection"] || recoveryManifest?.collections?.groups,
    usersCollection: args["users-collection"] || recoveryManifest?.collections?.users,
    messagesCollection: args["messages-collection"] || recoveryManifest?.collections?.messages,
  });

  const mongoClient = new MongoClient(mongoConfig.uri);
  await mongoClient.connect();

  try {
    const db = mongoClient.db(mongoConfig.dbName);

    if (recoveryRunDirectory) {
      const recoveryPlan = await loadRecoveryPlanFromArtifacts(
        db,
        mongoConfig,
        recoveryRunDirectory,
        runId,
      );
      const parsed = parseBatchOutputRecords(
        recoveryPlan.outputText,
        recoveryPlan.contextsByCustomId,
      );
      const writtenCount = await insertGeneratedMessages(
        db,
        mongoConfig.messagesCollection,
        recoveryPlan.groupContexts,
        parsed.parsedChunksByGroupId,
        runId,
        args.lookbackDays,
      );

      if (recoveryManifestPath && existsSync(recoveryManifestPath)) {
        await writeUpdatedManifest(recoveryManifestPath, {
          recoveredAt: new Date().toISOString(),
          batchStatus:
            parsed.summary.failedChunks.length > 0 || parsed.summary.partialChunks > 0
              ? "completed_with_partial_recovery"
              : "completed",
          insertedOrUpdatedDocuments: writtenCount,
          parseSummary: parsed.summary,
        });
      }

      console.log(
        `Recovered ${parsed.summary.importedMessages} messages from ${recoveryRunDirectory} and wrote ${writtenCount} documents to ${mongoConfig.messagesCollection}.`,
      );
      if (parsed.summary.failedChunks.length > 0) {
        console.warn(
          `Some chunks were still unrecoverable: ${parsed.summary.failedChunks.length}. See ${recoveryManifestPath ?? recoveryRunDirectory}.`,
        );
      }
      return;
    }

    ensureOpenAiConfigured();
    const generationPlan = await buildGenerationPlan(db, mongoConfig, args, runId);

    if (generationPlan.groupContexts.length === 0) {
      console.log("No groups with usable user profiles were found for generation.");
      return;
    }

    const requestFilePath = path.join(runtimeDir, "input.jsonl");
    await writeFile(requestFilePath, generationPlan.requestJsonl, "utf8");

    const manifestPath = path.join(runtimeDir, "manifest.json");
    await writeFile(
      manifestPath,
      `${JSON.stringify(
        {
          runId,
          createdAt: new Date().toISOString(),
          options: {
            limit: args.limit ?? null,
            skip: args.skip,
            messagesPerGroup: args.messagesPerGroup,
            chunkSize: args.chunkSize,
            maxAuthors: args.maxAuthors,
            model: args.model,
            lookbackDays: args.lookbackDays,
            submitOnly: args.submitOnly,
          },
          collections: {
            groups: mongoConfig.groupsCollection,
            users: mongoConfig.usersCollection,
            messages: mongoConfig.messagesCollection,
          },
          groups: generationPlan.groupContexts.map((context) => ({
            groupId: context.group.groupId,
            subject: context.group.subject ?? "",
            authorCount: context.authors.length,
            chunkCount: context.chunks.length,
          })),
        },
        null,
        2,
      )}\n`,
      "utf8",
    );

    const openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY,
    });

    console.log(
      `Uploading batch input for ${generationPlan.groupContexts.length} groups and ${generationPlan.groupContexts.reduce((acc, context) => acc + context.chunks.length, 0)} requests...`,
    );

    const inputFile = await openai.files.create({
      file: createReadStream(requestFilePath),
      purpose: "batch",
    });

    const batch = await openai.batches.create({
      input_file_id: inputFile.id,
      endpoint: "/v1/responses",
      completion_window: "24h",
      metadata: {
        runId,
        purpose: "synthetic-group-messages",
      },
    });

    await writeFile(
      manifestPath,
      `${JSON.stringify(
        {
          ...(JSON.parse(await readFile(manifestPath, "utf8"))),
          batchId: batch.id,
          inputFileId: inputFile.id,
          batchStatus: batch.status,
        },
        null,
        2,
      )}\n`,
      "utf8",
    );

    console.log(`Created OpenAI batch ${batch.id}. Manifest: ${manifestPath}`);

    if (args.submitOnly) {
      console.log("Submit-only mode enabled. Batch created and script is exiting before import.");
      return;
    }

    const completedBatch = await waitForBatchCompletion(
      openai,
      batch.id,
      args.pollIntervalMs,
      args.maxWaitMs,
    );

    if (!completedBatch.output_file_id) {
      throw new Error(`Batch ${completedBatch.id} completed without an output_file_id.`);
    }

    const outputFilePath = path.join(runtimeDir, "output.jsonl");
    const outputText = await downloadFileText(openai, completedBatch.output_file_id, outputFilePath);

    if (completedBatch.error_file_id) {
      const errorFilePath = path.join(runtimeDir, "errors.jsonl");
      await downloadFileText(openai, completedBatch.error_file_id, errorFilePath);
    }

    const contextsByCustomId = new Map(
      generationPlan.groupContexts.flatMap((context) =>
        context.chunks.map((chunk) => [chunk.customId, chunk]),
      ),
    );
    const parsed = parseBatchOutputRecords(outputText, contextsByCustomId);

    const writtenCount = await insertGeneratedMessages(
      db,
      mongoConfig.messagesCollection,
      generationPlan.groupContexts,
      parsed.parsedChunksByGroupId,
      runId,
      args.lookbackDays,
    );

    await writeFile(
      manifestPath,
      `${JSON.stringify(
        {
          ...(JSON.parse(await readFile(manifestPath, "utf8"))),
          completedAt: new Date().toISOString(),
          batchStatus:
            parsed.summary.failedChunks.length > 0 || parsed.summary.partialChunks > 0
              ? "completed_with_partial_recovery"
              : "completed",
          outputFileId: completedBatch.output_file_id,
          errorFileId: completedBatch.error_file_id ?? null,
          insertedOrUpdatedDocuments: writtenCount,
          parseSummary: parsed.summary,
        },
        null,
        2,
      )}\n`,
      "utf8",
    );

    console.log(
      `Finished. Synthetic messages written to ${mongoConfig.messagesCollection}. Output artifacts: ${runtimeDir}`,
    );
    if (parsed.summary.partialChunks > 0 || parsed.summary.failedChunks.length > 0) {
      console.warn(
        `Imported ${parsed.summary.importedMessages} messages with ${parsed.summary.partialChunks} recovered partial chunks and ${parsed.summary.failedChunks.length} unrecoverable chunks.`,
      );
    }
  } finally {
    await mongoClient.close();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
