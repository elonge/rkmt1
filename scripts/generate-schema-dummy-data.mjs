#!/usr/bin/env node

import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ENTRY_COUNT = 500;
const DAY_IN_MS = 24 * 60 * 60 * 1000;
const HOUR_IN_MS = 60 * 60 * 1000;
const BASE_NOW = Date.UTC(2026, 2, 8, 9, 0, 0);

const accounts = [
  "north-command",
  "center-command",
  "south-command",
  "field-research",
  "policy-watch",
  "grassroots-lab",
];

const firstNames = [
  "Noam",
  "Maya",
  "Lior",
  "Yael",
  "Daniel",
  "Tamar",
  "Eitan",
  "Shira",
  "Amit",
  "Neta",
  "Omer",
  "Roni",
];

const lastNames = [
  "Levi",
  "Cohen",
  "Mizrahi",
  "Ben-David",
  "Harel",
  "Peretz",
  "Shalev",
  "Dayan",
  "Mor",
  "Biton",
  "Amar",
  "Navon",
];

const politicalLeanings = ["left", "center-left", "center", "center-right", "right", "mixed"];
const demographics = [
  { label: "students", age: "18-24", gender: "mixed" },
  { label: "young professionals", age: "25-34", gender: "mixed" },
  { label: "parents", age: "30-45", gender: "mixed" },
  { label: "women leaders", age: "25-54", gender: "female" },
  { label: "veterans", age: "35-65", gender: "mixed" },
  { label: "retirees", age: "60+", gender: "mixed" },
];
const topics = [
  "Local leadership",
  "Cost of living",
  "Security updates",
  "Community volunteering",
  "Education reform",
  "Transportation planning",
  "Healthcare access",
  "Small business support",
  "Housing policy",
  "Municipal budgets",
];
const regions = ["Jerusalem", "Tel Aviv", "Haifa", "Beer Sheva", "Galilee", "Sharon", "Negev"];
const districts = ["North", "Central", "South", "Jerusalem", "Coastal"];
const organizationTypes = ["campaign", "municipality", "ngo", "business-forum", "activist-network"];
const lifeEvents = ["reserve duty", "new parents", "student elections", "municipal campaign", "community drive"];
const strategicMarkets = ["students", "small business owners", "parents", "swing voters", "community leaders"];
const reactionPool = ["👍", "🔥", "👏", "❤️", "🤝", "✅", "👀", "🎯"];
const mediaMimeTypes = ["image/jpeg", "image/png", "video/mp4", "application/pdf"];
const statusTemplates = [
  "Following local issues closely",
  "Tracking group activity",
  "Interested in policy updates",
  "Community organizer",
  "Watching narrative shifts",
  "Focused on field operations",
];

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const dataDir = path.join(repoRoot, "data");

function objectId(prefix, index) {
  return `${prefix}${index.toString(16).padStart(22, "0")}`;
}

function oid(prefix, index) {
  return { $oid: objectId(prefix, index) };
}

function dateValue(value) {
  return { $date: new Date(value).toISOString() };
}

function hash(value) {
  let acc = 2166136261;
  for (const char of String(value)) {
    acc ^= char.charCodeAt(0);
    acc = Math.imul(acc, 16777619);
  }
  return acc >>> 0;
}

function pick(list, seed) {
  return list[hash(seed) % list.length];
}

function numberInRange(seed, min, max) {
  const span = max - min + 1;
  return min + (hash(seed) % span);
}

function decimalInRange(seed, min, max, precision = 2) {
  const fraction = (hash(seed) % 10000) / 10000;
  return Number((min + fraction * (max - min)).toFixed(precision));
}

function uniqueAccountList(index) {
  const count = 1 + (index % 3);
  const selected = new Set();
  let cursor = index;

  while (selected.size < count) {
    selected.add(accounts[cursor % accounts.length]);
    cursor += 2;
  }

  return [...selected];
}

function buildTag(tagValue, tagExplanation, model, timestamp) {
  return {
    tagValue,
    tagExplanation,
    model,
    timestamp,
  };
}

function buildGroups() {
  return Array.from({ length: ENTRY_COUNT }, (_, index) => {
    const entryNumber = index + 1;
    const createdAt = BASE_NOW - numberInRange(`group-created-${entryNumber}`, 120, 900) * DAY_IN_MS;
    const lastActivityTimestamp = createdAt + numberInRange(`group-active-${entryNumber}`, 20, 110) * DAY_IN_MS;
    const topic = pick(topics, `group-topic-${entryNumber}`);
    const region = pick(regions, `group-region-${entryNumber}`);
    const district = pick(districts, `group-district-${entryNumber}`);
    const demographic = pick(demographics, `group-demographic-${entryNumber}`);
    const politicalLeaning = pick(politicalLeanings, `group-leaning-${entryNumber}`);
    const organizationType = pick(organizationTypes, `group-org-type-${entryNumber}`);
    const organizationName = `${region} ${organizationType} hub`;
    const memberCount = numberInRange(`group-members-${entryNumber}`, 45, 1024);
    const activeMemberPercentage30d = decimalInRange(
      `group-active-members-${entryNumber}`,
      12,
      88,
    );
    const avgMessagesPerDay30d = decimalInRange(`group-msg-day-${entryNumber}`, 4, 85);
    const avgAuthorsPerDay30d = decimalInRange(
      `group-authors-day-${entryNumber}`,
      3,
      Math.max(6, Math.round(memberCount * 0.18)),
    );
    const avgReactionsPerMessage30d = decimalInRange(`group-react-${entryNumber}`, 0.2, 5.4);
    const avgRepliesPerMessage30d = decimalInRange(`group-replies-${entryNumber}`, 0.1, 2.2);
    const activeDays30d = numberInRange(`group-active-days-${entryNumber}`, 7, 30);
    const engagementScore = decimalInRange(`group-engagement-${entryNumber}`, 15, 98);
    const normalizedEngagementScore = Math.min(5, Math.max(1, Math.round(engagementScore / 20)));
    const joinedAt = createdAt + numberInRange(`group-joined-${entryNumber}`, 1, 30) * DAY_IN_MS;
    const lastTaggedTimestamp = lastActivityTimestamp - numberInRange(`group-tagged-${entryNumber}`, 1, 12) * HOUR_IN_MS;

    return {
      _id: oid("64", entryNumber),
      groupId: `group_${String(entryNumber).padStart(4, "0")}@g.us`,
      description: `${topic} discussion group for ${region.toLowerCase()} operators and volunteers.`,
      subject: `${region} ${topic} Forum ${String(entryNumber).padStart(3, "0")}`,
      imgSrc: `https://dummy.example/groups/${entryNumber}.jpg`,
      profilePictureMediaUrl: `https://dummy.example/groups/${entryNumber}/profile.jpg`,
      tags: {
        politicalLeaning: buildTag(
          politicalLeaning,
          `Estimated leaning based on recurring language in ${region}.`,
          "dummy-tag-v1",
          lastTaggedTimestamp,
        ),
        demographic: {
          ...buildTag(
            demographic.label,
            `Primary audience appears to be ${demographic.label}.`,
            "dummy-tag-v1",
            lastTaggedTimestamp,
          ),
          age: demographic.age,
          gender: demographic.gender,
        },
        topic: buildTag(
          topic,
          `Most common theme across recent conversations is ${topic.toLowerCase()}.`,
          "dummy-tag-v1",
          lastTaggedTimestamp,
        ),
        region: buildTag(
          region,
          `Most participants are clustered around ${region}.`,
          "dummy-tag-v1",
          lastTaggedTimestamp,
        ),
        organization: {
          ...buildTag(
            organizationName,
            `Affiliated with a ${organizationType} network in ${region}.`,
            "dummy-tag-v1",
            lastTaggedTimestamp,
          ),
          organizationType,
        },
        geographic: [
          {
            street: `${numberInRange(`street-${entryNumber}`, 10, 140)} ${pick(
              ["Herzl", "Ben Yehuda", "Jabotinsky", "HaPalmach", "Rothschild"],
              `street-name-${entryNumber}`,
            )} St`,
            neighborhood: `${pick(
              ["Old Town", "City Center", "Harbor", "University Quarter", "Industrial Park"],
              `neighborhood-${entryNumber}`,
            )}`,
            settlement: region,
            regionalCouncil: `${region} Council`,
            district,
            tagExplanation: `Primary geographic signal points to ${region}.`,
          },
        ],
        freeform: buildTag(
          `${pick(["high-trust", "volatile", "organizing-heavy", "broadcast-style"], `freeform-${entryNumber}`)}`,
          "Dummy freeform analyst note.",
          "dummy-tag-v1",
          lastTaggedTimestamp,
        ),
        lifeEvent: buildTag(
          pick(lifeEvents, `life-event-${entryNumber}`),
          "Recent lifecycle event inferred from chat context.",
          "dummy-tag-v1",
          lastTaggedTimestamp,
        ),
        strategicMarkets: buildTag(
          pick(strategicMarkets, `strategic-market-${entryNumber}`),
          "Likely strategic market for outreach prioritization.",
          "dummy-tag-v1",
          lastTaggedTimestamp,
        ),
      },
      memberCount,
      accounts: uniqueAccountList(entryNumber),
      lastActivityTimestamp,
      creationTimestamp: createdAt,
      announcementOnly: entryNumber % 7 === 0,
      memberAddMode: pick(["all_members", "admins_only", "invite_link"], `add-mode-${entryNumber}`),
      membershipApproval: entryNumber % 5 === 0,
      lastTaggedTimestamp,
      lastTagSignature: `sig-${entryNumber}-${hash(`signature-${entryNumber}`).toString(16).slice(0, 8)}`,
      joinedAt: dateValue(joinedAt),
      inviteLink: `https://chat.whatsapp.com/invite-${entryNumber.toString(16).padStart(8, "0")}`,
      avgMessagesPerDay30d,
      avgAuthorsPerDay30d,
      avgReactionsPerMessage30d,
      avgRepliesPerMessage30d,
      activeMemberPercentage30d,
      activeDays30d,
      engagementScore,
      normalizedEngagementScore,
    };
  });
}

function buildUsers(groups) {
  return Array.from({ length: ENTRY_COUNT }, (_, index) => {
    const entryNumber = index + 1;
    const membershipCount = numberInRange(`membership-count-${entryNumber}`, 1, 4);
    const memberships = [];
    const usedGroupIndexes = new Set();

    for (let membershipIndex = 0; membershipIndex < membershipCount; membershipIndex += 1) {
      let groupIndex = (entryNumber * 11 + membershipIndex * 17) % groups.length;
      while (usedGroupIndexes.has(groupIndex)) {
        groupIndex = (groupIndex + 1) % groups.length;
      }
      usedGroupIndexes.add(groupIndex);

      const group = groups[groupIndex];
      const joinedAtMs =
        BASE_NOW - numberInRange(`user-joined-${entryNumber}-${membershipIndex}`, 30, 720) * DAY_IN_MS;
      const hasLeft = (entryNumber + membershipIndex) % 6 === 0;
      const role = pick(["MEMBER", "ADMIN", "SUPER_ADMIN"], `role-${entryNumber}-${membershipIndex}`);
      const leftAtMs = hasLeft
        ? joinedAtMs + numberInRange(`user-left-${entryNumber}-${membershipIndex}`, 7, 180) * DAY_IN_MS
        : undefined;

      memberships.push({
        group: group._id,
        joinedAt: dateValue(joinedAtMs),
        ...(leftAtMs ? { leftAt: dateValue(leftAtMs) } : {}),
        role,
        status: hasLeft ? "LEFT" : "JOINED",
      });
    }

    const firstName = pick(firstNames, `first-name-${entryNumber}`);
    const lastName = pick(lastNames, `last-name-${entryNumber}`);

    return {
      _id: oid("65", entryNumber),
      userId: `user_${String(entryNumber).padStart(4, "0")}@c.us`,
      name: `${firstName} ${lastName}`,
      status: pick(statusTemplates, `status-${entryNumber}`),
      profilePicUrl: `https://dummy.example/users/${entryNumber}.jpg`,
      groups: memberships,
    };
  });
}

function buildMessageBody(group, authorId, index) {
  const topic = group.tags?.topic?.tagValue ?? "community updates";
  const region = group.tags?.region?.tagValue ?? "the area";
  const angle = pick(
    [
      "field feedback",
      "rapid response",
      "member coordination",
      "volunteer turnout",
      "message testing",
      "narrative watch",
    ],
    `body-angle-${index}`,
  );

  return `${topic}: sharing ${angle} from ${region}. Author ${authorId} flagged this as item ${index}.`;
}

function buildMessages(groups, users) {
  const messages = Array.from({ length: ENTRY_COUNT }, (_, index) => {
    const entryNumber = index + 1;
    const author = users[(entryNumber * 13) % users.length];
    const memberships = author.groups.length > 0 ? author.groups : [{ group: groups[0]._id }];
    const membership = memberships[entryNumber % memberships.length];
    const group = groups.find((candidate) => candidate._id.$oid === membership.group.$oid) ?? groups[0];
    const timestamp = BASE_NOW - numberInRange(`message-ts-${entryNumber}`, 1, 75) * DAY_IN_MS + index * 900000;
    const reactionCount = numberInRange(`reaction-count-${entryNumber}`, 0, 4);
    const messageReactions = Array.from({ length: reactionCount }, (_, reactionIndex) => {
      const reactor = users[(entryNumber * 7 + reactionIndex * 19) % users.length];
      return {
        timestamp: timestamp + (reactionIndex + 1) * 60000,
        senderId: reactor.userId,
        reaction: pick(reactionPool, `reaction-${entryNumber}-${reactionIndex}`),
      };
    });
    const hasMedia = entryNumber % 5 === 0;

    return {
      _id: oid("66", entryNumber),
      timestamp,
      messageId: `msg_${String(entryNumber).padStart(5, "0")}`,
      groupId: group.groupId,
      authorId: author.userId,
      body: buildMessageBody(group, author.userId, entryNumber),
      messageReactions,
      ...(hasMedia
        ? {
            messageMedia: {
              fileSize: numberInRange(`media-size-${entryNumber}`, 20_000, 8_000_000),
              mimeType: pick(mediaMimeTypes, `mime-${entryNumber}`),
              mediaHash: `mediahash_${hash(`media-${entryNumber}`).toString(16).padStart(10, "0")}`,
            },
          }
        : {}),
      firstImported: dateValue(timestamp + 3 * 60000),
      lastImported: dateValue(timestamp + 15 * 60000),
      lastMessageObjectID: oid("70", entryNumber),
      ...(messageReactions.length > 0 ? { lastReactionObjectID: oid("71", entryNumber) } : {}),
      ...(hasMedia ? { lastMediaObjectID: oid("72", entryNumber) } : {}),
      messageReplies: [],
      forwardingScore: numberInRange(`forwarding-${entryNumber}`, 0, 6),
      body_embedding: Array.from({ length: 8 }, (_, embeddingIndex) =>
        decimalInRange(`embedding-${entryNumber}-${embeddingIndex}`, -1, 1, 4),
      ),
    };
  });

  for (let index = 1; index < messages.length; index += 1) {
    if ((index + 1) % 4 !== 0) {
      continue;
    }

    const currentMessage = messages[index];
    const targetIndex = Math.max(0, index - numberInRange(`reply-target-${index}`, 1, Math.min(8, index)));
    const targetMessage = messages[targetIndex];
    currentMessage.quotedMessageId = targetMessage.messageId;
    targetMessage.messageReplies.push(currentMessage.messageId);
  }

  return messages;
}

function writeJson(filename, data) {
  writeFileSync(path.join(dataDir, filename), `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function main() {
  mkdirSync(dataDir, { recursive: true });

  const groups = buildGroups();
  const users = buildUsers(groups);
  const messages = buildMessages(groups, users);

  writeJson("dummy-groups.json", groups);
  writeJson("dummy-user-profiles.json", users);
  writeJson("dummy-messages.json", messages);

  console.log(`Generated ${groups.length} groups, ${users.length} user profiles, and ${messages.length} messages.`);
  console.log(`Files written to ${dataDir}`);
}

main();
