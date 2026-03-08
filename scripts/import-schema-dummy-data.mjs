#!/usr/bin/env node

import { existsSync } from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

function printHelp() {
  console.log(`Usage:
  node scripts/import-schema-dummy-data.mjs --uri <mongodb-uri> [--db <database>] [--drop]

Options:
  --uri           MongoDB connection string. Falls back to MONGO_URI.
  --db            Database name. Optional if the URI already includes one.
  --drop          Drop each collection before import.
  --mongoimport   Path to the mongoimport binary. Falls back to MONGOIMPORT_BIN or "mongoimport".

Collection/file overrides:
  --groups-file       Path to groups JSON file
  --users-file        Path to user profiles JSON file
  --messages-file     Path to messages JSON file
  --groups-collection Collection name for Group documents
  --users-collection  Collection name for UserProfile documents
  --messages-collection Collection name for Message documents

Examples:
  npm run import:schema-dummy-data -- --uri "mongodb://127.0.0.1:27017/rkmt"
  npm run import:schema-dummy-data -- --uri "mongodb://127.0.0.1:27017" --db rkmt --drop
`);
}

function parseArgs(argv) {
  const parsed = {
    drop: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--help" || arg === "-h") {
      parsed.help = true;
      continue;
    }

    if (arg === "--drop") {
      parsed.drop = true;
      continue;
    }

    if (!arg.startsWith("--")) {
      throw new Error(`Unexpected argument: ${arg}`);
    }

    const value = argv[index + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`Missing value for ${arg}`);
    }

    parsed[arg.slice(2)] = value;
    index += 1;
  }

  return parsed;
}

function runImport({
  mongoimportBin,
  uri,
  db,
  collection,
  file,
  drop,
}) {
  const args = [
    "--uri",
    uri,
    "--collection",
    collection,
    "--file",
    file,
    "--jsonArray",
    "--mode",
    "upsert",
    "--upsertFields",
    "_id",
  ];

  if (db) {
    args.push("--db", db);
  }

  if (drop) {
    args.push("--drop");
  }

  console.log(`Importing ${path.basename(file)} into collection "${collection}"...`);
  execFileSync(mongoimportBin, args, { stdio: "inherit" });
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printHelp();
    return;
  }

  const mongoimportBin = args.mongoimport ?? process.env.MONGOIMPORT_BIN ?? "mongoimport";
  const uri = args.uri ?? process.env.MONGO_URI;
  const db = args.db ?? process.env.MONGO_DB_NAME;

  if (!uri) {
    throw new Error("Missing MongoDB URI. Pass --uri or set MONGO_URI.");
  }

  const imports = [
    {
      collection: args["groups-collection"] ?? process.env.MONGO_GROUPS_COLLECTION ?? "groups",
      file:
        args["groups-file"] ??
        path.join(repoRoot, "data", "dummy-groups.json"),
    },
    {
      collection: args["users-collection"] ?? process.env.MONGO_USERS_COLLECTION ?? "userprofiles",
      file:
        args["users-file"] ??
        path.join(repoRoot, "data", "dummy-user-profiles.json"),
    },
    {
      collection: args["messages-collection"] ?? process.env.MONGO_MESSAGES_COLLECTION ?? "messages",
      file:
        args["messages-file"] ??
        path.join(repoRoot, "data", "dummy-messages.json"),
    },
  ];

  for (const entry of imports) {
    if (!existsSync(entry.file)) {
      throw new Error(`Missing JSON file: ${entry.file}`);
    }

    runImport({
      mongoimportBin,
      uri,
      db,
      collection: entry.collection,
      file: entry.file,
      drop: args.drop,
    });
  }

  console.log("Dummy schema data import completed.");
}

try {
  main();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
