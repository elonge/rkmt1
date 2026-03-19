#!/usr/bin/env node

import path from "node:path";
import process from "node:process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

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
  node scripts/run-semantic-sync.mjs [--mode <groups_backfill|messages_backfill|incremental>] [--limit <positive integer>]

Examples:
  npm run semantic:sync -- --mode groups_backfill
  npm run semantic:sync -- --mode groups_backfill --limit 500
  npm run semantic:sync -- --mode messages_backfill
  npm run semantic:sync -- --mode messages_backfill --limit 1000
  npm run semantic:sync
`);
}

function parseArgs(argv) {
  const parsed = {
    mode: "incremental",
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--help" || arg === "-h") {
      parsed.help = true;
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

function parsePositiveIntegerArg(flagName, value) {
  if (value == null) {
    return undefined;
  }

  const normalized = String(value).trim();
  const parsed = Number(normalized);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${flagName} must be a positive integer, received "${value}"`);
  }

  return parsed;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printHelp();
    return;
  }

  const [semanticModule, mongoModule] = await Promise.all([
    import("../lib/data/semantic.ts"),
    import("../lib/data/mongo.ts"),
  ]);

  try {
    const mode = semanticModule.semanticSyncModeSchema.parse(args.mode);
    const limit = parsePositiveIntegerArg("--limit", args.limit);
    const result = await semanticModule.runSemanticSync(mode, {
      limit,
      log: (message) => {
        console.error(`[semantic-sync] ${message}`);
      },
    });
    console.log(JSON.stringify(result, null, 2));
  } finally {
    await mongoModule.closeMongoClient();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
