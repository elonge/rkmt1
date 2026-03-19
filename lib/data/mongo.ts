import { MongoClient } from "mongodb";
import type { Db } from "mongodb";

type MongoConfig = {
  uri: string;
  dbName: string;
  groupsCollection: string;
  usersCollection: string;
  messagesCollection: string;
  groupSemanticCollection: string;
  messageSemanticCollection: string;
};

declare global {
  // eslint-disable-next-line no-var
  var __rkmtMongoClient: MongoClient | undefined;
  // eslint-disable-next-line no-var
  var __rkmtMongoClientPromise: Promise<MongoClient> | undefined;
}

function inferDbNameFromUri(uri: string): string | null {
  const match = uri.match(/^mongodb(?:\+srv)?:\/\/(?:[^@/]+@)?[^/]+\/([^?]+)/i);
  if (!match) {
    return null;
  }

  const candidate = decodeURIComponent(match[1]).trim();
  return candidate.length > 0 ? candidate : null;
}

function readMongoConfig(): MongoConfig {
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
    groupsCollection: process.env.MONGO_GROUPS_COLLECTION?.trim() || "groups",
    usersCollection: process.env.MONGO_USERS_COLLECTION?.trim() || "userprofiles",
    messagesCollection: process.env.MONGO_MESSAGES_COLLECTION?.trim() || "messages",
    groupSemanticCollection:
      process.env.MONGO_GROUP_SEMANTIC_COLLECTION?.trim() || "group_semantic",
    messageSemanticCollection:
      process.env.MONGO_MESSAGE_SEMANTIC_COLLECTION?.trim() || "message_semantic",
  };
}

export function getMongoCollectionNames() {
  const config = readMongoConfig();
  return {
    groupsCollection: config.groupsCollection,
    usersCollection: config.usersCollection,
    messagesCollection: config.messagesCollection,
    groupSemanticCollection: config.groupSemanticCollection,
    messageSemanticCollection: config.messageSemanticCollection,
  };
}

export function dummyDatasetSummary(): string {
  try {
    const config = readMongoConfig();
    return `Mongo DB "${config.dbName}" configured for collections ${config.groupsCollection}, ${config.usersCollection}, ${config.messagesCollection}, ${config.groupSemanticCollection}, and ${config.messageSemanticCollection}.`;
  } catch (error) {
    return error instanceof Error
      ? `${error.message} Mongo-backed tools will fail until env is configured.`
      : "Mongo-backed tools are not configured.";
  }
}

export async function closeMongoClient(): Promise<void> {
  const client = globalThis.__rkmtMongoClient;
  globalThis.__rkmtMongoClient = undefined;
  globalThis.__rkmtMongoClientPromise = undefined;

  if (!client) {
    return;
  }

  await client.close();
}

export async function getMongoDb(): Promise<Db> {
  const { uri, dbName } = readMongoConfig();

  if (!globalThis.__rkmtMongoClientPromise) {
    const client = new MongoClient(uri);
    globalThis.__rkmtMongoClient = client;
    globalThis.__rkmtMongoClientPromise = client.connect().catch((error) => {
      globalThis.__rkmtMongoClient = undefined;
      globalThis.__rkmtMongoClientPromise = undefined;
      throw error;
    });
  }

  const connectedClient = await globalThis.__rkmtMongoClientPromise;
  return connectedClient.db(dbName);
}
