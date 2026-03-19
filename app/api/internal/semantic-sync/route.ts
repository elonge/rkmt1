import { timingSafeEqual } from "node:crypto";
import { NextResponse } from "next/server";
import { ZodError } from "zod";
import {
  runSemanticSync,
  SemanticConfigError,
  SemanticSearchError,
  semanticSyncModeSchema,
} from "@/lib/data/semantic";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function readSyncSecret(): string {
  const secret = process.env.SEMANTIC_SYNC_SECRET?.trim() || process.env.CRON_SECRET?.trim();
  if (!secret) {
    throw new SemanticConfigError(
      "Missing SEMANTIC_SYNC_SECRET or CRON_SECRET for /api/internal/semantic-sync.",
    );
  }
  return secret;
}

function secretsMatch(expected: string, received: string): boolean {
  const expectedBuffer = Buffer.from(expected);
  const receivedBuffer = Buffer.from(received);
  return (
    expectedBuffer.length === receivedBuffer.length &&
    timingSafeEqual(expectedBuffer, receivedBuffer)
  );
}

function isAuthorized(request: Request): boolean {
  const secret = readSyncSecret();
  const authorization = request.headers.get("authorization")?.trim();
  const headerSecret = request.headers.get("x-semantic-sync-secret")?.trim();
  const bearerSecret =
    authorization && authorization.startsWith("Bearer ")
      ? authorization.slice("Bearer ".length).trim()
      : "";
  const provided = bearerSecret || headerSecret || "";

  return Boolean(provided) && secretsMatch(secret, provided);
}

async function readModeFromRequest(request: Request): Promise<string> {
  const url = new URL(request.url);
  const searchParamMode = url.searchParams.get("mode");
  if (searchParamMode) {
    return searchParamMode;
  }

  if (request.method !== "POST") {
    return "incremental";
  }

  const rawBody = await request.text();
  if (!rawBody.trim()) {
    return "incremental";
  }

  const parsed = JSON.parse(rawBody) as { mode?: string };
  return parsed.mode ?? "incremental";
}

async function handleSync(request: Request) {
  if (!isAuthorized(request)) {
    return NextResponse.json(
      {
        ok: false,
        error: "Unauthorized",
      },
      { status: 401 },
    );
  }

  try {
    const mode = semanticSyncModeSchema.parse(await readModeFromRequest(request));
    const result = await runSemanticSync(mode);
    return NextResponse.json({
      ok: true,
      result,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Semantic sync failed";
    const status =
      error instanceof SyntaxError || error instanceof ZodError
        ? 400
        : error instanceof SemanticConfigError || error instanceof SemanticSearchError
          ? 500
          : 500;

    return NextResponse.json(
      {
        ok: false,
        error: message,
      },
      { status },
    );
  }
}

export async function GET(request: Request) {
  return handleSync(request);
}

export async function POST(request: Request) {
  return handleSync(request);
}
