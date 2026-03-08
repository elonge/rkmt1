import { NextResponse } from "next/server";
import {
  revisePlan,
  PlannerExecutionError,
  PlannerUnavailableError,
} from "@/lib/agents/orchestrator";
import { appendRevisionNote, getPlanJob, replacePlan } from "@/lib/store";
import { revisePlanRequestSchema } from "@/lib/types";
import { ZodError } from "zod";

type Context = {
  params: Promise<{ id: string }>;
};

export async function GET(_: Request, context: Context) {
  const { id } = await context.params;
  const job = getPlanJob(id);

  if (!job) {
    return NextResponse.json(
      {
        ok: false,
        error: "Plan not found",
      },
      { status: 404 },
    );
  }

  return NextResponse.json({
    ok: true,
    job,
  });
}

export async function POST(request: Request, context: Context) {
  const { id } = await context.params;
  const job = getPlanJob(id);

  if (!job) {
    return NextResponse.json(
      {
        ok: false,
        error: "Plan not found",
      },
      { status: 404 },
    );
  }

  if (job.status === "running") {
    return NextResponse.json(
      {
        ok: false,
        error: "Cannot revise a running plan",
      },
      { status: 409 },
    );
  }

  try {
    const payload = revisePlanRequestSchema.parse(await request.json());
    const revised = await revisePlan(job.question, job.plan, payload.feedback);
    appendRevisionNote(id, payload.feedback);
    const updated = replacePlan(id, revised);

    return NextResponse.json({
      ok: true,
      job: updated,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Invalid request";
    const status =
      error instanceof ZodError
        ? 400
        : error instanceof PlannerUnavailableError
          ? 503
          : error instanceof PlannerExecutionError
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
