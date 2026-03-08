import { NextResponse } from "next/server";
import {
  draftPlan,
  PlannerExecutionError,
  PlannerUnavailableError,
} from "@/lib/agents/orchestrator";
import { createPlanJob } from "@/lib/store";
import { createQuestionRequestSchema } from "@/lib/types";
import { ZodError } from "zod";

export async function POST(request: Request) {
  try {
    const payload = createQuestionRequestSchema.parse(await request.json());
    const plan = await draftPlan(payload.question);
    const job = createPlanJob(payload.question, plan);

    return NextResponse.json({
      ok: true,
      job,
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
