import { NextResponse } from "next/server";
import { runPlanInBackground } from "@/lib/runtime/runner";
import { getPlanJob, setPlanStatus } from "@/lib/store";

type Context = {
  params: Promise<{ id: string }>;
};

export async function POST(_: Request, context: Context) {
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
        ok: true,
        job,
      },
      { status: 202 },
    );
  }

  if (job.status === "completed") {
    return NextResponse.json(
      {
        ok: true,
        job,
      },
      { status: 200 },
    );
  }

  setPlanStatus(id, "running");
  void runPlanInBackground(id);

  const refreshed = getPlanJob(id);
  return NextResponse.json(
    {
      ok: true,
      job: refreshed,
    },
    { status: 202 },
  );
}
