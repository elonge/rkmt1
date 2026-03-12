import { after, NextResponse } from "next/server";
import { runPlanInBackground } from "@/lib/runtime/runner";
import { getPlanJob, resetPlanExecution } from "@/lib/store";

type Context = {
  params: Promise<{ id: string }>;
};

export async function POST(_: Request, context: Context) {
  const { id } = await context.params;
  const job = await getPlanJob(id);

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
        error: "Plan is already running",
      },
      { status: 409 },
    );
  }

  const resetJob = await resetPlanExecution(id, "running");
  if (!resetJob) {
    return NextResponse.json(
      {
        ok: false,
        error: "Plan not found",
      },
      { status: 404 },
    );
  }

  after(() => runPlanInBackground(id));

  return NextResponse.json(
    {
      ok: true,
      job: resetJob,
    },
    { status: 202 },
  );
}
