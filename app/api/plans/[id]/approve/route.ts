import { after, NextResponse } from "next/server";
import { runPlanInBackground } from "@/lib/runtime/runner";
import { getPlanJob, setPlanStatus } from "@/lib/store";

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

  await setPlanStatus(id, "running");
  after(() => runPlanInBackground(id));

  const refreshed = await getPlanJob(id);
  return NextResponse.json(
    {
      ok: true,
      job: refreshed,
    },
    { status: 202 },
  );
}
