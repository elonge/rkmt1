import { executePlan } from "../agents/orchestrator";
import {
  appendArtifact,
  getPlanJob,
  setFinalAnswer,
  setPlanStatus,
  updateStepAgentToolCalls,
  updateStepStatus,
} from "../store";
import { writeAgentToolTraceFile } from "./agent-trace-file";

const activeRuns = new Set<string>();

export async function runPlanInBackground(jobId: string): Promise<void> {
  if (activeRuns.has(jobId)) {
    return;
  }

  activeRuns.add(jobId);

  try {
    const job = await getPlanJob(jobId);
    if (!job) {
      return;
    }

    const result = await executePlan(job.question, job.plan, {
      async onStepStart(step) {
        await updateStepStatus(jobId, step.id, "running");
      },
      async onAgentToolCalls(step, agentToolCalls) {
        await updateStepAgentToolCalls(jobId, step.id, agentToolCalls);
        await writeAgentToolTraceFile({
          jobId,
          stepId: step.id,
          stepTitle: step.title,
          stepToolId: step.toolId,
          agentToolCalls,
        });
      },
      async onStepComplete(step, artifact, summary) {
        await appendArtifact(jobId, artifact);
        await updateStepStatus(jobId, step.id, "completed", summary);
      },
      async onStepFailed(step, error) {
        await updateStepStatus(jobId, step.id, "failed", error);
      },
    });

    await setFinalAnswer(jobId, result.finalAnswer);
    await setPlanStatus(jobId, "completed");
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown execution failure";
    await setPlanStatus(jobId, "failed", message);
  } finally {
    activeRuns.delete(jobId);
  }
}
