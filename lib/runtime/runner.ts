import { executePlan } from "../agents/orchestrator";
import {
  appendArtifact,
  getPlanJob,
  setFinalAnswer,
  setPlanStatus,
  updateStepStatus,
} from "../store";

const activeRuns = new Set<string>();

export async function runPlanInBackground(jobId: string): Promise<void> {
  if (activeRuns.has(jobId)) {
    return;
  }

  activeRuns.add(jobId);

  try {
    const job = getPlanJob(jobId);
    if (!job) {
      return;
    }

    const result = await executePlan(job.question, job.plan, {
      onStepStart(step) {
        updateStepStatus(jobId, step.id, "running");
      },
      onStepComplete(step, artifact, summary) {
        appendArtifact(jobId, artifact);
        updateStepStatus(jobId, step.id, "completed", summary);
      },
      onStepFailed(step, error) {
        updateStepStatus(jobId, step.id, "failed", error);
      },
    });

    setFinalAnswer(jobId, result.finalAnswer);
    setPlanStatus(jobId, "completed");
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown execution failure";
    setPlanStatus(jobId, "failed", message);
  } finally {
    activeRuns.delete(jobId);
  }
}
