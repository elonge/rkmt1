"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { signOut } from "firebase/auth";
import type { ExecutionArtifact, PlanJob } from "@/lib/types";
import { getFirebaseAuth } from "@/lib/firebase/client";
import { useAuth } from "@/lib/firebase/auth-provider";

type ApiJobResponse = {
  ok: boolean;
  error?: string;
  job?: PlanJob;
};

const isPendingApproval = (job: PlanJob | null): boolean =>
  Boolean(job && job.status === "awaiting_approval");

export default function PlannerRuntime() {
  const { user } = useAuth();
  const [question, setQuestion] = useState("");
  const [feedback, setFeedback] = useState("");
  const [job, setJob] = useState<PlanJob | null>(null);
  const [revisionHistory, setRevisionHistory] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const jobId = job?.id;
  const jobStatus = job?.status;

  const canSubmit = useMemo(() => question.trim().length >= 3 && !loading, [question, loading]);
  const canApplyRevision = useMemo(
    () => Boolean(job && feedback.trim().length >= 3 && !loading),
    [feedback, job, loading],
  );
  const artifactsByStepId = useMemo(() => {
    const map = new Map<string, ExecutionArtifact>();
    if (!job) {
      return map;
    }

    for (const artifact of job.artifacts) {
      map.set(artifact.stepId, artifact);
    }

    return map;
  }, [job]);

  async function requestJson(url: string, init?: RequestInit): Promise<ApiJobResponse> {
    const response = await fetch(url, {
      headers: {
        "Content-Type": "application/json",
      },
      ...init,
    });

    const payload = (await response.json()) as ApiJobResponse;
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error ?? "Request failed");
    }

    return payload;
  }

  async function createPlan() {
    setLoading(true);
    setError(null);

    try {
      const payload = await requestJson("/api/questions", {
        method: "POST",
        body: JSON.stringify({ question }),
      });

      if (payload.job) {
        setJob(payload.job);
      }
      setFeedback("");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  const refreshJob = useCallback(
    async (id?: string) => {
      const targetId = id ?? jobId;
      if (!targetId) {
        return;
      }

      try {
        const payload = await requestJson(`/api/plans/${targetId}`);
        if (payload.job) {
          setJob(payload.job);
        }
      } catch (requestError) {
        setError(requestError instanceof Error ? requestError.message : "Unknown error");
      }
    },
    [jobId],
  );

  async function reviseCurrentPlan() {
    if (!job || feedback.trim().length < 3) {
      return;
    }

    const currentJob = job;
    const trimmedFeedback = feedback.trim();
    const nextRevisionHistory = [...currentJob.revisionNotes, trimmedFeedback];

    setLoading(true);
    setError(null);
    setRevisionHistory(nextRevisionHistory);
    setJob(null);

    try {
      const payload = await requestJson(`/api/plans/${currentJob.id}`, {
        method: "POST",
        body: JSON.stringify({ feedback: trimmedFeedback }),
      });
      if (payload.job) {
        setJob(payload.job);
      }
      setFeedback("");
    } catch (requestError) {
      setJob(currentJob);
      setError(requestError instanceof Error ? requestError.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  async function approvePlan() {
    if (!job) {
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const payload = await requestJson(`/api/plans/${job.id}/approve`, {
        method: "POST",
      });
      if (payload.job) {
        setJob(payload.job);
      }
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  async function signOutCurrentUser() {
    setError(null);

    try {
      await signOut(getFirebaseAuth());
    } catch (signOutError) {
      setError(signOutError instanceof Error ? signOutError.message : "Failed to sign out");
    }
  }

  useEffect(() => {
    if (!jobId || jobStatus !== "running") {
      return;
    }

    const interval = setInterval(() => {
      void refreshJob(jobId);
    }, 1500);

    return () => clearInterval(interval);
  }, [jobId, jobStatus, refreshJob]);

  useEffect(() => {
    if (job) {
      setRevisionHistory(job.revisionNotes);
    }
  }, [job]);

  return (
    <main className="page">
      <section className="card">
        <div className="row">
          <div>
            <h1>Question Planner Runtime</h1>
            <p className="subtle">
              Flow: submit question, inspect/revise plan, approve, then backend executes in the
              background.
            </p>
          </div>
          <div className="session-actions">
            <span className="subtle">{user?.email ?? "Signed in"}</span>
            <button type="button" className="ghost" onClick={signOutCurrentUser}>
              Sign Out
            </button>
          </div>
        </div>

        <label htmlFor="question">Question</label>
        <textarea
          id="question"
          rows={4}
          value={question}
          onChange={(event) => setQuestion(event.target.value)}
          placeholder="Ask an analytics question..."
        />

        {revisionHistory.length > 0 ? (
          <div className="revision-history">
            <p className="subtle">Revisions to original plan</p>
            <ol>
              {revisionHistory.map((note, index) => (
                <li key={`${index}-${note}`}>{note}</li>
              ))}
            </ol>
          </div>
        ) : null}

        <div className="actions">
          <button type="button" disabled={!canSubmit} onClick={createPlan}>
            {loading ? "Working..." : "Generate Plan"}
          </button>
          {job ? (
            <button type="button" className="ghost" onClick={() => void refreshJob()}>
              Refresh
            </button>
          ) : null}
        </div>

        {error ? <p className="error">{error}</p> : null}
      </section>

      {job ? (
        <section className="card">
          <div className="row">
            <h2>Current Plan</h2>
            <span className={`status status-${job.status}`}>{job.status}</span>
          </div>

          <p className="subtle">Objective: {job.plan.objective}</p>

          <ol className="steps">
            {job.plan.steps.map((step) => {
              const artifact = artifactsByStepId.get(step.id);
              const hasStepOutput =
                (step.status === "completed" || step.status === "failed") &&
                Boolean(artifact || step.outputSummary);

              return (
                <li key={step.id}>
                  <div className="step-body">
                    <div className="row">
                      <strong>{step.title}</strong>
                      <span className={`status status-${step.status}`}>{step.status}</span>
                    </div>
                    <p>{step.rationale}</p>
                    {step.outputSummary ? <p className="subtle">{step.outputSummary}</p> : null}
                  </div>

                  <details className="step-detail">
                    <summary>DEBUG</summary>
                    <pre>{JSON.stringify(step, null, 2)}</pre>
                  </details>

                  {hasStepOutput ? (
                    <details className="step-detail">
                      <summary>step output</summary>
                      {artifact ? (
                        <pre>{JSON.stringify(artifact.data, null, 2)}</pre>
                      ) : (
                        <p className="subtle">
                          No structured result payload was recorded for this step.
                        </p>
                      )}
                    </details>
                  ) : null}
                </li>
              );
            })}
          </ol>

          {isPendingApproval(job) ? (
            <>
              <label htmlFor="feedback">Suggest changes to plan</label>
              <textarea
                id="feedback"
                rows={3}
                value={feedback}
                onChange={(event) => setFeedback(event.target.value)}
                placeholder="Optional revision feedback"
              />

              <div className="actions">
                <button
                  type="button"
                  className="ghost"
                  disabled={!canApplyRevision}
                  onClick={reviseCurrentPlan}
                >
                  Apply Revision
                </button>
                <button type="button" onClick={approvePlan}>
                  Approve and Run
                </button>
              </div>
            </>
          ) : null}

          {job.finalAnswer ? (
            <>
              <h3>Final Answer</h3>
              <pre>{JSON.stringify(job.finalAnswer, null, 2)}</pre>
            </>
          ) : null}

          {job.error ? <p className="error">Execution error: {job.error}</p> : null}
        </section>
      ) : null}
    </main>
  );
}
