"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { GoogleAuthProvider, signInWithPopup, signOut } from "firebase/auth";
import { useAuth } from "@/lib/firebase/auth-provider";
import {
  getAllowedAuthEmails,
  getFirebaseAuth,
  getMissingClientAuthEnvVars,
  isWhitelistedEmail,
} from "@/lib/firebase/client";

export default function LoginPage() {
  const router = useRouter();
  const { configured, loading, user } = useAuth();
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const canSubmit = configured && !submitting;

  useEffect(() => {
    if (!loading && user) {
      router.replace("/");
    }
  }, [loading, router, user]);

  async function submitAuth() {
    if (!canSubmit) {
      return;
    }

    setSubmitting(true);
    setError(null);

    try {
      const auth = getFirebaseAuth();
      const provider = new GoogleAuthProvider();
      provider.setCustomParameters({ prompt: "select_account" });
      const result = await signInWithPopup(auth, provider);

      if (!isWhitelistedEmail(result.user.email)) {
        await signOut(auth);
        setError("This Google account is not on the allowlist.");
        return;
      }

      router.replace("/");
    } catch (authError) {
      setError(authError instanceof Error ? authError.message : "Authentication failed");
    } finally {
      setSubmitting(false);
    }
  }

  if (loading && user) {
    return null;
  }

  return (
    <main className="auth-page">
      <section className="card auth-card">
        <h1>Login</h1>
        <p className="subtle">
          Use Firebase Authentication with an allowlisted Google account to access the planner
          runtime.
        </p>

        {!configured ? (
          <div className="auth-warning">
            <p className="error">Client auth is not configured.</p>
            <p className="subtle">
              Missing env vars: {getMissingClientAuthEnvVars().join(", ")}
            </p>
          </div>
        ) : null}

        <div className="actions">
          <button type="button" disabled={!canSubmit} onClick={submitAuth}>
            {submitting ? "Working..." : "Continue with Google"}
          </button>
        </div>

        {error ? <p className="error">{error}</p> : null}
      </section>
    </main>
  );
}
