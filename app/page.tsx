"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import PlannerRuntime from "./planner-runtime";
import { useAuth } from "@/lib/firebase/auth-provider";

export default function HomePage() {
  const router = useRouter();
  const { configured, loading, user } = useAuth();

  useEffect(() => {
    if (!loading && (!configured || !user)) {
      router.replace("/login");
    }
  }, [configured, loading, router, user]);

  if (loading) {
    return (
      <main className="auth-page">
        <section className="card auth-card">
          <h1>Checking session</h1>
          <p className="subtle">Verifying Firebase authentication state.</p>
        </section>
      </main>
    );
  }

  if (!configured || !user) {
    return null;
  }

  return <PlannerRuntime />;
}
