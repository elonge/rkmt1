import { getApp, getApps, initializeApp } from "firebase/app";
import { getAuth } from "firebase/auth";

const firebaseEnv = {
  apiKey: process.env.NEXT_PUBLIC_FIREBASE_API_KEY,
  authDomain: process.env.NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN,
  projectId: process.env.NEXT_PUBLIC_FIREBASE_PROJECT_ID,
  appId: process.env.NEXT_PUBLIC_FIREBASE_APP_ID,
  messagingSenderId: process.env.NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID,
  storageBucket: process.env.NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET,
};

const allowedAuthEmails = (process.env.NEXT_PUBLIC_AUTH_ALLOWED_EMAILS ?? "")
  .split(",")
  .map((email) => email.trim().toLowerCase())
  .filter((email) => email.length > 0);

const requiredClientAuthEnv = [
  {
    key: "NEXT_PUBLIC_FIREBASE_API_KEY",
    value: firebaseEnv.apiKey,
  },
  {
    key: "NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN",
    value: firebaseEnv.authDomain,
  },
  {
    key: "NEXT_PUBLIC_FIREBASE_PROJECT_ID",
    value: firebaseEnv.projectId,
  },
  {
    key: "NEXT_PUBLIC_FIREBASE_APP_ID",
    value: firebaseEnv.appId,
  },
  {
    key: "NEXT_PUBLIC_AUTH_ALLOWED_EMAILS",
    value: allowedAuthEmails.length > 0 ? "configured" : "",
  },
] as const;

export function getMissingClientAuthEnvVars(): string[] {
  return requiredClientAuthEnv.filter((entry) => !entry.value).map((entry) => entry.key);
}

export function isClientAuthConfigured(): boolean {
  return getMissingClientAuthEnvVars().length === 0;
}

export function getAllowedAuthEmails(): string[] {
  return allowedAuthEmails;
}

export function isWhitelistedEmail(email: string | null | undefined): boolean {
  return Boolean(email && allowedAuthEmails.includes(email.trim().toLowerCase()));
}

export function getFirebaseApp() {
  if (!isClientAuthConfigured()) {
    throw new Error(
      `Client auth is not configured. Missing env vars: ${getMissingClientAuthEnvVars().join(", ")}`,
    );
  }

  return getApps().length > 0 ? getApp() : initializeApp(firebaseEnv);
}

export function getFirebaseAuth() {
  return getAuth(getFirebaseApp());
}
