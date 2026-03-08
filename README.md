# RKMT Planner Runtime Prototype

This repository now includes a prototype web runtime that matches the meeting flow:

1. User submits a question.
2. Backend drafts a plan and waits for approval.
3. User can revise the plan with feedback.
4. On approval, backend runs the plan in background.
5. Final answer is returned as strict typed JSON for the UI.

## Tech choices

- Web framework: `Next.js` (App Router + API routes)
- LLM framework: `OpenAI Agent SDK` (`@openai/agents`)
- Data source: MongoDB collections populated with dummy schema-based JSON data

## Implemented architecture

- `Planner` agent: creates plan draft JSON.
- `PlanReviser` agent: updates plan after feedback.
- `SpecialistManager` agent: treats specialist agents as tools.
- Specialist agents:
  - `AudienceBuilder` (Mongo-backed audience + influencer tools + stats tool)
  - `NarrativeExplorer` (Mongo-backed narratives/message search + dummy summarization)
  - `StatsQuery` (shared stats tool)
- `Synthesizer` agent: outputs strict JSON shape for UI.

## Dummy vs real tools

- Mongo-backed tools:
  - `db_stats_query`
  - `find_narratives_in_timeframe`
  - `audience_lookup_dummy`
  - `influencer_lookup_dummy`
  - `vector_search_dummy` (mock scoring, real message retrieval from Mongo)
- Still simplified/dummy:
  - summarization
  - narrative probe

## Potential questions handling

`potential questions.txt` is loaded as planner/synthesis context only.
The runtime does not hardcode support only for those questions.

## Run locally

1. Install dependencies:
   - `npm install`
2. Set env vars:
   - copy `.env.example` to `.env.local`
   - set `OPENAI_API_KEY`
   - optional: set `OPENAI_MODEL` (default `gpt-4.1`)
   - set `MONGO_URI`
   - set `MONGO_DB_NAME`
   - optional: set `MONGO_PLAN_JOBS_COLLECTION` (default `plan_jobs`)
   - optional: set `PLAN_STORE_DIR` only if you want file-backed fallback storage when Mongo is not configured
   - set `NEXT_PUBLIC_FIREBASE_API_KEY`
   - set `NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN`
   - set `NEXT_PUBLIC_FIREBASE_PROJECT_ID`
   - set `NEXT_PUBLIC_FIREBASE_APP_ID`
   - optional: set `NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID`
   - optional: set `NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET`
   - set `NEXT_PUBLIC_AUTH_ALLOWED_EMAILS` to a comma-separated list of allowed emails
3. Start dev server:
   - `npm run dev`
4. Enable Google sign-in in your Firebase project.
5. The UI only allows Google accounts whose email appears in `NEXT_PUBLIC_AUTH_ALLOWED_EMAILS`.

Plan jobs are persisted in MongoDB when `MONGO_URI` is configured, using the `plan_jobs` collection by default. The file-backed store remains only as a local fallback, and `PLAN_STORE_DIR` applies to that fallback path.
