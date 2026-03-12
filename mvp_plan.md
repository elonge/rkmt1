# MVP PLAN: Analysis Tool

## Phase 1: Orchestrator Core (✅ Done)

The execution engine is strict, type-safe, and observable.

* [x] Establish the core execution engine.
* [x] Draft, validate, revise, and execute plans securely.
* [x] Eliminate silent fallbacks and string-parsing vulnerabilities.

## Phase 2: Audience Handling (Agent & Pre-processing)

Transition the `audience_builder_agent` from using hardcoded dummy math to true semantic reasoning.

* [ ] **Data Pipeline Setup:** Build a background worker (cron/queue) to compute and store vector embeddings for incoming messages and group metadata.
* [ ] **Tool Upgrades:** Rewrite the `audience_lookup` and `influencer_lookup` tools to use native vector search combined with LLM synthesis (replacing the limited 250-group heuristic).
* [ ] **Agent Integration:** Wire the upgraded tools into the `audience_builder_agent` for dynamic audience segmentation.

## Phase 3: Narrative Handling (Agent & Pre-processing)

Empower the `narrative_explorer_agent` to semantically understand trending community discussions.

* [ ] **Clustering Pipeline:** Implement a scheduled background job to identify, group, and cluster trending topics (preventing the agent from reading raw database rows on the fly).
* [ ] **Tool Upgrades:** Implement `narrative_probe` and `find_narratives_in_timeframe` to query these pre-processed clusters.
* [ ] **Sentiment & Volume Analysis:** Configure the tools to accurately assess the sentiment and volume of the extracted narratives.

## Phase 4: Finalization (Stats & Pre-execution Evaluation)

Lock down the remaining data layer and stress-test the LLM's reasoning against real-world chaos.

* [ ] **Stats Query Hardening:** Upgrade `db_stats_query` to safely and dynamically translate the `stats_query_agent`'s parameters into secure MongoDB aggregation pipelines.
* [ ] **Evaluation Suite:** Run a test suite of ~50 real-world questions against a snapshot of the real database.
* [ ] **Prompt & Schema Tuning:** Tune the agent prompts and adjust Zod schemas based on the evaluation.
* [ ] **Synthesis Verification:** Verify that the `synthesis_agent` accurately reports data without hallucinating conclusions.

## Phase 5: Tests and Integrations

The final leap to production.

* [ ] **Codebase Integration:** Merge this orchestrator codebase into the broader parent project architecture.
* [ ] **Live DB Wiring:** Connect the production MongoDB instance.
* [ ] **Auth & UI:** Configure real user authentication and wire up the final frontend interface.


## Next Steps (Post-MVP)
Once the core system is answering real questions reliably, the focus will shift to expanding the richness of the data and tracking capabilities.

[ ] Support Media Messages: Upgrade the ingestion pipeline to run OCR on images, transcribe audio/video clips, and embed those transcripts so the agents can analyze memes, voice notes, and forwarded videos.

[ ] Flow of a Narrative: Implement tracking for how a specific narrative spreads. (e.g., Did it start in a fringe group and move to a mainstream community? Which influencer crossed the boundary to share it?)

[ ] Engagement Scoring Engine: Build a more sophisticated, weighted scoring system for posts and reactions. Move beyond simple counts to evaluate "impact" (e.g., an organic reply is worth more than a generic thumbs-up; a forward from a high-trust user carries a higher weight).