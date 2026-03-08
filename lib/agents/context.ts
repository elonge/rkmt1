import { readFileSync } from "node:fs";
import { join } from "node:path";

let cachedPotentialQuestions: string[] | null = null;

function readTextFile(relativePath: string): string {
  const abs = join(process.cwd(), relativePath);
  return readFileSync(abs, "utf8");
}

export function getPotentialQuestions(): string[] {
  if (cachedPotentialQuestions) {
    return cachedPotentialQuestions;
  }

  try {
    const raw = readTextFile("potential questions.txt");
    cachedPotentialQuestions = raw
      .split("\n")
      .map((line) => line.trim())
      .map((line) => line.replace(/^\uFEFF/, ""))
      .map((line) => line.replace(/^\d+\.\s*/, ""))
      .filter((line) => line.length > 0);
    return cachedPotentialQuestions;
  } catch {
    cachedPotentialQuestions = [];
    return cachedPotentialQuestions;
  }
}

export function plannerReferenceContext(): string {
  const potentialQuestions = getPotentialQuestions();

  const samples = potentialQuestions.slice(0, 8);
  const sampleBlock = samples.length
    ? samples.map((sample, idx) => `${idx + 1}. ${sample}`).join("\n")
    : "No sample questions available.";

  return [
    "Potential question examples (context only; do not constrain capability):",
    sampleBlock,
  ].join("\n");
}
