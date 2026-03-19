import { tmpdir } from "node:os";
import { join } from "node:path";

export function resolveRuntimeDir(): string {
  if (process.env.PLAN_STORE_DIR) {
    return process.env.PLAN_STORE_DIR;
  }

  if (process.env.VERCEL || process.env.LAMBDA_TASK_ROOT || process.env.AWS_EXECUTION_ENV) {
    return join(tmpdir(), "rkmt-runtime");
  }

  return join(process.cwd(), ".runtime");
}
