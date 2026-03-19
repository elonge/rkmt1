import { AsyncLocalStorage } from "node:async_hooks";

type ToolDebugSink = {
  appendLog: (message: string, data?: unknown) => Promise<void>;
};

const toolDebugStorage = new AsyncLocalStorage<ToolDebugSink>();

export async function runWithToolDebugLogging<T>(
  appendLog: ToolDebugSink["appendLog"],
  execute: () => Promise<T>,
): Promise<T> {
  return toolDebugStorage.run({ appendLog }, execute);
}

export async function logToolDebug(message: string, data?: unknown): Promise<void> {
  const sink = toolDebugStorage.getStore();
  if (!sink) {
    return;
  }

  await sink.appendLog(message, data);
}
