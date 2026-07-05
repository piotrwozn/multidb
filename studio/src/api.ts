export {
  ControlPlaneClient,
  ControlPlaneError,
  errorMessage,
  parseJsonObject,
  stringifyJson,
} from "@multidb/client";

export type { ConfigPlanRequest, ControlPlaneClientOptions } from "@multidb/client";

export const defaultApiBase = (): string =>
  import.meta.env.VITE_MULTIDB_API_BASE ?? "/api";
