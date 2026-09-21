import { useSyncExternalStore } from "react";

/**
 * The correlation ID sent with each query, kept where the UI can show it.
 *
 * It is the thread the whole stack is grepped by: the console mints it, Ontop adopts it as its
 * query id, the JDBC connection carries it to Spark Connect as gRPC metadata, and the propagation
 * plugin passes it to Polaris as X-Request-ID. `make cid CID=<the value shown>` prints every line
 * that mentions it.
 *
 * A UUID because Ontop only adopts the header when it parses as one; anything else is logged but
 * leaves Ontop minting its own id, and the chain breaks in the middle.
 */
let current: string | null = null;
const listeners = new Set<() => void>();

function emit(): void {
  listeners.forEach((listener) => listener());
}

/** Mints the ID for the next request and publishes it. Called from YASGUI's header callback. */
export function nextCorrelationId(): string {
  current = crypto.randomUUID();
  emit();
  return current;
}

export function useCorrelationId(): string | null {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => current,
    () => null,
  );
}
