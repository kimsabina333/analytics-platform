import { useEffect, useRef } from "react";

export function usePolling(fn: () => Promise<void>, intervalMs: number, enabled = true) {
  const fnRef = useRef(fn);
  fnRef.current = fn;

  useEffect(() => {
    if (!enabled) return;
    let timeoutId: ReturnType<typeof setTimeout>;

    const tick = async () => {
      await fnRef.current();
      timeoutId = setTimeout(tick, intervalMs);
    };

    tick();
    return () => clearTimeout(timeoutId);
  }, [intervalMs, enabled]);
}
