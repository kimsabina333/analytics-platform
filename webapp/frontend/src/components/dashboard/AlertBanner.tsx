import { SegmentPredictionResult } from "../../types/api";

export function AlertBanner({ alerts }: { alerts: SegmentPredictionResult[] }) {
  if (alerts.length === 0) return null;

  return (
    <div className="border border-red-600 bg-red-950 rounded px-4 py-3 mb-4">
      <p className="text-red-400 font-bold text-sm mb-1">
        ⚠ {alerts.length} ACTIVE ALERT{alerts.length > 1 ? "S" : ""}
      </p>
      <div className="flex flex-wrap gap-2">
        {alerts.map((a) => (
          <span
            key={`${a.dimension}=${a.value}`}
            className="text-xs bg-red-900 text-red-300 px-2 py-0.5 rounded"
          >
            {a.dimension}={a.value}
            {a.daily[a.daily.length - 1]?.actual_sr != null
              ? ` ${(a.daily[a.daily.length - 1].actual_sr! * 100).toFixed(1)}%`
              : ""}
          </span>
        ))}
      </div>
    </div>
  );
}
