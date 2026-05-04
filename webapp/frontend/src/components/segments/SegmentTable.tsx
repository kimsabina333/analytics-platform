import { SegmentPredictionResult } from "../../types/api";

interface Props {
  result: SegmentPredictionResult;
}

function pct(v: number | null | undefined) {
  if (v == null) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

export function SegmentTable({ result }: Props) {
  const { daily, dimension, value, q_threshold, is_alert, ci_width } = result;
  const ciLabel = `CI ${Math.round((1 - 2 * q_threshold) * 100)}%`;

  return (
    <div className="border border-gray-700 rounded overflow-hidden">
      <div className="px-4 py-3 border-b border-gray-700 flex items-center justify-between">
        <div>
          <span className="text-cyan-400 font-bold">
            {dimension}={value}
          </span>
          <span className="ml-3 text-xs text-gray-400">
            {ciLabel} · avg CI width: {(ci_width * 100).toFixed(1)}pp
          </span>
        </div>
        {is_alert && (
          <span className="text-xs bg-red-900 text-red-300 px-2 py-0.5 rounded">⚠ ALERT</span>
        )}
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-800 text-gray-400 text-xs">
            <th className="text-left px-4 py-2">Date</th>
            <th className="text-right px-4 py-2">Actual SR</th>
            <th className="text-right px-4 py-2">Mean</th>
            <th className="text-right px-4 py-2">CI Low</th>
            <th className="text-right px-4 py-2">CI High</th>
            <th className="text-right px-4 py-2">Count</th>
            <th className="text-left px-4 py-2">Top decline</th>
            <th className="text-center px-4 py-2">Status</th>
          </tr>
        </thead>
        <tbody>
          {daily.map((d) => (
            <tr
              key={d.date}
              className={`border-b border-gray-800 ${
                d.is_alert ? "bg-red-950" : "hover:bg-gray-900"
              }`}
            >
              <td className="px-4 py-2 text-gray-300">{d.date}</td>
              <td className={`px-4 py-2 text-right font-mono ${d.is_alert ? "text-red-400" : "text-white"}`}>
                {pct(d.actual_sr)}
              </td>
              <td className="px-4 py-2 text-right font-mono text-cyan-400">{pct(d.mean)}</td>
              <td className="px-4 py-2 text-right font-mono text-gray-400">{pct(d.ci_low)}</td>
              <td className="px-4 py-2 text-right font-mono text-gray-400">{pct(d.ci_high)}</td>
              <td className="px-4 py-2 text-right text-gray-400">
                {d.count.toLocaleString()}
              </td>
              <td className="px-4 py-2 text-xs text-amber-300">
                {d.top_decline_category
                  ? `${d.top_decline_category} (${(d.decline_count ?? 0).toLocaleString()})`
                  : "None"}
              </td>
              <td className="px-4 py-2 text-center">
                {d.is_alert ? (
                  <span className="text-xs text-red-400">⚠</span>
                ) : (
                  <span className="text-xs text-green-500">✓</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
