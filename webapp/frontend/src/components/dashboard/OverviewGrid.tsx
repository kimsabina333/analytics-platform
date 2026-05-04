import { SegmentPredictionResult } from "../../types/api";

interface Props {
  segments: SegmentPredictionResult[];
  onSelect: (s: SegmentPredictionResult) => void;
  selected: SegmentPredictionResult | null;
}

function pct(v: number | null | undefined) {
  if (v == null) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function labelizeKey(key: string) {
  const labels: Record<string, string> = {
    mid: "MID",
    utm_source: "UTM Source",
    payment_method: "Payment method",
    card_brand: "Card brand",
    card_type: "Card type",
  };
  if (labels[key]) return labels[key];
  return key.replace(/_/g, " ").replace(/\b\w/g, (ch) => ch.toUpperCase());
}

function labelizeValue(value: string) {
  const labels: Record<string, string> = {
    applepay: "Apple Pay",
    googlepay: "Google Pay",
    card: "Card",
    visa: "Visa",
    mastercard: "Mastercard",
    facebook: "Facebook",
    google: "Google",
    checkout: "Checkout",
    adyen: "Adyen",
    adyen_us: "Adyen US",
  };
  return labels[value] ?? value;
}

function segmentTitle(segment: SegmentPredictionResult) {
  if (segment.dimension !== "combo") {
    return <>{labelizeValue(segment.value)}</>;
  }

  return (
    <span className="flex flex-col gap-1">
      {segment.value.split("&").map((part) => {
        const [key, ...valueParts] = part.split("=");
        return (
          <span key={part} className="flex gap-1.5">
            <span className="shrink-0 text-gray-400 font-medium">
              {labelizeKey(key)}:
            </span>
            <span className="min-w-0 break-words [overflow-wrap:anywhere]">
              {labelizeValue(valueParts.join("="))}
            </span>
          </span>
        );
      })}
    </span>
  );
}

function groupTitle(dimension: string) {
  if (dimension === "combo") return "Subsegments";
  return labelizeKey(dimension);
}

function groupRank(dimension: string) {
  const order: Record<string, number> = {
    mid: 1,
    offer: 2,
    utm_source: 3,
    combo: 4,
  };
  return order[dimension] ?? 99;
}

export function OverviewGrid({ segments, onSelect, selected }: Props) {
  const groups = Object.entries(
    segments.reduce<Record<string, SegmentPredictionResult[]>>((acc, segment) => {
      (acc[segment.dimension] ||= []).push(segment);
      return acc;
    }, {})
  ).sort(([a], [b]) => groupRank(a) - groupRank(b) || groupTitle(a).localeCompare(groupTitle(b)));

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-3">
      {groups.map(([dimension, groupSegments]) => (
        <div key={dimension} className="contents">
          <div
            className="col-span-full mt-2 first:mt-0 pt-3 first:pt-0 border-t first:border-t-0 border-gray-800 flex items-center gap-2"
          >
            <span className="text-xs uppercase tracking-wider font-bold text-gray-400">
              {groupTitle(dimension)}
            </span>
            <span className="text-xs text-gray-600">
              {groupSegments.length} segment{groupSegments.length === 1 ? "" : "s"}
            </span>
          </div>
          {groupSegments.map((s) => {
        const last = s.daily[s.daily.length - 1];
        const isSelected = selected?.dimension === s.dimension && selected?.value === s.value;
        return (
          <button
            key={`${s.dimension}=${s.value}`}
            onClick={() => onSelect(s)}
            className={`text-left p-3 rounded border transition-all ${
              s.is_alert
                ? "border-red-600 bg-red-950 hover:bg-red-900"
                : isSelected
                ? "border-cyan-400 bg-gray-900"
                : "border-gray-700 bg-gray-900 hover:border-gray-500"
            }`}
          >
            <p className="text-xs text-gray-400 truncate">
              {labelizeKey(s.dimension)}
            </p>
            <p className="text-sm font-bold text-white break-words [overflow-wrap:anywhere]">
              {segmentTitle(s)}
            </p>
            <p className={`text-lg font-mono mt-1 ${s.is_alert ? "text-red-400" : "text-cyan-400"}`}>
              {pct(last?.actual_sr)}
            </p>
            <p className="text-xs text-gray-500">
              CI: {pct(last?.ci_low)} – {pct(last?.ci_high)}
            </p>
            {s.is_alert && (
              <p className="text-xs text-red-400 mt-1">⚠ ALERT</p>
            )}
          </button>
        );
          })}
        </div>
      ))}
    </div>
  );
}
