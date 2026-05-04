import { ChartData } from "../../types/api";
import { ChartEmbed } from "./ChartEmbed";

interface Props {
  tool: string;
  input: unknown;
  result: unknown;
}

function isChartData(r: unknown): r is ChartData {
  return (
    typeof r === "object" &&
    r !== null &&
    (r as ChartData).chart_type === "sr_timeseries"
  );
}

export function ToolResultCard({ tool, result }: Props) {
  const toolLabels: Record<string, string> = {
    get_segment_sr: "Fetched SR data",
    explain_declines: "Explained decline drivers",
    list_alerts: "Checked active alerts",
    compare_segments: "Compared segments",
    get_top_performing: "Ranked segments",
    generate_chart_data: "Generated chart",
  };

  return (
    <div className="my-1">
      <p className="text-xs text-gray-500 italic">
        ⚙ {toolLabels[tool] || tool}
      </p>
      {isChartData(result) && <ChartEmbed chart={result} />}
    </div>
  );
}
