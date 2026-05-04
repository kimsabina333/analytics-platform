import Plot from "react-plotly.js";
import { ChartData } from "../../types/api";

export function ChartEmbed({ chart }: { chart: ChartData }) {
  const { traces, title, is_alert, ci_label } = chart;

  const data: Plotly.Data[] = [
    {
      name: ci_label,
      x: [...traces.dates, ...traces.dates.slice().reverse()],
      y: [...traces.ci_high, ...traces.ci_low.slice().reverse()],
      fill: "toself",
      fillcolor: "rgba(0,255,255,0.12)",
      line: { color: "transparent" },
      type: "scatter",
      hoverinfo: "skip",
    },
    {
      name: "Bayesian Mean",
      x: traces.dates,
      y: traces.mean,
      line: { color: "#00FFFF", dash: "dash", width: 1.5 },
      type: "scatter",
      hovertemplate: "%{y:.1%}<extra>Mean</extra>",
    },
    {
      name: "Actual SR",
      x: traces.dates,
      y: traces.actual,
      mode: "lines+markers",
      line: { color: "#FF4444", width: 2 },
      marker: { size: 5 },
      type: "scatter",
      hovertemplate: "%{y:.1%}<extra>Actual</extra>",
    },
  ];

  return (
    <div className="border border-gray-700 rounded mt-2">
      <Plot
        data={data}
        layout={{
          title: {
            text: title + (is_alert ? " ⚠ ALERT" : ""),
            font: { color: "#FF4444", size: 12, family: "monospace" },
          },
          paper_bgcolor: "#0a0a0a",
          plot_bgcolor: "#0a0a0a",
          font: { color: "#FFFFFF", family: "monospace", size: 11 },
          xaxis: { gridcolor: "#1a1a1a", linecolor: "#1a1a1a" },
          yaxis: { gridcolor: "#1a1a1a", linecolor: "#1a1a1a", tickformat: ".1%" },
          legend: { bgcolor: "#0a0a0a", bordercolor: "#222", borderwidth: 1, x: 0, y: 1 },
          margin: { t: 36, b: 36, l: 55, r: 12 },
          hovermode: "x unified",
        }}
        config={{ displayModeBar: false, responsive: true }}
        style={{ width: "100%", height: "240px" }}
      />
    </div>
  );
}
