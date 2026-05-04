import Plot from "react-plotly.js";
import { SegmentPredictionResult } from "../../types/api";

interface Props {
  result: SegmentPredictionResult;
}

export function SRTimeSeriesChart({ result }: Props) {
  const { daily, q_threshold, dimension, value } = result;
  const ciLabel = `CI ${Math.round((1 - 2 * q_threshold) * 100)}%`;
  const dates = daily.map((d) => d.date);
  const declineCategories = Array.from(
    new Set(daily.flatMap((d) => d.declines?.map((item) => item.category) ?? []))
  );
  const declineColors: Record<string, string> = {
    INSUFFICIENT_FUNDS: "rgba(245,158,11,0.72)",
    FRAUD_RISK: "rgba(239,68,68,0.72)",
    DO_NOT_HONOR: "rgba(168,85,247,0.72)",
    CARD_ISSUE: "rgba(59,130,246,0.70)",
    BANK_DECLINE: "rgba(236,72,153,0.70)",
    TECH_ERROR: "rgba(6,182,212,0.70)",
    OTHER: "rgba(148,163,184,0.62)",
  };

  const traces: Plotly.Data[] = [
    {
      name: ciLabel,
      x: [...dates, ...dates.slice().reverse()],
      y: [
        ...daily.map((d) => d.ci_high),
        ...daily
          .slice()
          .reverse()
          .map((d) => d.ci_low),
      ],
      fill: "toself",
      fillcolor: "rgba(0,255,255,0.12)",
      line: { color: "transparent" },
      type: "scatter",
      hoverinfo: "skip",
    },
    {
      name: "Bayesian Mean",
      x: dates,
      y: daily.map((d) => d.mean),
      line: { color: "#00FFFF", dash: "dash", width: 1.5 },
      type: "scatter",
      hovertemplate: "%{y:.1%}<extra>Mean</extra>",
    },
    {
      name: "Actual SR",
      x: dates,
      y: daily.map((d) => d.actual_sr),
      mode: "lines+markers",
      line: { color: "#FF4444", width: 2 },
      marker: { size: 5, color: "#FF4444" },
      type: "scatter",
      hovertemplate: "%{y:.1%}<extra>Actual</extra>",
    },
    ...(declineCategories.length
      ? declineCategories.map((category) => ({
          name: category.replace(/_/g, " "),
          x: dates,
          y: daily.map((day) => {
            const item = day.declines?.find((d) => d.category === category);
            return item?.share_of_attempts ?? 0;
          }),
          type: "bar" as const,
          marker: { color: declineColors[category] ?? "rgba(148,163,184,0.62)" },
          xaxis: "x2",
          yaxis: "y2",
          hovertemplate: "%{y:.2%}<extra>%{fullData.name}</extra>",
        }))
      : [
          {
            name: "Total declines",
            x: dates,
            y: daily.map((day) => {
              if (day.decline_count && day.count) return day.decline_count / day.count;
              return day.actual_sr == null ? 0 : Math.max(0, 1 - day.actual_sr);
            }),
            type: "bar" as const,
            marker: { color: "rgba(245,158,11,0.62)" },
            xaxis: "x2",
            yaxis: "y2",
            hovertemplate: "%{y:.2%}<extra>Total declines</extra>",
          },
        ]),
  ];

  return (
    <Plot
      data={traces}
      layout={{
        title: {
          text: `${dimension}=${value}${result.is_alert ? " ⚠ ALERT" : ""}`,
          font: { color: result.is_alert ? "#FF4444" : "#FF4444", size: 14, family: "monospace" },
        },
        paper_bgcolor: "#000000",
        plot_bgcolor: "#000000",
        font: { color: "#FFFFFF", family: "monospace" },
        xaxis: {
          domain: [0, 1],
          anchor: "y",
          gridcolor: "#222222",
          linecolor: "#222222",
          showticklabels: false,
        },
        yaxis: {
          domain: [0.38, 1],
          gridcolor: "#222222",
          linecolor: "#222222",
          tickformat: ".1%",
          range: [0, 1],
          title: { text: "SR", font: { size: 10 } },
        },
        xaxis2: {
          domain: [0, 1],
          anchor: "y2",
          matches: "x",
          gridcolor: "#222222",
          linecolor: "#222222",
        },
        yaxis2: {
          domain: [0, 0.26],
          gridcolor: "#222222",
          linecolor: "#222222",
          tickformat: ".1%",
          showgrid: false,
          title: { text: "Declines", font: { size: 10 } },
        },
        barmode: "stack",
        legend: { bgcolor: "#000000", bordercolor: "#222222", borderwidth: 1 },
        margin: { t: 40, b: 92, l: 60, r: 20 },
        hovermode: "x unified",
      }}
      config={{ displayModeBar: false, responsive: true }}
      style={{ width: "100%", height: "520px" }}
    />
  );
}
