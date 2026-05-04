import json
from typing import Any

MARKETING_TOOL_SCHEMAS_OPENAI = [
    {
        "type": "function",
        "function": {
            "name": "get_marketing_roi",
            "description": "Get ROI, CAC, spend, LTV, GP, CPM breakdown by marketing source (channel). Leave source empty for all channels combined.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "description": "Channel name (e.g. 'facebook', 'google', 'tiktok', 'adq'). Leave empty for all channels.",
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_marketing_chart",
            "description": "Generate a Plotly bar chart comparing marketing channels on a chosen metric. Call when user asks to 'show', 'visualize', or 'compare' channels.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metric": {
                        "type": "string",
                        "enum": ["roi_ml", "cac", "spend", "gp", "cpm", "ltv_ml"],
                        "description": "Metric to visualize across channels",
                    },
                    "title": {"type": "string", "description": "Chart title"},
                },
                "required": ["metric", "title"],
            },
        },
    },
]

METRIC_LABELS = {
    "roi_ml": "ROI (ML LTV)",
    "cac": "CAC ($)",
    "spend": "Spend ($)",
    "gp": "Gross Profit ($)",
    "cpm": "CPM ($)",
    "ltv_ml": "LTV ML ($)",
}

COLORS = ["rgba(59,130,246,0.8)", "rgba(16,185,129,0.8)", "rgba(245,158,11,0.8)",
          "rgba(168,85,247,0.8)", "rgba(236,72,153,0.8)", "rgba(6,182,212,0.8)"]


async def dispatch_marketing_tool(name: str, args: dict, marketing_svc: Any) -> Any:
    if name == "get_marketing_roi":
        source = args.get("source") or None
        rows = await marketing_svc.get_roi(source)
        return [
            {
                "source": r.get("source") or r.get("utm_source") or "unknown",
                "spend": round(float(r.get("spend") or 0), 2),
                "ltv_ml": round(float(r.get("ltv_ml") or r.get("ltv_ml_fast") or 0), 2),
                "roi_ml": round(float(r.get("roi_ml") or r.get("roi_ml_fast") or 0), 4),
                "cac": round(float(r.get("cac") or 0), 2),
                "gp": round(float(r.get("gp") or 0), 2),
                "cpm": round(float(r.get("cpm") or 0), 2),
                "purch_count": int(r.get("purch_count") or 0),
            }
            for r in rows
        ]

    if name == "generate_marketing_chart":
        metric = args.get("metric", "roi_ml")
        title = args.get("title", "Marketing Channel Comparison")
        rows = await marketing_svc.get_roi(None)

        sources, values = [], []
        for r in rows:
            src = r.get("source") or r.get("utm_source") or "unknown"
            val = float(r.get(metric) or 0)
            sources.append(src)
            values.append(round(val, 4))

        traces = [{
            "type": "bar",
            "x": sources,
            "y": values,
            "marker": {"color": [COLORS[i % len(COLORS)] for i in range(len(sources))]},
            "hovertemplate": f"<b>%{{x}}</b><br>{METRIC_LABELS.get(metric, metric)}: %{{y:.3f}}<extra></extra>",
        }]
        layout = {
            "title": title,
            "paper_bgcolor": "transparent", "plot_bgcolor": "transparent",
            "font": {"color": "#94a3b8", "size": 12},
            "xaxis": {"gridcolor": "rgba(255,255,255,0.05)"},
            "yaxis": {"gridcolor": "rgba(255,255,255,0.05)", "title": METRIC_LABELS.get(metric, metric)},
            "margin": {"t": 40, "b": 60, "l": 60, "r": 16},
        }
        return {"plotly_traces": traces, "plotly_layout": layout}

    return {"error": f"Unknown tool: {name}"}
