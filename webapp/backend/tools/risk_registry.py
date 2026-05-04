import json
from typing import Any

RISK_TOOL_SCHEMAS_OPENAI = [
    {
        "type": "function",
        "function": {
            "name": "get_risk_summary",
            "description": "Get latest month CB rate, Fraud rate and VAMP rate for all MIDs with alert status",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_risk_trends",
            "description": "Get monthly historical CB/Fraud/VAMP rates for a specific MID or all MIDs combined",
            "parameters": {
                "type": "object",
                "properties": {
                    "mid": {
                        "type": "string",
                        "description": "MID name (e.g. 'checkout', 'adyen uae'). Leave empty for all MIDs combined.",
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_risk_anomalies",
            "description": "Get list of MIDs that currently exceed warning or alert thresholds for any risk metric",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_risk_chart",
            "description": (
                "Generate a Plotly chart spec showing risk metric trends over time. "
                "Call this whenever the user asks to 'show', 'visualize', or 'draw' a chart."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "mid": {
                        "type": "string",
                        "description": "MID to filter by, or empty for all MIDs",
                    },
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["cb_rate", "fraud_rate", "vamp_rate"]},
                        "description": "Which metrics to include in the chart",
                    },
                    "title": {"type": "string", "description": "Chart title"},
                },
                "required": ["metrics", "title"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_cor_breakdown",
            "description": "Get monthly Cost of Revenue USD totals by breakdown_type from CKO financial actions",
            "parameters": {
                "type": "object",
                "properties": {
                    "breakdown_type": {
                        "type": "string",
                        "description": "Optional breakdown_type filter. Leave empty for all breakdown types.",
                    },
                    "merchant_account": {
                        "type": "string",
                        "description": "Optional merchant_account filter, for example Adyen US or Adyen UAE merchant accounts.",
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_cor_chart",
            "description": "Generate a Plotly chart for monthly Cost of Revenue USD totals by breakdown_type",
            "parameters": {
                "type": "object",
                "properties": {
                    "breakdown_type": {
                        "type": "string",
                        "description": "Optional breakdown_type filter. Leave empty for all breakdown types.",
                    },
                    "merchant_account": {
                        "type": "string",
                        "description": "Optional merchant_account filter, for example Adyen US or Adyen UAE merchant accounts.",
                    },
                    "title": {"type": "string", "description": "Chart title"},
                },
                "required": ["title"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_cor_revenue_ratio",
            "description": "Get monthly CoR, revenue, and CoR / Revenue percentage by MID",
            "parameters": {
                "type": "object",
                "properties": {
                    "mid": {"type": "string", "description": "Optional MID filter, e.g. checkout, adyen us, adyen uae"},
                    "event_type": {
                        "type": "string",
                        "enum": ["Authorization", "Settlement", "Lifecycle Settled"],
                        "description": "Which revenue event date to use. Defaults to Settlement.",
                    },
                },
                "required": [],
            },
        },
    },
]


async def dispatch_risk_tool(name: str, args: dict, risk_svc: Any) -> Any:
    if name == "get_risk_summary":
        rows = await risk_svc.get_summary()
        return [
            {
                "mid": r.get("mid"),
                "month": r.get("month"),
                "cb_rate_pct":    round((r.get("cb_rate") or 0) * 100, 3),
                "fraud_rate_pct": round((r.get("fraud_rate") or 0) * 100, 3),
                "vamp_rate_pct":  round((r.get("vamp_rate") or 0) * 100, 3),
                "settled_count":  int(r.get("settled_count") or 0),
                "cb_status":      r.get("cb_rate_status", "ok"),
                "fraud_status":   r.get("fraud_rate_status", "ok"),
                "vamp_status":    r.get("vamp_rate_status", "ok"),
            }
            for r in rows
        ]

    if name == "get_risk_trends":
        mid = args.get("mid") or None
        rows = await risk_svc.get_trends(mid)
        return [
            {
                "month": r.get("month"),
                "mid": r.get("mid"),
                "cb_rate_pct":    round((r.get("cb_rate") or 0) * 100, 3),
                "fraud_rate_pct": round((r.get("fraud_rate") or 0) * 100, 3),
                "vamp_rate_pct":  round((r.get("vamp_rate") or 0) * 100, 3),
                "settled_count":  int(r.get("settled_count") or 0),
            }
            for r in rows
        ]

    if name == "get_risk_anomalies":
        return await risk_svc.get_anomalies()

    if name == "generate_risk_chart":
        mid = args.get("mid") or None
        metrics = args.get("metrics", ["cb_rate", "fraud_rate", "vamp_rate"])
        title = args.get("title", "Risk Metrics")
        rows = await risk_svc.get_trends(mid)

        THRESHOLDS = {"cb_rate": 0.9, "fraud_rate": 2.0, "vamp_rate": 0.9}
        LABELS = {"cb_rate": "CB Rate (%)", "fraud_rate": "Fraud Rate (%)", "vamp_rate": "VAMP Rate (%)"}
        COLORS = {"cb_rate": "#f87171", "fraud_rate": "#fb923c", "vamp_rate": "#a78bfa"}

        mids = list(dict.fromkeys(r["mid"] for r in rows if r.get("mid")))
        months = sorted(set(r["month"] for r in rows if r.get("month")))
        MID_COLORS = ["rgba(59,130,246,0.8)", "rgba(16,185,129,0.8)", "rgba(245,158,11,0.8)",
                      "rgba(168,85,247,0.8)", "rgba(236,72,153,0.8)", "rgba(6,182,212,0.8)"]

        traces = []
        for mi, m in enumerate(mids):
            for metric in metrics:
                vals = []
                for mo in months:
                    row = next((r for r in rows if r["mid"] == m and r["month"] == mo), None)
                    vals.append(round((row.get(metric) or 0) * 100, 3) if row else None)
                traces.append({
                    "x": months, "y": vals, "name": f"{m}",
                    "type": "scatter", "mode": "lines+markers",
                    "line": {"color": MID_COLORS[mi % len(MID_COLORS)], "width": 2},
                    "legendgroup": m, "showlegend": metric == metrics[0],
                    "hovertemplate": f"<b>{m}</b><br>{LABELS[metric]}: %{{y:.3f}}%<extra></extra>",
                })

        for metric in metrics:
            thr = THRESHOLDS.get(metric, 1.0)
            traces.append({
                "x": [months[0], months[-1]], "y": [thr, thr],
                "type": "scatter", "mode": "lines", "name": f"{LABELS[metric]} threshold",
                "line": {"color": COLORS[metric], "dash": "dot", "width": 1},
                "showlegend": False, "hoverinfo": "skip",
            })

        layout = {
            "title": title, "paper_bgcolor": "transparent", "plot_bgcolor": "transparent",
            "font": {"color": "#94a3b8", "size": 12},
            "xaxis": {"gridcolor": "rgba(255,255,255,0.05)", "tickangle": -30, "automargin": True},
            "yaxis": {"gridcolor": "rgba(255,255,255,0.05)", "ticksuffix": "%", "tickformat": ".2f", "automargin": True},
            "legend": {
                "bgcolor": "transparent",
                "orientation": "h",
                "x": 0,
                "xanchor": "left",
                "y": -0.26,
                "yanchor": "top",
            },
            "margin": {"t": 52, "b": 128, "l": 68, "r": 28},
            "height": 560,
            "hovermode": "x unified",
        }
        return {"plotly_traces": traces, "plotly_layout": layout}

    if name == "get_cor_breakdown":
        breakdown_type = args.get("breakdown_type") or None
        merchant_account = args.get("merchant_account") or None
        rows = await risk_svc.get_cor_breakdown(breakdown_type, merchant_account)
        return [
            {
                "month": r.get("month"),
                "merchant_account": r.get("merchant_account"),
                "breakdown_type": r.get("breakdown_type"),
                "total_usd": round(float(r.get("total_usd") or 0), 2),
            }
            for r in rows
        ]

    if name == "generate_cor_chart":
        breakdown_type = args.get("breakdown_type") or None
        merchant_account = args.get("merchant_account") or None
        title = args.get("title", "Cost of Revenue")
        rows = await risk_svc.get_cor_breakdown(breakdown_type, merchant_account)
        months = sorted(set(r["month"] for r in rows if r.get("month")))
        types = list(dict.fromkeys(r["breakdown_type"] for r in rows if r.get("breakdown_type")))
        colors = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#a855f7", "#06b6d4", "#ec4899"]

        traces = []
        for i, item in enumerate(types):
            vals = []
            for month in months:
                total = sum(
                    float(r.get("total_usd") or 0)
                    for r in rows
                    if r["month"] == month and r["breakdown_type"] == item
                )
                vals.append(round(total, 2))
            traces.append({
                "x": months,
                "y": vals,
                "type": "bar",
                "name": item,
                "marker": {"color": colors[i % len(colors)]},
                "hovertemplate": "<b>%{fullData.name}</b><br>%{x}<br>Cost: $%{y:,.2f}<extra></extra>",
            })

        layout = {
            "title": title,
            "paper_bgcolor": "transparent",
            "plot_bgcolor": "transparent",
            "font": {"color": "#94a3b8", "size": 12},
            "xaxis": {"gridcolor": "rgba(255,255,255,0.05)", "tickangle": -30, "automargin": True},
            "yaxis": {"gridcolor": "rgba(255,255,255,0.05)", "tickprefix": "$", "tickformat": ",.0f", "automargin": True},
            "legend": {
                "bgcolor": "transparent",
                "orientation": "h",
                "x": 0,
                "xanchor": "left",
                "y": -0.26,
                "yanchor": "top",
            },
            "barmode": "stack",
            "margin": {"t": 52, "b": 128, "l": 76, "r": 28},
            "height": 560,
            "hovermode": "x unified",
        }
        return {"plotly_traces": traces, "plotly_layout": layout}

    if name == "get_cor_revenue_ratio":
        rows = await risk_svc.get_cor_revenue_ratio(
            mid=args.get("mid") or None,
            event_type=args.get("event_type") or "Settlement",
        )
        return [
            {
                "month": r.get("month"),
                "mid": r.get("mid"),
                "cor_usd": round(float(r.get("cor_usd") or 0), 2),
                "revenue_usd": round(float(r.get("revenue_usd") or 0), 2),
                "cor_revenue_pct": round(float(r.get("cor_revenue_pct") or 0) * 100, 3),
                "order_count": int(r.get("order_count") or 0),
                "event_type": r.get("event_type"),
            }
            for r in rows
        ]

    return {"error": f"Unknown tool: {name}"}
