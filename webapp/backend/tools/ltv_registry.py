import asyncio as _asyncio
from typing import Any, Dict

LTV_TOOL_SCHEMAS = [
    {
        "name": "get_ltv_overview",
        "description": (
            "Get overall LTV KPIs: average LTV, ARPPU, recurring LTV, churn rate, and customer count. "
            "Call this first when the user asks about general LTV performance."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_ltv_by_segment",
        "description": (
            "Get LTV breakdown grouped by a specific dimension. "
            "Returns avg_ltv, avg_arppu, avg_ltv_recurring, churn_rate, count for each value."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dimension": {
                    "type": "string",
                    "enum": ["offer", "geo", "utm_source", "channel", "gender",
                             "age", "payment_method", "card_type", "card_brand"],
                    "description": "Which dimension to group by",
                },
            },
            "required": ["dimension"],
        },
    },
    {
        "name": "generate_ltv_chart",
        "description": (
            "Generate an interactive chart for LTV data.\n"
            "- 'bar_comparison': grouped bar chart (LTV + ARPPU bars, churn line) for a dimension\n"
            "- 'influence': horizontal bars showing LTV spread per dimension — use when asked 'what drives LTV most'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "enum": ["bar_comparison", "influence"],
                },
                "dimension": {
                    "type": "string",
                    "description": "Required for bar_comparison (e.g. 'offer', 'geo')",
                },
            },
            "required": ["chart_type"],
        },
    },
]

LTV_TOOL_SCHEMAS_OPENAI = [
    {
        "type": "function",
        "function": {
            "name": s["name"],
            "description": s["description"],
            "parameters": s["input_schema"],
        },
    }
    for s in LTV_TOOL_SCHEMAS
]


async def dispatch_ltv_tool(name: str, args: Dict[str, Any], ltv_svc) -> Any:
    if name == "get_ltv_overview":
        return await ltv_svc.get_overview()

    if name == "get_ltv_by_segment":
        dimension = args.get("dimension", "offer")
        rows = await ltv_svc.get_by_dimension(dimension)
        rows = sorted(rows, key=lambda r: r.get("avg_ltv") or 0, reverse=True)
        return {"dimension": dimension, "breakdown": rows}

    if name == "generate_ltv_chart":
        chart_type = args.get("chart_type", "bar_comparison")

        if chart_type == "bar_comparison":
            dimension = args.get("dimension", "offer")
            rows = await ltv_svc.get_by_dimension(dimension)
            rows = sorted(rows, key=lambda r: r.get("avg_ltv") or 0, reverse=True)
            values = [r["value"] for r in rows]
            ltv_vals = [round(r.get("avg_ltv") or 0, 2) for r in rows]
            arppu_vals = [round(r.get("avg_arppu") or 0, 2) for r in rows]
            churn_vals = [round((r.get("churn_rate") or 0) * 100, 1) for r in rows]
            return {
                "chart_type": "bar_comparison",
                "plotly": {
                    "data": [
                        {
                            "type": "bar", "name": "Avg LTV",
                            "x": values, "y": ltv_vals,
                            "marker": {"color": "#238636"}, "yaxis": "y",
                            "hovertemplate": "<b>%{x}</b><br>LTV: $%{y:.2f}<extra></extra>",
                        },
                        {
                            "type": "bar", "name": "Avg ARPPU",
                            "x": values, "y": arppu_vals,
                            "marker": {"color": "#1f6feb"}, "yaxis": "y",
                            "hovertemplate": "<b>%{x}</b><br>ARPPU: $%{y:.2f}<extra></extra>",
                        },
                        {
                            "type": "scatter", "mode": "lines+markers", "name": "Churn %",
                            "x": values, "y": churn_vals,
                            "line": {"color": "#f85149", "width": 2}, "marker": {"size": 8},
                            "yaxis": "y2",
                            "hovertemplate": "<b>%{x}</b><br>Churn: %{y:.1f}%<extra></extra>",
                        },
                    ],
                    "layout": {
                        "title": f"LTV by {dimension}",
                        "barmode": "group",
                        "xaxis": {"title": dimension, "tickangle": -30},
                        "yaxis": {"title": "Amount ($)", "tickprefix": "$"},
                        "yaxis2": {"title": "Churn Rate (%)", "overlaying": "y", "side": "right", "showgrid": False, "ticksuffix": "%"},
                        "legend": {"orientation": "h", "y": -0.3},
                        "height": 420,
                        "margin": {"b": 100},
                    },
                },
            }

        if chart_type == "influence":
            dims = ["offer", "geo", "utm_source", "channel", "gender", "age", "payment_method", "card_type", "card_brand"]
            tasks = [ltv_svc.get_by_dimension(d) for d in dims]
            results = await _asyncio.gather(*tasks, return_exceptions=True)

            labels, best_vals, worst_vals, spreads = [], [], [], []
            for dim, rows in zip(dims, results):
                if isinstance(rows, Exception) or not rows or len(rows) < 2:
                    continue
                vals = sorted([r.get("avg_ltv") or 0 for r in rows], reverse=True)
                best = round(vals[0], 2)
                worst = round(vals[-1], 2)
                spread = round(best - worst, 2)
                labels.append(dim)
                best_vals.append(best)
                worst_vals.append(worst)
                spreads.append(spread)

            order = sorted(range(len(labels)), key=lambda i: spreads[i], reverse=True)
            labels = [labels[i] for i in order]
            spreads = [spreads[i] for i in order]
            worst_vals = [worst_vals[i] for i in order]
            best_vals = [best_vals[i] for i in order]

            return {
                "chart_type": "influence",
                "plotly": {
                    "data": [{
                        "type": "bar", "orientation": "h",
                        "name": "LTV spread",
                        "y": labels, "x": spreads, "base": worst_vals,
                        "marker": {"color": "rgba(35,134,54,0.75)", "line": {"color": "#238636", "width": 1}},
                        "text": [f"${w:.0f} → ${b:.0f}  (+${s:.0f})" for w, b, s in zip(worst_vals, best_vals, spreads)],
                        "textposition": "inside", "insidetextanchor": "middle",
                        "hovertemplate": "<b>%{y}</b><br>Worst: $%{base:.2f}<br>Spread: +$%{x:.2f}<extra></extra>",
                    }],
                    "layout": {
                        "title": "LTV influence by dimension (range best→worst)",
                        "xaxis": {"title": "LTV spread ($)", "tickprefix": "$"},
                        "yaxis": {"autorange": "reversed"},
                        "height": max(300, len(labels) * 50 + 80),
                        "showlegend": False,
                        "bargap": 0.3,
                    },
                },
            }

        return {"error": f"Unknown chart_type: {chart_type}"}

    return {"error": f"Unknown LTV tool: {name}"}
