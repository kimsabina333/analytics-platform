from typing import Any, Dict

TOOL_SCHEMAS = [
    {
        "name": "get_segment_sr",
        "description": (
            "Get the Success Rate time series with Bayesian confidence intervals "
            "for a specific segment dimension and value. Returns actual SR vs model "
            "predictions for the last 3 days."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dimension": {
                    "type": "string",
                    "enum": [
                        "utm_source", "geo", "device", "age", "gender",
                        "payment_method", "card_type", "mid", "offer",
                        "card_brand", "weekday", "bank_tier",
                    ],
                    "description": "Which feature dimension to filter on",
                },
                "value": {
                    "type": "string",
                    "description": "The specific value (e.g. 'google', '4Week', 'adyen')",
                },
                "confidence_level": {
                    "type": "number",
                    "description": "CI width as percentage: 80, 90 (default), or 95",
                    "default": 90,
                },
            },
            "required": ["dimension", "value"],
        },
    },
    {
        "name": "list_alerts",
        "description": (
            "List all segments currently in alert state — actual SR dropped below "
            "the Bayesian lower confidence bound. Returns ranked list by severity."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dimension_filter": {
                    "type": "string",
                    "description": "Optional: limit results to one dimension (e.g. 'offer')",
                },
            },
        },
    },
    {
        "name": "explain_declines",
        "description": (
            "Explain what decline categories are driving the latest SR drop for "
            "a specific segment. Use this when SR is down, alerting, or the user asks why."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dimension": {
                    "type": "string",
                    "enum": [
                        "utm_source", "geo", "device", "age", "gender",
                        "payment_method", "card_type", "mid", "offer",
                        "card_brand", "weekday", "bank_tier",
                    ],
                    "description": "Which feature dimension to filter on",
                },
                "value": {
                    "type": "string",
                    "description": "The specific value to inspect",
                },
                "confidence_level": {
                    "type": "number",
                    "description": "CI width as percentage: 80, 90 (default), or 95",
                    "default": 90,
                },
            },
            "required": ["dimension", "value"],
        },
    },
    {
        "name": "compare_segments",
        "description": (
            "Compare SR performance across all values of the same dimension "
            "(e.g. all offers, all UTM sources). Returns ranked comparison table."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dimension": {
                    "type": "string",
                    "enum": [
                        "utm_source", "geo", "device", "age", "gender",
                        "payment_method", "card_type", "mid", "offer",
                        "card_brand", "weekday", "bank_tier",
                    ],
                },
                "metric": {
                    "type": "string",
                    "enum": ["latest_sr", "mean_sr"],
                    "default": "latest_sr",
                },
            },
            "required": ["dimension"],
        },
    },
    {
        "name": "get_top_performing",
        "description": "Find the best or worst performing segments by Success Rate.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dimension": {
                    "type": "string",
                    "enum": [
                        "utm_source", "geo", "device", "age", "gender",
                        "payment_method", "card_type", "mid", "offer",
                        "card_brand", "weekday", "bank_tier",
                    ],
                    "description": "Which dimension to rank within",
                },
                "n": {"type": "integer", "default": 5},
                "order": {"type": "string", "enum": ["best", "worst"], "default": "best"},
            },
            "required": ["dimension"],
        },
    },
    {
        "name": "generate_chart_data",
        "description": (
            "Generate an interactive chart. Choose chart_type based on context:\n"
            "- 'timeseries': SR over time with Bayesian CI band for ONE segment (needs dimension+value)\n"
            "- 'bar_comparison': ranked bar chart comparing ALL values within a dimension by SR (needs dimension)\n"
            "- 'influence': horizontal range bars showing SR spread (best→worst) per dimension — use when asked 'which parameters matter most' or 'what affects SR'"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "enum": ["timeseries", "bar_comparison", "influence"],
                    "description": "Type of chart to generate",
                },
                "dimension": {
                    "type": "string",
                    "description": "Required for timeseries and bar_comparison",
                },
                "value": {
                    "type": "string",
                    "description": "Required for timeseries only",
                },
                "dimensions": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "For influence chart: list of dimensions to compare. Defaults to all main dimensions.",
                },
            },
            "required": ["chart_type"],
        },
    },
]

# OpenAI/OpenRouter format (wraps each schema in {"type":"function","function":{...}})
TOOL_SCHEMAS_OPENAI = [
    {
        "type": "function",
        "function": {
            "name": s["name"],
            "description": s["description"],
            "parameters": s["input_schema"],
        },
    }
    for s in TOOL_SCHEMAS
]

_CI_LEVEL_TO_Q = {80: 0.10, 90: 0.05, 95: 0.025}


async def dispatch_tool(name: str, tool_input: Dict[str, Any], pred_svc) -> Any:
    if name == "get_segment_sr":
        q = _CI_LEVEL_TO_Q.get(tool_input.get("confidence_level", 90), 0.05)
        result = await pred_svc.compute_segment_sr(
            tool_input["dimension"], tool_input["value"], q
        )
        if result is None:
            return {"error": f"No data for {tool_input['dimension']}={tool_input['value']} (insufficient transactions or CI too wide)"}
        return result.model_dump()

    if name == "explain_declines":
        q = _CI_LEVEL_TO_Q.get(tool_input.get("confidence_level", 90), 0.05)
        result = await pred_svc.get_decline_explanation(
            tool_input["dimension"], tool_input["value"], q
        )
        if result is None:
            return {"error": f"No decline data for {tool_input['dimension']}={tool_input['value']}"}
        return result

    if name == "list_alerts":
        overview = await pred_svc.get_overview()
        alerts = [r for r in overview if r.is_alert]
        dim_filter = tool_input.get("dimension_filter")
        if dim_filter:
            alerts = [r for r in alerts if r.dimension == dim_filter]
        return {
            "alert_count": len(alerts),
            "alerts": [
                {
                    "dimension": r.dimension,
                    "value": r.value,
                    "latest_actual_sr": r.daily[-1].actual_sr if r.daily else None,
                    "bayesian_ci_low": r.daily[-1].ci_low if r.daily else None,
                    "drop_below_ci": (
                        round((r.daily[-1].ci_low - (r.daily[-1].actual_sr or 0)) * 100, 1)
                        if r.daily and r.daily[-1].actual_sr is not None else None
                    ),
                    "decline_count": r.daily[-1].decline_count if r.daily else 0,
                    "top_decline_category": r.daily[-1].top_decline_category if r.daily else None,
                    "q_threshold": r.q_threshold,
                }
                for r in alerts
            ],
        }

    if name == "compare_segments":
        metric = tool_input.get("metric", "latest_sr")
        top = await pred_svc.get_top_segments(
            tool_input["dimension"], n=20, order="best"
        )
        if metric == "mean_sr":
            top = sorted(top, key=lambda x: x.get("mean_sr") or 0, reverse=True)
        return {"dimension": tool_input["dimension"], "ranked": top}

    if name == "get_top_performing":
        return await pred_svc.get_top_segments(
            tool_input["dimension"],
            n=tool_input.get("n", 5),
            order=tool_input.get("order", "best"),
        )

    if name == "generate_chart_data":
        chart_type = tool_input.get("chart_type", "timeseries")

        # ── timeseries ──────────────────────────────────────────────────────────
        if chart_type == "timeseries":
            dim = tool_input.get("dimension")
            val = tool_input.get("value")
            if not dim or not val:
                return {"error": "dimension and value required for timeseries"}
            result = await pred_svc.compute_segment_sr(dim, val)
            if result is None:
                return {"error": f"No data for {dim}={val}"}
            q = result.q_threshold
            ci_pct = int((1 - 2 * q) * 100)
            dates = [d.date for d in result.daily]
            is_alert = result.is_alert
            actual = [d.actual_sr for d in result.daily]
            ci_low = [d.ci_low for d in result.daily]
            ci_high = [d.ci_high for d in result.daily]
            mean = [d.mean for d in result.daily]
            decline_rate = [
                (d.decline_count / d.count) if d.count else 0
                for d in result.daily
            ]
            color_actual = "#f85149" if is_alert else "#3fb950"
            return {
                "chart_type": "timeseries",
                "title": f"SR: {dim}={val}",
                "plotly": {
                    "data": [
                        {
                            "type": "scatter", "mode": "lines",
                            "x": dates + dates[::-1],
                            "y": [v*100 for v in ci_high] + [v*100 for v in ci_low[::-1]],
                            "fill": "toself", "fillcolor": "rgba(31,111,235,0.12)",
                            "line": {"color": "transparent"},
                            "name": f"CI {ci_pct}%", "hoverinfo": "skip",
                        },
                        {
                            "type": "scatter", "mode": "lines",
                            "x": dates, "y": [v*100 for v in mean],
                            "line": {"color": "#00d4ff", "width": 2, "dash": "dash"},
                            "name": "Прогноз (среднее)",
                        },
                        {
                            "type": "scatter", "mode": "lines+markers",
                            "x": dates, "y": [v*100 if v is not None else None for v in actual],
                            "line": {"color": color_actual, "width": 2},
                            "marker": {"size": 8, "color": color_actual},
                            "name": "Реальный SR", "connectgaps": False,
                        },
                        {
                            "type": "bar",
                            "x": dates,
                            "y": [v * 100 for v in decline_rate],
                            "marker": {"color": "rgba(255,180,0,0.28)"},
                            "name": "Decline rate",
                            "yaxis": "y2",
                        },
                    ],
                    "layout": {
                        "title": f"SR: {dim}={val}",
                        "xaxis": {"title": "Дата"},
                        "yaxis": {"title": "SR (%)", "ticksuffix": "%"},
                        "yaxis2": {
                            "title": "Declines (%)",
                            "ticksuffix": "%",
                            "overlaying": "y",
                            "side": "right",
                            "showgrid": False,
                        },
                        "barmode": "overlay",
                        "hovermode": "x unified",
                    },
                },
            }

        # ── bar_comparison ──────────────────────────────────────────────────────
        elif chart_type == "bar_comparison":
            dim = tool_input.get("dimension")
            if not dim:
                return {"error": "dimension required for bar_comparison"}
            ranked = await pred_svc.get_top_segments(dim, n=20, order="best")
            if not ranked:
                return {"error": f"No data for dimension {dim}"}
            values = [r["value"] for r in ranked]
            sr_vals = [round((r["latest_sr"] or 0) * 100, 1) for r in ranked]
            colors = ["#f85149" if r["is_alert"] else "#3fb950" for r in ranked]
            counts = [r.get("count", 0) for r in ranked]
            return {
                "chart_type": "bar_comparison",
                "title": f"SR по {dim}",
                "plotly": {
                    "data": [{
                        "type": "bar",
                        "x": values,
                        "y": sr_vals,
                        "marker": {"color": colors},
                        "text": [f"{v}%" for v in sr_vals],
                        "textposition": "outside",
                        "customdata": counts,
                        "hovertemplate": "<b>%{x}</b><br>SR: %{y:.1f}%<br>Транзакций: %{customdata}<extra></extra>",
                        "name": "SR",
                    }],
                    "layout": {
                        "title": f"Сравнение SR: {dim}",
                        "xaxis": {"title": dim},
                        "yaxis": {
                            "title": "SR (%)", "ticksuffix": "%",
                            "range": [max(0, min(sr_vals) - 8), min(100, max(sr_vals) + 8)],
                        },
                        "showlegend": False,
                    },
                },
            }

        # ── influence ───────────────────────────────────────────────────────────
        elif chart_type == "influence":
            import asyncio as _asyncio
            dims = tool_input.get("dimensions") or [
                "utm_source", "offer", "mid", "payment_method",
                "geo", "card_type", "device", "bank_tier",
            ]
            tasks = [pred_svc.get_top_segments(d, n=20, order="best") for d in dims]
            all_results = await _asyncio.gather(*tasks, return_exceptions=True)

            labels, sr_best, sr_worst, spreads = [], [], [], []
            for dim, res in zip(dims, all_results):
                if isinstance(res, Exception) or not res or len(res) < 2:
                    continue
                best = round((res[0]["latest_sr"] or 0) * 100, 1)
                worst = round((res[-1]["latest_sr"] or 0) * 100, 1)
                spread = round(best - worst, 1)
                labels.append(dim)
                sr_best.append(best)
                sr_worst.append(worst)
                spreads.append(spread)

            # sort by spread descending
            order = sorted(range(len(labels)), key=lambda i: spreads[i], reverse=True)
            labels = [labels[i] for i in order]
            sr_best = [sr_best[i] for i in order]
            sr_worst = [sr_worst[i] for i in order]
            spreads = [spreads[i] for i in order]

            return {
                "chart_type": "influence",
                "title": "Влияние параметров на SR (диапазон лучший→худший)",
                "plotly": {
                    "data": [
                        {
                            "type": "bar", "orientation": "h",
                            "name": "Диапазон влияния",
                            "y": labels,
                            "x": spreads,
                            "base": sr_worst,
                            "marker": {
                                "color": ["rgba(31,111,235,0.7)" if s > 15 else "rgba(31,111,235,0.45)" for s in spreads],
                                "line": {"color": "#1f6feb", "width": 1},
                            },
                            "text": [f"{w}% → {b}%  (±{s}pp)" for w, b, s in zip(sr_worst, sr_best, spreads)],
                            "textposition": "inside",
                            "insidetextanchor": "middle",
                            "hovertemplate": "<b>%{y}</b><br>Лучший: %{base:.1f}% + %{x:.1f}pp<br>Худший: %{base:.1f}%<extra></extra>",
                        },
                    ],
                    "layout": {
                        "title": "Рычаги влияния на SR (чем длиннее — тем важнее параметр)",
                        "xaxis": {"title": "Диапазон SR (pp)", "ticksuffix": "pp"},
                        "yaxis": {"title": "", "autorange": "reversed"},
                        "height": max(300, len(labels) * 45 + 80),
                        "showlegend": False,
                        "bargap": 0.3,
                    },
                },
            }

        return {"error": f"Unknown chart_type: {chart_type}"}

    return {"error": f"Unknown tool: {name}"}
