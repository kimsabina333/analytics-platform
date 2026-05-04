export interface DeclineCategoryStat {
  category: string;
  count: number;
  share_of_declines: number;
  share_of_attempts: number;
}

export interface DailyPrediction {
  date: string;
  mean: number;
  ci_low: number;
  ci_high: number;
  actual_sr: number | null;
  count: number;
  decline_count: number;
  declines: DeclineCategoryStat[];
  top_decline_category: string | null;
  is_alert: boolean;
}

export interface SegmentPredictionResult {
  dimension: string;
  value: string;
  q_threshold: number;
  is_alert: boolean;
  ci_width: number;
  daily: DailyPrediction[];
}

export interface OverviewResponse {
  segments: SegmentPredictionResult[];
  poll_interval_seconds: number;
  last_updated: string;
}

export interface TopSegment {
  dimension: string;
  value: string;
  latest_sr: number | null;
  mean_sr: number | null;
  is_alert: boolean;
  count: number;
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

export type SSEEventType = "text_delta" | "tool_start" | "tool_result" | "done" | "error";

export interface SSEEvent {
  type: SSEEventType;
  data: unknown;
}

export interface ChartData {
  chart_type: "sr_timeseries";
  title: string;
  is_alert: boolean;
  ci_label: string;
  traces: {
    dates: string[];
    ci_low: number[];
    ci_high: number[];
    mean: number[];
    actual: (number | null)[];
  };
}
