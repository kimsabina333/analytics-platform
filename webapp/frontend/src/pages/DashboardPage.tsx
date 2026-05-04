import { useEffect } from "react";
import { AlertBanner } from "../components/dashboard/AlertBanner";
import { OverviewGrid } from "../components/dashboard/OverviewGrid";
import { SRTimeSeriesChart } from "../components/dashboard/SRTimeSeriesChart";
import { usePolling } from "../hooks/usePolling";
import { useDashboardStore } from "../store/dashboardSlice";

const POLL_MS = 3 * 60 * 1000; // 3 min

interface Props {
  model?: "first" | "recurring";
  title?: string;
}

export function DashboardPage({ model = "first", title = "SYSTEM MONITORING: SUCCESS RATE FIRST" }: Props) {
  const { overview, selectedSegment, loading, error, lastUpdated, refresh, setSelectedSegment, setActiveModel } =
    useDashboardStore();

  useEffect(() => {
    setActiveModel(model);
  }, [model, setActiveModel]);

  usePolling(() => refresh(model), POLL_MS);

  const alerts = overview?.segments.filter((s) => s.is_alert) ?? [];

  if (error) {
    return (
      <div className="text-red-400 p-4 border border-red-600 rounded">
        Failed to load data: {error}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-sm font-bold text-cyan-400 tracking-widest">
          {title}
        </h1>
        <div className="text-xs text-gray-500 flex items-center gap-3">
          {loading && <span className="text-cyan-400 animate-pulse">Updating...</span>}
          {lastUpdated && (
            <span>Last updated: {lastUpdated.toLocaleTimeString()}</span>
          )}
          <button
            onClick={() => refresh(model)}
            className="text-gray-400 hover:text-white px-2 py-0.5 border border-gray-700 rounded"
          >
            Refresh
          </button>
        </div>
      </div>

      <AlertBanner alerts={alerts} />

      {overview && (
        <>
          <OverviewGrid
            segments={overview.segments}
            onSelect={setSelectedSegment}
            selected={selectedSegment}
          />

          {selectedSegment && (
            <div className="mt-4">
              <SRTimeSeriesChart result={selectedSegment} />
            </div>
          )}

          {!selectedSegment && overview.segments.length > 0 && (
            <p className="text-xs text-gray-500 text-center mt-6">
              Click a segment card to see its time series chart
            </p>
          )}
        </>
      )}

      {!overview && !loading && (
        <div className="text-center py-12 text-gray-500">No data available</div>
      )}
    </div>
  );
}
