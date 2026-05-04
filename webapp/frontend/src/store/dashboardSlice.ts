import { create } from "zustand";
import { OverviewResponse, SegmentPredictionResult } from "../types/api";
import { fetchOverview } from "../api/dashboard";

interface DashboardState {
  overview: OverviewResponse | null;
  selectedSegment: SegmentPredictionResult | null;
  loading: boolean;
  error: string | null;
  lastUpdated: Date | null;
  activeModel: string;
  setActiveModel: (model: string) => void;
  setSelectedSegment: (s: SegmentPredictionResult | null) => void;
  refresh: (model?: string) => Promise<void>;
}

export const useDashboardStore = create<DashboardState>((set, get) => ({
  overview: null,
  selectedSegment: null,
  loading: false,
  error: null,
  lastUpdated: null,
  activeModel: "first",

  setActiveModel: (model) => set({ activeModel: model, overview: null, selectedSegment: null }),

  setSelectedSegment: (s) => set({ selectedSegment: s }),

  refresh: async (model) => {
    const selectedModel = model ?? get().activeModel;
    set({ loading: true, error: null });
    try {
      const data = await fetchOverview(selectedModel);
      set({ overview: data, loading: false, lastUpdated: new Date(), activeModel: selectedModel });
    } catch (e) {
      set({ error: String(e), loading: false });
    }
  },
}));
