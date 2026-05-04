import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { AppShell } from "./components/layout/AppShell";
import { DashboardPage } from "./pages/DashboardPage";
import { SegmentExplorerPage } from "./pages/SegmentExplorerPage";
import { ChatPage } from "./pages/ChatPage";

export default function App() {
  return (
    <BrowserRouter>
      <AppShell>
        <Routes>
          <Route path="/" element={<Navigate to="/model/sr-first" replace />} />
          <Route
            path="/model/sr-first"
            element={<DashboardPage model="first" title="SYSTEM MONITORING: SR FIRST" />}
          />
          <Route
            path="/model/sr-first/segments"
            element={<SegmentExplorerPage model="first" title="SR FIRST SEGMENT EXPLORER" />}
          />
          <Route
            path="/model/sr-recurring"
            element={<DashboardPage model="recurring" title="SYSTEM MONITORING: SR RECURRING" />}
          />
          <Route
            path="/model/sr-recurring/segments"
            element={<SegmentExplorerPage model="recurring" title="SR RECURRING SEGMENT EXPLORER" />}
          />
          <Route
            path="/model/ltv"
            element={<ChatPage model="ltv" title="LTV MODEL INTERPRETER" />}
          />
          <Route
            path="/payment/risk"
            element={<ChatPage model="risk" endpoint="/api/risk/chat" title="PAYMENT RISK ASSISTANT" />}
          />
          <Route
            path="/payment/cor"
            element={<ChatPage model="risk" endpoint="/api/risk/chat" title="COST OF REVENUE ASSISTANT" />}
          />
          <Route
            path="/payment/revenue"
            element={<ChatPage model="risk" endpoint="/api/risk/chat" title="PAYMENT REVENUE ASSISTANT" />}
          />
          <Route path="/segments" element={<Navigate to="/model/sr-first/segments" replace />} />
          <Route path="/chat" element={<Navigate to="/model/sr-first" replace />} />
        </Routes>
      </AppShell>
    </BrowserRouter>
  );
}
