import React from "react";
import { NavLink, useLocation } from "react-router-dom";

const WORKSPACE = [
  {
    label: "Model Interpreter",
    icon: "▥",
    items: [
      { path: "/model/sr-first", label: "SR First", meta: "First charge" },
      { path: "/model/sr-recurring", label: "SR Recurring", meta: "Renewals" },
      { path: "/model/ltv", label: "LTV", meta: "Lifetime value" },
    ],
  },
  {
    label: "Payment Assistant",
    icon: "▤",
    items: [
      { path: "/payment/risk", label: "Risk", meta: "CB, fraud, VAMP" },
      { path: "/payment/cor", label: "CoR", meta: "Cost of revenue" },
      { path: "/payment/revenue", label: "Revenue", meta: "MID settlements" },
    ],
  },
];

function isSectionActive(pathname: string, items: { path: string }[]) {
  return items.some((item) => pathname === item.path || pathname.startsWith(`${item.path}/`));
}

export function AppShell({ children }: { children: React.ReactNode }) {
  const { pathname } = useLocation();

  return (
    <div className="min-h-screen bg-black text-white font-mono flex">
      <aside className="w-64 shrink-0 border-r border-gray-800 bg-gray-950/95 px-3 py-4">
        <div className="px-3 pb-4 text-[11px] uppercase tracking-[0.25em] text-gray-500">
          Workspace
        </div>

        <nav className="space-y-3">
          {WORKSPACE.map((section) => {
            const active = isSectionActive(pathname, section.items);
            return (
              <div key={section.label}>
                <div
                  className={`flex items-center gap-3 rounded-md px-3 py-2 text-sm ${
                    active ? "bg-blue-950/60 text-blue-300" : "text-gray-300"
                  }`}
                >
                  <span className="text-sm text-amber-300">{section.icon}</span>
                  <span className="font-semibold">{section.label}</span>
                </div>

                <div className="mt-1 space-y-0.5 pl-7">
                  {section.items.map((item) => (
                    <NavLink
                      key={item.path}
                      to={item.path}
                      className={({ isActive }) =>
                        `group flex items-center justify-between rounded-md px-3 py-2 text-sm transition-colors ${
                          isActive
                            ? "bg-gray-900 text-blue-300"
                            : "text-gray-400 hover:bg-gray-900/70 hover:text-white"
                        }`
                      }
                    >
                      <span className="flex items-center gap-2">
                        <span className="h-1.5 w-1.5 rounded-full bg-current opacity-70" />
                        <span>{item.label}</span>
                      </span>
                      <span className="ml-3 truncate text-xs text-gray-600 group-hover:text-gray-500">
                        {item.meta}
                      </span>
                    </NavLink>
                  ))}
                </div>
              </div>
            );
          })}
        </nav>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="border-b border-gray-800 px-6 py-3">
          <span className="text-cyan-400 font-bold tracking-widest text-sm">
            SR MONITORING
          </span>
        </header>
        <main className="flex-1 overflow-auto p-6">{children}</main>
      </div>
    </div>
  );
}
