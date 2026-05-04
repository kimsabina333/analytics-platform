import { create } from "zustand";
import { ChatMessage, ChartData } from "../types/api";

export interface AssistantMessage {
  role: "assistant";
  content: string;
  charts: ChartData[];
  toolCalls: { tool: string; input: unknown; result: unknown }[];
  isStreaming: boolean;
}

export type DisplayMessage = ChatMessage | AssistantMessage;

interface ChatState {
  messages: DisplayMessage[];
  isLoading: boolean;
  addUserMessage: (content: string) => void;
  startAssistantMessage: () => void;
  appendAssistantText: (text: string) => void;
  addToolCall: (tool: string, input: unknown, result: unknown) => void;
  addChart: (chart: ChartData) => void;
  finishAssistantMessage: () => void;
  clearMessages: () => void;
}

export const useChatStore = create<ChatState>((set, get) => ({
  messages: [],
  isLoading: false,

  addUserMessage: (content) =>
    set((s) => ({ messages: [...s.messages, { role: "user", content }] })),

  startAssistantMessage: () =>
    set((s) => ({
      isLoading: true,
      messages: [
        ...s.messages,
        { role: "assistant", content: "", charts: [], toolCalls: [], isStreaming: true },
      ],
    })),

  appendAssistantText: (text) =>
    set((s) => {
      const msgs = [...s.messages];
      const last = msgs[msgs.length - 1] as AssistantMessage;
      msgs[msgs.length - 1] = { ...last, content: last.content + text };
      return { messages: msgs };
    }),

  addToolCall: (tool, input, result) =>
    set((s) => {
      const msgs = [...s.messages];
      const last = msgs[msgs.length - 1] as AssistantMessage;
      msgs[msgs.length - 1] = {
        ...last,
        toolCalls: [...last.toolCalls, { tool, input, result }],
      };
      return { messages: msgs };
    }),

  addChart: (chart) =>
    set((s) => {
      const msgs = [...s.messages];
      const last = msgs[msgs.length - 1] as AssistantMessage;
      msgs[msgs.length - 1] = { ...last, charts: [...last.charts, chart] };
      return { messages: msgs };
    }),

  finishAssistantMessage: () =>
    set((s) => {
      const msgs = [...s.messages];
      const last = msgs[msgs.length - 1] as AssistantMessage;
      msgs[msgs.length - 1] = { ...last, isStreaming: false };
      return { messages: msgs, isLoading: false };
    }),

  clearMessages: () => set({ messages: [], isLoading: false }),
}));
