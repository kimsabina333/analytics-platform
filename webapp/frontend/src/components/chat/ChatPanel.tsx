import { useRef, useState, useEffect } from "react";
import { streamChat } from "../../api/chat";
import { useChatStore, AssistantMessage } from "../../store/chatSlice";
import { ChatMessage, ChartData } from "../../types/api";
import { ToolResultCard } from "./ToolResultCard";

const SUGGESTIONS = [
  "How can we improve SR for first payments?",
  "Which offer has the best success rate today?",
  "Are there any active alerts right now?",
  "Compare performance across all UTM sources",
  "Which payment processor performs best?",
];

function isChartData(r: unknown): r is ChartData {
  return typeof r === "object" && r !== null && (r as ChartData).chart_type === "sr_timeseries";
}

interface Props {
  model?: string;
  endpoint?: string;
}

export function ChatPanel({ model = "first", endpoint = "/api/chat" }: Props) {
  const {
    messages,
    isLoading,
    addUserMessage,
    startAssistantMessage,
    appendAssistantText,
    addToolCall,
    addChart,
    finishAssistantMessage,
    clearMessages,
  } = useChatStore();

  const [input, setInput] = useState("");
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const sendMessage = async (text: string) => {
    if (!text.trim() || isLoading) return;
    setInput("");

    addUserMessage(text);
    startAssistantMessage();

    const allMessages: ChatMessage[] = messages
      .filter((m) => m.role === "user" || (m.role === "assistant" && !(m as AssistantMessage).isStreaming))
      .map((m) => ({ role: m.role as "user" | "assistant", content: m.role === "assistant" ? (m as AssistantMessage).content : (m as ChatMessage).content }))
      .concat({ role: "user", content: text });

    try {
      for await (const event of streamChat(allMessages, model, endpoint)) {
        if (event.type === "text_delta") {
          appendAssistantText(event.data as string);
        } else if (event.type === "tool_result") {
          const { tool, result } = event.data as { tool: string; result: unknown };
          addToolCall(tool, null, result);
          if (isChartData(result)) {
            addChart(result);
          }
        }
      }
    } catch (e) {
      appendAssistantText(`\n\n[Error: ${String(e)}]`);
    } finally {
      finishAssistantMessage();
    }
  };

  return (
    <div className="flex flex-col h-[calc(100vh-120px)]">
      <div className="flex-1 overflow-y-auto space-y-4 pr-2">
        {messages.length === 0 && (
          <div className="text-center pt-12">
            <p className="text-gray-400 text-sm mb-6">
              Ask about success rates, alerts, or how to improve performance.
            </p>
            <div className="flex flex-wrap gap-2 justify-center">
              {SUGGESTIONS.map((s) => (
                <button
                  key={s}
                  onClick={() => sendMessage(s)}
                  className="text-xs border border-gray-600 rounded px-3 py-1.5 text-gray-300 hover:border-cyan-400 hover:text-cyan-300 transition-colors"
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => {
          if (msg.role === "user") {
            return (
              <div key={i} className="flex justify-end">
                <div className="bg-gray-800 rounded-lg px-4 py-2 max-w-lg text-sm text-white">
                  {(msg as ChatMessage).content}
                </div>
              </div>
            );
          }

          const am = msg as AssistantMessage;
          return (
            <div key={i} className="space-y-1">
              {am.toolCalls.map((tc, j) => (
                <ToolResultCard key={j} tool={tc.tool} input={tc.input} result={tc.result} />
              ))}
              {am.content && (
                <div className="bg-gray-900 border border-gray-700 rounded-lg px-4 py-3 text-sm text-gray-100 whitespace-pre-wrap">
                  {am.content}
                  {am.isStreaming && <span className="animate-pulse text-cyan-400">▌</span>}
                </div>
              )}
              {am.charts.map((chart, j) => (
                <div key={j} className="mt-2" />
              ))}
            </div>
          );
        })}
        <div ref={bottomRef} />
      </div>

      <div className="mt-4 flex gap-2 border-t border-gray-700 pt-4">
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              sendMessage(input);
            }
          }}
          placeholder="Ask about SR, alerts, or how to improve performance..."
          disabled={isLoading}
          rows={2}
          className="flex-1 bg-gray-900 border border-gray-600 rounded px-3 py-2 text-sm text-white placeholder-gray-500 resize-none focus:outline-none focus:border-cyan-400 disabled:opacity-50"
        />
        <div className="flex flex-col gap-1">
          <button
            onClick={() => sendMessage(input)}
            disabled={isLoading || !input.trim()}
            className="bg-cyan-700 hover:bg-cyan-600 disabled:opacity-40 text-white text-sm px-4 py-2 rounded"
          >
            Send
          </button>
          {messages.length > 0 && (
            <button
              onClick={clearMessages}
              className="text-xs text-gray-500 hover:text-gray-300 text-center"
            >
              Clear
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
