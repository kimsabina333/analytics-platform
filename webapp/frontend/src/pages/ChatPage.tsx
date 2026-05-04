import { ChatPanel } from "../components/chat/ChatPanel";

interface Props {
  model?: string;
  endpoint?: string;
  title?: string;
}

export function ChatPage({
  model = "first",
  endpoint = "/api/chat",
  title = "AI ANALYTICS ASSISTANT",
}: Props) {
  return (
    <div className="max-w-3xl mx-auto">
      <h1 className="text-sm font-bold text-cyan-400 tracking-widest mb-4">{title}</h1>
      <ChatPanel model={model} endpoint={endpoint} />
    </div>
  );
}
