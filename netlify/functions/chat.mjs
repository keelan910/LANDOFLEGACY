import Anthropic from "@anthropic-ai/sdk";
import { neon } from "@neondatabase/serverless";

const anthropic = new Anthropic();

const SYSTEM_PROMPT = `You are Legacy AI, the intelligent assistant for Land of Legacy — an insurance sales team.
You help agents with:
- Insurance product knowledge (life, health, final expense, mortgage protection)
- Sales scripts and objection handling
- Lead qualification tips
- Appointment booking strategies
- Motivational coaching for the team

Keep responses concise, actionable, and encouraging. Use a confident, team-oriented tone.
If asked about something outside insurance sales, politely redirect to how you can help with their sales goals.`;

export default async (req) => {
  const origin = req.headers.get("origin") || "";
  const siteHost = new URL(req.url).origin;
  const allowedOrigin = origin === siteHost ? origin : siteHost;
  const H = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": allowedOrigin,
    "Access-Control-Allow-Methods": "POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
  };
  if (req.method === "OPTIONS") return new Response("ok", { headers: H });

  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405, headers: H });
  }

  // Require valid session token
  const authHeader = req.headers.get("authorization") || "";
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
  if (!token || token.length < 32) {
    return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: H });
  }
  try {
    const sql = neon(process.env.NETLIFY_DATABASE_URL);
    const sessions = await sql`SELECT agent_id FROM sessions WHERE token = ${token} AND expires_at > NOW()`;
    if (sessions.length === 0) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: H });
    }
  } catch {
    return new Response(JSON.stringify({ error: "Auth service unavailable" }), { status: 500, headers: H });
  }

  try {
    const { message, history } = await req.json();

    if (!message || typeof message !== "string" || message.length > 2000) {
      return new Response(JSON.stringify({ error: "Message is required and must be under 2000 characters" }), { status: 400, headers: H });
    }

    // Build messages array from history + new message
    const messages = [];
    if (Array.isArray(history)) {
      for (const h of history.slice(-20)) {
        if (h.role === "user" || h.role === "assistant") {
          messages.push({ role: h.role, content: h.content });
        }
      }
    }
    messages.push({ role: "user", content: message });

    const response = await anthropic.messages.create({
      model: "claude-sonnet-4-5",
      max_tokens: 1024,
      system: SYSTEM_PROMPT,
      messages,
    });

    const reply = response.content[0].text;

    return new Response(JSON.stringify({ reply }), { headers: H });
  } catch (e) {
    console.error("Chat error:", e);
    return new Response(JSON.stringify({ error: "AI service unavailable" }), { status: 500, headers: H });
  }
};

export const config = { path: "/.netlify/functions/chat" };
