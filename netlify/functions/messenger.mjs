import { neon } from "@neondatabase/serverless";
import Anthropic from "@anthropic-ai/sdk";

// Tokens must be set as environment variables in Netlify dashboard
const PAGE_ACCESS_TOKEN = process.env.FB_PAGE_ACCESS_TOKEN || "";
const VERIFY_TOKEN = process.env.FB_VERIFY_TOKEN || "";

const MESSENGER_API = "https://graph.facebook.com/v21.0/me/messages";
const anthropic = new Anthropic();

const MESSENGER_AI_PROMPT = `You are Legacy AI, a friendly and professional insurance sales assistant for Land of Legacy.
Your goal is to qualify leads and book appointments through Messenger conversations.

RULES:
- Keep messages SHORT (2-3 sentences max) — this is Messenger, not email
- Be warm, personal, and encouraging
- Never be pushy — build trust first
- Use the lead's first name when you have it
- Use emojis sparingly but naturally

QUALIFICATION FLOW:
1. Greet warmly and ask if they're interested in learning about coverage options
2. If interested: ask what type of coverage (life, health, final expense, mortgage protection)
3. Gauge urgency and intent
4. Book an appointment with a specialist

RESPOND WITH JSON only:
{
  "reply": "your message to the lead",
  "intent": "high" | "medium" | "low",
  "status": "new" | "contacted" | "booked" | "discarded" | null,
  "appointment_requested": true | false
}

Set status to "booked" only when they explicitly agree to an appointment.
Set status to "discarded" only when they clearly say no/stop/unsubscribe.
Set intent to "high" when they mention specific needs, urgency, or family protection.
Return null for status to keep the current status unchanged.`;

// Send a message back to the user via Messenger
async function sendMessage(recipientId, text) {
  await fetch(`${MESSENGER_API}?access_token=${PAGE_ACCESS_TOKEN}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      recipient: { id: recipientId },
      message: { text },
    }),
  });
}

// Send quick reply buttons
async function sendQuickReplies(recipientId, text, replies) {
  await fetch(`${MESSENGER_API}?access_token=${PAGE_ACCESS_TOKEN}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      recipient: { id: recipientId },
      message: {
        text,
        quick_replies: replies.map(r => ({ content_type: "text", title: r, payload: r.toUpperCase().replace(/\s/g, "_") })),
      },
    }),
  });
}

// AI conversation logic - qualifies leads and books appointments via Claude
async function handleMessage(senderId, messageText, senderName) {
  const sql = neon(process.env.NETLIFY_DATABASE_URL);

  // Check if we already have this lead in progress
  const existing = await sql`SELECT * FROM leads WHERE profile_url = ${senderId} AND status NOT IN ('closed','discarded') ORDER BY created_at DESC LIMIT 1`;

  // Build conversation context for Claude
  let conversationContext = "";
  let lead = null;

  if (existing.length > 0) {
    lead = existing[0];
    conversationContext = `EXISTING LEAD:
- Name: ${lead.name}
- Current status: ${lead.status}
- Intent level: ${lead.intent}
- Notes so far: ${lead.notes || "none"}
- Appointment time: ${lead.appointment_time || "not set"}

The lead is replying to an ongoing conversation.`;
  } else {
    conversationContext = `NEW LEAD — this is their first message. Create a warm welcome and start qualifying.`;
  }

  try {
    const response = await anthropic.messages.create({
      model: "claude-haiku-4-5",
      max_tokens: 300,
      system: MESSENGER_AI_PROMPT,
      messages: [
        {
          role: "user",
          content: `Lead name: ${senderName || "Unknown"}
${conversationContext}

Their message: "${messageText}"

Respond with JSON only.`,
        },
      ],
    });

    const raw = response.content[0].text;
    let parsed;
    try {
      // Extract JSON from response (handle markdown code blocks)
      const jsonMatch = raw.match(/\{[\s\S]*\}/);
      parsed = JSON.parse(jsonMatch ? jsonMatch[0] : raw);
    } catch {
      // Fallback if Claude doesn't return valid JSON
      parsed = { reply: raw, intent: "medium", status: null, appointment_requested: false };
    }

    const { reply, intent, status, appointment_requested } = parsed;

    if (!lead) {
      // Create new lead
      await sql`INSERT INTO leads (name, source, post_text, profile_url, intent, status, ai_draft)
        VALUES (${senderName || 'Messenger Lead'}, 'Facebook Messenger', ${messageText}, ${senderId}, ${intent || 'medium'}, 'new', ${reply || ''})`;
    } else {
      // Update existing lead
      const updates = [];
      if (status) {
        await sql`UPDATE leads SET status = ${status}, intent = ${intent || lead.intent}, notes = COALESCE(notes,'') || ' | AI: ' || ${messageText} WHERE id = ${lead.id}`;
      } else {
        await sql`UPDATE leads SET intent = ${intent || lead.intent}, notes = COALESCE(notes,'') || ' | AI: ' || ${messageText} WHERE id = ${lead.id}`;
      }

      if (appointment_requested) {
        await sql`UPDATE leads SET appointment_time = 'Requested via Messenger' WHERE id = ${lead.id} AND (appointment_time IS NULL OR appointment_time = '')`;
      }
    }

    // Send the AI-generated reply
    await sendMessage(senderId, reply);

  } catch (e) {
    console.error("Claude AI error:", e);
    // Fallback to a simple response if AI fails
    if (!lead) {
      await sql`INSERT INTO leads (name, source, post_text, profile_url, intent, status, ai_draft)
        VALUES (${senderName || 'Messenger Lead'}, 'Facebook Messenger', ${messageText}, ${senderId}, 'medium', 'new', 'Auto-created from Messenger')`;
    }
    await sendMessage(senderId, `Hey ${senderName || 'there'}! Thanks for reaching out to Land of Legacy. One of our specialists will get back to you shortly!`);
  }
}

export default async (req) => {
  // ── GET: Meta webhook verification ──
  if (req.method === "GET") {
    const url = new URL(req.url);
    const mode = url.searchParams.get("hub.mode");
    const token = url.searchParams.get("hub.verify_token");
    const challenge = url.searchParams.get("hub.challenge");

    if (!VERIFY_TOKEN) {
      console.error("FB_VERIFY_TOKEN not configured");
      return new Response("Server misconfigured", { status: 500 });
    }
    if (mode === "subscribe" && token === VERIFY_TOKEN) {
      console.log("Webhook verified!");
      return new Response(challenge, { status: 200 });
    }
    return new Response("Forbidden", { status: 403 });
  }

  // ── POST: Incoming messages ──
  if (req.method === "POST") {
    if (!PAGE_ACCESS_TOKEN) {
      console.error("FB_PAGE_ACCESS_TOKEN not configured");
      return new Response("Server misconfigured", { status: 500 });
    }
    try {
      const body = await req.json();

      if (body.object === "page") {
        for (const entry of body.entry || []) {
          for (const event of entry.messaging || []) {
            if (event.message && event.message.text) {
              const senderId = event.sender.id;
              const messageText = event.message.text;

              // Get sender name
              let senderName = "there";
              try {
                const profileRes = await fetch(`https://graph.facebook.com/v21.0/${senderId}?fields=first_name&access_token=${PAGE_ACCESS_TOKEN}`);
                const profile = await profileRes.json();
                if (profile.first_name) senderName = profile.first_name;
              } catch (e) { console.error("Profile fetch error:", e); }

              await handleMessage(senderId, messageText, senderName);
            }
          }
        }
      }

      return new Response("EVENT_RECEIVED", { status: 200 });
    } catch (e) {
      console.error("Messenger webhook error:", e);
      return new Response("ERROR", { status: 500 });
    }
  }

  return new Response("OK", { status: 200 });
};

export const config = { path: "/.netlify/functions/messenger" };
