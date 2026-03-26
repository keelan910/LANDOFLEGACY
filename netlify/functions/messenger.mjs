import { neon } from "@netlify/neon";

// ═══════════════════════════════════════════════
//  PASTE YOUR TOKENS HERE AFTER SETUP
// ═══════════════════════════════════════════════
const PAGE_ACCESS_TOKEN = "PASTE_YOUR_PAGE_ACCESS_TOKEN_HERE";
const VERIFY_TOKEN = "landoflegacy2025";
// ═══════════════════════════════════════════════

const MESSENGER_API = "https://graph.facebook.com/v21.0/me/messages";

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

// AI conversation logic - qualifies leads and books appointments
async function handleMessage(senderId, messageText, senderName) {
  const sql = neon();
  const msg = messageText.toLowerCase();

  // Check if we already have this lead in progress
  const existing = await sql`SELECT * FROM leads WHERE profile_url = ${senderId} AND status NOT IN ('closed','discarded') ORDER BY created_at DESC LIMIT 1`;

  if (existing.length > 0) {
    const lead = existing[0];

    // Already booked
    if (lead.status === "booked") {
      await sendMessage(senderId, `Hey ${senderName}! You already have an appointment set. One of our specialists will be reaching out. Looking forward to helping you! 🙌`);
      return;
    }

    // They replied to our outreach - check for booking intent
    if (msg.includes("yes") || msg.includes("sure") || msg.includes("okay") || msg.includes("interested") || msg.includes("tell me more") || msg.includes("book") || msg.includes("schedule") || msg.includes("appointment")) {
      // Update lead to booked
      await sql`UPDATE leads SET status = 'booked', notes = COALESCE(notes,'') || ' | Replied YES via Messenger: ' || ${messageText} WHERE id = ${lead.id}`;
      await sendMessage(senderId, `Awesome ${senderName}! 🎉 I'm setting you up with one of our top specialists right now. They'll reach out within the next few hours to get you taken care of. What's the best time for you - morning, afternoon, or evening?`);
      return;
    }

    if (msg.includes("morning") || msg.includes("afternoon") || msg.includes("evening") || msg.includes("pm") || msg.includes("am")) {
      await sql`UPDATE leads SET appointment_time = ${messageText}, notes = COALESCE(notes,'') || ' | Preferred time: ' || ${messageText} WHERE id = ${lead.id}`;
      await sendMessage(senderId, `Perfect, I've noted that down. Our specialist will reach out during that time. Thanks ${senderName}, talk soon! 💪`);
      return;
    }

    // Not sure / maybe
    if (msg.includes("maybe") || msg.includes("not sure") || msg.includes("think about")) {
      await sql`UPDATE leads SET status = 'contacted', notes = COALESCE(notes,'') || ' | Said maybe: ' || ${messageText} WHERE id = ${lead.id}`;
      await sendMessage(senderId, `No pressure at all ${senderName}! Just know we've helped hundreds of families get protected. When you're ready, just message us back and we'll get you set up. 🤝`);
      return;
    }

    // No / not interested
    if (msg.includes("no") || msg.includes("not interested") || msg.includes("stop") || msg.includes("unsubscribe")) {
      await sql`UPDATE leads SET status = 'discarded', notes = COALESCE(notes,'') || ' | Declined: ' || ${messageText} WHERE id = ${lead.id}`;
      await sendMessage(senderId, `Totally understand ${senderName}. If anything changes, we're always here. Have a great day! 👋`);
      return;
    }

    // General follow up
    await sendMessage(senderId, `Thanks for reaching out ${senderName}! Would you like to schedule a quick 10-minute call with one of our specialists? No pressure, just seeing if we can help. 😊`);
    return;
  }

  // NEW conversation - create lead and qualify
  await sql`INSERT INTO leads (name, source, post_text, profile_url, intent, status, ai_draft) VALUES (${senderName || 'Messenger Lead'}, 'Facebook Messenger', ${messageText}, ${senderId}, 'medium', 'new', 'Auto-created from Messenger conversation')`;

  // Welcome message with qualification
  await sendQuickReplies(
    senderId,
    `Hey ${senderName || 'there'}! 👋 Thanks for reaching out to Land of Legacy. We help families and individuals protect what matters most.\n\nAre you looking to learn more about coverage options?`,
    ["Yes, tell me more", "Book an appointment", "Just browsing"]
  );
}

export default async (req) => {
  // ── GET: Meta webhook verification ──
  if (req.method === "GET") {
    const url = new URL(req.url);
    const mode = url.searchParams.get("hub.mode");
    const token = url.searchParams.get("hub.verify_token");
    const challenge = url.searchParams.get("hub.challenge");

    if (mode === "subscribe" && token === VERIFY_TOKEN) {
      console.log("Webhook verified!");
      return new Response(challenge, { status: 200 });
    }
    return new Response("Forbidden", { status: 403 });
  }

  // ── POST: Incoming messages ──
  if (req.method === "POST") {
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
