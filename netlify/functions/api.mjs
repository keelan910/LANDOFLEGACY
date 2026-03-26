import { neon } from "@netlify/neon";

export default async (req) => {
  const sql = neon();
  const H = {"Content-Type":"application/json","Access-Control-Allow-Origin":"*","Access-Control-Allow-Methods":"GET,POST,OPTIONS","Access-Control-Allow-Headers":"Content-Type"};
  if (req.method === "OPTIONS") return new Response("ok", { headers: H });

  try {
    const url = new URL(req.url);
    const a = url.searchParams.get("a");

    if (a === "init") {
      await sql`CREATE TABLE IF NOT EXISTS daily_sales (date_key TEXT PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}'::jsonb, updated_at TIMESTAMPTZ DEFAULT NOW())`;
      await sql`CREATE TABLE IF NOT EXISTS leads (id SERIAL PRIMARY KEY, name TEXT DEFAULT '', source TEXT DEFAULT '', post_text TEXT DEFAULT '', profile_url TEXT DEFAULT '', phone TEXT DEFAULT '', email TEXT DEFAULT '', location TEXT DEFAULT '', company TEXT DEFAULT '', intent TEXT DEFAULT 'medium', status TEXT DEFAULT 'new', grabbed_by TEXT DEFAULT '', grabbed_at TIMESTAMPTZ, appointment_time TEXT DEFAULT '', notes TEXT DEFAULT '', ai_draft TEXT DEFAULT '', created_at TIMESTAMPTZ DEFAULT NOW())`;
      await sql`CREATE TABLE IF NOT EXISTS kpis (id SERIAL PRIMARY KEY, agent_id TEXT NOT NULL, date_key TEXT NOT NULL, dials INT DEFAULT 0, contacts INT DEFAULT 0, appointments INT DEFAULT 0, quotes INT DEFAULT 0, apps_submitted INT DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(agent_id, date_key))`;
      // Add missing columns to existing leads table
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS phone TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS email TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS location TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS company TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      return new Response(JSON.stringify({ ok: true }), { headers: H });
    }

    // ── LEADS READ ──
    if (a === "leads-list") { const s = url.searchParams.get("status") || "new"; const rows = s === "all" ? await sql`SELECT * FROM leads ORDER BY created_at DESC` : await sql`SELECT * FROM leads WHERE status = ${s} ORDER BY created_at DESC`; return new Response(JSON.stringify({ data: rows }), { headers: H }); }
    if (a === "leads-my") { const agent = url.searchParams.get("agent"); const rows = await sql`SELECT * FROM leads WHERE grabbed_by = ${agent} AND status NOT IN ('discarded','new') ORDER BY CASE status WHEN 'booked' THEN 1 WHEN 'contacted' THEN 2 WHEN 'grabbed' THEN 3 WHEN 'closed' THEN 4 END, created_at DESC`; return new Response(JSON.stringify({ data: rows }), { headers: H }); }
    if (a === "leads-stats") { const rows = await sql`SELECT status, COUNT(*)::int as count FROM leads GROUP BY status`; const stats = {}; for (const r of rows) stats[r.status] = r.count; return new Response(JSON.stringify({ data: stats }), { headers: H }); }

    // ── LEAD LOCK CHECK ── returns how many leads an agent can grab today
    if (a === "leads-lock-check") {
      const agent = url.searchParams.get("agent");
      const today = url.searchParams.get("date") || new Date().toISOString().slice(0, 10);

      // Get agent's dials for today
      const kpiRows = await sql`SELECT dials FROM kpis WHERE agent_id = ${agent} AND date_key = ${today}`;
      const dials = kpiRows.length > 0 ? (kpiRows[0].dials || 0) : 0;

      // Count how many leads agent grabbed today
      const grabRows = await sql`SELECT COUNT(*)::int as cnt FROM leads WHERE grabbed_by = ${agent} AND grabbed_at::date = ${today}::date`;
      const grabbedToday = grabRows[0]?.cnt || 0;

      // Check if agent has any closed deals today
      const saleRows = await sql`SELECT COUNT(*)::int as cnt FROM leads WHERE grabbed_by = ${agent} AND status = 'closed' AND grabbed_at::date = ${today}::date`;
      const salesToday = saleRows[0]?.cnt || 0;

      // Calculate allowance
      let maxLeads = 0;
      let highIntentOnly = false;
      let unlocked = false;

      if (dials >= 600) {
        unlocked = true;
        if (salesToday > 0) {
          // Made a sale: only 5 high intent leads
          maxLeads = 5;
          highIntentOnly = true;
        } else {
          // No sale yet: 10 leads
          maxLeads = 10;
          highIntentOnly = false;
        }
      }

      const remaining = Math.max(0, maxLeads - grabbedToday);

      return new Response(JSON.stringify({
        data: {
          dials,
          unlocked,
          maxLeads,
          grabbedToday,
          remaining,
          highIntentOnly,
          salesToday
        }
      }), { headers: H });
    }

    // ── KPI READ ──
    if (a === "kpi-get") { const rows = await sql`SELECT * FROM kpis WHERE agent_id = ${url.searchParams.get("agent")} AND date_key = ${url.searchParams.get("date")}`; return new Response(JSON.stringify({ data: rows[0] || null }), { headers: H }); }
    if (a === "kpi-team") { const rows = await sql`SELECT * FROM kpis WHERE date_key = ${url.searchParams.get("date")} ORDER BY dials DESC`; return new Response(JSON.stringify({ data: rows }), { headers: H }); }

    // ── POST ──
    if (req.method === "POST") {
      const body = await req.json();

      if (a === "leads-add") {
        const { name, source, post_text, intent, ai_draft, phone, email, location, company, profile_url } = body;
        const rows = await sql`INSERT INTO leads (name,source,post_text,intent,ai_draft,phone,email,location,company,profile_url) VALUES (${name||''},${source||''},${post_text||''},${intent||'medium'},${ai_draft||''},${phone||''},${email||''},${location||''},${company||''},${profile_url||''}) RETURNING *`;
        return new Response(JSON.stringify({ data: rows[0] }), { headers: H });
      }

      if (a === "leads-grab") {
        const agent = body.agent || "admin";
        const today = body.date || new Date().toISOString().slice(0, 10);

        // Admin bypass - no lock
        if (agent !== "admin") {
          // Check dials
          const kpiRows = await sql`SELECT dials FROM kpis WHERE agent_id = ${agent} AND date_key = ${today}`;
          const dials = kpiRows.length > 0 ? (kpiRows[0].dials || 0) : 0;

          if (dials < 600) {
            return new Response(JSON.stringify({ error: `Locked! You need 600 dials to unlock leads. You have ${dials}. Get on the phone!` }), { status: 403, headers: H });
          }

          // Count grabbed today
          const grabRows = await sql`SELECT COUNT(*)::int as cnt FROM leads WHERE grabbed_by = ${agent} AND grabbed_at::date = ${today}::date`;
          const grabbedToday = grabRows[0]?.cnt || 0;

          // Check sales
          const saleRows = await sql`SELECT COUNT(*)::int as cnt FROM leads WHERE grabbed_by = ${agent} AND status = 'closed' AND grabbed_at::date = ${today}::date`;
          const salesToday = saleRows[0]?.cnt || 0;

          const maxLeads = salesToday > 0 ? 5 : 10;

          if (grabbedToday >= maxLeads) {
            const reason = salesToday > 0 ? "You made a sale — 5 lead max (high intent only)." : "10 lead max reached for today.";
            return new Response(JSON.stringify({ error: `Lead limit reached! ${reason} Come back tomorrow or close more deals.` }), { status: 403, headers: H });
          }

          // If agent has a sale, only allow high intent leads
          if (salesToday > 0) {
            const leadCheck = await sql`SELECT intent FROM leads WHERE id = ${body.id}`;
            if (leadCheck.length > 0 && leadCheck[0].intent !== 'high') {
              return new Response(JSON.stringify({ error: "You made a sale today — you can only grab HIGH INTENT leads now. Earn it." }), { status: 403, headers: H });
            }
          }
        }

        const rows = await sql`UPDATE leads SET status='grabbed',grabbed_by=${agent},grabbed_at=NOW() WHERE id=${body.id} AND status='new' RETURNING *`;
        if (!rows.length) return new Response(JSON.stringify({ error: "Already grabbed" }), { status: 409, headers: H });
        return new Response(JSON.stringify({ data: rows[0] }), { headers: H });
      }

      if (a === "leads-update") { const { id, status, notes, appointment_time } = body; await sql`UPDATE leads SET status=COALESCE(${status||null},status), notes=COALESCE(${notes||null},notes), appointment_time=COALESCE(${appointment_time||null},appointment_time) WHERE id=${id}`; return new Response(JSON.stringify({ ok: true }), { headers: H }); }
      if (a === "leads-release") { await sql`UPDATE leads SET status='new',grabbed_by='',grabbed_at=NULL WHERE id=${body.id}`; return new Response(JSON.stringify({ ok: true }), { headers: H }); }

      if (a === "sales-get") { const rows = await sql`SELECT data FROM daily_sales WHERE date_key=${body.date}`; return new Response(JSON.stringify({ data: rows.length ? rows[0].data : null }), { headers: H }); }
      if (a === "sales-all") { const rows = await sql`SELECT date_key,data FROM daily_sales ORDER BY date_key`; const out = {}; for (const r of rows) out[r.date_key] = r.data; return new Response(JSON.stringify({ data: out }), { headers: H }); }
      if (a === "sales-save") { const j = JSON.stringify(body.data); await sql`INSERT INTO daily_sales (date_key,data,updated_at) VALUES (${body.date},${j}::jsonb,NOW()) ON CONFLICT (date_key) DO UPDATE SET data=${j}::jsonb,updated_at=NOW()`; return new Response(JSON.stringify({ ok: true }), { headers: H }); }

      if (a === "kpi-save") { const { agent_id, date_key, dials, contacts, appointments, quotes, apps_submitted } = body; await sql`INSERT INTO kpis (agent_id,date_key,dials,contacts,appointments,quotes,apps_submitted,updated_at) VALUES (${agent_id},${date_key},${dials||0},${contacts||0},${appointments||0},${quotes||0},${apps_submitted||0},NOW()) ON CONFLICT (agent_id,date_key) DO UPDATE SET dials=${dials||0},contacts=${contacts||0},appointments=${appointments||0},quotes=${quotes||0},apps_submitted=${apps_submitted||0},updated_at=NOW()`; return new Response(JSON.stringify({ ok: true }), { headers: H }); }
    }

    return new Response(JSON.stringify({ error: "Unknown action" }), { status: 400, headers: H });
  } catch (e) { console.error("API Error:", e); return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: H }); }
};

export const config = { path: "/.netlify/functions/api" };
