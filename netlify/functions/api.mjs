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
      await sql`CREATE TABLE IF NOT EXISTS recruits (id SERIAL PRIMARY KEY, agent_id TEXT NOT NULL, name TEXT DEFAULT '', phone TEXT DEFAULT '', email TEXT DEFAULT '', location TEXT DEFAULT '', source TEXT DEFAULT '', notes TEXT DEFAULT '', stage TEXT DEFAULT 'prospect', licensed BOOLEAN DEFAULT FALSE, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`;
      await sql`CREATE TABLE IF NOT EXISTS crm_leads (id SERIAL PRIMARY KEY, agent_id TEXT NOT NULL, name TEXT DEFAULT '', phone TEXT DEFAULT '', email TEXT DEFAULT '', company TEXT DEFAULT '', location TEXT DEFAULT '', source TEXT DEFAULT '', status TEXT DEFAULT 'new', notes TEXT DEFAULT '', last_contacted TIMESTAMPTZ, next_follow_up TEXT DEFAULT '', created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`;
      await sql`CREATE TABLE IF NOT EXISTS agents (id TEXT PRIMARY KEY, name TEXT NOT NULL, pin TEXT NOT NULL, color TEXT DEFAULT '#5B8BD4', goal INT DEFAULT 0, active BOOLEAN DEFAULT TRUE, created_at TIMESTAMPTZ DEFAULT NOW())`;
      // Seed default agents if table is empty
      const agentCount = await sql`SELECT COUNT(*)::int as cnt FROM agents`;
      if (agentCount[0].cnt === 0) {
        const defaults = [
          {id:"ryan",name:"Ryan",pin:"1001",color:"#5B8BD4"},
          {id:"john",name:"John",pin:"1002",color:"#4EBF8B"},
          {id:"matt",name:"Matthew",pin:"1003",color:"#9B7ED8"},
          {id:"chris",name:"Christian",pin:"1004",color:"#D46B8C"},
          {id:"keel",name:"Keelan",pin:"1005",color:"#D4845B"},
          {id:"kent",name:"Kente",pin:"1006",color:"#4EB8B5"},
        ];
        for (const ag of defaults) {
          await sql`INSERT INTO agents (id,name,pin,color) VALUES (${ag.id},${ag.name},${ag.pin},${ag.color}) ON CONFLICT (id) DO NOTHING`;
        }
      }
      // Add missing columns to existing leads table
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS phone TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS email TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS location TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS company TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      return new Response(JSON.stringify({ ok: true }), { headers: H });
    }

    // ── LEADS READ ──
    if (a === "leads-list") {
      const s = url.searchParams.get("status") || "new";
      const agent = url.searchParams.get("agent");
      const rows = s === "all" ? await sql`SELECT * FROM leads ORDER BY created_at DESC` : await sql`SELECT * FROM leads WHERE status = ${s} ORDER BY created_at DESC`;

      // If a non-admin agent is requesting, check their dials — hide phone/email if under 600
      if (agent && agent !== "admin") {
        const today = url.searchParams.get("date") || new Date().toISOString().slice(0, 10);
        const kpiRows = await sql`SELECT dials FROM kpis WHERE agent_id = ${agent} AND date_key = ${today}`;
        const dials = kpiRows.length > 0 ? (kpiRows[0].dials || 0) : 0;
        if (dials < 600) {
          for (const row of rows) {
            row.phone = row.phone ? "🔒 Unlock with 600 dials" : "";
            row.email = row.email ? "🔒 Unlock with 600 dials" : "";
          }
        }
      }

      return new Response(JSON.stringify({ data: rows }), { headers: H });
    }
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

    // ── CRM READ ──
    if (a === "crm-list") { const agent = url.searchParams.get("agent"); if (!agent) return new Response(JSON.stringify({ error: "agent required" }), { status: 400, headers: H }); const status = url.searchParams.get("status"); const rows = status && status !== "all" ? await sql`SELECT * FROM crm_leads WHERE agent_id = ${agent} AND status = ${status} ORDER BY CASE status WHEN 'follow_up' THEN 1 WHEN 'appointment' THEN 2 WHEN 'contacted' THEN 3 WHEN 'new' THEN 4 WHEN 'quoted' THEN 5 WHEN 'closed' THEN 6 WHEN 'lost' THEN 7 END, updated_at DESC` : await sql`SELECT * FROM crm_leads WHERE agent_id = ${agent} ORDER BY CASE status WHEN 'follow_up' THEN 1 WHEN 'appointment' THEN 2 WHEN 'contacted' THEN 3 WHEN 'new' THEN 4 WHEN 'quoted' THEN 5 WHEN 'closed' THEN 6 WHEN 'lost' THEN 7 END, updated_at DESC`; return new Response(JSON.stringify({ data: rows }), { headers: H }); }
    if (a === "crm-stats") { const agent = url.searchParams.get("agent"); if (!agent) return new Response(JSON.stringify({ error: "agent required" }), { status: 400, headers: H }); const rows = await sql`SELECT status, COUNT(*)::int as count FROM crm_leads WHERE agent_id = ${agent} GROUP BY status`; const stats = {}; for (const r of rows) stats[r.status] = r.count; return new Response(JSON.stringify({ data: stats }), { headers: H }); }

    // ── AGENTS READ ──
    if (a === "agents-list") {
      const rows = await sql`SELECT id, name, pin, color, goal, active FROM agents WHERE active = TRUE ORDER BY created_at`;
      return new Response(JSON.stringify({ data: rows }), { headers: H });
    }

    // ── RECRUITS READ ──
    if (a === "recruits-list") { const agent = url.searchParams.get("agent"); const rows = agent ? await sql`SELECT * FROM recruits WHERE agent_id = ${agent} ORDER BY CASE stage WHEN 'interviewed' THEN 1 WHEN 'contacted' THEN 2 WHEN 'prospect' THEN 3 WHEN 'licensed' THEN 4 WHEN 'dropped' THEN 5 END, created_at DESC` : await sql`SELECT * FROM recruits ORDER BY created_at DESC`; return new Response(JSON.stringify({ data: rows }), { headers: H }); }
    if (a === "recruits-stats") { const agent = url.searchParams.get("agent"); const rows = agent ? await sql`SELECT stage, COUNT(*)::int as count FROM recruits WHERE agent_id = ${agent} GROUP BY stage` : await sql`SELECT stage, COUNT(*)::int as count FROM recruits GROUP BY stage`; const stats = {}; for (const r of rows) stats[r.stage] = r.count; return new Response(JSON.stringify({ data: stats }), { headers: H }); }

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

      // ── CRM WRITE ──
      if (a === "crm-add") {
        const { agent_id, name, phone, email, company, location, source, notes, next_follow_up } = body;
        if (!agent_id) return new Response(JSON.stringify({ error: "agent_id required" }), { status: 400, headers: H });
        const rows = await sql`INSERT INTO crm_leads (agent_id,name,phone,email,company,location,source,notes,next_follow_up) VALUES (${agent_id},${name||''},${phone||''},${email||''},${company||''},${location||''},${source||''},${notes||''},${next_follow_up||''}) RETURNING *`;
        return new Response(JSON.stringify({ data: rows[0] }), { headers: H });
      }
      if (a === "crm-update") {
        const { id, agent_id, name, phone, email, company, location, source, status, notes, next_follow_up } = body;
        if (!agent_id) return new Response(JSON.stringify({ error: "agent_id required" }), { status: 400, headers: H });
        const setLC = (status === 'contacted' || status === 'follow_up') ? `,last_contacted=NOW()` : '';
        await sql`UPDATE crm_leads SET name=COALESCE(${name||null},name), phone=COALESCE(${phone||null},phone), email=COALESCE(${email||null},email), company=COALESCE(${company||null},company), location=COALESCE(${location||null},location), source=COALESCE(${source||null},source), status=COALESCE(${status||null},status), notes=COALESCE(${notes||null},notes), next_follow_up=COALESCE(${next_follow_up||null},next_follow_up), updated_at=NOW() WHERE id=${id} AND agent_id=${agent_id}`;
        if (status === 'contacted' || status === 'follow_up') { await sql`UPDATE crm_leads SET last_contacted=NOW() WHERE id=${id} AND agent_id=${agent_id}`; }
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }
      if (a === "crm-delete") {
        const { id, agent_id } = body;
        if (!agent_id) return new Response(JSON.stringify({ error: "agent_id required" }), { status: 400, headers: H });
        await sql`DELETE FROM crm_leads WHERE id=${id} AND agent_id=${agent_id}`;
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }
      if (a === "crm-import") {
        const { agent_id, leads: importLeads } = body;
        if (!agent_id) return new Response(JSON.stringify({ error: "agent_id required" }), { status: 400, headers: H });
        const inserted = [];
        for (const r of (importLeads || [])) {
          const rows = await sql`INSERT INTO crm_leads (agent_id,name,phone,email,company,location,source,notes) VALUES (${agent_id},${r.name||''},${r.phone||''},${r.email||''},${r.company||''},${r.location||''},${r.source||''},${r.notes||''}) RETURNING *`;
          inserted.push(rows[0]);
        }
        return new Response(JSON.stringify({ data: inserted, count: inserted.length }), { headers: H });
      }

      if (a === "recruits-add") {
        const { agent_id, name, phone, email, location, source, notes } = body;
        const rows = await sql`INSERT INTO recruits (agent_id,name,phone,email,location,source,notes) VALUES (${agent_id||''},${name||''},${phone||''},${email||''},${location||''},${source||''},${notes||''}) RETURNING *`;
        return new Response(JSON.stringify({ data: rows[0] }), { headers: H });
      }
      if (a === "recruits-update") {
        const { id, name, phone, email, location, source, notes, stage, licensed } = body;
        await sql`UPDATE recruits SET name=COALESCE(${name||null},name), phone=COALESCE(${phone||null},phone), email=COALESCE(${email||null},email), location=COALESCE(${location||null},location), source=COALESCE(${source||null},source), notes=COALESCE(${notes||null},notes), stage=COALESCE(${stage||null},stage), licensed=COALESCE(${typeof licensed==='boolean'?licensed:null},licensed), updated_at=NOW() WHERE id=${id}`;
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }
      if (a === "recruits-delete") { await sql`DELETE FROM recruits WHERE id=${body.id}`; return new Response(JSON.stringify({ ok: true }), { headers: H }); }
      if (a === "recruits-import") {
        const { agent_id, recruits } = body;
        const inserted = [];
        for (const r of (recruits || [])) {
          const rows = await sql`INSERT INTO recruits (agent_id,name,phone,email,location,source,notes) VALUES (${agent_id||''},${r.name||''},${r.phone||''},${r.email||''},${r.location||''},${r.source||''},${r.notes||''}) RETURNING *`;
          inserted.push(rows[0]);
        }
        return new Response(JSON.stringify({ data: inserted, count: inserted.length }), { headers: H });
      }

      if (a === "sales-get") { const rows = await sql`SELECT data FROM daily_sales WHERE date_key=${body.date}`; return new Response(JSON.stringify({ data: rows.length ? rows[0].data : null }), { headers: H }); }
      if (a === "sales-all") { const rows = await sql`SELECT date_key,data FROM daily_sales ORDER BY date_key`; const out = {}; for (const r of rows) out[r.date_key] = r.data; return new Response(JSON.stringify({ data: out }), { headers: H }); }
      if (a === "sales-save") { const j = JSON.stringify(body.data); await sql`INSERT INTO daily_sales (date_key,data,updated_at) VALUES (${body.date},${j}::jsonb,NOW()) ON CONFLICT (date_key) DO UPDATE SET data=${j}::jsonb,updated_at=NOW()`; return new Response(JSON.stringify({ ok: true }), { headers: H }); }

      if (a === "kpi-save") { const { agent_id, date_key, dials, contacts, appointments, quotes, apps_submitted } = body; await sql`INSERT INTO kpis (agent_id,date_key,dials,contacts,appointments,quotes,apps_submitted,updated_at) VALUES (${agent_id},${date_key},${dials||0},${contacts||0},${appointments||0},${quotes||0},${apps_submitted||0},NOW()) ON CONFLICT (agent_id,date_key) DO UPDATE SET dials=${dials||0},contacts=${contacts||0},appointments=${appointments||0},quotes=${quotes||0},apps_submitted=${apps_submitted||0},updated_at=NOW()`; return new Response(JSON.stringify({ ok: true }), { headers: H }); }

      // ── AGENTS WRITE ──
      if (a === "agents-add") {
        const { id, name, pin, color } = body;
        if (!id || !name || !pin) return new Response(JSON.stringify({ error: "id, name, and pin are required" }), { status: 400, headers: H });
        if (pin.length !== 4 || !/^\d{4}$/.test(pin)) return new Response(JSON.stringify({ error: "PIN must be exactly 4 digits" }), { status: 400, headers: H });
        if (pin === "9999") return new Response(JSON.stringify({ error: "PIN 9999 is reserved for admin" }), { status: 400, headers: H });
        // Check for duplicate PIN
        const existing = await sql`SELECT id FROM agents WHERE pin = ${pin} AND active = TRUE`;
        if (existing.length > 0) return new Response(JSON.stringify({ error: "This PIN is already in use by another agent" }), { status: 409, headers: H });
        // Check for duplicate ID
        const existingId = await sql`SELECT id FROM agents WHERE id = ${id}`;
        if (existingId.length > 0) {
          // Reactivate if was deactivated
          await sql`UPDATE agents SET name=${name}, pin=${pin}, color=${color||'#5B8BD4'}, active=TRUE WHERE id=${id}`;
        } else {
          await sql`INSERT INTO agents (id,name,pin,color) VALUES (${id},${name},${pin},${color||'#5B8BD4'})`;
        }
        const rows = await sql`SELECT id, name, pin, color, goal, active FROM agents WHERE id = ${id}`;
        return new Response(JSON.stringify({ data: rows[0] }), { headers: H });
      }
      if (a === "agents-delete") {
        const { id } = body;
        if (!id) return new Response(JSON.stringify({ error: "id required" }), { status: 400, headers: H });
        await sql`UPDATE agents SET active = FALSE WHERE id = ${id}`;
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }
    }

    return new Response(JSON.stringify({ error: "Unknown action" }), { status: 400, headers: H });
  } catch (e) { console.error("API Error:", e); return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: H }); }
};

export const config = { path: "/.netlify/functions/api" };
