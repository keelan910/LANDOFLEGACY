import { neon } from "@netlify/neon";

// ── CRYPTO HELPERS ──
async function hashPin(pin, salt) {
  const enc = new TextEncoder();
  const data = enc.encode(salt + ":" + pin);
  const buf = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, "0")).join("");
}

function generateToken() {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return Array.from(bytes).map(b => b.toString(16).padStart(2, "0")).join("");
}

function generateSalt() {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes).map(b => b.toString(16).padStart(2, "0")).join("");
}

// ── SESSION VALIDATION ──
async function validateSession(sql, req) {
  const authHeader = req.headers.get("authorization") || "";
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
  if (!token || token.length < 32) return null;
  const rows = await sql`SELECT agent_id, role, expires_at FROM sessions WHERE token = ${token} AND expires_at > NOW()`;
  if (rows.length === 0) return null;
  return { agentId: rows[0].agent_id, role: rows[0].role };
}

// ── RATE LIMITING ──
async function checkRateLimit(sql, ip) {
  await sql`DELETE FROM login_attempts WHERE attempted_at < NOW() - INTERVAL '15 minutes'`;
  const rows = await sql`SELECT COUNT(*)::int as cnt FROM login_attempts WHERE ip_address = ${ip} AND attempted_at > NOW() - INTERVAL '15 minutes'`;
  return (rows[0]?.cnt || 0) < 10;
}

async function recordLoginAttempt(sql, ip) {
  await sql`INSERT INTO login_attempts (ip_address) VALUES (${ip})`;
}

export default async (req) => {
  const sql = neon();
  const origin = req.headers.get("origin") || "";
  const siteHost = new URL(req.url).origin;
  const allowedOrigin = origin === siteHost ? origin : siteHost;
  const H = {"Content-Type":"application/json","Access-Control-Allow-Origin":allowedOrigin,"Access-Control-Allow-Methods":"GET,POST,OPTIONS","Access-Control-Allow-Headers":"Content-Type,Authorization","Access-Control-Max-Age":"86400"};
  if (req.method === "OPTIONS") return new Response("ok", { headers: H });

  try {
    const url = new URL(req.url);
    const a = url.searchParams.get("a");

    // ── INIT (public — sets up tables) ──
    if (a === "init") {
      await sql`CREATE TABLE IF NOT EXISTS daily_sales (date_key TEXT PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}'::jsonb, updated_at TIMESTAMPTZ DEFAULT NOW())`;
      await sql`CREATE TABLE IF NOT EXISTS leads (id SERIAL PRIMARY KEY, name TEXT DEFAULT '', source TEXT DEFAULT '', post_text TEXT DEFAULT '', profile_url TEXT DEFAULT '', phone TEXT DEFAULT '', email TEXT DEFAULT '', location TEXT DEFAULT '', company TEXT DEFAULT '', intent TEXT DEFAULT 'medium', status TEXT DEFAULT 'new', grabbed_by TEXT DEFAULT '', grabbed_at TIMESTAMPTZ, appointment_time TEXT DEFAULT '', notes TEXT DEFAULT '', ai_draft TEXT DEFAULT '', created_at TIMESTAMPTZ DEFAULT NOW())`;
      await sql`CREATE TABLE IF NOT EXISTS kpis (id SERIAL PRIMARY KEY, agent_id TEXT NOT NULL, date_key TEXT NOT NULL, dials INT DEFAULT 0, contacts INT DEFAULT 0, appointments INT DEFAULT 0, quotes INT DEFAULT 0, apps_submitted INT DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(agent_id, date_key))`;
      await sql`CREATE TABLE IF NOT EXISTS recruits (id SERIAL PRIMARY KEY, agent_id TEXT NOT NULL, name TEXT DEFAULT '', phone TEXT DEFAULT '', email TEXT DEFAULT '', location TEXT DEFAULT '', source TEXT DEFAULT '', notes TEXT DEFAULT '', stage TEXT DEFAULT 'prospect', licensed BOOLEAN DEFAULT FALSE, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`;
      await sql`CREATE TABLE IF NOT EXISTS crm_leads (id SERIAL PRIMARY KEY, agent_id TEXT NOT NULL, name TEXT DEFAULT '', phone TEXT DEFAULT '', email TEXT DEFAULT '', company TEXT DEFAULT '', location TEXT DEFAULT '', source TEXT DEFAULT '', status TEXT DEFAULT 'new', notes TEXT DEFAULT '', last_contacted TIMESTAMPTZ, next_follow_up TEXT DEFAULT '', created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`;
      await sql`CREATE TABLE IF NOT EXISTS agents (id TEXT PRIMARY KEY, name TEXT NOT NULL, pin_hash TEXT NOT NULL, pin_salt TEXT NOT NULL, color TEXT DEFAULT '#5B8BD4', goal INT DEFAULT 0, active BOOLEAN DEFAULT TRUE, created_at TIMESTAMPTZ DEFAULT NOW())`;
      await sql`CREATE TABLE IF NOT EXISTS sessions (id SERIAL PRIMARY KEY, token TEXT UNIQUE NOT NULL, agent_id TEXT, role TEXT NOT NULL DEFAULT 'agent', created_at TIMESTAMPTZ DEFAULT NOW(), expires_at TIMESTAMPTZ NOT NULL)`;
      await sql`CREATE TABLE IF NOT EXISTS login_attempts (id SERIAL PRIMARY KEY, ip_address TEXT NOT NULL, attempted_at TIMESTAMPTZ DEFAULT NOW())`;

      try {
        const colCheck = await sql`SELECT column_name FROM information_schema.columns WHERE table_name = 'agents' AND column_name = 'pin' AND data_type = 'text'`;
        const hashColCheck = await sql`SELECT column_name FROM information_schema.columns WHERE table_name = 'agents' AND column_name = 'pin_hash'`;
        if (colCheck.length > 0 && hashColCheck.length === 0) {
          await sql`ALTER TABLE agents ADD COLUMN pin_hash TEXT DEFAULT ''`;
          await sql`ALTER TABLE agents ADD COLUMN pin_salt TEXT DEFAULT ''`;
          const oldAgents = await sql`SELECT id, pin FROM agents`;
          for (const ag of oldAgents) {
            const salt = generateSalt();
            const hash = await hashPin(ag.pin, salt);
            await sql`UPDATE agents SET pin_hash = ${hash}, pin_salt = ${salt} WHERE id = ${ag.id}`;
          }
          await sql`ALTER TABLE agents DROP COLUMN pin`;
        } else if (colCheck.length > 0 && hashColCheck.length > 0) {
          const unhashed = await sql`SELECT id, pin FROM agents WHERE (pin_hash = '' OR pin_hash IS NULL) AND pin IS NOT NULL AND pin != ''`;
          for (const ag of unhashed) {
            const salt = generateSalt();
            const hash = await hashPin(ag.pin, salt);
            await sql`UPDATE agents SET pin_hash = ${hash}, pin_salt = ${salt} WHERE id = ${ag.id}`;
          }
          try { await sql`ALTER TABLE agents DROP COLUMN pin`; } catch {}
        }
      } catch {}

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
          const salt = generateSalt();
          const hash = await hashPin(ag.pin, salt);
          await sql`INSERT INTO agents (id,name,pin_hash,pin_salt,color) VALUES (${ag.id},${ag.name},${hash},${salt},${ag.color}) ON CONFLICT (id) DO NOTHING`;
        }
      }

      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS phone TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS email TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS location TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;
      await sql`DO $$ BEGIN ALTER TABLE leads ADD COLUMN IF NOT EXISTS company TEXT DEFAULT ''; EXCEPTION WHEN OTHERS THEN NULL; END $$`;

      await sql`DELETE FROM sessions WHERE expires_at < NOW()`;
      await sql`DELETE FROM login_attempts WHERE attempted_at < NOW() - INTERVAL '1 hour'`;

      return new Response(JSON.stringify({ ok: true }), { headers: H });
    }

    // ── LOGIN (public — rate-limited, POST only) ──
    if (a === "login") {
      if (req.method !== "POST") return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405, headers: H });

      const ip = req.headers.get("x-nf-client-connection-ip") || req.headers.get("x-forwarded-for") || "unknown";
      const allowed = await checkRateLimit(sql, ip);
      if (!allowed) return new Response(JSON.stringify({ error: "Too many login attempts. Try again in 15 minutes." }), { status: 429, headers: H });

      let body;
      try { body = await req.json(); } catch { return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400, headers: H }); }
      const pin = String(body?.pin || "");
      if (!pin || !/^\d{4}$/.test(pin)) return new Response(JSON.stringify({ error: "Invalid PIN" }), { status: 400, headers: H });

      await recordLoginAttempt(sql, ip);

      // Check admin PIN (must be set as env var — no hardcoded fallback)
      const ADMIN_PIN = process.env.ADMIN_PIN;
      if (ADMIN_PIN && pin === ADMIN_PIN) {
        const token = generateToken();
        await sql`INSERT INTO sessions (token, agent_id, role, expires_at) VALUES (${token}, NULL, 'admin', NOW() + INTERVAL '12 hours')`;
        return new Response(JSON.stringify({ data: { role: "admin", agent: null, token } }), { headers: H });
      }

      const allAgents = await sql`SELECT id, name, color, goal, pin_hash, pin_salt FROM agents WHERE active = TRUE`;
      let matchedAgent = null;
      for (const ag of allAgents) {
        if (!ag.pin_hash || !ag.pin_salt) continue;
        const testHash = await hashPin(pin, ag.pin_salt);
        if (testHash === ag.pin_hash) { matchedAgent = ag; break; }
      }

      if (!matchedAgent) return new Response(JSON.stringify({ error: "Invalid PIN" }), { status: 401, headers: H });

      const token = generateToken();
      await sql`INSERT INTO sessions (token, agent_id, role, expires_at) VALUES (${token}, ${matchedAgent.id}, 'agent', NOW() + INTERVAL '12 hours')`;
      return new Response(JSON.stringify({ data: { role: "agent", agent: { id: matchedAgent.id, name: matchedAgent.name, color: matchedAgent.color, goal: matchedAgent.goal }, token } }), { headers: H });
    }

    // ═══════════════════════════════════════════
    // ALL ENDPOINTS BELOW REQUIRE AUTHENTICATION
    // ═══════════════════════════════════════════
    const session = await validateSession(sql, req);
    if (!session) return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: H });

    const isAdmin = session.role === "admin";
    const sessionAgent = session.agentId;

    // ── LOGOUT ──
    if (a === "logout") {
      const authHeader = req.headers.get("authorization") || "";
      const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
      if (token) await sql`DELETE FROM sessions WHERE token = ${token}`;
      return new Response(JSON.stringify({ ok: true }), { headers: H });
    }

    // ── AGENTS READ ──
    if (a === "agents-list") {
      const rows = await sql`SELECT id, name, color, goal, active FROM agents WHERE active = TRUE ORDER BY created_at`;
      return new Response(JSON.stringify({ data: rows }), { headers: H });
    }

    // ── LEADS READ ──
    if (a === "leads-list") {
      const s = url.searchParams.get("status") || "new";
      const rows = s === "all" ? await sql`SELECT * FROM leads ORDER BY created_at DESC` : await sql`SELECT * FROM leads WHERE status = ${s} ORDER BY created_at DESC`;

      if (!isAdmin && sessionAgent) {
        const today = url.searchParams.get("date") || new Date().toISOString().slice(0, 10);
        const kpiRows = await sql`SELECT dials FROM kpis WHERE agent_id = ${sessionAgent} AND date_key = ${today}`;
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

    if (a === "leads-my") {
      const agent = isAdmin ? url.searchParams.get("agent") : sessionAgent;
      if (!agent) return new Response(JSON.stringify({ data: [] }), { headers: H });
      const rows = await sql`SELECT * FROM leads WHERE grabbed_by = ${agent} AND status NOT IN ('discarded','new') ORDER BY CASE status WHEN 'booked' THEN 1 WHEN 'contacted' THEN 2 WHEN 'grabbed' THEN 3 WHEN 'closed' THEN 4 END, created_at DESC`;
      return new Response(JSON.stringify({ data: rows }), { headers: H });
    }

    if (a === "leads-stats") {
      const rows = await sql`SELECT status, COUNT(*)::int as count FROM leads GROUP BY status`;
      const stats = {}; for (const r of rows) stats[r.status] = r.count;
      return new Response(JSON.stringify({ data: stats }), { headers: H });
    }

    // ── LEAD LOCK CHECK ──
    if (a === "leads-lock-check") {
      const agent = isAdmin ? url.searchParams.get("agent") : sessionAgent;
      if (!agent) return new Response(JSON.stringify({ data: { dials: 0, unlocked: true, maxLeads: 999, grabbedToday: 0, remaining: 999, highIntentOnly: false, salesToday: 0 } }), { headers: H });
      const today = url.searchParams.get("date") || new Date().toISOString().slice(0, 10);

      const kpiRows = await sql`SELECT dials FROM kpis WHERE agent_id = ${agent} AND date_key = ${today}`;
      const dials = kpiRows.length > 0 ? (kpiRows[0].dials || 0) : 0;
      const grabRows = await sql`SELECT COUNT(*)::int as cnt FROM leads WHERE grabbed_by = ${agent} AND grabbed_at::date = ${today}::date`;
      const grabbedToday = grabRows[0]?.cnt || 0;
      const saleRows = await sql`SELECT COUNT(*)::int as cnt FROM leads WHERE grabbed_by = ${agent} AND status = 'closed' AND grabbed_at::date = ${today}::date`;
      const salesToday = saleRows[0]?.cnt || 0;

      let maxLeads = 0, highIntentOnly = false, unlocked = false;
      if (dials >= 600) {
        unlocked = true;
        if (salesToday > 0) { maxLeads = 5; highIntentOnly = true; } else { maxLeads = 10; }
      }
      const remaining = Math.max(0, maxLeads - grabbedToday);

      return new Response(JSON.stringify({ data: { dials, unlocked, maxLeads, grabbedToday, remaining, highIntentOnly, salesToday } }), { headers: H });
    }

    // ── KPI READ ──
    if (a === "kpi-get") {
      const agent = isAdmin ? (url.searchParams.get("agent") || sessionAgent) : sessionAgent;
      const rows = await sql`SELECT * FROM kpis WHERE agent_id = ${agent} AND date_key = ${url.searchParams.get("date")}`;
      return new Response(JSON.stringify({ data: rows[0] || null }), { headers: H });
    }

    if (a === "kpi-team") {
      const rows = await sql`SELECT * FROM kpis WHERE date_key = ${url.searchParams.get("date")} ORDER BY dials DESC`;
      return new Response(JSON.stringify({ data: rows }), { headers: H });
    }

    // ── CRM READ ──
    if (a === "crm-list") {
      const agent = isAdmin ? url.searchParams.get("agent") : sessionAgent;
      if (!agent) return new Response(JSON.stringify({ error: "agent required" }), { status: 400, headers: H });
      const status = url.searchParams.get("status");
      const rows = status && status !== "all"
        ? await sql`SELECT * FROM crm_leads WHERE agent_id = ${agent} AND status = ${status} ORDER BY CASE status WHEN 'follow_up' THEN 1 WHEN 'appointment' THEN 2 WHEN 'contacted' THEN 3 WHEN 'new' THEN 4 WHEN 'quoted' THEN 5 WHEN 'closed' THEN 6 WHEN 'lost' THEN 7 END, updated_at DESC`
        : await sql`SELECT * FROM crm_leads WHERE agent_id = ${agent} ORDER BY CASE status WHEN 'follow_up' THEN 1 WHEN 'appointment' THEN 2 WHEN 'contacted' THEN 3 WHEN 'new' THEN 4 WHEN 'quoted' THEN 5 WHEN 'closed' THEN 6 WHEN 'lost' THEN 7 END, updated_at DESC`;
      return new Response(JSON.stringify({ data: rows }), { headers: H });
    }

    if (a === "crm-stats") {
      const agent = isAdmin ? url.searchParams.get("agent") : sessionAgent;
      if (!agent) return new Response(JSON.stringify({ error: "agent required" }), { status: 400, headers: H });
      const rows = await sql`SELECT status, COUNT(*)::int as count FROM crm_leads WHERE agent_id = ${agent} GROUP BY status`;
      const stats = {}; for (const r of rows) stats[r.status] = r.count;
      return new Response(JSON.stringify({ data: stats }), { headers: H });
    }

    // ── RECRUITS READ ──
    if (a === "recruits-list") {
      const agent = isAdmin ? (url.searchParams.get("agent") || null) : sessionAgent;
      const rows = agent
        ? await sql`SELECT * FROM recruits WHERE agent_id = ${agent} ORDER BY CASE stage WHEN 'interviewed' THEN 1 WHEN 'contacted' THEN 2 WHEN 'prospect' THEN 3 WHEN 'licensed' THEN 4 WHEN 'dropped' THEN 5 END, created_at DESC`
        : await sql`SELECT * FROM recruits ORDER BY created_at DESC`;
      return new Response(JSON.stringify({ data: rows }), { headers: H });
    }

    if (a === "recruits-stats") {
      const agent = isAdmin ? (url.searchParams.get("agent") || null) : sessionAgent;
      const rows = agent
        ? await sql`SELECT stage, COUNT(*)::int as count FROM recruits WHERE agent_id = ${agent} GROUP BY stage`
        : await sql`SELECT stage, COUNT(*)::int as count FROM recruits GROUP BY stage`;
      const stats = {}; for (const r of rows) stats[r.stage] = r.count;
      return new Response(JSON.stringify({ data: stats }), { headers: H });
    }

    // ── POST ──
    if (req.method === "POST") {
      const contentLength = parseInt(req.headers.get("content-length") || "0", 10);
      if (contentLength > 102400) return new Response(JSON.stringify({ error: "Request too large" }), { status: 413, headers: H });

      let body;
      try { body = await req.json(); } catch { return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400, headers: H }); }
      if (!body || typeof body !== "object") return new Response(JSON.stringify({ error: "Invalid request body" }), { status: 400, headers: H });

      const clean = (val, maxLen = 500) => {
        if (val === null || val === undefined) return '';
        const s = String(val).slice(0, maxLen);
        return s.replace(/<[^>]*>/g, '');
      };

      if (a === "leads-add") {
        if (!isAdmin) return new Response(JSON.stringify({ error: "Admin only" }), { status: 403, headers: H });
        const name = clean(body.name, 200), source = clean(body.source, 200), post_text = clean(body.post_text, 2000), intent = ['high','medium','low'].includes(body.intent) ? body.intent : 'medium', ai_draft = clean(body.ai_draft, 2000), phone = clean(body.phone, 50), email = clean(body.email, 200), location = clean(body.location, 300), company = clean(body.company, 300), profile_url = clean(body.profile_url, 500);
        const rows = await sql`INSERT INTO leads (name,source,post_text,intent,ai_draft,phone,email,location,company,profile_url) VALUES (${name},${source},${post_text},${intent},${ai_draft},${phone},${email},${location},${company},${profile_url}) RETURNING *`;
        return new Response(JSON.stringify({ data: rows[0] }), { headers: H });
      }

      if (a === "leads-grab") {
        const agent = isAdmin ? (body.agent || "admin") : sessionAgent;
        const today = body.date || new Date().toISOString().slice(0, 10);

        if (!isAdmin) {
          const kpiRows = await sql`SELECT dials FROM kpis WHERE agent_id = ${agent} AND date_key = ${today}`;
          const dials = kpiRows.length > 0 ? (kpiRows[0].dials || 0) : 0;
          if (dials < 600) {
            return new Response(JSON.stringify({ error: `Locked! You need 600 dials to unlock leads. You have ${dials}. Get on the phone!` }), { status: 403, headers: H });
          }
          const grabRows = await sql`SELECT COUNT(*)::int as cnt FROM leads WHERE grabbed_by = ${agent} AND grabbed_at::date = ${today}::date`;
          const grabbedToday = grabRows[0]?.cnt || 0;
          const saleRows = await sql`SELECT COUNT(*)::int as cnt FROM leads WHERE grabbed_by = ${agent} AND status = 'closed' AND grabbed_at::date = ${today}::date`;
          const salesToday = saleRows[0]?.cnt || 0;
          const maxLeads = salesToday > 0 ? 5 : 10;
          if (grabbedToday >= maxLeads) {
            const reason = salesToday > 0 ? "You made a sale — 5 lead max (high intent only)." : "10 lead max reached for today.";
            return new Response(JSON.stringify({ error: `Lead limit reached! ${reason} Come back tomorrow or close more deals.` }), { status: 403, headers: H });
          }
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

      // ── FIX: leads-update — agents can only update their own grabbed leads ──
      if (a === "leads-update") {
        const { id, status, notes, appointment_time } = body;
        if (!isAdmin) {
          const owner = await sql`SELECT grabbed_by FROM leads WHERE id=${id}`;
          if (!owner.length || owner[0].grabbed_by !== sessionAgent) {
            return new Response(JSON.stringify({ error: "Forbidden" }), { status: 403, headers: H });
          }
        }
        await sql`UPDATE leads SET status=COALESCE(${status||null},status), notes=COALESCE(${notes||null},notes), appointment_time=COALESCE(${appointment_time||null},appointment_time) WHERE id=${id}`;
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }

      // ── FIX: leads-release — admin only ──
      if (a === "leads-release") {
        if (!isAdmin) return new Response(JSON.stringify({ error: "Admin only" }), { status: 403, headers: H });
        await sql`UPDATE leads SET status='new',grabbed_by='',grabbed_at=NULL WHERE id=${body.id}`;
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }

      // ── CRM WRITE ──
      if (a === "crm-add") {
        const agent_id = isAdmin ? (body.agent_id || sessionAgent) : sessionAgent;
        if (!agent_id) return new Response(JSON.stringify({ error: "agent_id required" }), { status: 400, headers: H });
        const { name, phone, email, company, location, source, notes, next_follow_up } = body;
        const rows = await sql`INSERT INTO crm_leads (agent_id,name,phone,email,company,location,source,notes,next_follow_up) VALUES (${agent_id},${name||''},${phone||''},${email||''},${company||''},${location||''},${source||''},${notes||''},${next_follow_up||''}) RETURNING *`;
        return new Response(JSON.stringify({ data: rows[0] }), { headers: H });
      }

      if (a === "crm-update") {
        const agent_id = isAdmin ? (body.agent_id || sessionAgent) : sessionAgent;
        if (!agent_id) return new Response(JSON.stringify({ error: "agent_id required" }), { status: 400, headers: H });
        const { id, name, phone, email, company, location, source, status, notes, next_follow_up } = body;
        await sql`UPDATE crm_leads SET name=COALESCE(${name||null},name), phone=COALESCE(${phone||null},phone), email=COALESCE(${email||null},email), company=COALESCE(${company||null},company), location=COALESCE(${location||null},location), source=COALESCE(${source||null},source), status=COALESCE(${status||null},status), notes=COALESCE(${notes||null},notes), next_follow_up=COALESCE(${next_follow_up||null},next_follow_up), updated_at=NOW() WHERE id=${id} AND agent_id=${agent_id}`;
        if (status === 'contacted' || status === 'follow_up') { await sql`UPDATE crm_leads SET last_contacted=NOW() WHERE id=${id} AND agent_id=${agent_id}`; }
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }

      if (a === "crm-delete") {
        const agent_id = isAdmin ? (body.agent_id || sessionAgent) : sessionAgent;
        if (!agent_id) return new Response(JSON.stringify({ error: "agent_id required" }), { status: 400, headers: H });
        await sql`DELETE FROM crm_leads WHERE id=${body.id} AND agent_id=${agent_id}`;
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }

      if (a === "crm-import") {
        const agent_id = isAdmin ? (body.agent_id || sessionAgent) : sessionAgent;
        if (!agent_id) return new Response(JSON.stringify({ error: "agent_id required" }), { status: 400, headers: H });
        const inserted = [];
        for (const r of (body.leads || []).slice(0, 200)) {
          const rows = await sql`INSERT INTO crm_leads (agent_id,name,phone,email,company,location,source,notes) VALUES (${agent_id},${r.name||''},${r.phone||''},${r.email||''},${r.company||''},${r.location||''},${r.source||''},${r.notes||''}) RETURNING *`;
          inserted.push(rows[0]);
        }
        return new Response(JSON.stringify({ data: inserted, count: inserted.length }), { headers: H });
      }

      if (a === "recruits-add") {
        const agent_id = isAdmin ? (body.agent_id || sessionAgent) : sessionAgent;
        const { name, phone, email, location, source, notes } = body;
        const rows = await sql`INSERT INTO recruits (agent_id,name,phone,email,location,source,notes) VALUES (${agent_id||''},${name||''},${phone||''},${email||''},${location||''},${source||''},${notes||''}) RETURNING *`;
        return new Response(JSON.stringify({ data: rows[0] }), { headers: H });
      }

      // ── FIX: recruits-update — agents can only update their own recruits ──
      if (a === "recruits-update") {
        const { id, name, phone, email, location, source, notes, stage, licensed } = body;
        if (isAdmin) {
          await sql`UPDATE recruits SET name=COALESCE(${name||null},name), phone=COALESCE(${phone||null},phone), email=COALESCE(${email||null},email), location=COALESCE(${location||null},location), source=COALESCE(${source||null},source), notes=COALESCE(${notes||null},notes), stage=COALESCE(${stage||null},stage), licensed=COALESCE(${typeof licensed==='boolean'?licensed:null},licensed), updated_at=NOW() WHERE id=${id}`;
        } else {
          await sql`UPDATE recruits SET name=COALESCE(${name||null},name), phone=COALESCE(${phone||null},phone), email=COALESCE(${email||null},email), location=COALESCE(${location||null},location), source=COALESCE(${source||null},source), notes=COALESCE(${notes||null},notes), stage=COALESCE(${stage||null},stage), licensed=COALESCE(${typeof licensed==='boolean'?licensed:null},licensed), updated_at=NOW() WHERE id=${id} AND agent_id=${sessionAgent}`;
        }
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }

      // ── FIX: recruits-delete — agents can only delete their own recruits ──
      if (a === "recruits-delete") {
        if (isAdmin) {
          await sql`DELETE FROM recruits WHERE id=${body.id}`;
        } else {
          await sql`DELETE FROM recruits WHERE id=${body.id} AND agent_id=${sessionAgent}`;
        }
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }

      if (a === "recruits-import") {
        const agent_id = isAdmin ? (body.agent_id || sessionAgent) : sessionAgent;
        const inserted = [];
        for (const r of (body.recruits || []).slice(0, 200)) {
          const rows = await sql`INSERT INTO recruits (agent_id,name,phone,email,location,source,notes) VALUES (${agent_id||''},${r.name||''},${r.phone||''},${r.email||''},${r.location||''},${r.source||''},${r.notes||''}) RETURNING *`;
          inserted.push(rows[0]);
        }
        return new Response(JSON.stringify({ data: inserted, count: inserted.length }), { headers: H });
      }

      if (a === "sales-get") {
        const rows = await sql`SELECT data FROM daily_sales WHERE date_key=${body.date}`;
        return new Response(JSON.stringify({ data: rows.length ? rows[0].data : null }), { headers: H });
      }

      if (a === "sales-all") {
        const rows = await sql`SELECT date_key,data FROM daily_sales ORDER BY date_key`;
        const out = {}; for (const r of rows) out[r.date_key] = r.data;
        return new Response(JSON.stringify({ data: out }), { headers: H });
      }

      if (a === "sales-save") {
        if (!isAdmin) return new Response(JSON.stringify({ error: "Admin only" }), { status: 403, headers: H });
        const j = JSON.stringify(body.data);
        await sql`INSERT INTO daily_sales (date_key,data,updated_at) VALUES (${body.date},${j}::jsonb,NOW()) ON CONFLICT (date_key) DO UPDATE SET data=${j}::jsonb,updated_at=NOW()`;
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }

      if (a === "kpi-save") {
        const agent_id = isAdmin ? (body.agent_id || sessionAgent) : sessionAgent;
        if (!agent_id) return new Response(JSON.stringify({ error: "agent_id required" }), { status: 400, headers: H });
        const { date_key, dials, contacts, appointments, quotes, apps_submitted } = body;
        await sql`INSERT INTO kpis (agent_id,date_key,dials,contacts,appointments,quotes,apps_submitted,updated_at) VALUES (${agent_id},${date_key},${dials||0},${contacts||0},${appointments||0},${quotes||0},${apps_submitted||0},NOW()) ON CONFLICT (agent_id,date_key) DO UPDATE SET dials=${dials||0},contacts=${contacts||0},appointments=${appointments||0},quotes=${quotes||0},apps_submitted=${apps_submitted||0},updated_at=NOW()`;
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }

      // ── AGENTS WRITE (admin only) ──
      if (a === "agents-add") {
        if (!isAdmin) return new Response(JSON.stringify({ error: "Admin only" }), { status: 403, headers: H });
        const id = clean(body.id, 30).toLowerCase().replace(/[^a-z0-9_-]/g, '');
        const name = clean(body.name, 100);
        const pin = String(body.pin || '');
        const color = clean(body.color, 20);
        if (!id || !name || !pin) return new Response(JSON.stringify({ error: "id, name, and pin are required" }), { status: 400, headers: H });
        if (pin.length !== 4 || !/^\d{4}$/.test(pin)) return new Response(JSON.stringify({ error: "PIN must be exactly 4 digits" }), { status: 400, headers: H });

        const ADMIN_PIN = process.env.ADMIN_PIN;
        if (ADMIN_PIN && pin === ADMIN_PIN) return new Response(JSON.stringify({ error: "This PIN is reserved" }), { status: 400, headers: H });

        const allAgents = await sql`SELECT id, pin_hash, pin_salt FROM agents WHERE active = TRUE`;
        for (const ag of allAgents) {
          if (!ag.pin_hash || !ag.pin_salt) continue;
          const testHash = await hashPin(pin, ag.pin_salt);
          if (testHash === ag.pin_hash) return new Response(JSON.stringify({ error: "This PIN is already in use by another agent" }), { status: 409, headers: H });
        }

        const salt = generateSalt();
        const pinHash = await hashPin(pin, salt);

        const existingId = await sql`SELECT id FROM agents WHERE id = ${id}`;
        if (existingId.length > 0) {
          await sql`UPDATE agents SET name=${name}, pin_hash=${pinHash}, pin_salt=${salt}, color=${color||'#5B8BD4'}, active=TRUE WHERE id=${id}`;
        } else {
          await sql`INSERT INTO agents (id,name,pin_hash,pin_salt,color) VALUES (${id},${name},${pinHash},${salt},${color||'#5B8BD4'})`;
        }
        const rows = await sql`SELECT id, name, color, goal, active FROM agents WHERE id = ${id}`;
        return new Response(JSON.stringify({ data: rows[0] }), { headers: H });
      }

      if (a === "agents-delete") {
        if (!isAdmin) return new Response(JSON.stringify({ error: "Admin only" }), { status: 403, headers: H });
        const { id } = body;
        if (!id) return new Response(JSON.stringify({ error: "id required" }), { status: 400, headers: H });
        await sql`UPDATE agents SET active = FALSE WHERE id = ${id}`;
        await sql`DELETE FROM sessions WHERE agent_id = ${id}`;
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }
    }

    return new Response(JSON.stringify({ error: "Unknown action" }), { status: 400, headers: H });
  } catch (e) { console.error("API Error:", e); return new Response(JSON.stringify({ error: "Internal server error" }), { status: 500, headers: H }); }
};

export const config = { path: "/.netlify/functions/api" };
