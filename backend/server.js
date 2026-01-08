// server.js
import express from "express";
import axios from "axios";
import dotenv from "dotenv";
import cors from "cors";
import fs from "fs";
import path from "path";

dotenv.config();
const app = express();
app.use(cors());
app.use(express.json());
// --------------------------------------------------
// NOTES / KEY CHANGES
// - business-map now returns account_id, business_id, business_uuid mapped clearly
// - chart_of_accounts uses use_ledger_entries=true and accepts date filters (only here)
// - ledger_accounts (advanced ledger) will NOT be passed date filters\
// - time_entries uses the comments/business/{business_id}/time_entries endpoint with recommended query params
// - safely handles token refresh collisions with a queue
// - improved response parsing for FreshBooks API varying shapes
// --------------------------------------------------
const FRESHBOOKS_BASE = process.env.FRESHBOOKS_API || "https://api.freshbooks.com";
const ENV_PATH = path.resolve(".env");
let accessToken = process.env.ACCESS_TOKEN || "";
let refreshToken = process.env.REFRESH_TOKEN || "";
let tokenExpiry = 0;
const HISTORY_PATH = path.resolve("history.log.json");
const MAX_HISTORY_ITEMS = 500;
const pretty = (obj) => {

  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return String(obj);
  }
};
// Normalize incoming date values to YYYY-MM-DD without timezone shifts
const normalizeDateParam = (value) => {
  if (!value) return "";
  const str = String(value).trim();
  const match = str.match(/^(\d{4}-\d{2}-\d{2})/);
  if (match) return match[1];
  const parsed = new Date(str);
  if (Number.isNaN(parsed.getTime())) return "";
  const yyyy = parsed.getFullYear();
  const mm = String(parsed.getMonth() + 1).padStart(2, "0");
  const dd = String(parsed.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
};
const loadHistory = () => {
  try {
    if (!fs.existsSync(HISTORY_PATH)) return [];
    const parsed = JSON.parse(fs.readFileSync(HISTORY_PATH, "utf-8"));
    return Array.isArray(parsed) ? parsed : [];
  } catch (err) {
    console.warn("[History] Failed to read history:", err.message);
    return [];
  }
};
const persistHistory = (items) => {
  try {
    fs.writeFileSync(
      HISTORY_PATH,
      JSON.stringify(items.slice(0, MAX_HISTORY_ITEMS), null, 2)
    );
  } catch (err) {
    console.warn("[History] Failed to write history:", err.message);
  }
};
const appendHistoryEntry = (entry) => {
  const next = loadHistory();
  next.unshift(entry);
  persistHistory(next);
};
const userKey = (u) => {
  if (!u) return "";
  if (u.id) return `id:${u.id}`;
  const name = (u.name || "").trim().toLowerCase();
  return `name:${name}`;
};
const upsertSessionHistory = (user, updateFn) => {
  const list = loadHistory();
  const key = userKey(user);
  const idx = list.findIndex(
    (item) => item.event === "session" && userKey(item.user) === key
  );
  const base =
    idx >= 0
      ? list[idx]
      : {
          id: `session_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
          event: "session",
          user,
          timestamp: new Date().toISOString(),
          actions: [],
          meta: {},
        };
  const updated = updateFn({ ...base, user: { ...base.user, ...user } }) || base;
  const filtered = idx >= 0 ? list.filter((_, i) => i !== idx) : list;
  filtered.unshift(updated);
  persistHistory(filtered);
  return updated;
};
const summarizeUserFromWhoami = (whoami) => {
  const user =
    whoami?.user ||
    whoami?.response?.user ||
    whoami?.response?.user ||
    whoami?.user ||
    {};
  const fullName = [user.fname, user.lname].filter(Boolean).join(" ").trim();
  return {
    id: user.id ?? user.userid ?? user.user_id ?? null,
    name: fullName || user.organization || user.email || "Unknown User",
    email: user.email || null,
  };
};
const sanitizeUserName = (name) => {
  if (!name) return "";
  return String(name).trim().slice(0, 120);
};
async function getUserSummary(token) {
  try {
    const whoami = await fetchWhoami(token);
    return summarizeUserFromWhoami(whoami);
  } catch (err) {
    console.warn("[History] Could not fetch user profile for history:", err.message);
    return { id: null, name: "Unknown User", email: null };
  }
}
// ---------------------------
// safe .env write helper
// ---------------------------
function safeReplaceEnv(key, value) {
  try {
    let envFile = fs.existsSync(ENV_PATH) ? fs.readFileSync(ENV_PATH, "utf-8") : "";
    const regex = new RegExp(`^${key}=.*$`, "m");
    if (regex.test(envFile)) {
      envFile = envFile.replace(regex, `${key}=${value}`);
    } else {
      if (envFile && !envFile.endsWith("\n")) envFile += "\n";
      envFile += `${key}=${value}\n`;
    }
    fs.writeFileSync(ENV_PATH, envFile);
  } catch (err) {
    console.warn("⚠️ Warning: could not write to .env", err.message);
  }
}
// ---------------------------
// Token refresh machinery
// ---------------------------
let isRefreshing = false;
let refreshQueue = [];
async function ensureAccessTokenValid() {
  const now = Math.floor(Date.now() / 1000);
  if (accessToken && now < tokenExpiry - 60) return accessToken;
  if (!refreshToken) throw new Error("No refresh token available — please reauthorize.");
  console.log("📡 Refreshing access token...");
  try {
    const res = await axios.post(
      `${FRESHBOOKS_BASE}/auth/oauth/token`,
      {
        grant_type: "refresh_token",
        refresh_token: refreshToken,
        client_id: process.env.CLIENT_ID,
        client_secret: process.env.CLIENT_SECRET,

      },
      { timeout: 25000 }
    );
    accessToken = res.data.access_token;
    refreshToken = res.data.refresh_token;
    tokenExpiry = Math.floor(Date.now() / 1000) + (res.data.expires_in || 3600);
    safeReplaceEnv("ACCESS_TOKEN", accessToken);
    safeReplaceEnv("REFRESH_TOKEN", refreshToken);
    console.log("✅ Token refreshed successfully!");
    return accessToken;
  } catch (err) {
    console.error("❌ Token refresh failed:", pretty(err.response?.data || err.message));
    throw new Error("Token refresh failed, please reauthorize manually.");

  }

}
async function getFreshTokenSafely() {
  if (isRefreshing) {
    return new Promise((resolve, reject) => {
      refreshQueue.push({ resolve, reject });

    });

  }
  isRefreshing = true;
  try {
    const token = await ensureAccessTokenValid();
    refreshQueue.forEach((p) => p.resolve(token));
    refreshQueue = [];
    isRefreshing = false;
    return token;
  } catch (err) {
    refreshQueue.forEach((p) => p.reject(err));
    refreshQueue = [];
    isRefreshing = false;
    throw err;
  }
}



// ---------------------------

// Basic routes

// ---------------------------

app.get("/", (_req, res) => res.send("✅ FreshBooks API Backend is running fine!"));

app.get("/test", (_req, res) => res.send("Server running successfully 🚀"));



// ---------------------------

// Auth redirect / callback

// ---------------------------

app.get("/auth", (req, res) => {
  const incomingName = sanitizeUserName(req.query.user_name);
  const statePayload = incomingName ? { user_name: incomingName } : null;
  const state = statePayload ? JSON.stringify(statePayload) : "";

  const url = new URL("https://auth.freshbooks.com/oauth/authorize");
  url.searchParams.set("client_id", process.env.CLIENT_ID);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("redirect_uri", process.env.REDIRECT_URI);
  url.searchParams.set("scope", process.env.SCOPE);
  url.searchParams.set("prompt", "login");
  url.searchParams.set("access_type", "offline");
  if (state) url.searchParams.set("state", state);
  const authUrl = url.toString();

  console.log("?? Redirecting user to FreshBooks login page:", authUrl);
  res.redirect(authUrl);
});



app.get("/callback", async (req, res) => {

  const { code, state } = req.query;
  let stateName = "";
  try {
    if (state) {
      const decoded = JSON.parse(state);
      stateName = sanitizeUserName(decoded?.user_name);
    }
  } catch {
    // ignore malformed state
  }

  if (!code) return res.send("❌ Missing authorization code");



  try {

    const tokenRes = await axios.post(

      `${FRESHBOOKS_BASE}/auth/oauth/token`,

      {

        grant_type: "authorization_code",

        client_id: process.env.CLIENT_ID,

        client_secret: process.env.CLIENT_SECRET,

        redirect_uri: process.env.REDIRECT_URI,

        code,

      },

      { headers: { "Content-Type": "application/json" }, timeout: 25000 }

    );



    accessToken = tokenRes.data.access_token;

    refreshToken = tokenRes.data.refresh_token;

    tokenExpiry = Math.floor(Date.now() / 1000) + tokenRes.data.expires_in;



    safeReplaceEnv("ACCESS_TOKEN", accessToken);

    safeReplaceEnv("REFRESH_TOKEN", refreshToken);

    try {
      const userProfile = await getUserSummary(accessToken);
      const user =
        stateName && stateName.length
          ? { ...userProfile, name: stateName }
          : userProfile;
      upsertSessionHistory(user, (session) => {
        return {
          ...session,
          timestamp: new Date().toISOString(),
          meta: { ...session.meta, last_login: new Date().toISOString() },
          actions: session.actions || [],
        };
      });
    } catch (historyErr) {
      console.warn("[History] Unable to log login event:", historyErr.message);
    }


    const frontendUrl = process.env.FRONTEND_URL || "http://localhost:5173";

    const redirectUrl = `${frontendUrl}?access=${encodeURIComponent(

      accessToken

    )}&refresh=${encodeURIComponent(refreshToken)}&expires=${tokenRes.data.expires_in}${
      stateName ? `&user_name=${encodeURIComponent(stateName)}` : ""
    }`;



    console.log("📡 Redirecting to frontend:", redirectUrl);

    res.redirect(redirectUrl);

  } catch (error) {

    console.error("❌ Auth Error:", pretty(error.response?.data || error.message));

    res.status(500).send(`<pre>${pretty(error.response?.data || error.message)}</pre>`);

  }

});



// ---------------------------

// Business map: returns list of businesses with all three ids clearly mapped

// ---------------------------

app.get("/api/business-map", async (_req, res) => {

  try {

    const token = await getFreshTokenSafely();

    const whoami = await axios.get(`${FRESHBOOKS_BASE}/auth/api/v1/users/me`, {

      headers: { Authorization: `Bearer ${token}` },

      timeout: 120000,

    });



    const memberships = whoami.data?.response?.business_memberships || [];



    // Map each membership to a clear object

    const businesses = memberships.map((m) => {

      const b = m.business || {};

      return {

        name: b.name || m.name || "Unnamed Business",

        account_id: (b.account_id || b.accounting_systemid || "").toString(),

        business_id: b.id ?? b.business_id ?? null,

        business_uuid: b.business_uuid ?? null,

      };

    });



    console.log(`📋 Found ${businesses.length} businesses for user.`);

    res.json({ success: true, businesses });

  } catch (error) {

    console.error("❌ /api/business-map failed:", pretty(error.response?.data || error.message));

    res.status(500).json({ error: error.response?.data || error.message });

  }

});



// ---------------------------

// ---------------------------
// Who am I (frontend display)
// ---------------------------
app.get("/api/whoami", async (req, res) => {
  try {
    const headerToken = (req.headers.authorization || "").replace(/Bearer\s+/i, "").trim();
    const token = headerToken || (await getFreshTokenSafely());

    if (!token) return res.status(401).json({ error: "No token available" });

    const r = await axios.get(`${FRESHBOOKS_BASE}/auth/api/v1/users/me`, {
      headers: { Authorization: `Bearer ${token}` },
      timeout: 120000,
    });

    res.json(r.data);
  } catch (error) {
    console.error("? /api/whoami failed:", pretty(error.response?.data || error.message));
    res.status(error.response?.status || 500).json({ error: error.response?.data || error.message });
  }
});

// Helpers to fetch business_uuid given account_id or business_id

// ---------------------------

async function fetchWhoami(token) {

  const response = await axios.get(`${FRESHBOOKS_BASE}/auth/api/v1/users/me`, {

    headers: { Authorization: `Bearer ${token}` },

    timeout: 120000,

  });

  return response.data?.response || response.data || {};

}



async function resolveBusinessUuid({ token, account_id, business_id }) {

  // Try to get from whoami memberships

  const whoami = await fetchWhoami(token);

  const memberships = whoami?.business_memberships || [];

  if (account_id) {

    const match = memberships.find((m) => {

      const b = m.business || {};

      return (b.account_id || b.accounting_systemid || "").toString() === account_id.toString();

    });

    if (match && match.business && match.business.business_uuid) return match.business.business_uuid;

  }



  if (business_id) {

    const match = memberships.find((m) => {

      const b = m.business || {};

      return String(b.id) === String(business_id);

    });

    if (match && match.business && match.business.business_uuid) return match.business.business_uuid;

  }



  // fallback: return any non-empty business_uuid if only one membership exists

  const firstUuid = memberships.find((m) => m.business?.business_uuid)?.business?.business_uuid;

  return firstUuid || null;

}



// ---------------------------

// Generic extraction endpoint

// - supports types defined in cfgMap

// - uses the correct id type (account, business_id, business_uuid)

// - applies date filters only when allowed (chart_of_accounts supports dates; ledger_accounts DOES NOT)

// ---------------------------

// ---------------------------

// Generic extraction endpoint (WITH LINE ITEM SUPPORT)

// ---------------------------

// ---------------------------
// Generic extraction endpoint (WITH LINE ITEM SUPPORT)
// ---------------------------
app.get("/api/extract", async (req, res) => {
  try {
    console.log("=========================================");
    console.log("📥 NEW EXTRACT REQUEST RECEIVED");
    console.log("Query:", req.query);
    console.log("=========================================");

    // Allow very long requests; frontend also uses a 180s timeout
    req.setTimeout(0);
    res.setTimeout?.(0);

    let { start_date, end_date, type, account_id, business_id, business_uuid } = req.query;
    const lineMode =
      String(req.query.line_mode || "")
        .toLowerCase()
        .trim() === "true" || req.query.line_mode === "1";
    const maxPagesParam = Number(req.query.max_pages);
    const MAX_PAGE_OVERRIDE = Number.isFinite(maxPagesParam) && maxPagesParam > 0 ? maxPagesParam : null;
    const INCLUDE_RAW =
      String(req.query.include_raw || "")
        .toLowerCase()
        .trim() === "true" || req.query.include_raw === "1";

    if (!type) return res.status(400).json({ error: "Missing 'type' parameter" });

    start_date = normalizeDateParam(start_date);
    end_date = normalizeDateParam(end_date);

    console.log(`Dates applied -> start: ${start_date || "(none)"} | end: ${end_date || "(none)"}`);

    const validToken = await getFreshTokenSafely();

    account_id = account_id || process.env.ACCOUNT_ID;
    business_id = business_id || process.env.BUSINESS_ID;
    business_uuid = business_uuid || process.env.BUSINESS_UUID;

    if (!business_uuid && (account_id || business_id)) {
      try {
        business_uuid = await resolveBusinessUuid({ token: validToken, account_id, business_id });
      } catch {
        // ignore resolve error, will be validated below
      }
    }

    let cachedHistoryUser = null;
    const getHistoryUser = async () => {
      if (cachedHistoryUser) return cachedHistoryUser;
      const manualName = sanitizeUserName(
        req.headers["x-user-name"] || req.query.user_name
      );
      const profile = await getUserSummary(validToken);
      cachedHistoryUser = manualName ? { ...profile, name: manualName } : profile;
      return cachedHistoryUser;
    };
    const logExtractEvent = async (payload) => {
      try {
        const user = await getHistoryUser();
        upsertSessionHistory(user, (session) => {
          const actions = Array.isArray(session.actions) ? [...session.actions] : [];
          actions.unshift({
            kind: "extract",
            type,
            start_date,
            end_date,
            account_id,
            business_id,
            business_uuid,
            total: payload?.total ?? (Array.isArray(payload?.data) ? payload.data.length : null),
            line_mode: lineMode,
            file_name: req.query.file_name || null,
            at: new Date().toISOString(),
          });
          return {
            ...session,
            timestamp: new Date().toISOString(),
            actions,
            meta: {
              ...session.meta,
              last_type: type,
              last_total: payload?.total ?? null,
              last_range: { start_date, end_date },
              last_file: req.query.file_name || session.meta?.last_file || null,
            },
          };
        });
      } catch (err) {
        console.warn("[History] Failed to log extraction:", err.message);
      }
    };
    const respond = async (payload) => {
      await logExtractEvent(payload);
      return res.json(payload);
    };

    const makeAccountUrl = (s) => `/accounting/account/${account_id}${s}`;
    const makeBizUuidUrl = (s) => `/accounting/businesses/${business_uuid}${s}`;
    const makeProjectUrl = () => `/projects/business/${business_id}/projects?per_page=100`;
    const makeTimeUrl = (s) => `/comments/business/${business_id}${s}`;
    const applyDateParams = (urlObj) => {
      if (!endpoint.allowDates) return;
      if (start_date) {
        urlObj.searchParams.set("start_date", start_date);
        urlObj.searchParams.set("search[start_date]", start_date);
      }
      if (end_date) {
        urlObj.searchParams.set("end_date", end_date);
        urlObj.searchParams.set("search[end_date]", end_date);
      }
    };

    // ---------------------------
    // Line Item Extractor
    // ---------------------------
    function extractLineItems(type, record) {
      if (!record) return [];

      const pickLinesArray = (candidate, depth = 0) => {
        if (!candidate || depth > 3) return [];

        if (Array.isArray(candidate)) return candidate;

        if (typeof candidate === "object") {
          const knownKeys = ["invoice_lines", "estimate_lines", "bill_lines", "lines", "line_items"];
          for (const key of knownKeys) {
            const val = candidate[key];
            if (Array.isArray(val)) return val;
          }

          for (const val of Object.values(candidate)) {
            const found = pickLinesArray(val, depth + 1);
            if (found.length) return found;
          }
        }

        return [];
      };

      switch (type) {
        case "invoices":
          return pickLinesArray(record.line_items || record.lines || record);

        case "estimates":
          return pickLinesArray(record.line_items || record.lines || record);

        case "bills":
          return pickLinesArray(record.line_items || record.lines || record);

        case "credit_notes":
          return pickLinesArray(record.line_items || record.lines || record);

        case "expenses":
          return [
            {
              name: record.notes || record.category_name,
              qty: 1,
              unit_cost: record.amount?.amount || 0,
              total: record.amount?.amount || 0,
            },
          ];

        case "payments":
          return [
            {
              name: "Payment",
              qty: 1,
              unit_cost: record.amount?.amount || 0,
              total: record.amount?.amount || 0,
            },
          ];

        default:
          return [];
      }
    }

    // ---------------------------
    // Endpoint config
    // ---------------------------
    const cfgMap = {
      profile: { url: `/auth/api/v1/users/me`, key: null, idType: "none", direct: true },

      business: {
        url: `/auth/api/v1/businesses/${business_id}`,
        key: null,
        idType: "business_id",
        direct: true,
      },

      invoices: {
        url: makeAccountUrl("/invoices/invoices"),
        key: "invoices",
        idType: "account",
        allowDates: true,
        include: ["lines", "taxes", "client"],
      },

      credit_notes: {
        url: makeAccountUrl("/credit_notes/credit_notes"),
        key: "credit_notes",
        idType: "account",
        allowDates: true,
        include: ["lines", "client"],
      },

      bill_payments: {
        url: makeAccountUrl("/bill_payments/bill_payments"),
        key: "bill_payments",
        idType: "account",
        allowDates: true,
        perPage: 15, // observed max per page
        include: ["bill"], // include bill to expose bill_number
      },

      billable_items: {
        url: makeAccountUrl("/billable_items/billable_items"),
        key: "billable_items",
        idType: "account",
        allowDates: false,
      },

      bill_vendors: {
        url: makeAccountUrl("/bill_vendors/bill_vendors"),
        key: "bill_vendors",
        idType: "account",
        allowDates: false,
        perPage: 15,
      },

      
      // ✅ FINAL FIX: correct Other Income path
      other_income: {
        // Official path: /other_incomes/other_incomes
        url: makeAccountUrl("/other_incomes/other_incomes"),
        key: "other_income",
        idType: "account",
        allowDates: true,
      },

      payments: {
        url: makeAccountUrl("/payments/payments"),
        key: "payments",
        idType: "account",
        allowDates: true,
        include: ["invoice", "client"],
      },

      expenses: {
        url: makeAccountUrl("/expenses/expenses"),
        key: "expenses",
        idType: "account",
        allowDates: true,
        include: ["category"],
      },

      bills: {
        url: makeAccountUrl("/bills/bills"),
        key: "bills",
        idType: "account",
        allowDates: true,
        perPage: 100, // API cap
        include: ["lines", "bill_lines", "vendor"],
      },

      estimates: {
        url: makeAccountUrl("/estimates/estimates"),
        key: "estimates",
        idType: "account",
        allowDates: true,
        include: ["lines"],
      },

      clients: {
        url: makeAccountUrl("/users/clients"),
        key: "clients",
        idType: "account",
        allowDates: false,
      },

      taxes: {
        url: makeAccountUrl("/taxes/taxes"),
        key: "taxes",
        idType: "account",
        allowDates: false,
      },

      projects: {
        url: makeProjectUrl(),
        key: "projects",
        idType: "business_id",
        allowDates: false,
      },

      time_entries: {
        url: makeTimeUrl("/time_entries"),
        key: "time_entries",
        idType: "business_id",
        allowDates: false,
      },

      journal_entries: {
        // Adjustment journal entries (business-level)
        url: makeBizUuidUrl("/journal_entries"),
        key: "manualJournalEntries", // per API docs
        idType: "business_uuid",
        allowDates: true, // we still accept dates; FB may ignore for adjustment list
        forcePagination: true,
        perPage: 15, // API caps at 15 for adjustment journals
        altUrls: [
          // Fallback: account-scoped "journal entries by account"
          () => makeAccountUrl("/journal_entries/journal_entries"),
        ],
      },

      ledger_accounts: {
        url: makeBizUuidUrl("/ledger_accounts/accounts"),
        key: "accounts",
        idType: "business_uuid",
        allowDates: false,
        singleCall: true,
      },

      chart_of_accounts: {
        url: makeBizUuidUrl(
          "/reports/chart_of_accounts?use_ledger_entries=true&state=active&sort=account_number_asc"
        ),
        key: "accounts",
        idType: "business_uuid",
        allowDates: true,
        singleCall: true,
      },
    };

    const endpoint = cfgMap[type];
    if (!endpoint) return res.status(400).json({ error: "Invalid type" });

    const altUrls = Array.isArray(endpoint.altUrls) ? [...endpoint.altUrls] : [];

    if (endpoint.idType === "account" && !account_id)
      return res.status(400).json({ error: "Missing account_id" });

    if (endpoint.idType === "business_id" && !business_id)
      return res.status(400).json({ error: "Missing business_id" });

    if (endpoint.idType === "business_uuid" && !business_uuid)
      return res.status(400).json({ error: "Missing business_uuid" });

    console.log(`📡 Fetching → ${type.toUpperCase()}`);

    // ---------------------------
    // Record formatter (trim noisy payloads)
    // ---------------------------
    function formatRecord(recordType, item) {
      const toAmount = (val) => {
        if (val && typeof val === "object") return Number(val.amount ?? val.total ?? val.value ?? 0);
        return Number(val ?? 0);
      };

      if (recordType === "bill_payments") {
        return {
          amount: item.amount?.amount ?? item.amount ?? null,
          billid: item.billid ?? item.bill_id ?? item.bill?.id ?? null,
          payment_id: item.id ?? null,
          paid_date: item.paid_date ?? item.date ?? null,
          payment_type: item.payment_type ?? item.payment_type_name ?? item.payment_method ?? null,
          bill_number: item.bill?.bill_number ?? item.bill_number ?? item.billid ?? null,
        };
      }

      if (recordType === "payments") {
        const lines = extractLineItems(recordType, item) || [];
        const clientObj = item.client || item.invoice?.client || {};
        const clientName =
          clientObj.organization ||
          (clientObj.fname && clientObj.lname && `${clientObj.fname} ${clientObj.lname}`.trim()) ||
          clientObj.fname ||
          clientObj.lname ||
          clientObj.name ||
          item.client_name ||
          null;
        return {
          amount: toAmount(item.amount),
          clientid: item.clientid ?? item.client_id ?? null,
          client_name: clientName,
          creditid: item.creditid ?? null,
          date: item.date ?? null,
          invoice_number: item.invoice?.invoice_number ?? item.invoice_number ?? null,
        };
      }

      if (recordType === "credit_notes") {
        const linesRaw =
          item.lines ||
          item.line_items ||
          (Array.isArray(item.items) ? item.items : []) ||
          [];
        const line_items = linesRaw
          .map((ln) => ({
            description: ln.description || ln.name || null,
            name: ln.name || null,
            qty: ln.qty ?? ln.quantity ?? ln.qty_delta ?? 1,
            taskno: ln.taskno ?? ln.task_no ?? null,
            taxAmount1: ln.taxAmount1 ?? ln.tax_amount1 ?? ln.tax1_amount ?? null,
            taxAmount2: ln.taxAmount2 ?? ln.tax_amount2 ?? ln.tax2_amount ?? null,
            taxName1: ln.taxName1 ?? ln.tax_name1 ?? null,
            taxName2: ln.taxName2 ?? ln.tax_name2 ?? null,
            unit_cost: toAmount(ln.unit_cost ?? ln.unitcost ?? ln.unit_cost_amount),
            amount: toAmount(ln.amount),
          }))
          .filter((l) => {
            const meaningful =
              (l.description && l.description.trim()) ||
              (l.name && l.name.trim()) ||
              (Number(l.amount) || Number(l.unit_cost));
            return meaningful;
          });
        const firstLine = line_items[0] || {};

        const amt = toAmount(item.amount);
        const currency = item.currency_code || item.amount?.code || null;
        const clientObj = item.client || item.clientinfo || item.client_info || {};
        const clientName =
          clientObj.organization ||
          (clientObj.fname && clientObj.lname && `${clientObj.fname} ${clientObj.lname}`.trim()) ||
          clientObj.fname ||
          clientObj.lname ||
          clientObj.name ||
          item.client_name ||
          null;

        const result = {
          accounting_systemid: item.accounting_systemid ?? null,
          amount: amt,
          city: item.city ?? null,
          clientid: item.clientid ?? item.client_id ?? null,
          code: item.code ?? currency ?? null,
          country: item.country ?? null,
          create_date: item.create_date ?? item.created_at ?? null,
          credit_number: item.credit_number ?? item.number ?? null,
          credit_type: item.credit_type ?? item.type ?? null,
          creditid: item.creditid ?? item.credit_id ?? item.id ?? null,
          currency_code: currency,
          current_organization: item.current_organization ?? item.organization ?? null,
          client_name: clientName,
          description: item.description ?? null,
          display_status: item.display_status ?? item.status ?? null,
          dispute_status: item.dispute_status ?? null,
          ext_archive: item.ext_archive ?? null,
          fname: item.fname ?? item.first_name ?? null,
          id: item.id ?? item.creditid ?? null,
          language: item.language ?? null,
          last_order_status: item.last_order_status ?? null,
          lines: Array.isArray(line_items) ? line_items.length : 0,
          lname: item.lname ?? item.last_name ?? null,
          notes: item.notes ?? item.note ?? null,
          organization: item.organization ?? item.current_organization ?? null,
          paid: toAmount(item.paid),
          payment_status: item.payment_status ?? null,
          payment_type: item.payment_type ?? null,
          province: item.province ?? null,
          sentid: item.sentid ?? null,
          status: item.status ?? item.display_status ?? null,
          street: item.street ?? null,
          street2: item.street2 ?? null,
          template: item.template ?? null,
          terms: item.terms ?? null,
          vat_name: item.vat_name ?? null,
          vat_number: item.vat_number ?? null,
          vis_state: item.vis_state ?? null,
          // Primary line fields flattened for CSV
          line_items: firstLine.description || firstLine.name || null,
          qty: firstLine.qty ?? null,
          unit: firstLine.unit_cost ?? null,
          amt: firstLine.amount ?? null,
          tax1: firstLine.taxAmount1 ?? null,
          tax2: firstLine.taxAmount2 ?? null,
        };

        // Keep full lines only when explicitly asked (line_mode=true)
        if (lineMode) {
          result.line_items_array = line_items;
          result.line_items_count = line_items.length || 0;
        }

        return result;
      }

      if (recordType === "other_income") {
        const lines = extractLineItems(recordType, item) || [];
        const firstLine = lines[0] || {};
        return {
          amount: toAmount(item.amount),
          currency_code: item.amount?.code ?? item.currency_code ?? null,
          category: item.category ?? item.type ?? item.income_type ?? null,
          created_at: item.created_at ?? item.updated ?? item.date ?? null,
          date: item.date ?? item.created_at ?? null,
          description: item.description ?? item.notes ?? firstLine.description ?? firstLine.name ?? null,
          reference: item.reference ?? item.bank_transaction_id ?? item.bank_entry_id ?? null,
          source: item.source ?? item.bank_account_name ?? item.bank_account ?? null,
          status: item.status ?? item.state ?? null,
          transaction_number:
            item.transaction_number ?? item.accounting_systemid ?? item.accounting_system_id ?? null,
          line_items: lines.length,
          line_items_raw: lines,
          line_description: firstLine.description ?? firstLine.name ?? "",
          line_qty: firstLine.qty ?? firstLine.quantity ?? 1,
          line_unit_cost: toAmount(firstLine.unit_cost),
          line_total: toAmount(firstLine.total ?? firstLine.amount),
        };
      }

      if (recordType === "bills") {
        const lines = extractLineItems(recordType, item) || [];
        const firstLine = lines[0] || {};

        const toDate = (val, withTime = false) => {
          if (!val) return null;
          const d = new Date(val);
          if (Number.isNaN(d.getTime())) return null;
          const datePart = `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
          if (!withTime) return datePart;
          const hh = String(d.getHours()).padStart(2, "0");
          const mm = String(d.getMinutes()).padStart(2, "0");
          return `${datePart} ${hh}:${mm}`;
        };

        return {
          amount: toAmount(item.amount),
          bill_number: item.bill_number ?? null,
          created_at: toDate(item.created_at || item.create_date, true),
          currency_code: item.currency_code ?? null,
          due_date: toDate(item.due_date),
          due_offset_days: Number(item.due_offset_days ?? 0),
          issue_date: toDate(item.issue_date),
          outstanding: toAmount(item.outstanding),
          overall_category:
            item.overall_category ?? firstLine?.category?.category ?? firstLine?.category ?? null,
          paid: toAmount(item.paid),
          status: item.status ?? null,
          tax_amount: toAmount(item.tax_amount),
          total_amount: toAmount(item.total_amount),
          line_items: lines.length,
          parent_id: item.id ?? item.billid ?? item.bill_id ?? null,
          line_description: firstLine.description || firstLine.name || "",
          quantity: firstLine.quantity || firstLine.qty || 1,
          category: firstLine.category?.category || firstLine.category || "",
          tax_amount1: firstLine.tax_amount1 ?? "",
          tax_amount2: firstLine.tax_amount2 ?? "",
          tax_name1: firstLine.tax_name1 ?? "",
          tax_name2: firstLine.tax_name2 ?? "",
          tax_percent1: firstLine.tax_percent1 ?? "",
          tax_percent2: firstLine.tax_percent2 ?? "",
          line_total_amount: toAmount(firstLine.total_amount ?? firstLine.total),
          unit_cost: toAmount(firstLine.unit_cost),
          line_date: toDate(firstLine.date || item.issue_date),
          description: item.description ?? item.notes ?? firstLine.description ?? firstLine.name ?? null,
          vendor:
            item.vendor?.vendor_name ??
            item.vendor?.name ??
            item.vendor?.display_name ??
            item.vendor?.organization ??
            item.vendor ??
            item.vendorname ??
            item.vendor_name ??
            item.vendor_display_name ??
            item.bill_vendor?.vendor_name ??
            item.bill_vendor?.name ??
            item.vendorid ??
            item.vendor_id ??
            null,
          vendorid:
            item.vendorid ??
            item.vendor_id ??
            item.vendor?.id ??
            item.vendor?.vendorid ??
            item.vendor?.vendor_id ??
            item.vendor?.accountid ??
            item.vendor?.account_id ??
            item.vendor?.userid ??
            item.vendor?.uuid ??
            item.bill_vendor?.vendor_id ??
            item.bill_vendor?.id ??
            null,
          // keep full lines for combined sheet + line extract
          line_items_array: lines,
        };
      }

      if (recordType === "bill_vendors") {
        return {
          city: item.city ?? null,
          country: item.country ?? null,
          currency_code: item.currency_code ?? null,
          phone: item.phone ?? item.phone_number ?? null,
          postal_code: item.postal_code ?? item.zip_code ?? null,
          primary_contact_email: item.primary_contact_email ?? item.email ?? null,
          primary_contact_first_name: item.primary_contact_first_name ?? item.fname ?? null,
          primary_contact_last_name: item.primary_contact_last_name ?? item.lname ?? null,
          province: item.province ?? null,
          street: item.street ?? null,
          street2: item.street2 ?? null,
          vendor_name: item.vendor_name ?? item.name ?? null,
          website: item.website ?? null,
        };
      }

      if (recordType === "journal_entries") {
        // Adjustment journal entries (manualJournalEntries)
        return {
          name: item.name ?? null,
          journalEntryNumber: item.journalEntryNumber ?? item.journal_entry_number ?? null,
          description: item.description ?? null,
          reverseDepth: item.reverseDepth ?? item.reverse_depth ?? null,
          details: item.details ?? [],
          line_items: item.line_items ?? [],
        };
      }

      if (recordType === "clients") {
        return {
          currency_code: item.currency_code ?? null,
          email: item.email ?? null,
          fname: item.fname ?? item.first_name ?? null,
          lname: item.lname ?? item.last_name ?? null,
          mob_phone: item.mob_phone ?? item.mobile_phone ?? item.phone_mobile ?? null,
          note: item.note ?? item.notes ?? null,
          organization: item.organization ?? item.company ?? null,
          p_city: item.p_city ?? item.primary_city ?? null,
          p_code: item.p_code ?? item.primary_postal_code ?? null,
          p_country: item.p_country ?? item.primary_country ?? null,
          p_province: item.p_province ?? item.primary_province ?? null,
          p_street: item.p_street ?? item.primary_street ?? null,
          p_street2: item.p_street2 ?? item.primary_street2 ?? null,
          s_city: item.s_city ?? item.secondary_city ?? null,
          s_code: item.s_code ?? item.secondary_postal_code ?? null,
          s_country: item.s_country ?? item.secondary_country ?? null,
          s_province: item.s_province ?? item.secondary_province ?? null,
          s_street: item.s_street ?? item.secondary_street ?? null,
          s_street2: item.s_street2 ?? item.secondary_street2 ?? null,
          username: item.username ?? null,
        };
      }

      if (recordType === "chart_of_accounts") {
        const subs =
          (Array.isArray(item.sub_accounts) && item.sub_accounts) ||
          (Array.isArray(item.subaccounts) && item.subaccounts) ||
          [];

        const currencyFromSubs = subs.find((s) => s.currency_code)?.currency_code || null;
        const parentRow = {
          account_name: item.account_name ?? item.name ?? null,
          account_number: item.account_number ?? null,
          account_type: item.account_type ?? item.type ?? null,
          account_sub_type: item.account_sub_type ?? item.sub_type ?? item.subtype ?? null,
          currency_code: item.currency_code ?? currencyFromSubs ?? null,
          sub_accounts: subs
            .map((s) => s.account_name || s.name || s.system_account_name)
            .filter(Boolean)
            .join(", ") || null,
          is_sub_account: false,
          parent_account_name: null,
          parent_account_number: null,
        };

        const subRows = subs.map((s) => ({
          account_name: s.account_name || s.name || s.system_account_name || null,
          account_number: s.account_number ?? s.number ?? s.accountnumber ?? null,
          account_type: s.account_type ?? s.type ?? parentRow.account_type ?? null,
          account_sub_type: s.account_sub_type ?? s.sub_type ?? s.subtype ?? parentRow.account_sub_type ?? null,
          currency_code: s.currency_code ?? parentRow.currency_code ?? null,
          sub_accounts: null,
          is_sub_account: true,
          parent_account_name: parentRow.account_name,
          parent_account_number: parentRow.account_number,
        }));

        // Return parent + each sub-account as its own row for "line-wise" export
        return [parentRow, ...subRows];
      }

      if (recordType === "expenses") {
  // 🔹 Category object se naam nikalna (Cost of Sales - Sub Contractor etc.)
  const categoryObj = item.category || {};
  const categoryName =
    item.category_name ||               // agar kabhi direct aaye
    categoryObj.category ||             // common FreshBooks field
    categoryObj.name ||
    categoryObj.fullname ||
    null;

  const lines = extractLineItems(recordType, item) || [];
  const line_items = lines.map((line) => ({
    line_date: item.date ?? line.date ?? null,
    line_description:
      line.name ??
      line.description ??
      item.notes ??
      categoryName ??
      null,
    qty: line.qty ?? line.quantity ?? 1,
    unit_cost: toAmount(line.unit_cost),
    line_total: toAmount(line.total ?? line.total_amount ?? line.amount),
    category: line.category ?? categoryName ?? null,
  }));

  return {
    account_name: item.account_name ?? null,
    amount: item.amount?.amount ?? item.amount ?? null,
    bank_name: item.bank_name ?? null,
    taxAmount1: item.tax_amount1 ?? null,
    taxAmount2: item.tax_amount2 ?? null,
    taxName1: item.tax_name1 ?? null,
    taxName2: item.tax_name2 ?? null,
    taxPercent1: item.tax_percent1 ?? null,
    taxPercent2: item.tax_percent2 ?? null,
    vendor: item.vendor ?? null,
    notes: item.notes ?? null,
    line_items,
    line_date: item.date ?? null,
    qty: line_items[0]?.qty ?? 1,
    unit_cost: line_items[0]?.unit_cost ?? null,
    line_total:
      line_items.reduce((sum, l) => sum + toAmount(l.line_total), 0) || null,
    category: categoryName, // 👈 yahan final category aa jayegi
    categoryid:
      item.categoryid ??
      item.category_id ??
      categoryObj.categoryid ??
      categoryObj.id ??
      null,
    vendorid:
      item.vendorid ??
      item.vendor_id ??
      item.vendor?.id ??
      item.vendor?.vendorid ??
      item.vendor?.vendor_id ??
      null,
    date: item.date ?? null,
  };
}

      return {
        ...item,
        line_items: extractLineItems(recordType, item),
      };
    }

    // ---------------------------
    // DIRECT CALL
    // ---------------------------
    if (endpoint.direct) {
      const url = `${FRESHBOOKS_BASE}${endpoint.url}`;

      try {
        const r = await axios.get(url, {
          headers: { Authorization: `Bearer ${validToken}` },
          timeout: 120000,
        });

        return respond({
          success: true,
          total: 1,
          data: [
            {
              ...r.data,
              line_items: extractLineItems(type, r.data),
            },
          ],
        });
      } catch (err) {
        // Fallback for business details when scope / ID is off
        if (type === "business") {
          try {
            const whoami = await fetchWhoami(validToken);
            const memberships = whoami?.business_memberships || [];
            const match =
              memberships.find((m) => String(m.business?.id) === String(business_id)) ||
              memberships[0];
            if (match?.business) {
              return respond({
                success: true,
                total: 1,
                data: [match.business],
                fallback: "whoami",
              });
            }
          } catch {
            // ignore, use original error
          }
        }
        throw err;
      }
    }

    // ---------------------------
    // SINGLE CALL (no pagination)
    // ---------------------------
    if (endpoint.singleCall) {
      while (true) {
        const urlObj = new URL(`${FRESHBOOKS_BASE}${endpoint.url}`);

        applyDateParams(urlObj);

        if (endpoint.include?.length) {
          const includes = Array.isArray(endpoint.include)
            ? endpoint.include
            : [endpoint.include];
          includes.forEach((inc) => {
            urlObj.searchParams.append("include", inc);
            urlObj.searchParams.append("include[]", inc);
          });
        }

        try {
          const r = await axios.get(urlObj.toString(), {
            headers: { Authorization: `Bearer ${validToken}` },
            timeout: 120000,
          });

          const body = r.data?.response?.result || r.data?.response || r.data || {};
          const arr =
            body[endpoint.key] ||
            body.result?.[endpoint.key] ||
            Object.values(body.result || {}).find((v) => Array.isArray(v)) ||
            Object.values(body).find((v) => Array.isArray(v)) ||
            [];

          const enriched = arr.flatMap((item) => {
            const rec = formatRecord(type, item);
            if (rec === null || rec === undefined) return [];
            return Array.isArray(rec) ? rec : [rec];
          });

          return respond({
            success: true,
            total: enriched.length,
            data: enriched,
            raw: INCLUDE_RAW ? arr : undefined,
          });
        } catch (err) {
          const status = err.response?.status;
          if (status === 404 && altUrls.length) {
            endpoint.url = altUrls.shift();
            continue;
          }
          throw err;
        }
      }
    }

    // ---------------------------
    // JOURNAL → FORCE PAGINATION (business + account fallback)
    // ---------------------------
    if (endpoint.forcePagination) {
      const per_page = Math.min(Number(endpoint.perPage) || 150, 150);
      const maxPages = MAX_PAGE_OVERRIDE ?? 500;
      let page = 1;
      const allData = [];
      let lastFirstId = null;
      const rawCollector = INCLUDE_RAW ? [] : null;

      while (page <= maxPages) {
        const rawBase =
          typeof endpoint.url === "function" ? endpoint.url() : `${endpoint.url}`;
        const baseUrl = rawBase.startsWith("http")
          ? rawBase
          : `${FRESHBOOKS_BASE}${rawBase}`;

        const urlObj = new URL(baseUrl);

        const isAdjJournal =
          baseUrl.includes("/businesses/") &&
          baseUrl.includes("/journal_entries") &&
          !baseUrl.includes("/reports/");
        const isAccountJournal =
          baseUrl.includes("/account/") &&
          baseUrl.includes("/journal_entries/journal_entries");

        if (isAdjJournal) {
          // Adjustment journals ? page_number / page_size
          urlObj.searchParams.set("page_number", String(page));
          urlObj.searchParams.set("page_size", String(per_page));
        } else {
          // Normal account-scoped journal entries by account ? page / per_page
          urlObj.searchParams.set("page", String(page));
          urlObj.searchParams.set("per_page", String(per_page));

          if (isAccountJournal) {
            // Required for ledger-style journal entries
            urlObj.searchParams.set("use_ledger_entries", "true");
            urlObj.searchParams.set("include_children", "true");
          }
        }

        applyDateParams(urlObj);
const url = urlObj.toString();
        console.log(`[Pagination] ${type} page=${page} url=${url}`);

        try {
          const r = await axios.get(url, {
            headers: {
              Authorization: `Bearer ${validToken}`,
              "x-api-version": "2023-09-25",
            },
            timeout: 120000,
          });

          const body = r.data?.response?.result || r.data?.response || r.data || {};
          const arr =
            body[endpoint.key] ||
            body.result?.[endpoint.key] ||
            Object.values(body.result || {}).find((v) => Array.isArray(v)) ||
            Object.values(body).find((v) => Array.isArray(v)) ||
            [];

          if (!Array.isArray(arr) || arr.length === 0) break;

          // Stop if server ignores page and repeats first row
          const firstId = arr[0]?.id || arr[0]?.uuid || null;
          if (page > 1 && firstId && firstId === lastFirstId) break;
          lastFirstId = firstId;

          allData.push(...arr);
          if (rawCollector) rawCollector.push(arr);

          const metaCandidate = body?.meta || body?.pagination || body;
          const totalPages = Number(
            metaCandidate?.pages ?? metaCandidate?.total_pages
          );

          // For account-based journal entries, docs say pages is accurate.
          if (
            type !== "journal_entries" &&
            Number.isFinite(totalPages) &&
            totalPages > 0 &&
            page >= totalPages
          )
            break;

          page++;
        } catch (err) {
          const status = err.response?.status;
          if ((status === 404 || status === 400) && altUrls.length) {
            endpoint.url = altUrls.shift();
            lastFirstId = null;
            console.log(
              `[Pagination] switching to alt URL due to status ${status}`
            );
            continue;
          }
          throw err;
        }
      }

      const truncated = page > maxPages;
      if (truncated) {
        console.warn(
          `Pagination stopped at ${maxPages} pages for ${type}; endpoint may be ignoring page params.`
        );
      }

      const enriched = allData.flatMap((item) => {
        const rec = formatRecord(type, item);
        if (rec === null || rec === undefined) return [];
        return Array.isArray(rec) ? rec : [rec];
      });

      return respond({
        success: true,
        total: enriched.length,
        data: enriched,
        truncated,
        raw: rawCollector ? rawCollector.flat() : undefined,
      });
    }

    // ---------------------------
    // MULTI-PAGE FETCH (generic)
    // ---------------------------
    const endpointPerPage = Number(endpoint.perPage);
    const per_page = Number.isFinite(endpointPerPage) ? endpointPerPage : 150;
    const maxPages = MAX_PAGE_OVERRIDE ?? 500;
    let page = 1;
    const allData = [];
    let lastFirstId = null;
    const rawCollector = INCLUDE_RAW ? [] : null;

    while (page <= maxPages) {
      const baseUrl =
        typeof endpoint.url === "function"
          ? endpoint.url()
          : `${FRESHBOOKS_BASE}${endpoint.url}`;
      const urlObj = new URL(baseUrl);

      urlObj.searchParams.set("page", String(page));
      urlObj.searchParams.set("per_page", String(per_page));

      applyDateParams(urlObj);

      if (endpoint.include?.length) {
        const includes = Array.isArray(endpoint.include)
          ? endpoint.include
          : [endpoint.include];
        includes.forEach((inc) => {
          urlObj.searchParams.append("include", inc);
          urlObj.searchParams.append("include[]", inc);
        });
      }

    try {
      const r = await axios.get(urlObj.toString(), {
        headers: { Authorization: `Bearer ${validToken}` },
        timeout: 120000,
      });

        const body = r.data?.response?.result || r.data?.response || r.data || {};
      const arr =
        body[endpoint.key] ||
        body.result?.[endpoint.key] ||
        Object.values(body.result || {}).find((v) => Array.isArray(v)) ||
        Object.values(body).find((v) => Array.isArray(v)) ||
        [];

      if (arr.length === 0) break;

        const firstId = arr[0]?.id || arr[0]?.uuid || null;
        if (page > 1 && firstId && firstId === lastFirstId) break;
        lastFirstId = firstId;

      allData.push(...arr);
      if (rawCollector) rawCollector.push(arr);

        const metaCandidate = body?.meta || body?.pagination || body;
        const totalPages = Number(
          metaCandidate?.pages ?? metaCandidate?.total_pages
        );

        if (
          Number.isFinite(totalPages) &&
          totalPages > 0 &&
          page >= totalPages
        )
          break;

        page++;
      } catch (err) {
        const status = err.response?.status;
        if (status === 404 && altUrls.length) {
          endpoint.url = altUrls.shift();
          continue;
        }
        throw err;
      }
    }

    const hitPageLimit = page > maxPages;
    if (hitPageLimit) {
      console.warn(
        `Pagination stopped at ${maxPages} pages for ${type}; FreshBooks may be ignoring page/per_page params.`
      );
    }

    const enriched = allData.flatMap((item) => {
      const rec = formatRecord(type, item);
      if (rec === null || rec === undefined) return [];
      return Array.isArray(rec) ? rec : [rec];
    });

    let headers = null;
    if (type === "payments") {
      headers = ["amount", "clientid", "creditid", "date", "invoice_number"];
    }

    return respond({
      success: true,
      total: enriched.length,
      data: enriched,
      headers,
      truncated: hitPageLimit,
      raw: rawCollector ? rawCollector.flat() : undefined,
    });
  } catch (error) {
    const status = error.response?.status || 500;
    const payload = error.response?.data || error.message || error;
    const url = error.config?.url || "";

    // Hint when scope missing (e.g., time_entries)
    if (status === 403 && typeof payload === "object") {
      const msg = payload?.error?.message || payload?.message || "";
      if (typeof msg === "string" && msg.toLowerCase().includes("insufficient_scope")) {
        payload.hint =
          "Token is missing required scope. Re-authorize with user:time_entries:read (and user:journal_entries:read for journals) in SCOPE.";
      }
    }

    console.log(`❓ UNHANDLED EXTRACT ERROR [${status}] ${url}`);
    console.log(pretty(payload));

    res.status(status).json({ error: payload, status, url });
  }
});




// ---------------------------

// Test endpoints (diagnostic) — exercises many endpoints and returns simple statuses

// ---------------------------

app.get("/api/test-endpoints", async (req, res) => {

  try {

    req.setTimeout(0);



    let { business_id, account_id, business_uuid } = req.query;

    const envAccount = (process.env.ACCOUNT_ID || "").toString();

    const envBusinessId = (process.env.BUSINESS_ID || "").toString();

    const envBusinessUuid = (process.env.BUSINESS_UUID || "").toString();



    account_id = account_id || envAccount;

    business_id = business_id || envBusinessId;

    business_uuid = business_uuid || envBusinessUuid;



    if (!business_id && !account_id && !business_uuid)

      return res.status(400).json({ error: "Missing at least one of business_id, account_id, or business_uuid" });



    const validToken = accessToken || (await getFreshTokenSafely());



    const makeAccountUrl = (pathSuffix) => `${FRESHBOOKS_BASE}/accounting/account/${account_id}${pathSuffix}`;

    const makeBusinessUuidUrl = (pathSuffix) => `${FRESHBOOKS_BASE}/accounting/businesses/${business_uuid}${pathSuffix}`;

    const makeProjectsUrl = (pathSuffix) => `${FRESHBOOKS_BASE}/projects/business/${business_id}${pathSuffix}`;

    const makeTimeCommentsUrl = (pathSuffix) => `${FRESHBOOKS_BASE}/comments/business/${business_id}${pathSuffix}`;



    const cfgMap = {

      profile: { url: `${FRESHBOOKS_BASE}/auth/api/v1/users/me`, key: null, idType: "none" },

      business: { url: `${FRESHBOOKS_BASE}/auth/api/v1/users/me`, key: null, idType: "none" },



      invoices: { url: `${makeAccountUrl("/invoices/invoices")}`, key: "invoices", idType: "account" },

      payments: { url: `${makeAccountUrl("/payments/payments")}`, key: "payments", idType: "account" },

      expenses: { url: `${makeAccountUrl("/expenses/expenses")}`, key: "expenses", idType: "account" },

      bills: { url: `${makeAccountUrl("/bills/bills")}`, key: "bills", idType: "account" },

      bill_payments: { url: `${makeAccountUrl("/bill_payments/bill_payments")}`, key: "bill_payments", idType: "account" },

      billable_items: { url: `${makeAccountUrl("/billable_items/billable_items")}`, key: "billable_items", idType: "account" },

      credit_notes: { url: `${makeAccountUrl("/credit_notes/credit_notes")}`, key: "credit_notes", idType: "account" },

      taxes: { url: `${makeAccountUrl("/taxes/taxes")}`, key: "taxes", idType: "account" },

      estimates: { url: `${makeAccountUrl("/estimates/estimates")}`, key: "estimates", idType: "account" },

      projects: { url: `${makeProjectsUrl("/projects")}`, key: "projects", idType: "business_id" },

      clients: { url: `${makeAccountUrl("/users/clients")}`, key: "clients", idType: "account" },

      bill_vendors: {
        url: `${makeAccountUrl("/bill_vendors/bill_vendors")}`,
        key: "bill_vendors",
        idType: "account",
        perPage: 15,
      },

            other_income: {
        // ✅ same correct path as extract
        url: `${makeAccountUrl("/other_incomes/other_incomes")}`,
        key: "other_income",
        idType: "account",
      },


      journal_entries: {
        url: `${makeBusinessUuidUrl("/journal_entries")}`,
        key: "journal_entries",
        idType: "business_uuid",
        allowDates: true,
      },

      accounts: { url: `${makeAccountUrl("/accounts/accounts")}`, key: "accounts", idType: "account" },

      retainers: { url: `${makeAccountUrl("/retainers/retainers")}`, key: "retainers", idType: "account" },

      time_entries: { url: `${makeTimeCommentsUrl("/time_entries")}`, key: "time_entries", idType: "business_id" },

      teams: { url: `${makeAccountUrl("/teams/teams")}`, key: "teams", idType: "account" },

      chart_of_accounts: { url: `${makeBusinessUuidUrl("/reports/chart_of_accounts")}`, key: "accounts", idType: "business_uuid" },

      ledger_accounts: { url: `${makeBusinessUuidUrl("/ledger_accounts/accounts")}`, key: "accounts", idType: "business_uuid" },

    };



    const results = {};

    const entries = Object.entries(cfgMap);

    let done = 0;



    for (const [name, ep] of entries) {

      try {

        if (ep.idType === "account" && !account_id) {

          results[name] = "❌ Missing account_id";

          done++;

          continue;

        }

        if (ep.idType === "business_id" && !business_id) {

          results[name] = "❌ Missing business_id";

          done++;

          continue;

        }

        if (ep.idType === "business_uuid" && !business_uuid) {

          results[name] = "❌ Missing business_uuid";

          done++;

          continue;

        }



        const url = `${ep.url}?page=1&per_page=1`;



        const headers = {

          Authorization: `Bearer ${validToken}`

        };



        // 🟢 Only journal_entries need API version

        if (name === "journal_entries") {

          headers["x-api-version"] = "2023-09-25";

        }



        const response = await axios.get(url, {

          headers,

          timeout: 120000,

        });



        if (!ep.key) {

          results[name] = "✅ OK (Profile endpoint)";

        } else {

          const body = response.data || {};

          const result = body?.response?.result || body?.response || body;

          let records = [];

          if (result && Array.isArray(result[ep.key])) records = result[ep.key];

          else {

            const first = Object.values(result)[0];

            if (Array.isArray(first)) records = first;

          }

          results[name] = records.length ? `✅ ${records.length} record(s)` : "⚠️ No data (0 records)";

        }

      } catch (err) {

        const status = err.response?.status;

        const rawMsg =
          err.response?.data?.error?.message ||
          err.response?.data?.message ||
          err.response?.data ||
          err.message;

        const msg =
          typeof rawMsg === "string"
            ? rawMsg
            : (() => {
                try {
                  return JSON.stringify(rawMsg);
                } catch {
                  return String(rawMsg);
                }
              })();

        if (status === 404) results[name] = "\u26d4 Not supported (404)";
        else if (status === 403) results[name] = "\u274c Forbidden (Scope Missing)";
        else if (status === 405) results[name] = "\u274c Method Not Allowed (405)";
        else results[name] = `\u274c Error ${status || ""}: ${msg || "Unknown error"}`;


      }



      done++;

    }



    res.json({

      success: true,

      tested: done,

      total_endpoints: entries.length,

      results,

    });

  } catch (error) {

    res.status(500).json({ error: error.response?.data || error.message });

  }

});





// ---------------------------

// Custom journal generator (keeps your logic; robust parsing)

// ---------------------------

app.get("/api/generate-journal", async (req, res) => {

  try {

    let { start_date, end_date, account_id, business_id } = req.query;

    if (!business_id || !account_id)

      return res.status(400).json({ error: "Missing business_id or account_id" });



    const validToken = await getFreshTokenSafely();

    const baseURL = process.env.FRESHBOOKS_API || "https://api.freshbooks.com";



    start_date = normalizeDateParam(start_date);

    end_date = normalizeDateParam(end_date);



    const fetchData = async (type) => {

      try {

        const url = `${baseURL}/accounting/account/${account_id}/${type}/${type}?search[start_date]=${start_date}&search[end_date]=${end_date}&per_page=100`;

        const resp = await axios.get(url, {

          headers: { Authorization: `Bearer ${validToken}` },

          timeout: 25000,

        });



        const result = resp.data?.response?.result || {};

        const arr = result[type] ?? Object.values(result)[0] ?? [];

        const records = Array.isArray(arr) ? arr : [];

        console.log(`✅ ${type}: ${records.length} record(s)`);

        return records;

      } catch (err) {

        console.log(`⚠️ ${type} fetch failed: ${err.response?.status || err.message}`);

        return [];

      }

    };



    const [invoices = [], expenses = [], payments = [], bills = []] = await Promise.all([

      fetchData("invoices"),

      fetchData("expenses"),

      fetchData("payments"),

      fetchData("bills"),

    ]);



    const journal = [];



    // INVOICES → SALES ENTRIES

    if (Array.isArray(invoices)) {

      for (const inv of invoices) {

        journal.push({

          date: inv.create_date || inv.updated || inv.date || "",

          description: `Invoice #${inv.invoice_number || inv.invoiceid || "N/A"}`,

          debit_account: "Accounts Receivable",

          credit_account: "Sales Income",

          amount: Number(inv.amount?.amount ?? inv.amount ?? 0),

        });

      }

    }



    // EXPENSES → EXPENSE ENTRIES

    if (Array.isArray(expenses)) {

      for (const exp of expenses) {

        journal.push({

          date: exp.date || "",

          description: exp.notes || `Expense: ${exp.category_name || "General"}`,

          debit_account: exp.category_name || "Expense",

          credit_account: "Cash/Bank",

          amount: Number(exp.amount?.amount ?? exp.amount ?? 0),

        });

      }

    }



    // PAYMENTS → CASH INFLOW ENTRIES

    if (Array.isArray(payments)) {

      for (const pay of payments) {

        journal.push({

          date: pay.date || "",

          description: `Payment from ${pay.client?.organization || "Customer"}`,

          debit_account: "Cash/Bank",

          credit_account: "Accounts Receivable",

          amount: Number(pay.amount?.amount ?? pay.amount ?? 0),

        });

      }

    }



    // BILLS → LIABILITY ENTRIES

    if (Array.isArray(bills)) {

      for (const b of bills) {

        journal.push({

          date: b.create_date || "",

          description: `Bill #${b.bill_number || b.id || "N/A"}`,

          debit_account: b.expense_category || "Purchase Expense",

          credit_account: "Accounts Payable",

          amount: Number(b.amount?.amount ?? b.amount ?? 0),

        });

      }

    }



    console.log(

      `📊 Journal Summary → invoices:${invoices.length}, expenses:${expenses.length}, payments:${payments.length}, bills:${bills.length}`

    );



    res.json({

      success: true,

      total_entries: journal.length,

      business_id,

      account_id,

      data: journal,

    });

  } catch (error) {

    console.error("❌ /api/generate-journal failed:", pretty(error.response?.data || error.message));

    res.status(500).json({ error: error.response?.data || error.message });

  }

});



// ---------------------------

// Activity history (login + extract)

// ---------------------------

app.get("/api/history", (req, res) => {
  try {
    const limit = Number(req.query?.limit);
    const history = loadHistory();
    const unique = [];
    const seen = new Set();
    for (const item of history) {
      const key = item.event === "session" ? userKey(item.user) : `${item.event}:${item.id}`;
      if (seen.has(key)) continue;
      seen.add(key);
      unique.push(item);
    }
    const trimmed =
      Number.isFinite(limit) && limit > 0 ? unique.slice(0, limit) : unique;
    res.json({ success: true, history: trimmed });
  } catch (err) {
    console.error("[History] Failed to load history:", err.message);
    res.status(500).json({ error: "Unable to load history" });
  }
});



// ---------------------------

// Reset session / update tokens

// ---------------------------

app.post("/api/reset-session", async (req, res) => {

  try {

    const { business_id } = req.body || {};

    accessToken = "";

    refreshToken = "";

    tokenExpiry = 0;

    if (business_id) {

      safeReplaceEnv("BUSINESS_ID", "");

      safeReplaceEnv("ACCOUNT_ID", "");

      safeReplaceEnv("BUSINESS_UUID", "");

    }

    console.log("🧹 Backend session reset successful.");

    res.json({ success: true, message: "Session reset successful." });

  } catch (err) {

    console.error("❌ Reset session failed:", err.message);

    res.status(500).json({ error: "Failed to reset session" });

  }

});



app.post("/api/update-tokens", async (req, res) => {

  try {

    const { access_token, refresh_token, account_id, business_id, business_uuid, file_name, business_name } = req.body;

    if (!access_token || !refresh_token)

      return res.status(400).json({ error: "Access & Refresh tokens required" });



    accessToken = access_token;

    refreshToken = refresh_token;



    if (account_id) {

      safeReplaceEnv("ACCOUNT_ID", account_id);

      process.env.ACCOUNT_ID = account_id;

    }

    if (business_id) {
      safeReplaceEnv("BUSINESS_ID", business_id);
      process.env.BUSINESS_ID = business_id;
    }

    if (business_uuid) {

      safeReplaceEnv("BUSINESS_UUID", business_uuid);

      process.env.BUSINESS_UUID = business_uuid;

    }
    safeReplaceEnv("ACCESS_TOKEN", access_token);

    safeReplaceEnv("REFRESH_TOKEN", refresh_token);
    // Update session meta with business/file info
    try {
      const manualName = sanitizeUserName(req.headers["x-user-name"]);
      const userProfile = await getUserSummary(access_token);
      const user = manualName ? { ...userProfile, name: manualName } : userProfile;
      upsertSessionHistory(user, (session) => ({
        ...session,
        meta: {
          ...session.meta,
          last_business: business_name || session.meta?.last_business || null,
          last_file: file_name || session.meta?.last_file || null,
          account_id,
          business_id,
          business_uuid,
        },
      }));
    } catch (err) {
      console.warn("[History] Unable to update session meta on business update:", err.message);
    }
    console.log("✅ Tokens updated successfully (Memory + File Synced)");

    res.json({ success: true, message: "Tokens updated successfully" });

  } catch (err) {
    console.error("❌ Failed to update tokens:", err.message);
    res.status(500).json({ error: "Failed to update tokens" });
  }

});
// ---------------------------

// Start server

// ---------------------------

const PORT = process.env.PORT || 5050;

app.listen(PORT, "0.0.0.0", () => console.log(`🚀 Backend running on http://localhost:${PORT}`));
  
