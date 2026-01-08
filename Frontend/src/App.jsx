import { useEffect, useMemo, useState } from "react";
import axios from "axios";
import "./App.css";
// Bundle logo via Vite so the correct hashed path is used in build + respects base
import mmclogo from "./assets/mmc-logo.png";

// Skip ngrok browser warning for API calls
axios.defaults.headers.common["ngrok-skip-browser-warning"] = "true";

/* ------------------------------ helpers ------------------------------ */
const ICON = {
  ok: "✅",
  empty: "⚠️",
  unsupported: "⛔",
  error: "❌",
  running: "⏳",
};

function parseResult(str = "") {
  const s = String(str);
  if (s.startsWith("✅")) return { kind: "ok", label: s, records: extractCount(s) };
  if (s.startsWith("⚠️")) return { kind: "empty", label: s, records: 0 };
  if (s.includes("Not supported") || s.includes("(404)"))
    return { kind: "unsupported", label: s, records: 0 };
  if (s.includes("Error") || s.includes("Forbidden") || s.includes("(405)"))
    return { kind: "error", label: s, records: 0 };
  return { kind: "empty", label: s || "—", records: 0 };
}

function extractCount(s) {
  const m = s.match(/(\d[\d,]*)\s*record/);
  return m ? Number(m[1].replace(/,/g, "")) : 0;
}

const GROUPS = {
  "Accounting & Billing": [
    "invoices",
    "payments",
    "expenses",
    "bills",
    "bill_payments",
    "credit_notes",
    "taxes",
    "billable_items",
    "other_income",
    "journal_entries",
    "ledger_accounts",
    "chart_of_accounts",
  ],
  "Sales & Documents": ["estimates", "online_payments", "uploads"],
  "Contacts": ["clients", "vendors", "bill_vendors"],
  "Projects & Time": ["projects", "time_entries"],
  "Meta": ["profile", "business", "account"],
};

const DEFAULT_ENDPOINTS = [
  "profile",
  "business",
  "invoices",
  "credit_notes",
  "bill_payments",
  "billable_items",
  "bill_vendors",
  "other_income",
  "payments",
  "expenses",
  "bills",
  "estimates",
  "clients",
  "taxes",
  "projects",
  "time_entries",
  "journal_entries",
  "ledger_accounts",
  "chart_of_accounts",
];
const DEFAULT_ENDPOINT_STATUS = Object.fromEntries(
  DEFAULT_ENDPOINTS.map((key) => [key, `${ICON.ok} Ready (not tested)`])
);

function toTitle(key) {
  if (key === "payments") return "invoice payments";
  return key.replaceAll("_", " ");
}

// Numeric helper: safely convert various shapes (amount object, raw number/string) to a number, default 0 on NaN
const toNumberOrZero = (value) => {
  if (value && typeof value === "object") {
    const n = Number(value.amount ?? value.total ?? value.value ?? value);
    return Number.isNaN(n) ? 0 : n;
  }
  const n = Number(value ?? 0);
  return Number.isNaN(n) ? 0 : n;
};
const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;
// Normalize date inputs to YYYY-MM-DD without timezone shifts
const toDateParam = (value) => {
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
const toDateObj = (value) => {
  const iso = toDateParam(value);
  if (!iso) return null;
  const d = new Date(`${iso}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? null : d;
};
const DATE_KEYS = [
  "create_date",
  "issue_date",
  "date",
  "created_at",
  "due_date",
  "payment_date",
  "entry_date",
  "transaction_date",
  "paid_date",
  "line_date",
  "updated",
  "updated_at",
];
const pickDateValue = (obj, fallbackObj) => {
  for (const key of DATE_KEYS) {
    if (obj && obj[key]) return obj[key];
  }
  if (fallbackObj) {
    for (const key of DATE_KEYS) {
      if (fallbackObj[key]) return fallbackObj[key];
    }
  }
  return null;
};
const isWithinRange = (dateStr, start, end) => {
  const d = toDateObj(dateStr);
  const s = toDateObj(start);
  const e = toDateObj(end);
  if (!d) return false;
  if (s && d < s) return false;
  if (e && d > e) return false;
  return true;
};
const filterByDateRange = (arr, start, end) =>
  Array.isArray(arr) ? arr.filter((item) => isWithinRange(pickDateValue(item), start, end)) : [];
const updateCount = (updater, label, value) => {
  const safeLabel = label || "Unknown";
  updater((prev) => ({ ...prev, [safeLabel]: Number(value) || 0 }));
};
const formatDateTime = (value) => {
  const d = new Date(value);
  if (!Number.isNaN(d.getTime())) return d.toLocaleString();
  return value || "";
};
const toFileName = (type, businessId) => {
  const safeType = (type || "export").replace(/[^a-z0-9_-]+/gi, "_");
  return `${safeType}_${businessId || "export"}.csv`;
};

/* ---------------------------------------------------
   LINE-ITEM EXTRACTOR (UNIVERSAL FOR ALL TYPES)
--------------------------------------------------- */
function extractLineItems(type, items) {
  const lines = [];
  const parents = Array.isArray(items) ? items : [];

  const resolveLineArray = (parent) => {
    if (!parent) return [];
    if (Array.isArray(parent.line_items_array)) return parent.line_items_array;
    if (Array.isArray(parent.line_items)) return parent.line_items;
    if (Array.isArray(parent.line_items_raw)) return parent.line_items_raw;
    if (Array.isArray(parent.lines)) return parent.lines;
    if (Array.isArray(parent.bill_lines)) return parent.bill_lines;
    return [];
  };

  const toAmount = (value) => {
    if (value && typeof value === "object") return Number(value.amount ?? value.total ?? 0);
    return Number(value ?? 0);
  };

  parents.forEach((parent) => {
    const parentLines = resolveLineArray(parent);
    if (!parentLines.length) return;

    /* INVOICES ------------------------------ */
    if (type === "invoices") {
      parentLines.forEach((line) => {
        lines.push({
          parent_id: parent.invoiceid,
          parent_number: parent.invoice_number,
          date: parent.create_date,
          description: line.name || line.description,
          qty: line.qty || 1,
          unit_cost: toAmount(line.unit_cost),
          total: toAmount(line.amount),
        });
      });
    }

    /* CREDIT NOTES ------------------------------ */
    if (type === "credit_notes") {
      parentLines.forEach((line) => {
        lines.push({
          parent_id: parent.creditid || parent.id,
          parent_number: parent.credit_number || parent.number,
          date: parent.create_date,
          description: line.description || line.name,
          qty: line.qty || 1,
          unit_cost: toAmount(line.unit_cost),
          total: toAmount(line.amount),
          tax1: line.taxAmount1 ?? line.tax_amount1 ?? null,
          tax2: line.taxAmount2 ?? line.tax_amount2 ?? null,
          client_name:
            parent.client_name ||
            parent.client?.organization ||
            (parent.client?.fname && parent.client?.lname
              ? `${parent.client.fname} ${parent.client.lname}`.trim()
              : parent.client?.fname || parent.client?.lname || parent.client?.name || ""),
        });
      });
    }

    /* ESTIMATES ------------------------------ */
    if (type === "estimates") {
      parentLines.forEach((line) => {
        lines.push({
          parent_id: parent.estimateid,
          parent_number: parent.estimate_number,
          date: parent.create_date,
          description: line.name || line.description,
          qty: line.qty || 1,
          unit_cost: toAmount(line.unit_cost),
          total: toAmount(line.amount),
          tax1: line.taxAmount1 ?? line.tax_amount1 ?? null,
          tax2: line.taxAmount2 ?? line.tax_amount2 ?? null,
          taxName1: line.taxName1 ?? line.tax_name1 ?? null,
          taxName2: line.taxName2 ?? line.tax_name2 ?? null,
          name: line.name || "",
        });
      });
    }

    /* BILLS ------------------------------ */
    if (type === "bills") {
      parentLines.forEach((line) => {
        lines.push({
          parent_id: parent.id,
          parent_number: parent.bill_number,
          date: parent.issue_date,
          bill_total_amount: toAmount(parent.total_amount ?? parent.amount),
          due_date: parent.due_date,
          due_offset_days: parent.due_offset_days,
          description: line.description || line.name || parent.description || parent.notes || "",
          qty: line.quantity || line.qty || 1,
          unit_cost: toAmount(line.unit_cost),
          total: toAmount(line.total_amount),
          category: line.category?.category || line.category || parent.overall_category || "",
          "Tax Amount 1": line.tax_amount1 ?? "",
          "Tax Amount 2": line.tax_amount2 ?? "",
          "Tax Name 1": line.tax_name1 ?? "",
          "Tax Name 2": line.tax_name2 ?? "",
          "Tax percentage 1": line.tax_percent1 ?? "",
          "Tax percentage 2": line.tax_percent2 ?? "",
          vendorid:
            parent.vendorid ||
            parent.vendor_id ||
            parent.vendor?.id ||
            parent.vendor?.vendorid ||
            parent.vendor?.vendor_id ||
            parent.vendor?.accountid ||
            parent.vendor?.account_id ||
            parent.vendor?.userid ||
            parent.vendor?.uuid ||
            "",
          Vendor:
            parent.vendor ||
            parent.vendor_name ||
            parent.vendor_display_name ||
            parent.bill_vendor?.vendor_name ||
            parent.bill_vendor?.name ||
            (parent.vendor && parent.vendor.vendor_name) ||
            "",
        });
      });
    }

    /* EXPENSES ------------------------------ */
    if (type === "expenses") {
      parentLines.forEach((line) => {
        lines.push({
          parent_id: parent.expenseid || parent.id,
          parent_number: parent.reference || parent.expenseid || parent.id,
          date: parent.date,
          description: line.name || parent.notes || parent.category_name,
          qty: line.qty || line.quantity || 1,
          unit_cost: toAmount(line.unit_cost),
          total: toAmount(line.total ?? line.total_amount),
          category: parent.category_name || "",
        });
      });
    }

    /* JOURNAL ENTRIES ----------------------- */
    if (type === "journal_entries") {
      parentLines.forEach((line) => {
        lines.push({
          parent_id: parent.id,
          entry_date: parent.entry_date,
          memo: parent.memo,
          account: line.accountid,
          debit: toAmount(line.debit),
          credit: toAmount(line.credit),
        });
      });
    }
  });

  return lines;
}



/* ============================== APP ============================== */
function App() {
  const backend = (() => {
    const envBackend = import.meta.env.VITE_BACKEND;
    const localDefault = `${window.location.protocol}//${window.location.hostname}:5050`;
    // If developing on localhost, prefer hitting local backend directly to bypass ngrok/browser warning.
    if (window.location.hostname === "localhost") return localDefault;
    return envBackend || localDefault;
  })();

  /* THEME HANDLING */
  const [theme, setTheme] = useState(localStorage.getItem("theme") || "dark");
  const [customUserName, setCustomUserName] = useState(
    localStorage.getItem("custom_user_name") || ""
  );
  useEffect(() => {
    document.body.setAttribute("data-theme", theme);
    localStorage.setItem("theme", theme);
  }, [theme]);
  const toggleTheme = () => setTheme((prev) => (prev === "dark" ? "light" : "dark"));
  useEffect(() => {
    if (customUserName) {
      localStorage.setItem("custom_user_name", customUserName);
      setUserName(customUserName);
    } else {
      localStorage.removeItem("custom_user_name");
    }
  }, [customUserName]);

  /* AUTH + BUSINESS STATES */
  const [accessToken, setAccessToken] = useState("");
  const [refreshToken, setRefreshToken] = useState("");
  const [accountId, setAccountId] = useState("");
  const [businessId, setBusinessId] = useState("");
  const [businessUUID, setBusinessUUID] = useState("");

  const [businessList, setBusinessList] = useState([]);
  const [selectedBusiness, setSelectedBusiness] = useState(null);

  const [data, setData] = useState(null);
  const [raw, setRaw] = useState(null);
  const [progress, setProgress] = useState("");
  const [progressPercent, setProgressPercent] = useState(0);
  const [loading, setLoading] = useState(false);
  const [testing, setTesting] = useState(false);
  const [bizLoading, setBizLoading] = useState(false);
  const [typeCounts, setTypeCounts] = useState({});
  const [history, setHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [selectedHistory, setSelectedHistory] = useState(null);

  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [type, setType] = useState("");

  const [userName, setUserName] = useState("");
  const [showRaw, setShowRaw] = useState(false);
  const [endpointStatus, setEndpointStatus] = useState({});
  const [filter, setFilter] = useState("all");
  const [openGroups, setOpenGroups] = useState(Object.keys(GROUPS));

  const hasTestResults = Object.keys(endpointStatus).length > 0;
  const endpointsForUI = useMemo(
    () => (hasTestResults ? endpointStatus : DEFAULT_ENDPOINT_STATUS),
    [hasTestResults, endpointStatus]
  );

  const isLoggedIn = Boolean(accessToken && refreshToken);

  /* ----------------- RESTORE TOKENS ----------------- */
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const access = params.get("access");
    const refresh = params.get("refresh");
    const account = params.get("account");
    const business = params.get("business");
    const uuid = params.get("business_uuid");
    const incomingName = params.get("user_name");

    if (access && refresh) {
      setAccessToken(access);
      setRefreshToken(refresh);
      setAccountId(account || "");
      setBusinessId(business || "");
      setBusinessUUID(uuid || "");
      if (incomingName) {
        setCustomUserName(incomingName);
        localStorage.setItem("custom_user_name", incomingName);
      }

      localStorage.setItem("access", access);
      localStorage.setItem("refresh", refresh);
      localStorage.setItem("account", account || "");
      localStorage.setItem("business_id", business || "");
      localStorage.setItem("business_uuid", uuid || "");

      fetchUserName(access);
      window.history.replaceState({}, document.title, "/");
    } else {
      const savedAccess = localStorage.getItem("access");
      const savedRefresh = localStorage.getItem("refresh");
      const savedAccount = localStorage.getItem("account");
      const savedUUID = localStorage.getItem("business_uuid");
      const savedBusiness = localStorage.getItem("business_id");

      if (savedAccess && savedRefresh) {
        setAccessToken(savedAccess);
        setRefreshToken(savedRefresh);
        setAccountId(savedAccount || "");
        setBusinessId(savedBusiness || "");
        setBusinessUUID(savedUUID || "");
        fetchUserName(savedAccess);
      }
    }
  }, []);

  /* ------------- FETCH USER NAME -------------- */
  const fetchUserName = async (token) => {
    try {
      const res = await axios.get(`${backend}/api/whoami`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      setUserName(res.data?.response?.user?.fname || "Unknown User");
    } catch (err) {
      console.error("❌ Error fetching user:", err);
    }
  };

  const fetchHistory = async () => {
    if (!accessToken) return;
    setHistoryLoading(true);
    try {
      const res = await axios.get(`${backend}/api/history`, {
        params: { limit: 200 },
      });
      setHistory(res.data?.history || []);
    } catch (err) {
      console.error("❌ Failed to load history:", err);
    } finally {
      setHistoryLoading(false);
    }
  };

  useEffect(() => {
    if (isLoggedIn) fetchHistory();
  }, [isLoggedIn]);

  const authorize = () => {
    if (!customUserName.trim()) return alert("Please enter your name first.");
    const url = `${backend}/auth?user_name=${encodeURIComponent(customUserName.trim())}`;
    window.location.href = url;
  };

  const logout = () => {
    localStorage.clear();
    setAccessToken("");
    setRefreshToken("");
    setAccountId("");
    setBusinessId("");
    setBusinessUUID("");
    setBusinessList([]);
    setSelectedBusiness(null);
    setData(null);
    setHistory([]);
    setHistoryLoading(false);
    setSelectedHistory(null);
    window.location.href = "/";
  };

  /* ---------------- FETCH BUSINESS LIST ---------------- */
  const fetchBusinessList = async () => {
    setBizLoading(true);
    setProgress("⏳ Fetching business list...");
    try {
      const res = await axios.get(`${backend}/api/business-map`, {
        headers: { Authorization: `Bearer ${accessToken}` },
        timeout: 20000,
      });

      setBusinessList(res.data?.businesses || []);
      setProgress("✅ Business list loaded!");
    } catch (err) {
      console.error("❌ Error fetching business list:", err);
      setProgress("❌ Failed to fetch business list.");
    } finally {
      setBizLoading(false);
    }
  };

  /* ---------------- UPDATE BUSINESS ---------------- */
  const updateBusinessSelection = async () => {
    if (!selectedBusiness) return alert("Select business first!");

    setAccountId(selectedBusiness.account_id || "");
    setBusinessId(selectedBusiness.business_id || "");
    setBusinessUUID(selectedBusiness.business_uuid || "");

    localStorage.setItem("account", selectedBusiness.account_id || "");
    localStorage.setItem("business_id", selectedBusiness.business_id || "");
    localStorage.setItem("business_uuid", selectedBusiness.business_uuid || "");
    const fileName = toFileName(selectedBusiness.name || "export", selectedBusiness.business_id);

    setBizLoading(true);
    setProgress("💾 Updating business...");

    try {
      await axios.post(
        `${backend}/api/update-tokens`,
        {
          access_token: localStorage.getItem("access"),
          refresh_token: localStorage.getItem("refresh"),
          account_id: selectedBusiness.account_id,
          business_id: selectedBusiness.business_id,
          business_uuid: selectedBusiness.business_uuid,
          file_name: fileName,
          business_name: selectedBusiness.name,
        },
        { timeout: 20000, headers: { "x-user-name": customUserName } }
      );

      setProgress(`✅ Updated to: ${selectedBusiness.name}`);
      await fetchHistory();
    } catch (err) {
      console.error("❌ Failed to update business:", err);
      setProgress(`❌ Business update failed: ${formatAxiosError(err)}`);
    } finally {
      setBizLoading(false);
    }
  };

  /* ---------------- TEST ENDPOINTS ---------------- */
  const testEndpoints = async () => {
    if (!businessId || !accountId || !businessUUID)
      return alert("Business ID / UUID missing!");

    setTesting(true);
    setEndpointStatus({});
    setProgress("⏳ Testing endpoints...");

    try {
      const res = await axios.get(`${backend}/api/test-endpoints`, {
        params: {
          account_id: accountId,
          business_id: businessId,
          business_uuid: businessUUID,
        },
        timeout: 120000,
      });

      setEndpointStatus(res.data?.results || {});
      setProgress("✅ Endpoints tested!");
    } catch (err) {
      console.error("❌ Endpoint test failed:", err);
      setProgress(`❌ Failed: ${formatAxiosError(err)}`);
    }

    setTesting(false);
  };

  /* ---------------- EXTRACT SUMMARY DATA ---------------- */
  const extractData = async () => {
    if (!businessId || !accountId || !businessUUID)
      return alert("Select business again — UUID missing!");

    if (!type) return alert("Select data type!");
    if (!start || !end) return alert("Select date range!");

    setLoading(true);
    setData(null);
    setProgress("⏳ Extracting data...");
    setProgressPercent(5);

    try {
      const res = await axios.get(`${backend}/api/extract`, {
        params: {
          start_date: toDateParam(start),
          end_date: toDateParam(end),
          type,
          account_id: accountId,
          business_id: businessId,
          business_uuid: businessUUID,
          include_raw: true,
          file_name: toFileName(type, businessId),
        },
        headers: {
          "x-user-name": customUserName,
        },
        timeout: 180000,
      });

      setProgressPercent(100);
      setProgress("✅ Extraction complete!");
      const payload = res.data || {};
      const dataset = Array.isArray(payload.data) ? payload.data : [];
      const hasDateValues = dataset.some((item) => Boolean(pickDateValue(item)));
      // Some datasets (e.g., chart_of_accounts) do not carry date fields; skip date filtering for them.
      const filtered = hasDateValues ? filterByDateRange(dataset, start, end) : dataset;
      setRaw({ ...payload, data: filtered });
      setData({ ...payload, data: filtered, total: filtered.length });
      const label = toTitle(type);
      updateCount(setTypeCounts, label, filtered.length);
      setProgress(`✅ ${label} ready: ${filtered.length} rows`);
      await fetchHistory();
    } catch (err) {
      console.error("❌ Extraction failed:", err);
      setProgress(`❌ Failed: ${formatAxiosError(err)}`);
    }

    setLoading(false);
    setTimeout(() => setProgressPercent(0), 2000);
  };

  /* ---------------- EXTRACT LINE ITEM DATA ---------------- */
  const extractLineData = async () => {
    if (!type) return alert("Select type first!");
    if (!accountId) return alert("Account ID missing. Update business first.");
    if (!start || !end) return alert("Select date range!");

    setLoading(true);
    setProgress("Fetching line items...");
    setData(null);
    const finishLineExtraction = async () => {
      await fetchHistory();
      setLoading(false);
    };

    try {
      const res = await axios.get(`${backend}/api/extract`, {
        params: {
          start_date: toDateParam(start),
          end_date: toDateParam(end),
          type,
          account_id: accountId,
          business_id: businessId,
          business_uuid: businessUUID,
          line_mode: type === "credit_notes" ? "true" : undefined,
          max_pages: type === "journal_entries" ? 150 : undefined,
          include_raw: true,
          file_name: toFileName(type, businessId),
        },
        headers: { "x-user-name": customUserName },
        timeout: ["journal_entries", "invoices", "estimates"].includes(type) ? 300000 : 180000,
      });

      setRaw(res.data);
      const raw = res.data?.data || [];
      const hasDateValues = Array.isArray(raw) && raw.some((item) => Boolean(pickDateValue(item)));
      const parents = hasDateValues ? filterByDateRange(raw, start, end) : raw;

      // --- Bills one-sheet ---
      if (type === "bills") {
        const resolveLineArray = (parent) => {
          if (!parent) return [];
          if (Array.isArray(parent.line_items_array)) return parent.line_items_array;
          if (Array.isArray(parent.line_items_raw)) return parent.line_items_raw;
          if (Array.isArray(parent.line_items)) return parent.line_items;
          if (Array.isArray(parent.bill_lines_raw)) return parent.bill_lines_raw;
          if (Array.isArray(parent.bill_lines)) return parent.bill_lines;
          if (Array.isArray(parent.lines)) return parent.lines;
          return extractLineItems("bills", [parent]) || [];
        };
        const toAmount = (value) => {
          if (value && typeof value === "object") return Number(value.amount ?? value.total ?? value.value ?? 0);
          return Number(value ?? 0);
        };
        const parseDate = (value) => {
          if (!value) return null;
          const normalized = String(value).trim();
          const isoLike = /^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2}))?)?/;
          const isoMatch = isoLike.exec(normalized);
          if (isoMatch) {
            const [, y, m, d, hh = "0", mm = "0", ss = "0"] = isoMatch;
            return new Date(Number(y), Number(m) - 1, Number(d), Number(hh), Number(mm), Number(ss));
          }
          const match = /(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2}))?/.exec(normalized);
          if (match) {
            const [, month, day, year, hour = "0", minute = "0"] = match;
            return new Date(Number(year), Number(month) - 1, Number(day), Number(hour), Number(minute));
          }
          const direct = new Date(normalized);
          if (!Number.isNaN(direct.getTime())) return direct;
          return null;
        };
        const formatDate = (value, withTime = false) => {
          const d = parseDate(value);
          if (!d) return "";
          const datePart = `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
          if (!withTime) return datePart;
          const hh = String(d.getHours()).padStart(2, "0");
          const mm = String(d.getMinutes()).padStart(2, "0");
          return `${datePart} ${hh}:${mm}`;
        };

        const columns = [
          "amount",
          "bill_number",
          "created_at",
          "currency_code",
          "due_date",
          "due_offset_days",
          "issue_date",
          "outstanding",
          "overall_category",
          "paid",
          "status",
          "tax_amount",
          "total_amount",
          "line_items",
          "parent_id",
          "vendorid",
          "description",
          "line_description",
          "quantity",
          "category",
          "tax_amount1",
          "tax_amount2",
          "tax_name1",
          "tax_name2",
          "tax_percent1",
          "tax_percent2",
          "line_total_amount",
          "unit_cost",
          "line_date",
        ];

        if (!parents.length) {
          setData({ success: true, total: 0, data: [] });
          setProgress("No bills found for the selected range/account.");
          updateCount(setTypeCounts, "Bill sheet", 0);
          await finishLineExtraction();
          return;
        }

        const rows = [];
        parents.forEach((parent) => {
          const parentLines = resolveLineArray(parent);
          parentLines.forEach((line) => {
            rows.push({
              amount: toAmount(parent.amount),
              bill_number: parent.bill_number,
              created_at: formatDate(parent.created_at || parent.create_date, true),
              currency_code: parent.currency_code,
              due_date: formatDate(parent.due_date),
              due_offset_days: Number(parent.due_offset_days ?? 0),
              issue_date: formatDate(parent.issue_date),
              outstanding: toAmount(parent.outstanding),
              overall_category: parent.overall_category || line.category?.category || line.category || "",
              paid: toAmount(parent.paid),
              status: parent.status,
              tax_amount: toAmount(parent.tax_amount),
              total_amount: toAmount(parent.total_amount),
              line_items: parentLines.length,
              parent_id: parent.id ?? parent.billid ?? parent.bill_id,
              vendorid:
                parent.vendorid ||
                parent.vendor_id ||
                parent.vendor?.id ||
                parent.vendor?.vendorid ||
                parent.vendor?.vendor_id ||
                parent.vendor?.accountid ||
                parent.vendor?.account_id ||
                parent.vendor?.userid ||
                parent.vendor?.uuid ||
                parent.bill_vendor?.id ||
                parent.bill_vendor?.vendor_id ||
                "",
              description: parent.description || "",
              line_description: line.description || line.name || "",
              quantity: line.quantity || line.qty || 1,
              category: line.category?.category || line.category || "",
              tax_amount1: line.tax_amount1 ?? "",
              tax_amount2: line.tax_amount2 ?? "",
              tax_name1: line.tax_name1 ?? "",
              tax_name2: line.tax_name2 ?? "",
              tax_percent1: line.tax_percent1 ?? "",
              tax_percent2: line.tax_percent2 ?? "",
              line_total_amount: toAmount(line.total_amount ?? line.total),
              unit_cost: toAmount(line.unit_cost),
              line_date: formatDate(line.date || parent.issue_date),
            });
          });
        });

        setData({ success: true, total: rows.length, data: rows, headers: columns });
        setProgress(`Bills ready: ${rows.length} rows`);
        updateCount(setTypeCounts, "Bill sheet", rows.length);
        await finishLineExtraction();
        return;
      }

      // --- Expenses one-sheet ---
      if (type === "expenses") {
        const resolveLineArray = (parent) => {
          if (!parent) return [];
          if (Array.isArray(parent.line_items_array)) return parent.line_items_array;
          if (Array.isArray(parent.bill_lines)) return parent.bill_lines;
          if (Array.isArray(parent.line_items)) return parent.line_items;
          if (Array.isArray(parent.lines)) return parent.lines;
          return [];
        };
        const toAmount = (value) => {
          if (value && typeof value === "object") return Number(value.amount ?? value.total ?? value.value ?? 0);
          return Number(value ?? 0);
        };
        const amtOrBlank = (v) => (v === null || v === undefined || v === "" ? "" : toAmount(v));

        const columns = [
          "vendor",
          "vendorid",
          "category",
          "categoryid",
          "date",
          "line_date",
          "taxAmount1",
          "taxAmount2",
          "taxName1",
          "taxName2",
          "taxPercent1",
          "taxPercent2",
          "amount",
          "notes",
        ];

        const rows = [];

        const taxTuple = (line, parent, idx) => {
          const taxesArr =
            (Array.isArray(line.taxes) && line.taxes) ||
            (Array.isArray(line.tax_summary) && line.tax_summary) ||
            (Array.isArray(parent.tax_summary) && parent.tax_summary) ||
            [];
          const t = taxesArr[idx] || {};
          const amount = t.amount ?? t.tax_amount ?? t.value ?? null;
          const percent = t.percent ?? t.rate ?? null;
          const name = t.name ?? t.tax_name ?? "";
          return { amount, percent, name };
        };

        parents.forEach((parent) => {
          const parentLines = resolveLineArray(parent);
          parentLines.forEach((line) => {
            const t1 = taxTuple(line, parent, 0);
            const t2 = taxTuple(line, parent, 1);

            const taxAmount1 =
              t1.amount != null
                ? amtOrBlank(t1.amount)
                : amtOrBlank(line.tax_amount1 ?? line.taxAmount1 ?? parent.tax_amount1 ?? "");
            const taxAmount2 =
              t2.amount != null
                ? amtOrBlank(t2.amount)
                : amtOrBlank(line.tax_amount2 ?? line.taxAmount2 ?? parent.tax_amount2 ?? "");
            const taxName1 = t1.name || line.tax_name1 || line.taxName1 || parent.tax_name1 || "";
            const taxName2 = t2.name || line.tax_name2 || line.taxName2 || parent.tax_name2 || "";
            const taxPercent1 = t1.percent ?? line.tax_percent1 ?? line.taxPercent1 ?? parent.tax_percent1 ?? "";
            const taxPercent2 = t2.percent ?? line.tax_percent2 ?? line.taxPercent2 ?? parent.tax_percent2 ?? "";
            const amount = toAmount(line.total ?? line.total_amount ?? line.amount ?? parent.amount);
            const category =
              parent.category_name ||
              line.category?.category ||
              line.category ||
              parent.overall_category ||
              "";
            const lineDesc = line.name || line.description || parent.notes || category || "";
            const categoryId =
              line.category_id ||
              line.categoryid ||
              line.category?.categoryid ||
              line.category?.id ||
              line.category?.category_id ||
              parent.category?.id ||
              parent.category?.categoryid ||
              parent.categoryid ||
              parent.category_id ||
              parent.overall_category_id ||
              parent.overall_categoryid ||
              "";
            const vendorId =
              parent.vendorid ||
              parent.vendor_id ||
              parent.vendor?.id ||
              parent.vendor?.vendorid ||
              parent.vendor?.vendor_id ||
              parent.vendor?.accountid ||
              parent.vendor?.account_id ||
              parent.vendor?.userid ||
              parent.vendor?.uuid ||
              "";
            const dateVal =
              line.date ||
              parent.date ||
              parent.created_at ||
              parent.updated_at ||
              parent.create_date ||
              parent.transaction_date ||
              "";
            const lineDate = line.date || parent.date || "";
            const notes = parent.notes || lineDesc || "";

            rows.push({
              vendor: parent.vendor || "",
              vendorid: vendorId,
              category,
              categoryid: categoryId,
              date: dateVal,
              line_date: lineDate,
              taxAmount1,
              taxAmount2,
              taxName1,
              taxName2,
              taxPercent1,
              taxPercent2,
              amount,
              notes,
            });
          });
        });

        setData({ success: true, total: rows.length, data: rows, headers: columns });
        setProgress(`Expenses ready: ${rows.length} rows`);
        updateCount(setTypeCounts, "Expense sheet", rows.length);
        await finishLineExtraction();
        return;
      }

      // --- Bill payments (per payment row) ---
      if (type === "bill_payments") {
        const toAmount = (value) => {
          if (value && typeof value === "object") return Number(value.amount ?? value.total ?? value.value ?? 0);
          return Number(value ?? 0);
        };
        const toDate = (val) => {
          if (!val) return "";
          const d = new Date(val);
          if (Number.isNaN(d.getTime())) return "";
          return `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
        };

        const rows = parents.map((p) => {
          const amount = toAmount(p.amount);
          const billNum =
            p.bill_number ||
            p.bill?.bill_number ||
            p.bill?.number ||
            p.bill?.id ||
            p.billid ||
            "";
          return {
            payment_id: p.id || p.paymentid || "",
            bill_number: billNum,
            amount,
            currency_code: p.amount?.code || p.currency_code || "",
            payment_date: toDate(p.payment_date || p.date || p.created_at),
            status: p.status || "",
            method: p.payment_method || p.method || "",
            notes: p.notes || "",
            bill_id: p.bill?.id || p.bill_id || "",
          };
        });

        setData({ success: true, total: rows.length, data: rows });
        setProgress(`Bill payments ready: ${rows.length} rows`);
        updateCount(setTypeCounts, "Bill payments", rows.length);
        await finishLineExtraction();
        return;
      }

      // --- Invoices one-sheet ---
      if (type === "invoices") {
        const resolveLineArray = (parent) => {
          if (!parent) return [];
          if (Array.isArray(parent.line_items)) return parent.line_items;
          if (Array.isArray(parent.lines)) return parent.lines;
          return [];
        };
        const toAmount = (value) => {
          if (value && typeof value === "object") return Number(value.amount ?? value.total ?? value.value ?? 0);
          return Number(value ?? 0);
        };

        const toDate = (val) => {
          if (!val) return "";
          const d = new Date(val);
          if (Number.isNaN(d.getTime())) return "";
          return `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
        };

        const columns = [
          "Invoice ID",
          "id",
          "fname",
          "Invoice Number",
          "Client/Organization",
          "Date",
          "Due Date",
          "Item Name",
          "Item Description",
          "Qty",
          "Unit cost",
          "Total Amount",
          "Tax Name 1",
          "Tax Amount 1",
          "Tax Percentage1",
          "Tax Name 2",
          "Tax Amount 2",
          "Tax Percentage2",
          "discount_rate",
          "discount_type",
          "Total",
        ];

        const rows = [];

        parents.forEach((parent) => {
          const parentLines = resolveLineArray(parent);
          const parentTax1 =
            (Array.isArray(parent.taxes) && parent.taxes[0]) || parent.tax_summary?.[0] || null;
          const parentTax2 =
            (Array.isArray(parent.taxes) && parent.taxes[1]) || parent.tax_summary?.[1] || null;
          const parentDiscount = toNumberOrZero(
            parent.discount?.amount ??
              parent.discount?.rate ??
              parent.discount_value ??
              parent.discount_total?.amount ??
              parent.discount_total ??
              parent.discount ??
              0
          );
          parentLines.forEach((line) => {
            const lineDiscount = toNumberOrZero(
              line.discount?.amount ?? line.discount?.rate ?? line.discount ?? 0
            );
            const qty = line.qty || line.quantity || 1;
            const unitCost = toAmount(line.unit_cost);
            const lineTotal = toAmount(line.amount) || Number(unitCost * qty) || 0;

            const rawTaxAmt1 =
              line.tax_amount1 ?? line.taxAmount1 ?? parent.tax_amount1 ?? parentTax1?.amount ?? parentTax1?.tax_amount ?? "";
            let taxPerc1 = line.tax_percent1 ?? line.taxPercent1 ?? parentTax1?.percent ?? parentTax1?.rate ?? "";
            if (!taxPerc1 && rawTaxAmt1 !== "" && rawTaxAmt1 !== null && lineTotal > 0) {
              const candidate = Number(rawTaxAmt1);
              if (!Number.isNaN(candidate) && candidate > 0 && candidate <= 100) {
                taxPerc1 = candidate;
              }
            }
            const taxAmt1 =
              taxPerc1 !== ""
                ? round2((lineTotal * (Number(taxPerc1) || 0)) / 100)
                : rawTaxAmt1 !== "" && rawTaxAmt1 !== null
                ? toNumberOrZero(rawTaxAmt1)
                : "";

            const rawTaxAmt2 =
              line.tax_amount2 ?? line.taxAmount2 ?? parent.tax_amount2 ?? parentTax2?.amount ?? parentTax2?.tax_amount ?? "";
            let taxPerc2 = line.tax_percent2 ?? line.taxPercent2 ?? parentTax2?.percent ?? parentTax2?.rate ?? "";
            if (!taxPerc2 && rawTaxAmt2 !== "" && rawTaxAmt2 !== null && lineTotal > 0) {
              const candidate = Number(rawTaxAmt2);
              if (!Number.isNaN(candidate) && candidate > 0 && candidate <= 100) {
                taxPerc2 = candidate;
              }
            }
            const taxAmt2 =
              taxPerc2 !== ""
                ? round2((lineTotal * (Number(taxPerc2) || 0)) / 100)
                : rawTaxAmt2 !== "" && rawTaxAmt2 !== null
                ? toNumberOrZero(rawTaxAmt2)
                : "";

            const clientOrg =
              parent.client_name ||
              parent.organization ||
              parent.current_organization ||
              parent.client?.organization ||
              parent.client?.display_name ||
              parent.client?.name ||
              parent.customer?.organization ||
              "";

            rows.push({
              "Invoice ID": parent.invoiceid || parent.id || "",
              id: parent.id || parent.invoiceid || "",
              fname:
                parent.fname ||
                parent.client?.fname ||
                parent.client?.name ||
                parent.customer?.fname ||
                "",
              "Invoice Number": parent.invoice_number || parent.number || "",
              "Client/Organization": clientOrg,
              "Date": toDate(parent.create_date),
              "Due Date": toDate(parent.due_date),
              "Item Name": line.name || "",
              "Item Description": line.description || parent.description || "",
              Qty: qty,
              "Unit cost": unitCost,
              "Total Amount": lineTotal,
              "Tax Name 1":
                line.taxName1 ??
                line.tax_name1 ??
                parent.tax_name1 ??
                parentTax1?.name ??
                parentTax1?.tax_name ??
                "",
              "Tax Amount 1": taxAmt1,
              "Tax Percentage1": taxPerc1,
              "Tax Name 2":
                line.taxName2 ??
                line.tax_name2 ??
                parent.tax_name2 ??
                parentTax2?.name ??
                parentTax2?.tax_name ??
                "",
              "Tax Amount 2": taxAmt2,
              "Tax Percentage2": taxPerc2,
              discount_rate: (line.discount?.rate ?? parent.discount?.rate ?? lineDiscount) || "",
              discount_type: line.discount?.type ?? parent.discount?.type ?? "",
              Total: toAmount(parent.amount),
            });
          });
        });

        setData({ success: true, total: rows.length, data: rows });
        setProgress(`Invoices ready: ${rows.length} rows`);
        updateCount(setTypeCounts, "Invoice sheet", rows.length);
        await finishLineExtraction();
        return;
      }

      // --- Estimates one-sheet ---
      if (type === "estimates") {
        const columns = [
          "accepted",
          "amount",
          "code",
          "create_date",
          "currency_code",
          "current_organization",
          "customerid",
          "description",
          "discount_total",
          "discount_value",
          "display_status",
          "estimate_number",
          "estimateid",
          "id",
          "notes",
          "organization",
          "ownerid",
          "po_number",
          "rich_proposal",
          "status",
          "terms",
          "line_items",
          "parent_id",
          "parent_number",
          "line_date",
          "line_description",
          "qty",
          "unit_cost",
          "total",
          "Tax Amount 1",
          "Tax Amount 2",
          "Tax Name 1",
          "Tax Name 2",
          "Line item",
        ];

        const lines = extractLineItems("estimates", parents);
        const parentById = Object.fromEntries(
          parents
            .filter((p) => p && (p.estimateid || p.id))
            .map((p) => [p.estimateid || p.id, p])
        );

        const rows = lines.map((line) => {
          const parent = parentById[line.parent_id] || {};
          const parentLines = Array.isArray(parent.line_items)
            ? parent.line_items
            : Array.isArray(parent.lines)
            ? parent.lines
            : [];

          return {
            accepted: parent.accepted,
            amount: parent.amount?.amount ?? parent.amount ?? "",
            code: parent.amount?.code ?? parent.currency_code ?? "",
            create_date: parent.create_date,
            currency_code: parent.currency_code,
            current_organization: parent.current_organization,
            customerid: parent.customerid,
            description: parent.description,
            discount_total: parent.discount_total?.amount ?? parent.discount_total ?? "",
            discount_value: parent.discount_value,
            display_status: parent.display_status,
            estimate_number: parent.estimate_number,
            estimateid: parent.estimateid,
            id: parent.id,
            notes: parent.notes,
            organization: parent.organization,
            ownerid: parent.ownerid,
            po_number: parent.po_number,
            rich_proposal: parent.rich_proposal,
            status: parent.status,
            terms: parent.terms,
            line_items: parentLines.length,
            parent_id: line.parent_id,
            parent_number: line.parent_number,
            line_date: line.date,
            line_description: line.description,
            qty: line.qty,
            unit_cost: line.unit_cost,
            total: line.total,
            "Tax Amount 1": line.tax1 ?? "",
            "Tax Amount 2": line.tax2 ?? "",
            "Tax Name 1": line.taxName1 ?? line.tax_name1 ?? "",
            "Tax Name 2": line.taxName2 ?? line.tax_name2 ?? "",
            "Line item": line.name || line.description || "",
          };
        });

        setData({ success: true, total: rows.length, data: rows });
        setProgress(`Estimates ready: ${rows.length} rows`);
        updateCount(setTypeCounts, "Estimate sheet", rows.length);
        await finishLineExtraction();
        return;
      }

      // Default: generic line item extraction
      const lines = extractLineItems(type, parents);
      setData({ success: true, total: lines.length, data: lines });
      setProgress(`${toTitle(type)} line items: ${lines.length} rows`);
      updateCount(setTypeCounts, `${toTitle(type)} line items`, lines.length);
      await finishLineExtraction();
      return;
    } catch (err) {
      console.error("Line item extract failed:", err);
      alert(`Line item extract failed: ${formatAxiosError(err)}`);
    }

    setLoading(false);
  };

  /* ---------------- CSV DOWNLOAD ---------------- */
  const downloadCSV = () => {
    if (!data?.data?.length) return alert("No data!");

    const headers = Array.isArray(data.headers) && data.headers.length
      ? data.headers
      : Object.keys(data.data[0]);

    const rows = [
      headers.join(","),
      ...data.data.map((r) => headers.map((h) => JSON.stringify(r[h] ?? "")).join(",")),
    ].join("\n");

    const blob = new Blob([rows], { type: "text/csv" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `${type}_${data?.business_name || "export"}.csv`;
    a.click();
  };

  /* ---------------- PARSE TEST RESULTS ---------------- */
  const parsed = useMemo(
    () => Object.fromEntries(Object.entries(endpointsForUI).map(([k, v]) => [k, parseResult(v)])),
    [endpointsForUI]
  );

  const summary = useMemo(() => {
    if (!hasTestResults)
      return { total: 0, ok: 0, empty: 0, error: 0, unsupported: 0 };
    const all = Object.values(parsed);
    const c = (k) => all.filter((x) => x.kind === k).length;
    return {
      total: all.length,
      ok: c("ok"),
      empty: c("empty"),
      error: c("error"),
      unsupported: c("unsupported"),
    };
  }, [parsed]);

  const filteredKeys = useMemo(() => {
    if (filter === "all") return Object.keys(parsed);
    return Object.keys(parsed).filter((k) => parsed[k].kind === filter);
  }, [parsed, filter]);

  const sessionsForUI = useMemo(() => {
    const seen = new Set();
    const dedup = [];
    for (const item of history || []) {
      const name = (item.user?.name || "").trim();
      if (!name || name.toLowerCase() === "unknown user") continue;
      const key = item.user?.id || name.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      dedup.push(item);
    }
    return dedup;
  }, [history]);

  const summarizeSession = (item) => {
    const lastAction = Array.isArray(item.actions) ? item.actions[0] : null;
    const extractCount = Array.isArray(item.actions)
      ? item.actions.filter((a) => a.kind === "extract").length
      : 0;
    return {
      lastAction,
      extractCount,
    };
  };

  const formatAxiosError = (err) => {
    const status = err?.response?.status;
    const msg =
      err?.response?.data?.error ||
      err?.response?.data ||
      err?.message ||
      "Unknown error";
    const toText = (val) => {
      if (typeof val === "string") return val;
      try {
        return JSON.stringify(val);
      } catch {
        return String(val);
      }
    };
    return status ? `${status} - ${toText(msg)}` : toText(msg);
  };

  const grouped = useMemo(() => {
    const result = {};
    Object.entries(GROUPS).forEach(([group, keys]) => {
      result[group] = keys.filter((k) => filteredKeys.includes(k));
    });
    const extra = filteredKeys.filter((k) => !Object.values(GROUPS).flat().includes(k));
    if (extra.length) result["Other"] = extra;
    return result;
  }, [filteredKeys]);

  /* ================= LOGIN SCREEN ================= */
  if (!isLoggedIn) {
    return (
      <div className="login-container">
        <div className="login-card">
          <img src={mmclogo} className="login-logo" />
          <h1>Login to FreshBooks</h1>
          <p className="tagline">Accurate • Automated • Fast Data Conversion</p>
          <input
            type="text"
            placeholder="Your name (saved with history)"
            value={customUserName}
            onChange={(e) => setCustomUserName(e.target.value)}
            className="login-input"
          />
          <button className="primary-btn" onClick={authorize}>
            Login & Authorize
          </button>
        </div>
      </div>
    );
  }

  /* ================= DASHBOARD ================= */
  return (
    <div className="dashboard">
      {/* HEADER */}
      <header className="mmc-header">
        <div className="mmc-brand">
          <img src={mmclogo} className="mmc-header-logo" />
          <div className="mmc-title">
            <h1>FreshBooks Data Extractor</h1>
            <div className="brand-meta">
              <span className="brand-pill">Powered by MMC Convert</span>
            </div>
          </div>
        </div>

        <div className="mmc-actions">
          <button className="theme-toggle" onClick={toggleTheme}>
            {theme === "dark" ? "🌞 Light Mode" : "🌙 Dark Mode"}
          </button>

          <button className="logout-btn" onClick={logout}>
            🚪 Logout
          </button>
        </div>
      </header>

      {/* TOP PANEL */}
      <div className="two-column">
        <section className="card">
          <h2>🏢 Select Business</h2>

          {bizLoading ? (
            <div className="loader-container">
              <div className="spinner"></div>
              <p>{progress}</p>
            </div>
          ) : (
            <>
              <button className="secondary-btn" onClick={fetchBusinessList}>
                🔍 Get Business List
              </button>

              <select
                value={selectedBusiness?.business_id || ""}
                onChange={(e) => {
                  const sel = businessList.find((b) => String(b.business_id) === e.target.value);
                  setSelectedBusiness(sel || null);
                }}
              >
                <option value="">Select Business</option>
                {businessList.map((b) => (
                  <option key={b.business_id} value={b.business_id}>
                    {b.name} — {b.account_id} — {b.business_id}
                  </option>
                ))}
              </select>

              <button onClick={updateBusinessSelection} className="primary-btn">
                💾 Update Business
              </button>

              <p className="status-text">{progress}</p>
            </>
          )}
        </section>

        <section className="card">
          <h2>📅 Extract Data</h2>

          <div className="grid">
            <input type="date" value={start} onChange={(e) => setStart(e.target.value)} />
            <input type="date" value={end} onChange={(e) => setEnd(e.target.value)} />

            <select value={type} onChange={(e) => setType(e.target.value)}>
              <option value="">Select Endpoint</option>

              {Object.entries(endpointsForUI).map(([key, val]) => {
                const p = parseResult(val);
                const disabled = p.kind === "unsupported";
                return (
                  <option key={key} value={key} disabled={disabled}>
                    {ICON[p.kind]} {toTitle(key)}
                  </option>
                );
              })}
            </select>
          </div>

          <button onClick={extractData} className="primary-btn" disabled={loading}>
            {loading ? `Extracting... ${progressPercent}%` : "Extract Summary"}
          </button>

          {/* LINE ITEM EXTRACT */}
          <button
            onClick={extractLineData}
            className="secondary-btn"
            style={{ marginTop: 10 }}
          >
            📄 Extract Line Items
          </button>
          <p>{progress}</p>
        </section>
      </div>

      {/* TEST ENDPOINTS */}
      <section className="card">
        <div className="flex-row">
          <h2>🧪 Check Endpoint Access</h2>
          <button onClick={testEndpoints} className="secondary-btn" disabled={testing}>
            {testing ? "⏳ Testing..." : "🔎 Test Endpoints"}
          </button>
        </div>

        {!testing && summary.total > 0 && (
          <>
            <div className="summary-row">
              <span className="pill ok">{ICON.ok} Working: {summary.ok}</span>
              <span className="pill warn">{ICON.empty} No Data: {summary.empty}</span>
              <span className="pill err">{ICON.error} Errors: {summary.error}</span>
              <span className="pill off">{ICON.unsupported} Unsupported: {summary.unsupported}</span>
              <span className="pill neutral">Total: {summary.total}</span>
            </div>

            <div className="group-wrap">
              {Object.entries(grouped).map(([group, keys]) =>
                keys.length ? (
                  <div className="group" key={group}>
                    <div
                      className="group-head"
                      onClick={() =>
                        setOpenGroups((prev) =>
                          prev.includes(group)
                            ? prev.filter((g) => g !== group)
                            : [...prev, group]
                        )
                      }
                    >
                      <span>{openGroups.includes(group) ? "▾" : "▸"}</span>
                      <b>{group}</b>
                      <small>{keys.length} item(s)</small>
                    </div>

                    {openGroups.includes(group) && (
                      <table className="data-table compact">
                        <thead>
                          <tr>
                            <th>Endpoint</th>
                            <th>Status</th>
                            <th>Records</th>
                          </tr>
                        </thead>
                        <tbody>
                          {keys.map((k) => {
                            const p = parsed[k];
                            return (
                              <tr key={k}>
                                <td>{toTitle(k)}</td>
                                <td>
                                  {ICON[p.kind]} {p.label}
                                </td>
                                <td>{p.records}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    )}
                  </div>
                ) : null
              )}
            </div>
          </>
        )}
      </section>

      {/* EXTRACTED COUNTS */}
      {Object.keys(typeCounts).length > 0 && (
        <section className="card">
          <h2>📑 Extracted Counts</h2>
          <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "grid", gap: 8 }}>
            {Object.entries(typeCounts).map(([label, count]) => (
              <li
                key={label}
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  padding: "8px 12px",
                  background: "var(--surface-2, #111827)",
                  borderRadius: 8,
                }}
              >
                <span>{label}</span>
                <b>{count} rows</b>
              </li>
            ))}
          </ul>
        </section>
      )}

      {/* ACTIVITY HISTORY */}
      <section className="card">
        <div className="flex-row">
          <h2>🧾 Activity History</h2>
          <button className="secondary-btn" onClick={fetchHistory} disabled={historyLoading}>
            {historyLoading ? "Refreshing..." : "↻ Refresh"}
          </button>
        </div>
        {historyLoading && <p>Loading history...</p>}
        {!historyLoading && history.length === 0 && <p>No activity recorded yet.</p>}
        {!historyLoading && history.length > 0 && (
          <div className="history-table-wrapper">
            <table className="data-table compact">
              <thead>
                <tr>
                  <th>Event</th>
                  <th>User</th>
                  <th>When</th>
                </tr>
              </thead>
              <tbody>
                {sessionsForUI.slice(0, 25).map((item, idx) => {
                  const { lastAction, extractCount } = summarizeSession(item);
                  return (
                  <tr
                    key={item.id || idx}
                    onClick={() => setSelectedHistory(item)}
                    style={{ cursor: "pointer" }}
                  >
                    <td>Session</td>
                    <td>
                      <div>{item.user?.name || "Unknown User"}</div>
                      {item.user?.email ? <small>{item.user.email}</small> : null}
                    </td>
                    <td>{formatDateTime(item.timestamp)}</td>
                  </tr>
                );
              })}
              </tbody>
            </table>
          </div>
        )}
        {selectedHistory && (
          <div className="history-detail">
            <h4>Details</h4>
            <p>
              <b>User:</b> {selectedHistory.user?.name || "Unknown User"}
            </p>
            <p>
              <b>Recent extracts:</b>{" "}
              {Array.isArray(selectedHistory.actions)
                ? selectedHistory.actions.filter((a) => a.kind === "extract").length
                : 0}
            </p>
            {Array.isArray(selectedHistory.actions) && selectedHistory.actions.length > 0 && (
              <div className="action-list">
                {selectedHistory.actions
                  .filter((a) => a.kind === "extract")
                  .slice(0, 10)
                  .map((action, idx) => {
                    const businessLabel = selectedHistory.meta?.last_business || "";
                    const rawFile =
                      action.file_name || selectedHistory.meta?.last_file || "";
                    const fileDisplay = businessLabel
                      ? `${(action.type || "export").replace(/\s+/g, "_")}_${businessLabel}.csv`
                      : rawFile || "—";
                    const range = action.start_date
                      ? `${action.start_date}${action.end_date ? ` → ${action.end_date}` : ""}`
                      : "—";
                    return (
                      <div
                        key={idx}
                        className="action-item"
                        style={{ padding: "8px 0", lineHeight: 1.4 }}
                      >
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          <b>{toTitle(action.type || "")}</b>
                          <span>•</span>
                          <span>{fileDisplay}</span>
                        </div>
                        <div style={{ color: "#cbd5e1" }}>
                          Rows: {action.total ?? "—"} • {range} • {formatDateTime(action.at)}
                        </div>
                      </div>
                    );
                  })}
              </div>
            )}
          </div>
        )}
      </section>

      {/* DATA TABLE */}
      {data && (
        <section className="card">
          <h2>📂 Extracted Data</h2>

          <div className="data-actions">
            <button className="secondary-btn" onClick={downloadCSV}>
              ⬇️ Download CSV
            </button>
            <button
              className="secondary-btn"
              onClick={() => {
                if (!raw) return alert("No raw JSON available yet.");
                // If backend sent { raw: [...] }, prefer that; otherwise fall back to whole response
                const payload = raw.raw ?? raw;
                const blob = new Blob([JSON.stringify(payload, null, 2)], {
                  type: "application/json",
                });
                const a = document.createElement("a");
                a.href = URL.createObjectURL(blob);
                a.download = `${type || "export"}_raw.json`;
                a.click();
              }}
            >
              ⬇️ Download Raw JSON
            </button>
            <button
              className="secondary-btn"
              onClick={() => {
                if (!data) return alert("No formatted JSON available yet.");
                const blob = new Blob([JSON.stringify(raw, null, 2)], {
                  type: "application/json",
                });
                const a = document.createElement("a");
                a.href = URL.createObjectURL(blob);
                a.download = `${type || "export"}.json`;
                a.click();
              }}
            >
              ⬇️ Download JSON
            </button>
            <button className="secondary-btn" onClick={() => setShowRaw(!showRaw)}>
              {showRaw ? "Hide Raw JSON" : "Show Raw JSON"}
            </button>
          </div>
          <table className="data-table">
            {/* HEADER SAFE */}
            <thead>
              <tr>
                {Array.isArray(data?.data) &&
                  data.data.length > 0 &&
                  Object.keys(data.data[0] || {})
                    .slice(0, 6)
                    .map((h) => <th key={h}>{h}</th>)}

                {/* No Data Case */}
                {(!data?.data || data.data.length === 0) && <th>No data</th>}
              </tr>
            </thead>

            {/* BODY SAFE */}
            <tbody>
              {Array.isArray(data?.data) && data.data.length > 0 ? (
                data.data.slice(0, 50).map((row, i) => (
                  <tr key={i}>
                    {Object.keys(row || {})
                      .slice(0, 6)
                      .map((col, j) => (
                        <td key={j}>{String(row[col] ?? "").slice(0, 40)}</td>
                      ))}
                  </tr>
                ))
              ) : (
                <tr>
                  <td
                    colSpan="6"
                    style={{
                      textAlign: "center",
                      padding: "20px",
                      fontSize: "16px",
                      opacity: 0.7,
                    }}
                  >
                    No data found
                  </td>
                </tr>
              )}
            </tbody>
          </table>


          {showRaw && <pre>{JSON.stringify(data, null, 2)}</pre>}
        </section>
      )}
    </div>
  );
}

export default App;
