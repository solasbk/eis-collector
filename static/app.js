/* app.js — EIS Investor Collector Frontend */

/* API base: same origin when deployed, localhost for local dev */
const API = location.hostname === "localhost" || location.hostname === "127.0.0.1"
  ? "http://localhost:8000"
  : "";

/* ─── State ─── */
let currentPage = 1;
let totalPages = 1;
let sortBy = "date_found";
let sortDir = "desc";
let searchTimeout = null;
let sectors = [];
let sourceTypes = [];

/* ─── DOM References ─── */
const tableBody = document.getElementById("table-body");
const emptyState = document.getElementById("empty-state");
const pagination = document.getElementById("pagination");
const paginationInfo = document.getElementById("pagination-info");
const pageIndicator = document.getElementById("page-indicator");
const prevBtn = document.getElementById("prev-page");
const nextBtn = document.getElementById("next-page");
const searchInput = document.getElementById("search-input");
const filterSector = document.getElementById("filter-sector");
const filterDateFrom = document.getElementById("filter-date-from");
const filterDateTo = document.getElementById("filter-date-to");
const detailOverlay = document.getElementById("detail-overlay");
const detailPanel = document.getElementById("detail-panel");
const detailName = document.getElementById("detail-name");
const detailBody = document.getElementById("detail-body");
const detailClose = document.getElementById("detail-close");
const exportBtn = document.getElementById("export-csv-btn");
const exportExcelBtn = document.getElementById("export-excel-btn");
const exportNewBtn = document.getElementById("export-new-btn");
const exportNewBadge = document.getElementById("export-new-badge");
const runCollectionBtn = document.getElementById("run-collection-btn");
const runChBtn = document.getElementById("run-ch-btn");
const filterOrigin = document.getElementById("filter-origin");
const filterEntityType = document.getElementById("filter-entity-type");
const filterHolding = document.getElementById("filter-holding");
const toast = document.getElementById("toast");
// Per-scan status elements
var scanRows = {
  web: { bar: document.getElementById("status-web"), text: document.getElementById("status-web-text") },
  ch: { bar: document.getElementById("status-ch"), text: document.getElementById("status-ch-text") },
  tr1: { bar: document.getElementById("status-tr1"), text: document.getElementById("status-tr1-text") },
  enrich: { bar: document.getElementById("status-enrich"), text: document.getElementById("status-enrich-text") },
};

/* ─── Theme Toggle ─── */
(function initTheme() {
  const toggle = document.querySelector("[data-theme-toggle]");
  const root = document.documentElement;
  let theme = "dark";
  root.setAttribute("data-theme", theme);
  updateDateColorScheme(theme);

  if (toggle) {
    toggle.addEventListener("click", function () {
      theme = theme === "dark" ? "light" : "dark";
      root.setAttribute("data-theme", theme);
      updateDateColorScheme(theme);
      toggle.setAttribute("aria-label", "Switch to " + (theme === "dark" ? "light" : "dark") + " mode");
      toggle.innerHTML = theme === "dark"
        ? '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>'
        : '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';
    });
  }
})();

function updateDateColorScheme(theme) {
  document.querySelectorAll(".filter-date").forEach(function (el) {
    el.style.colorScheme = theme;
  });
}

/* ─── Toast ─── */
function showToast(message, duration) {
  duration = duration || 3000;
  toast.textContent = message;
  toast.classList.add("visible");
  setTimeout(function () {
    toast.classList.remove("visible");
  }, duration);
}

/* ─── Fetch Stats ─── */
async function fetchStats() {
  try {
    const res = await fetch(API + "/api/stats");
    const data = await res.json();

    animateNumber("stat-total", data.total_investors);
    animateNumber("stat-new-week", data.new_this_week);
    animateNumber("stat-individuals", data.individual_count || 0);
    animateNumber("stat-orgs", data.org_count || 0);
    animateNumber("stat-linkedin", data.linkedin_count || 0);

    // Populate filter options
    sectors = data.sectors || [];
    sourceTypes = data.source_types || [];
    populateFilters();
  } catch (err) {
    console.error("Failed to fetch stats:", err);
  }
}

function animateNumber(id, target) {
  const el = document.getElementById(id);
  const start = parseInt(el.textContent) || 0;
  if (start === target) {
    el.textContent = target;
    return;
  }
  const duration = 400;
  const startTime = performance.now();
  function step(now) {
    const progress = Math.min((now - startTime) / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3);
    el.textContent = Math.round(start + (target - start) * eased);
    if (progress < 1) {
      requestAnimationFrame(step);
    }
  }
  requestAnimationFrame(step);
}

function populateFilters() {
  // Sectors
  filterSector.innerHTML = '<option value="">All Sectors</option>';
  sectors.forEach(function (s) {
    var opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    filterSector.appendChild(opt);
  });
}

/* ─── Fetch Investors ─── */
async function fetchInvestors() {
  showSkeleton();

  var params = new URLSearchParams();
  params.set("page", currentPage);
  params.set("per_page", "50");
  params.set("sort_by", sortBy);
  params.set("sort_dir", sortDir);

  var search = searchInput.value.trim();
  if (search) { params.set("search", search); }

  var origin = filterOrigin.value;
  if (origin) { params.set("origin", origin); }

  var entityType = filterEntityType.value;
  if (entityType) { params.set("entity_type", entityType); }

  var holdingFilter = filterHolding.value;
  if (holdingFilter) { params.set("holding", holdingFilter); }

  var sector = filterSector.value;
  if (sector) { params.set("sector", sector); }

  var dateFrom = filterDateFrom.value;
  if (dateFrom) { params.set("date_from", dateFrom); }

  var dateTo = filterDateTo.value;
  if (dateTo) { params.set("date_to", dateTo); }

  try {
    var res = await fetch(API + "/api/investors?" + params.toString());
    var data = await res.json();

    totalPages = data.total_pages;
    renderTable(data.investors);
    renderPagination(data.total, data.page, data.total_pages);
  } catch (err) {
    console.error("Failed to fetch investors:", err);
    tableBody.innerHTML = '<tr><td colspan="8" style="text-align:center; padding: var(--space-8); color: var(--color-text-faint);">Failed to load data.</td></tr>';
  }
}

function showSkeleton() {
  var html = "";
  for (var i = 0; i < 8; i++) {
    html += '<tr class="skeleton-row"><td colspan="8"><div class="skeleton" style="width:' + (60 + Math.random() * 35) + '%;"></div></td></tr>';
  }
  tableBody.innerHTML = html;
  emptyState.style.display = "none";
  pagination.style.display = "none";
}

function renderTable(investors) {
  if (!investors || investors.length === 0) {
    tableBody.innerHTML = "";
    emptyState.style.display = "flex";
    pagination.style.display = "none";
    return;
  }

  emptyState.style.display = "none";
  var html = "";

  investors.forEach(function (inv, index) {
    var amountClass = inv.amount === "Undisclosed" ? "cell-amount-undisclosed" : "cell-amount";
    var sourceLink = inv.source_url
      ? '<a href="' + escapeHtml(inv.source_url) + '" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation();">' + escapeHtml(inv.source_name || "Link") + '</a>'
      : (inv.source_name || "—");

    var linkedinCell = inv.linkedin_url
      ? '<a href="' + escapeHtml(inv.linkedin_url) + '" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation();" aria-label="View LinkedIn profile"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/></svg></a>'
      : '<span style="color:var(--color-text-faint)">—</span>';

    var roleCompany = escapeHtml(inv.role || "");
    if (inv.company) {
      roleCompany += '<div class="cell-role-company">' + escapeHtml(inv.company) + '</div>';
    }

    html += '<tr data-id="' + inv.id + '" style="animation:fadeIn 180ms ' + (index * 30) + 'ms both;">';
    html += '<td class="cell-name">' + escapeHtml(inv.name) + '</td>';
    html += '<td class="cell-role">' + roleCompany + '</td>';
    html += '<td>' + escapeHtml(inv.eis_company || "—") + '</td>';
    html += '<td><span class="cell-badge">' + escapeHtml(inv.sector || "—") + '</span></td>';
    html += '<td class="' + amountClass + '">' + escapeHtml(inv.amount || "—") + '</td>';
    html += '<td class="cell-source">' + sourceLink + '</td>';
    html += '<td>' + escapeHtml(inv.date_found || "—") + '</td>';
    html += '<td class="cell-linkedin">' + linkedinCell + '</td>';
    html += '</tr>';
  });

  tableBody.innerHTML = html;

  // Add click handlers for row detail
  tableBody.querySelectorAll("tr[data-id]").forEach(function (row) {
    row.addEventListener("click", function () {
      openDetail(parseInt(row.dataset.id));
    });
  });
}

function renderPagination(total, page, pages) {
  if (total === 0) {
    pagination.style.display = "none";
    return;
  }

  pagination.style.display = "flex";
  var start = (page - 1) * 50 + 1;
  var end = Math.min(page * 50, total);
  paginationInfo.textContent = start + "–" + end + " of " + total + " investors";
  pageIndicator.textContent = "Page " + page + " of " + pages;

  prevBtn.disabled = page <= 1;
  nextBtn.disabled = page >= pages;
}

/* ─── Detail Panel ─── */
async function openDetail(id) {
  try {
    var res = await fetch(API + "/api/investors/" + id);
    var inv = await res.json();

    detailName.textContent = inv.name;

    var html = "";

    if (inv.role || inv.company) {
      html += '<div class="detail-field">';
      html += '<div class="detail-label">Role / Company</div>';
      html += '<div class="detail-value">' + escapeHtml(inv.role || "") + (inv.company ? " at " + escapeHtml(inv.company) : "") + '</div>';
      html += '</div>';
    }

    html += '<div class="detail-field">';
    html += '<div class="detail-label">Issuer</div>';
    html += '<div class="detail-value">' + escapeHtml(inv.eis_company || "—") + '</div>';
    html += '</div>';

    html += '<div class="detail-field">';
    html += '<div class="detail-label">Sector</div>';
    html += '<div class="detail-value">' + escapeHtml(inv.sector || "—") + '</div>';
    html += '</div>';

    html += '<div class="detail-field">';
    html += '<div class="detail-label">Amount</div>';
    html += '<div class="detail-value">' + escapeHtml(inv.amount || "Undisclosed") + '</div>';
    html += '</div>';

    if (inv.source_url) {
      html += '<div class="detail-field">';
      html += '<div class="detail-label">Source</div>';
      html += '<div class="detail-value"><a href="' + escapeHtml(inv.source_url) + '" target="_blank" rel="noopener noreferrer">' + escapeHtml(inv.source_name || "View Source") + '</a> (' + escapeHtml(inv.source_type || "") + ')</div>';
      html += '</div>';
    }

    html += '<div class="detail-field">';
    html += '<div class="detail-label">Date Found</div>';
    html += '<div class="detail-value">' + escapeHtml(inv.date_found || "—") + '</div>';
    html += '</div>';

    if (inv.linkedin_url) {
      html += '<div class="detail-field">';
      html += '<div class="detail-label">LinkedIn</div>';
      html += '<div class="detail-value"><a href="' + escapeHtml(inv.linkedin_url) + '" target="_blank" rel="noopener noreferrer">' + escapeHtml(inv.linkedin_url) + '</a></div>';
      html += '</div>';
    }

    html += '<div class="detail-field" style="margin-top:8px;">';
    html += '<button class="btn btn-primary btn-sm" onclick="researchInvestor(' + inv.id + ', this)" style="width:100%;">Research Investor</button>';
    html += '</div>';

    html += '<div class="detail-field" id="research-result-' + inv.id + '" style="display:none;">';
    html += '<div class="detail-label">Investor Summary</div>';
    html += '<div class="detail-value" id="research-text-' + inv.id + '" style="white-space:pre-wrap;line-height:1.5;"></div>';
    html += '</div>';

    if (inv.context_quote) {
      html += '<div class="detail-field">';
      html += '<div class="detail-label">Context</div>';
      html += '<div class="detail-quote">' + escapeHtml(inv.context_quote) + '</div>';
      html += '</div>';
    }

    detailBody.innerHTML = html;

    detailOverlay.classList.add("open");
  } catch (err) {
    console.error("Failed to fetch investor detail:", err);
    showToast("Failed to load investor details.");
  }
}

function closeDetail() {
  detailOverlay.classList.remove("open");
}

/* ─── Sorting ─── */
document.querySelectorAll("th.sortable").forEach(function (th) {
  th.addEventListener("click", function () {
    var field = th.dataset.sort;
    if (sortBy === field) {
      sortDir = sortDir === "desc" ? "asc" : "desc";
    } else {
      sortBy = field;
      sortDir = "desc";
    }

    // Update sort indicators
    document.querySelectorAll("th.sortable").forEach(function (el) {
      el.classList.remove("active-sort");
      el.querySelector(".sort-arrow").textContent = "";
    });
    th.classList.add("active-sort");
    th.querySelector(".sort-arrow").textContent = sortDir === "desc" ? "↓" : "↑";

    currentPage = 1;
    fetchInvestors();
  });
});

/* ─── Export CSV ─── */
async function exportCSV() {
  try {
    var params = new URLSearchParams();
    params.set("per_page", "100");
    params.set("sort_by", sortBy);
    params.set("sort_dir", sortDir);

    var search = searchInput.value.trim();
    if (search) { params.set("search", search); }
    var sector = filterSector.value;
    if (sector) { params.set("sector", sector); }

    var allInvestors = [];
    var page = 1;
    var hasMore = true;

    while (hasMore) {
      params.set("page", page);
      var res = await fetch(API + "/api/investors?" + params.toString());
      var data = await res.json();
      allInvestors = allInvestors.concat(data.investors);
      hasMore = page < data.total_pages;
      page++;
    }

    // Build CSV
    var headers = ["Name", "Role", "Company", "Issuer", "Sector", "Amount", "Source", "Source URL", "Source Type", "Date Found", "LinkedIn"];
    var rows = allInvestors.map(function (inv) {
      return [
        csvEscape(inv.name),
        csvEscape(inv.role),
        csvEscape(inv.company),
        csvEscape(inv.eis_company),
        csvEscape(inv.sector),
        csvEscape(inv.amount),
        csvEscape(inv.source_name),
        csvEscape(inv.source_url),
        csvEscape(inv.source_type),
        csvEscape(inv.date_found),
        csvEscape(inv.linkedin_url)
      ].join(",");
    });

    var csv = headers.join(",") + "\n" + rows.join("\n");
    var blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    var a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "eis_investors_" + new Date().toISOString().slice(0, 10) + ".csv";
    a.click();
    URL.revokeObjectURL(a.href);

    showToast("CSV exported with " + allInvestors.length + " records.");
  } catch (err) {
    console.error("Failed to export CSV:", err);
    showToast("Export failed.");
  }
}

function csvEscape(val) {
  if (!val) { return ""; }
  var str = String(val);
  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

/* ─── Export Excel ─── */
async function exportExcel() {
  try {
    exportExcelBtn.disabled = true;
    showToast("Generating Excel file…");

    var params = new URLSearchParams();
    params.set("sort_by", sortBy);
    params.set("sort_dir", sortDir);

    var search = searchInput.value.trim();
    if (search) { params.set("search", search); }
    var sector = filterSector.value;
    if (sector) { params.set("sector", sector); }
    var dateFrom = filterDateFrom.value;
    if (dateFrom) { params.set("date_from", dateFrom); }
    var dateTo = filterDateTo.value;
    if (dateTo) { params.set("date_to", dateTo); }

    var res = await fetch(API + "/api/export/excel?" + params.toString());
    if (!res.ok) {
      throw new Error("Server returned " + res.status);
    }

    var blob = await res.blob();
    var a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "eis_investors_" + new Date().toISOString().slice(0, 10) + ".xlsx";
    a.click();
    URL.revokeObjectURL(a.href);

    showToast("Excel file downloaded.");
    exportExcelBtn.disabled = false;
  } catch (err) {
    console.error("Failed to export Excel:", err);
    showToast("Excel export failed.");
    exportExcelBtn.disabled = false;
  }
}

/* ─── Export New (since last export) ─── */
async function exportNew() {
  try {
    exportNewBtn.disabled = true;
    showToast("Generating new investors export…");

    var res = await fetch(API + "/api/export/excel-new");
    if (res.status === 404) {
      showToast("No new investors since last export.");
      exportNewBtn.disabled = false;
      return;
    }
    if (!res.ok) {
      throw new Error("Server returned " + res.status);
    }

    var blob = await res.blob();
    var a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "eis_investors_new_" + new Date().toISOString().slice(0, 10) + ".xlsx";
    a.click();
    URL.revokeObjectURL(a.href);

    showToast("New investors exported.");
    exportNewBtn.disabled = false;
    updateExportNewBadge();
  } catch (err) {
    console.error("Failed to export new investors:", err);
    showToast("Export failed.");
    exportNewBtn.disabled = false;
  }
}

async function updateExportNewBadge() {
  try {
    var res = await fetch(API + "/api/export/last");
    var data = await res.json();
    var count = data.new_since_last_export || 0;
    exportNewBadge.textContent = count > 0 ? count : "";
  } catch (err) {
    exportNewBadge.textContent = "";
  }
}

/* ─── Run Collection (Scan) ─── */
let scanPollingInterval = null;

async function runCollection() {
  try {
    runCollectionBtn.disabled = true;
    setScanButtonState("scanning");

    var res = await fetch(API + "/api/scan", { method: "POST" });
    var data = await res.json();

    if (data.status === "already_running") {
      showToast("A scan is already running.");
      startScanPolling();
      return;
    }

    if (data.status === "started") {
      showToast("Scan started. Searching for EIS investors...", 5000);
      updateStatusRow("web", { running: true, phase: "searching", phase_detail: "Starting web search..." });
      startScanPolling();
    } else {
      showToast(data.message || "Failed to start scan.");
      runCollectionBtn.disabled = false;
      setScanButtonState("idle");
    }
  } catch (err) {
    console.error("Scan failed:", err);
    showToast("Failed to start scan.");
    runCollectionBtn.disabled = false;
    setScanButtonState("idle");
  }
}

function startScanPolling() {
  if (scanPollingInterval) clearInterval(scanPollingInterval);
  scanPollingInterval = setInterval(pollScanStatus, 2000);
}

async function pollScanStatus() {
  try {
    var res = await fetch(API + "/api/scan/status");
    var status = await res.json();

    // Update button text with phase info
    updateScanProgress(status);

    if (!status.running) {
      clearInterval(scanPollingInterval);
      scanPollingInterval = null;
      runCollectionBtn.disabled = false;
      setScanButtonState("idle");

      if (status.phase === "done") {
        var msg = status.phase_detail || (status.results_saved > 0
          ? status.results_saved + " new investor(s) added."
          : "Scan complete. No new investors found.");
        showToast(msg, 8000);
        // Show diagnostic log in UI
        showScanLog(status);
        // Refresh the table and stats
        fetchStats();
        fetchInvestors();
        updateExportNewBadge();
        fetchScanHistory();
      } else if (status.phase === "error") {
        showToast("Scan error: " + (status.error || "Unknown error"), 5000);
        showScanLog(status);
      }
    }
  } catch (err) {
    console.error("Polling error:", err);
  }
}

function setScanButtonState(state) {
  var btnSpan = runCollectionBtn.querySelector("span");
  var btnSvg = runCollectionBtn.querySelector("svg");
  if (state === "scanning") {
    if (btnSpan) btnSpan.textContent = "Scanning...";
    runCollectionBtn.classList.add("scanning");
    if (btnSvg) btnSvg.style.animation = "spin 1s linear infinite";
  } else {
    if (btnSpan) btnSpan.textContent = "Run Collection";
    runCollectionBtn.classList.remove("scanning");
    if (btnSvg) btnSvg.style.animation = "";
  }
}

function updateScanProgress(status) {
  var btnSpan = runCollectionBtn.querySelector("span");
  if (!btnSpan) return;
  if (status.phase === "searching") {
    btnSpan.textContent = "Searching...";
  } else if (status.phase === "extracting") {
    btnSpan.textContent = "Analyzing...";
  } else if (status.phase === "saving") {
    btnSpan.textContent = "Saving...";
  }
  updateStatusRow("web", status);
}

function updateStatusRow(scanType, status) {
  var row = scanRows[scanType];
  if (!row || !row.bar) return;

  row.bar.classList.remove("active", "error", "success");

  if (status.running) {
    row.bar.classList.add("active");
    row.text.textContent = status.phase_detail || "In progress...";
  } else if (status.phase === "done") {
    row.bar.classList.add("success");
    row.text.textContent = status.phase_detail || "Complete.";
    setTimeout(function() {
      row.bar.classList.remove("success");
      row.text.textContent = "Ready";
    }, 30000);
  } else if (status.phase === "error") {
    row.bar.classList.add("error");
    row.text.textContent = "Error: " + (status.phase_detail || status.error || "Unknown");
    setTimeout(function() {
      row.bar.classList.remove("error");
      row.text.textContent = "Ready";
    }, 30000);
  } else {
    row.text.textContent = "Ready";
  }
}


/* ─── Investor Research ─── */
async function researchInvestor(investorId, btn) {
  btn.textContent = "Researching...";
  btn.disabled = true;
  try {
    var res = await fetch(API + "/api/investor/" + investorId + "/research");
    var data = await res.json();
    var resultDiv = document.getElementById("research-result-" + investorId);
    var textDiv = document.getElementById("research-text-" + investorId);
    if (resultDiv && textDiv && data.summary) {
      textDiv.textContent = data.summary;
      resultDiv.style.display = "";
      btn.textContent = "Refresh Research";
      btn.disabled = false;
    } else {
      btn.textContent = "No results";
      btn.disabled = false;
    }
  } catch (err) {
    btn.textContent = "Research failed";
    btn.disabled = false;
  }
}

/* ─── Scan Log Display ─── */
function showScanLog(status) {
  // Remove any existing log panel
  var existing = document.getElementById("scan-log-panel");
  if (existing) existing.remove();

  if (!status.log || status.log.length === 0) return;

  var panel = document.createElement("div");
  panel.id = "scan-log-panel";
  panel.style.cssText = "position:fixed; bottom:20px; right:20px; width:480px; max-height:400px; background:var(--color-bg-card); border:1px solid var(--color-border); border-radius:12px; box-shadow:0 8px 32px rgba(0,0,0,0.3); z-index:1000; display:flex; flex-direction:column; font-family:var(--font-mono, monospace); font-size:12px;";

  var header = document.createElement("div");
  header.style.cssText = "display:flex; justify-content:space-between; align-items:center; padding:12px 16px; border-bottom:1px solid var(--color-border);";
  header.innerHTML = '<strong style="color:var(--color-text);">Scan Diagnostics</strong>';

  var summary = document.createElement("span");
  summary.style.cssText = "color:var(--color-text-faint); font-size:11px;";
  summary.textContent = "Results found: " + (status.results_found || 0) + " | Saved: " + (status.results_saved || 0) + " | Dupes: " + (status.results_duplicate || 0);
  header.appendChild(summary);

  var closeBtn = document.createElement("button");
  closeBtn.textContent = "✕";
  closeBtn.style.cssText = "background:none; border:none; color:var(--color-text-faint); cursor:pointer; font-size:16px; padding:0 0 0 12px;";
  closeBtn.onclick = function() { panel.remove(); };
  header.appendChild(closeBtn);

  var body = document.createElement("div");
  body.style.cssText = "overflow-y:auto; padding:12px 16px; flex:1;";

  var logHtml = "";
  status.log.forEach(function(line) {
    var color = "var(--color-text-faint)";
    if (line.includes("error") || line.includes("Error") || line.includes("blocked") || line.includes("NOT") || line.includes("not set")) {
      color = "#ef4444";
    } else if (line.includes("Found") || line.includes("yes") || line.includes("present")) {
      color = "#22c55e";
    } else if (line.includes("returned 0") || line.includes("no results") || line.includes("rate-limited")) {
      color = "#f59e0b";
    }
    logHtml += '<div style="color:' + color + '; padding:2px 0; line-height:1.5;">' + escapeHtml(line) + '</div>';
  });

  body.innerHTML = logHtml;
  panel.appendChild(header);
  panel.appendChild(body);
  document.body.appendChild(panel);

  // Auto-dismiss after 60 seconds
  setTimeout(function() { if (document.getElementById("scan-log-panel")) panel.remove(); }, 60000);
}

/* ─── Helpers ─── */
function escapeHtml(str) {
  if (!str) { return ""; }
  var div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

/* ─── Event Listeners ─── */
searchInput.addEventListener("input", function () {
  clearTimeout(searchTimeout);
  searchTimeout = setTimeout(function () {
    currentPage = 1;
    fetchInvestors();
  }, 300);
});

filterSector.addEventListener("change", function () {
  currentPage = 1;
  fetchInvestors();
});

filterDateFrom.addEventListener("change", function () {
  currentPage = 1;
  fetchInvestors();
});

filterDateTo.addEventListener("change", function () {
  currentPage = 1;
  fetchInvestors();
});

filterOrigin.addEventListener("change", function () {
  currentPage = 1;
  fetchInvestors();
});

filterEntityType.addEventListener("change", function () {
  currentPage = 1;
  fetchInvestors();
});

filterHolding.addEventListener("change", function () {
  currentPage = 1;
  fetchInvestors();
});

prevBtn.addEventListener("click", function () {
  if (currentPage > 1) {
    currentPage--;
    fetchInvestors();
  }
});

nextBtn.addEventListener("click", function () {
  if (currentPage < totalPages) {
    currentPage++;
    fetchInvestors();
  }
});

detailClose.addEventListener("click", closeDetail);
detailOverlay.addEventListener("click", function (e) {
  if (e.target === detailOverlay) {
    closeDetail();
  }
});

exportBtn.addEventListener("click", exportCSV);
exportExcelBtn.addEventListener("click", exportExcel);
exportNewBtn.addEventListener("click", exportNew);
runCollectionBtn.addEventListener("click", runCollection);
if (runChBtn) runChBtn.addEventListener("click", runChScan);

// Close detail on Escape
document.addEventListener("keydown", function (e) {
  if (e.key === "Escape" && detailOverlay.classList.contains("open")) {
    closeDetail();
  }
});

/* ─── Animations ─── */
var animStyle = document.createElement("style");
animStyle.textContent = "@keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } } @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }";
document.head.appendChild(animStyle);

/* ─── Init ─── */
fetchStats();
fetchInvestors();
updateExportNewBadge();

/* ─── Companies House Scan ─── */
let chPollingInterval = null;

async function runChScan() {
  try {
    runChBtn.disabled = true;
    var btnSpan = runChBtn.querySelector("span");
    var btnSvg = runChBtn.querySelector("svg");
    if (btnSpan) btnSpan.textContent = "Scanning...";
    if (btnSvg) btnSvg.style.animation = "spin 1s linear infinite";

    var recentCH = document.getElementById("recent-toggle").checked;
    var res = await fetch(API + "/api/ch-scan?recent_only=" + recentCH, { method: "POST" });
    var data = await res.json();

    if (data.status === "already_running") {
      showToast("Companies House scan already running.");
      startChPolling();
      return;
    }

    if (data.status === "started") {
      showToast("Companies House scan started...", 5000);
      updateStatusRow("ch", { running: true, phase: "searching", phase_detail: "Starting Companies House scan..." });
      startChPolling();
    } else {
      showToast(data.message || "Failed to start CH scan.");
      resetChButton();
    }
  } catch (err) {
    console.error("CH scan failed:", err);
    showToast("Failed to start Companies House scan.");
    resetChButton();
  }
}

function startChPolling() {
  if (chPollingInterval) clearInterval(chPollingInterval);
  chPollingInterval = setInterval(pollChStatus, 2000);
}

async function pollChStatus() {
  try {
    var res = await fetch(API + "/api/ch-scan/status");
    var status = await res.json();

    // Update status bar
    updateStatusRow("ch", status);

    if (!status.running) {
      clearInterval(chPollingInterval);
      chPollingInterval = null;
      resetChButton();

      if (status.phase === "done") {
        showToast(status.phase_detail || "Companies House scan complete.", 8000);
        showScanLog(status);
        fetchStats();
        fetchInvestors();
        updateExportNewBadge();
        fetchScanHistory();
      } else if (status.phase === "error") {
        showToast("CH scan error: " + (status.error || "Unknown error"), 5000);
        showScanLog(status);
      }
    }
  } catch (err) {
    console.error("CH polling error:", err);
  }
}

function resetChButton() {
  if (!runChBtn) return;
  runChBtn.disabled = false;
  var btnSpan = runChBtn.querySelector("span");
  var btnSvg = runChBtn.querySelector("svg");
  if (btnSpan) btnSpan.textContent = "Companies House";
  if (btnSvg) btnSvg.style.animation = "";
}

/* ─── TR1 Direct (Investegate Sequential) ─── */
var runDirectBtn = document.getElementById("run-direct-btn");
var stopDirectBtn = document.getElementById("stop-direct-btn");
var tr1McapInput = document.getElementById("tr1-mcap");
let directPollingInterval = null;

if (runDirectBtn) {
  runDirectBtn.addEventListener("click", async function () {
    try {
      runDirectBtn.disabled = true;
      runDirectBtn.style.display = "none";
      stopDirectBtn.style.display = "";
      var sp = runDirectBtn.querySelector("span");
      var sv = runDirectBtn.querySelector("svg");
      if (sp) sp.textContent = "Scanning...";
      if (sv) sv.style.animation = "spin 1s linear infinite";

      var mcap = parseInt(tr1McapInput.value) || 0;
      var recentTR1 = document.getElementById("recent-toggle").checked;
      var url = API + "/api/tr1-direct?max_market_cap=" + mcap + "&recent_only=" + recentTR1;
      var res = await fetch(url, { method: "POST" });
      var data = await res.json();

      if (data.status === "already_running") {
        showToast("TR1 scan already running.");
        startDirectPolling();
        return;
      }
      if (data.status === "started") {
        showToast(data.message || "TR1 scan started...", 5000);
        updateStatusRow("tr1", { running: true, phase: "scanning", phase_detail: "Starting TR1 scan..." });
        startDirectPolling();
      } else {
        showToast(data.message || "Failed to start.");
        resetDirectButton();
      }
    } catch (err) {
      showToast("Failed to start TR1 scan.");
      resetDirectButton();
    }
  });
}

if (stopDirectBtn) {
  stopDirectBtn.addEventListener("click", async function () {
    try {
      var res = await fetch(API + "/api/tr1-direct/stop", { method: "POST" });
      var data = await res.json();
      showToast(data.message || "Stopping...", 3000);
      var sp = stopDirectBtn.querySelector("span");
      if (sp) sp.textContent = "Stopping...";
      stopDirectBtn.disabled = true;
    } catch (err) {
      showToast("Failed to stop.");
    }
  });
}

var directPollCount = 0;

function startDirectPolling() {
  if (directPollingInterval) clearInterval(directPollingInterval);
  directPollCount = 0;
  directPollingInterval = setInterval(pollDirectStatus, 2000);
}

async function pollDirectStatus() {
  directPollCount++;
  try {
    var res = await fetch(API + "/api/tr1-direct/status");
    var status = await res.json();

    // Always update status bar if scan has started (phase != idle)
    if (status.phase !== "idle") {
      updateStatusRow("tr1", status);
    }

    if (!status.running && directPollCount > 3) {
      // Only stop polling after at least 3 polls (give scan time to start)
      clearInterval(directPollingInterval);
      directPollingInterval = null;
      resetDirectButton();
      if (status.phase === "done") {
        showToast(status.phase_detail || "TR1 scan complete.", 8000);
        showScanLog(status);
        fetchStats();
        fetchInvestors();
        updateExportNewBadge();
        fetchScanHistory();
      } else if (status.phase === "error") {
        showToast("TR1 scan error: " + (status.error || "Unknown"), 5000);
        showScanLog(status);
      }
    }
  } catch (err) {}
}

function resetDirectButton() {
  if (!runDirectBtn) return;
  runDirectBtn.disabled = false;
  runDirectBtn.style.display = "";
  stopDirectBtn.style.display = "none";
  stopDirectBtn.disabled = false;
  var sp = stopDirectBtn.querySelector("span");
  if (sp) sp.textContent = "Stop";
  var sp2 = runDirectBtn.querySelector("span");
  var sv2 = runDirectBtn.querySelector("svg");
  if (sp2) sp2.textContent = "TR1 Scan";
  if (sv2) sv2.style.animation = "";
}

/* ─── TR1 Sweep ─── */
var runSweepBtn = document.getElementById("run-sweep-btn");
let sweepPollingInterval = null;

if (runSweepBtn) {
  runSweepBtn.addEventListener("click", async function () {
    try {
      runSweepBtn.disabled = true;
      var sp = runSweepBtn.querySelector("span");
      var sv = runSweepBtn.querySelector("svg");
      if (sp) sp.textContent = "Sweeping...";
      if (sv) sv.style.animation = "spin 1s linear infinite";

      var res = await fetch(API + "/api/tr1-sweep", { method: "POST" });
      var data = await res.json();

      if (data.status === "already_running") {
        showToast("TR1 sweep already running.");
        startSweepPolling();
        return;
      }
      if (data.status === "started") {
        showToast("TR1 company sweep started (200 companies per batch)...", 5000);
        updateStatusRow("tr1", { running: true, phase: "searching", phase_detail: "Starting TR1 company sweep..." });
        startSweepPolling();
      } else {
        showToast(data.message || "Failed to start sweep.");
        resetSweepButton();
      }
    } catch (err) {
      showToast("Failed to start TR1 sweep.");
      resetSweepButton();
    }
  });
}

function startSweepPolling() {
  if (sweepPollingInterval) clearInterval(sweepPollingInterval);
  sweepPollingInterval = setInterval(pollSweepStatus, 3000);
}

async function pollSweepStatus() {
  try {
    var res = await fetch(API + "/api/tr1-sweep/status");
    var status = await res.json();
    updateStatusRow("tr1", status);

    if (!status.running) {
      clearInterval(sweepPollingInterval);
      sweepPollingInterval = null;
      resetSweepButton();
      if (status.phase === "done") {
        showToast(status.phase_detail || "TR1 sweep batch complete.", 8000);
        showScanLog(status);
        fetchStats();
        fetchInvestors();
        updateExportNewBadge();
        fetchScanHistory();
      } else if (status.phase === "error") {
        showToast("Sweep error: " + (status.error || "Unknown"), 5000);
        showScanLog(status);
      }
    }
  } catch (err) {}
}

function resetSweepButton() {
  if (!runSweepBtn) return;
  runSweepBtn.disabled = false;
  var sp = runSweepBtn.querySelector("span");
  var sv = runSweepBtn.querySelector("svg");
  if (sp) sp.textContent = "TR1 Sweep";
  if (sv) sv.style.animation = "";
}

/* ─── Daily Update Toggle ─── */
var dailyBtn = document.getElementById("daily-toggle-btn");
var dailyEnabled = false;

async function fetchDailyStatus() {
  try {
    var res = await fetch(API + "/api/daily/status");
    var data = await res.json();
    dailyEnabled = data.enabled;
    updateDailyButton();
    if (data.enabled && data.status && data.status.startsWith("running")) {
      var dailyScanType = data.status.replace("running_", "");
      updateStatusRow(dailyScanType === "web" ? "web" : dailyScanType === "ch" ? "ch" : "tr1", { running: true, phase: "extracting", phase_detail: "Daily: " + dailyScanType + " scan in progress..." });
    }
  } catch (e) {}
}

function updateDailyButton() {
  if (!dailyBtn) return;
  var span = dailyBtn.querySelector("span");
  if (dailyEnabled) {
    if (span) span.textContent = "Daily On";
    dailyBtn.classList.add("btn-primary");
    dailyBtn.title = "Daily auto-scan enabled (8am BST). Click to disable.";
  } else {
    if (span) span.textContent = "Daily Off";
    dailyBtn.classList.remove("btn-primary");
    dailyBtn.title = "Enable daily auto-scan at 8am BST";
  }
}

if (dailyBtn) {
  dailyBtn.addEventListener("click", async function () {
    try {
      var endpoint = dailyEnabled ? "/api/daily/disable" : "/api/daily/enable";
      var res = await fetch(API + endpoint, { method: "POST" });
      var data = await res.json();
      dailyEnabled = data.status === "enabled";
      updateDailyButton();
      if (dailyEnabled) {
        showToast("Daily auto-scan enabled. Next run: 8:00 AM BST", 5000);
      } else {
        showToast("Daily auto-scan disabled.", 3000);
      }
    } catch (err) {
      showToast("Failed to toggle daily scan.", 3000);
    }
  });
}

fetchDailyStatus();

/* ─── LinkedIn Enrichment ─── */
var runEnrichBtn = document.getElementById("run-enrich-btn");
let enrichPollingInterval = null;

if (runEnrichBtn) {
  runEnrichBtn.addEventListener("click", async function () {
    try {
      runEnrichBtn.disabled = true;
      var sp = runEnrichBtn.querySelector("span");
      var sv = runEnrichBtn.querySelector("svg");
      if (sp) sp.textContent = "Enriching...";
      if (sv) sv.style.animation = "spin 1s linear infinite";

      var res = await fetch(API + "/api/enrich", { method: "POST" });
      var data = await res.json();

      if (data.status === "already_running") {
        showToast("Enrichment already running.");
        startEnrichPolling();
        return;
      }
      if (data.status === "started") {
        showToast("LinkedIn enrichment started...", 5000);
        updateStatusRow("enrich", { running: true, phase: "enriching", phase_detail: "Starting LinkedIn enrichment..." });
        startEnrichPolling();
      } else {
        showToast(data.message || "Failed to start.");
        resetEnrichButton();
      }
    } catch (err) {
      showToast("Failed to start enrichment.");
      resetEnrichButton();
    }
  });
}

function startEnrichPolling() {
  if (enrichPollingInterval) clearInterval(enrichPollingInterval);
  enrichPollingInterval = setInterval(pollEnrichStatus, 2000);
}

async function pollEnrichStatus() {
  try {
    var res = await fetch(API + "/api/enrich/status");
    var status = await res.json();
    updateStatusRow("enrich", status);

    if (!status.running) {
      clearInterval(enrichPollingInterval);
      enrichPollingInterval = null;
      resetEnrichButton();
      if (status.phase === "done") {
        showToast(status.phase_detail || "Enrichment complete.", 8000);
        showScanLog(status);
        fetchStats();
        fetchInvestors();
        fetchScanHistory();
      } else if (status.phase === "error") {
        showToast("Enrichment error: " + (status.error || "Unknown"), 5000);
        showScanLog(status);
      }
    }
  } catch (err) {}
}

function resetEnrichButton() {
  if (!runEnrichBtn) return;
  runEnrichBtn.disabled = false;
  var sp = runEnrichBtn.querySelector("span");
  var sv = runEnrichBtn.querySelector("svg");
  if (sp) sp.textContent = "LinkedIn";
  if (sv) sv.style.animation = "";
}

/* ─── Scan History Dates ─── */
async function fetchScanHistory() {
  try {
    var res = await fetch(API + "/api/scan-history");
    var data = await res.json();
    var fmt = function(iso) {
      if (!iso) return "--";
      var d = new Date(iso);
      return d.toLocaleDateString("en-GB", { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
    };
    var el;
    el = document.getElementById("last-web");
    if (el) el.textContent = data.web ? fmt(data.web.last_run) : "Never";
    el = document.getElementById("last-ch");
    if (el) el.textContent = data.ch ? fmt(data.ch.last_run) : "Never";
    el = document.getElementById("last-tr1");
    if (el) el.textContent = data.tr1 ? fmt(data.tr1.last_run) : "Never";
    el = document.getElementById("last-enrich");
    if (el) el.textContent = data.enrich ? fmt(data.enrich.last_run) : "Never";
  } catch (e) {}
}
fetchScanHistory();

/* ─── Excel Import ─── */
var importFileInput = document.getElementById("import-file-input");
var importBtn = document.getElementById("import-btn");

if (importFileInput) {
  importFileInput.addEventListener("change", async function () {
    var file = importFileInput.files[0];
    if (!file) return;

    var importSpan = importBtn.querySelector("span");
    if (importSpan) importSpan.textContent = "Importing...";
    importBtn.style.pointerEvents = "none";
    importBtn.style.opacity = "0.6";

    try {
      var formData = new FormData();
      formData.append("file", file);

      var res = await fetch(API + "/api/import/excel", {
        method: "POST",
        body: formData,
      });
      var data = await res.json();

      if (res.ok) {
        showToast(data.message || "Import complete.", 8000);
        fetchStats();
        fetchInvestors();
        updateExportNewBadge();
        fetchScanHistory();
      } else {
        showToast("Import error: " + (data.detail || "Unknown error"), 5000);
      }
    } catch (err) {
      console.error("Import failed:", err);
      showToast("Import failed. Check the file format.", 5000);
    }

    // Reset
    if (importSpan) importSpan.textContent = "Import";
    importBtn.style.pointerEvents = "";
    importBtn.style.opacity = "";
    importFileInput.value = "";  // allow re-selecting same file
  });
}

/* ─── TR1 Filings Scan ─── */
let tr1PollingInterval = null;

async function runTr1Scan() {
  try {
    runTr1Btn.disabled = true;
    var btnSpan = runTr1Btn.querySelector("span");
    var btnSvg = runTr1Btn.querySelector("svg");
    if (btnSpan) btnSpan.textContent = "Scanning...";
    if (btnSvg) btnSvg.style.animation = "spin 1s linear infinite";

    var daysBack = document.getElementById("tr1-days").value || 30;
    var res = await fetch(API + "/api/tr1-scan?days_back=" + daysBack, { method: "POST" });
    var data = await res.json();

    if (data.status === "already_running") {
      showToast("TR1 scan already running.");
      startTr1Polling();
      return;
    }

    if (data.status === "started") {
      showToast("TR1 filings scan started...", 5000);
      updateStatusRow("tr1", { running: true, phase: "searching", phase_detail: "Starting TR1 announcement scan..." });
      startTr1Polling();
    } else {
      showToast(data.message || "Failed to start TR1 scan.");
      resetTr1Button();
    }
  } catch (err) {
    console.error("TR1 scan failed:", err);
    showToast("Failed to start TR1 scan.");
    resetTr1Button();
  }
}

function startTr1Polling() {
  if (tr1PollingInterval) clearInterval(tr1PollingInterval);
  tr1PollingInterval = setInterval(pollTr1Status, 2000);
}

async function pollTr1Status() {
  try {
    var res = await fetch(API + "/api/tr1-scan/status");
    var status = await res.json();
    updateStatusRow("tr1", status);

    if (!status.running) {
      clearInterval(tr1PollingInterval);
      tr1PollingInterval = null;
      resetTr1Button();

      if (status.phase === "done") {
        showToast(status.phase_detail || "TR1 scan complete.", 8000);
        showScanLog(status);
        fetchStats();
        fetchInvestors();
        updateExportNewBadge();
        fetchScanHistory();
      } else if (status.phase === "error") {
        showToast("TR1 scan error: " + (status.error || "Unknown error"), 5000);
        showScanLog(status);
      }
    }
  } catch (err) {
    console.error("TR1 polling error:", err);
  }
}

function resetTr1Button() {
  if (!runTr1Btn) return;
  runTr1Btn.disabled = false;
  var btnSpan = runTr1Btn.querySelector("span");
  var btnSvg = runTr1Btn.querySelector("svg");
  if (btnSpan) btnSpan.textContent = "TR1 Filings";
  if (btnSvg) btnSvg.style.animation = "";
}

// Check if any scan is already running on page load
(async function checkRunningStatus() {
  try {
    var res = await fetch(API + "/api/scan/status");
    var status = await res.json();
    if (status.running) {
      setScanButtonState("scanning");
      runCollectionBtn.disabled = true;
      updateStatusRow("web", status);
      startScanPolling();
    }
  } catch (e) {}
  try {
    var res2 = await fetch(API + "/api/ch-scan/status");
    var chStatus = await res2.json();
    if (chStatus.running) {
      if (runChBtn) {
        runChBtn.disabled = true;
        var s = runChBtn.querySelector("span");
        var v = runChBtn.querySelector("svg");
        if (s) s.textContent = "Scanning...";
        if (v) v.style.animation = "spin 1s linear infinite";
      }
      updateStatusRow("ch", chStatus);
      startChPolling();
    }
  } catch (e) {}
  try {
    var res3 = await fetch(API + "/api/tr1-direct/status");
    var tr1Status = await res3.json();
    if (tr1Status.running) {
      runDirectBtn.disabled = true;
      runDirectBtn.style.display = "none";
      stopDirectBtn.style.display = "";
      updateStatusRow("tr1", tr1Status);
      startTr1Polling();
    }
  } catch (e) {}
})();
