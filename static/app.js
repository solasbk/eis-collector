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
const filterSource = document.getElementById("filter-source");
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
const toast = document.getElementById("toast");

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
    document.getElementById("stat-top-sector").textContent = data.top_sector;
    animateNumber("stat-sources", data.sources_scanned);

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
  // Source types
  filterSource.innerHTML = '<option value="">All Sources</option>';
  sourceTypes.forEach(function (st) {
    var opt = document.createElement("option");
    opt.value = st;
    opt.textContent = st;
    filterSource.appendChild(opt);
  });

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

  var source = filterSource.value;
  if (source) { params.set("source_type", source); }

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
    html += '<div class="detail-label">EIS Company</div>';
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
      html += '<div class="detail-value"><a href="' + escapeHtml(inv.linkedin_url) + '" target="_blank" rel="noopener noreferrer">View Profile</a></div>';
      html += '</div>';
    }

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
    var source = filterSource.value;
    if (source) { params.set("source_type", source); }
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
    var headers = ["Name", "Role", "Company", "EIS Company", "Sector", "Amount", "Source", "Source URL", "Source Type", "Date Found", "LinkedIn"];
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
    var source = filterSource.value;
    if (source) { params.set("source_type", source); }
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
        // Log diagnostics to console
        if (status.log && status.log.length > 0) {
          console.log("[Scan Log]", status.log.join("\n"));
        }
        // Refresh the table and stats
        fetchStats();
        fetchInvestors();
        updateExportNewBadge();
      } else if (status.phase === "error") {
        showToast("Scan error: " + (status.error || "Unknown error"), 5000);
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

filterSource.addEventListener("change", function () {
  currentPage = 1;
  fetchInvestors();
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
