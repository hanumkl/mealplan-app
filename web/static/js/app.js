/* Stage 1 frontend: household setup + catalog + pipeline status.
   Plain ES modules, no build step - Databricks Apps runs `python app.py`
   and there's no npm available in the runtime. */

const HOUSEHOLD_ID = 1;

/* ---------------------------------------------------------------- utils */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  if (res.status === 204) return null;
  const text = await res.text();

  // A proxy or error page can return HTML/plain text where we expect JSON.
  // Parsing blindly would throw an opaque SyntaxError, so surface what
  // actually came back instead.
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      throw new Error(`Expected JSON from ${path}, got: ${text.slice(0, 80)}`);
    }
  }

  if (!res.ok) throw new Error(data?.error || `${res.status} ${res.statusText}`);
  return data;
}

function toast(message, kind = "ok") {
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.textContent = message;
  $("#toasts").appendChild(el);
  setTimeout(() => el.remove(), 3800);
}

const num = (v, digits = 0) =>
  v === null || v === undefined ? "—" : Number(v).toFixed(digits);

const RESTRICTION_LABELS = {
  halal: "Halal", vegetarian: "Vegetarian", vegan: "Vegan", no_pork: "No pork",
  no_alcohol: "No alcohol", lactose_free: "Lactose free", gluten_free: "Gluten free",
  nut_allergy: "Nut allergy", shellfish_allergy: "Shellfish allergy",
  egg_allergy: "Egg allergy", low_spice: "Low spice",
};

const SOURCE_COLORS = {
  receipt: "var(--green)",
  lidl_scrape: "var(--blue)",
  open_prices: "var(--amber)",
  manual_survey: "var(--text-faint)",
};

/* ------------------------------------------------------------ navigation */

$$(".nav-item[data-view]").forEach((btn) => {
  btn.addEventListener("click", () => {
    $$(".nav-item").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    const view = btn.dataset.view;
    ["household", "catalog", "pipeline"].forEach((v) =>
      $(`#view-${v}`).classList.toggle("hidden", v !== view));
    if (view === "catalog") loadCatalog();
    if (view === "pipeline") loadPipeline();
  });
});

$$("[data-close]").forEach((btn) =>
  btn.addEventListener("click", () => btn.closest("dialog").close()));

/* ------------------------------------------------------------- household */

let members = [];

async function loadHousehold() {
  try {
    members = await api(`/api/households/${HOUSEHOLD_ID}/members`);
  } catch (err) {
    $("#members-grid").innerHTML = `
      <div class="empty">
        <div class="empty-icon">🔌</div>
        <div class="empty-title">Can't reach Lakebase</div>
        <div class="empty-sub">${esc(err.message)}<br>
          Check that the SQL files in <span class="mono">sql/</span> have been run
          and <span class="mono">LAKEBASE_URL</span> is set.</div>
      </div>`;
    return;
  }
  renderMembers();
  loadConstraints();
}

function renderMembers() {
  const grid = $("#members-grid");
  if (!members.length) {
    grid.innerHTML = `
      <div class="empty">
        <div class="empty-icon">👋</div>
        <div class="empty-title">No members yet</div>
        <div class="empty-sub">Add everyone you cook for. Each person gets their own
          calorie and protein target, and their own dietary restrictions.</div>
      </div>`;
    return;
  }

  grid.innerHTML = members.map((m) => {
    const initials = m.name.trim().slice(0, 2).toUpperCase();
    const age = m.birth_year ? `${new Date().getFullYear() - m.birth_year}y` : null;
    const meta = [age, m.weight_kg && `${num(m.weight_kg, 1)} kg`,
                  m.height_cm && `${num(m.height_cm, 0)} cm`,
                  m.activity_level?.replace("_", " ")].filter(Boolean).join(" · ");

    const macros = m.goal_type
      ? `<div class="macro-row">
           <div class="macro"><div class="macro-val">${num(m.target_kcal)}</div>
             <div class="macro-key">kcal</div></div>
           <div class="macro"><div class="macro-val">${num(m.target_protein_g)}</div>
             <div class="macro-key">protein</div></div>
           <div class="macro"><div class="macro-val">${num(m.target_carb_g)}</div>
             <div class="macro-key">carbs</div></div>
           <div class="macro"><div class="macro-val">${num(m.target_fat_g)}</div>
             <div class="macro-key">fat</div></div>
         </div>`
      : `<div class="macro-row"><div class="macro-empty" style="grid-column:1/-1">
           No targets set yet</div></div>`;

    const restrictionBadges = m.restrictions.map((r) => `
      <span class="badge ${r.severity === "strict" ? "strict" : "soft"}">
        ${esc(RESTRICTION_LABELS[r.restriction] || r.restriction)}
        <span class="badge-x" data-del-restriction="${r.restriction_id}">×</span>
      </span>`).join("");

    return `
      <div class="member-card">
        <div class="member-top">
          <div class="avatar">${esc(initials)}</div>
          <div style="flex:1;min-width:0">
            <div class="member-name">${esc(m.name)}</div>
            <div class="member-meta">${esc(meta || m.role)}</div>
          </div>
          ${m.goal_type ? `<span class="badge goal">${esc(m.goal_type)}</span>` : ""}
        </div>

        ${macros}

        <div class="badges">
          ${restrictionBadges}
          <span class="badge" style="cursor:pointer" data-add-restriction="${m.member_id}">
            + restriction
          </span>
        </div>

        <div style="display:flex;gap:6px;margin-top:2px">
          <button class="btn btn-sm" data-goal="${m.member_id}">Set targets</button>
          <button class="btn btn-sm btn-ghost" data-edit="${m.member_id}">Edit</button>
          <button class="btn btn-sm btn-ghost btn-danger" style="margin-left:auto"
                  data-delete="${m.member_id}">Remove</button>
        </div>
      </div>`;
  }).join("");
}

async function loadConstraints() {
  const box = $("#constraint-summary");
  if (!members.length) { box.innerHTML = ""; return; }

  const c = await api(`/api/households/${HOUSEHOLD_ID}/constraints`);
  const t = c.daily_totals || {};

  const strictBadges = c.strict.map((s) =>
    `<span class="badge strict">${esc(RESTRICTION_LABELS[s.restriction] || s.restriction)}
      <span class="faint">· ${esc(s.members.join(", "))}</span></span>`).join("");
  const softBadges = c.preferences.map((s) =>
    `<span class="badge soft">${esc(RESTRICTION_LABELS[s.restriction] || s.restriction)}
      <span class="faint">· ${esc(s.members.join(", "))}</span></span>`).join("");

  box.innerHTML = `
    <div class="card">
      <div class="grid grid-3" style="margin-bottom:14px">
        <div class="stat">
          <div class="stat-label">Members</div>
          <div class="stat-value">${members.length}</div>
          <div class="stat-hint">${t.members_with_goals || 0} with targets set</div>
        </div>
        <div class="stat">
          <div class="stat-label">Daily energy</div>
          <div class="stat-value">${num(t.household_kcal)}</div>
          <div class="stat-hint">kcal across the household</div>
        </div>
        <div class="stat">
          <div class="stat-label">Daily protein</div>
          <div class="stat-value">${num(t.household_protein_g)}<span
            style="font-size:15px;color:var(--text-faint)"> g</span></div>
          <div class="stat-hint">drives the per-member add-ons</div>
        </div>
      </div>

      ${(strictBadges || softBadges) ? `
        <div style="display:flex;flex-direction:column;gap:8px">
          ${strictBadges ? `<div><div class="label" style="margin-bottom:5px">
            Hard constraints — every dish must satisfy these</div>
            <div class="badges">${strictBadges}</div></div>` : ""}
          ${softBadges ? `<div><div class="label" style="margin-bottom:5px">
            Preferences — soft, tradeable</div>
            <div class="badges">${softBadges}</div></div>` : ""}
        </div>` : ""}

      ${c.requires_split_protein ? `
        <div class="note info" style="margin-top:13px">
          <span>🍲</span>
          <div><strong>Split-protein mode.</strong> Someone in this household is
          vegetarian, so one pot can't feed everyone. Plans will use a vegetarian
          base dish with protein cooked separately per member — one cooking session,
          one pot plus one pan.</div>
        </div>` : ""}
    </div>`;
}

/* ----------------------------------------------------- member dialog */

let editingMemberId = null;

$("#btn-add-member").addEventListener("click", () => {
  editingMemberId = null;
  $("#member-dialog-title").textContent = "Add member";
  $("#member-form").reset();
  $("#member-dialog").showModal();
});

$("#members-grid").addEventListener("click", async (e) => {
  const t = e.target;

  if (t.dataset.edit) {
    const m = members.find((x) => x.member_id == t.dataset.edit);
    editingMemberId = m.member_id;
    $("#member-dialog-title").textContent = `Edit ${m.name}`;
    $("#m-name").value = m.name;
    $("#m-role").value = m.role || "adult";
    $("#m-sex").value = m.sex || "unspecified";
    $("#m-birth").value = m.birth_year ?? "";
    $("#m-weight").value = m.weight_kg ?? "";
    $("#m-height").value = m.height_cm ?? "";
    $("#m-activity").value = m.activity_level || "moderate";
    $("#member-dialog").showModal();
  }

  if (t.dataset.delete) {
    const m = members.find((x) => x.member_id == t.dataset.delete);
    if (!confirm(`Remove ${m.name} from the household?`)) return;
    await api(`/api/members/${m.member_id}`, { method: "DELETE" });
    toast(`Removed ${m.name}`);
    loadHousehold();
  }

  if (t.dataset.goal) {
    const m = members.find((x) => x.member_id == t.dataset.goal);
    goalMember = m;
    $("#goal-member-name").textContent = m.name;
    $("#g-type").value = m.goal_type || (m.role === "child" ? "growth" : "maintain");
    $("#g-kcal").value = m.target_kcal ?? "";
    $("#g-protein").value = m.target_protein_g ?? "";
    $("#g-carb").value = m.target_carb_g ?? "";
    $("#g-fat").value = m.target_fat_g ?? "";
    $("#calc-note").innerHTML = "";
    $("#goal-dialog").showModal();
  }

  if (t.dataset.addRestriction) {
    restrictionMember = members.find((x) => x.member_id == t.dataset.addRestriction);
    $("#restriction-member-name").textContent = restrictionMember.name;
    $("#restriction-form").reset();
    $("#restriction-dialog").showModal();
  }

  if (t.dataset.delRestriction) {
    await api(`/api/restrictions/${t.dataset.delRestriction}`, { method: "DELETE" });
    loadHousehold();
  }
});

$("#member-form").addEventListener("submit", async () => {
  const payload = {
    name: $("#m-name").value.trim(),
    role: $("#m-role").value,
    sex: $("#m-sex").value,
    birth_year: $("#m-birth").value ? Number($("#m-birth").value) : null,
    weight_kg: $("#m-weight").value ? Number($("#m-weight").value) : null,
    height_cm: $("#m-height").value ? Number($("#m-height").value) : null,
    activity_level: $("#m-activity").value,
  };
  try {
    if (editingMemberId) {
      await api(`/api/members/${editingMemberId}`, { method: "PATCH", body: payload });
      toast(`Updated ${payload.name}`);
    } else {
      await api(`/api/households/${HOUSEHOLD_ID}/members`, { method: "POST", body: payload });
      toast(`Added ${payload.name}`);
    }
    loadHousehold();
  } catch (err) {
    toast(err.message, "error");
  }
});

/* ------------------------------------------------------- goal dialog */

let goalMember = null;

$("#btn-calc").addEventListener("click", async () => {
  try {
    const r = await api(`/api/members/${goalMember.member_id}/suggest-targets`, {
      method: "POST",
      body: { goal_type: $("#g-type").value },
    });
    $("#g-kcal").value = r.target_kcal;
    $("#g-protein").value = r.target_protein_g;
    $("#g-carb").value = r.target_carb_g;
    $("#g-fat").value = r.target_fat_g;
    $("#calc-note").innerHTML = `
      <div class="note ${r.is_estimate_only ? "" : "info"}">
        <span>${r.is_estimate_only ? "⚠️" : "ℹ️"}</span>
        <div>BMR ${r.bmr} kcal · TDEE ${r.tdee} kcal at age ${r.age}.
        ${r.is_estimate_only
          ? "Mifflin-St Jeor is validated for adults, not children — treat this as rough guidance and adjust by hand."
          : "Mifflin-St Jeor equation. Edit any field before saving."}</div>
      </div>`;
    calculated = true;
  } catch (err) {
    $("#calc-note").innerHTML =
      `<div class="note"><span>⚠️</span><div>${esc(err.message)}</div></div>`;
  }
});

let calculated = false;

$("#goal-form").addEventListener("submit", async () => {
  try {
    await api(`/api/members/${goalMember.member_id}/goal`, {
      method: "PUT",
      body: {
        goal_type: $("#g-type").value,
        target_kcal: Number($("#g-kcal").value) || null,
        target_protein_g: Number($("#g-protein").value) || null,
        target_carb_g: Number($("#g-carb").value) || null,
        target_fat_g: Number($("#g-fat").value) || null,
        target_source: calculated ? "calculated" : "manual",
      },
    });
    calculated = false;
    toast(`Targets saved for ${goalMember.name}`);
    loadHousehold();
  } catch (err) {
    toast(err.message, "error");
  }
});

/* ------------------------------------------------ restriction dialog */

let restrictionMember = null;

$("#restriction-form").addEventListener("submit", async () => {
  try {
    await api(`/api/members/${restrictionMember.member_id}/restrictions`, {
      method: "POST",
      body: {
        restriction: $("#r-type").value,
        severity: $("#r-severity").value,
        note: $("#r-note").value.trim() || null,
      },
    });
    toast("Restriction added");
    loadHousehold();
  } catch (err) {
    toast(err.message, "error");
  }
});

/* --------------------------------------------------------------- catalog */

// Kept at module scope so the halal dialog can look a row up by id without
// re-fetching it.
let catalogRows = [];

async function loadCatalog() {
  const q = $("#ingredient-search").value.trim();
  const body = $("#catalog-body");
  body.innerHTML = `<div class="empty"><div class="empty-sub">Loading…</div></div>`;

  const rows = await api(`/api/ingredients?q=${encodeURIComponent(q)}&limit=100`);
  catalogRows = rows;

  if (!rows.length) {
    body.innerHTML = `
      <div class="empty">
        <div class="empty-icon">🥕</div>
        <div class="empty-title">${q ? "No matches" : "Catalog is empty"}</div>
        <div class="empty-sub">${q
          ? "Try a different term — Finnish and Indonesian names are searchable too."
          : "Run <span class='mono'>notebooks/ingest_openfoodfacts.py</span> to populate ingredients, then <span class='mono'>notebooks/extract_receipts.py</span> for prices."}</div>
      </div>`;
    return;
  }

  // The catalogue is Finnish. Show whatever English we have underneath the
  // product name: OFF's English name when it exists, otherwise the category
  // tag, which is always English ("Grillattu broileri" -> "roast chicken").
  const englishHint = (r) => {
    const name = (r.canonical_name || "").toLowerCase();
    if (r.name_en && r.name_en.toLowerCase() !== name) {
      return `<div class="faint" style="font-size:11.5px">🇬🇧 ${esc(r.name_en)}</div>`;
    }
    if (r.category_en) {
      return `<div class="faint" style="font-size:11.5px">≈ ${esc(r.category_en)}</div>`;
    }
    return "";
  };

  // Clickable: a household confirmation outranks the derived value, and a
  // padlock marks the ones that are no longer guesses.
  const halalBadge = (r) => {
    const confirmed = r.halal_source === "user_confirmed";
    const label = {
      certified: confirmed ? "halal ✓" : "certified",
      likely_ok: "likely ok",
      contains_flagged: confirmed ? "not halal" : "flagged",
      unknown: "unknown",
    }[r.halal_status] || r.halal_status;
    const tone = {
      certified: "ok", likely_ok: "ok", contains_flagged: "strict", unknown: "",
    }[r.halal_status] ?? "";
    return `<span class="badge ${tone} badge-action" data-halal="${r.ingredient_id}"
                  title="${esc(r.halal_reason || "")}">
              ${label}${confirmed ? ' <span class="lock">🔒</span>' : ""}
            </span>`;
  };

  body.innerHTML = `
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>Ingredient</th><th>Category</th>
          <th class="num">kcal/100g</th><th class="num">Protein</th>
          <th>Halal</th><th>Diet</th>
          <th class="num">Best price</th><th>Source</th>
        </tr></thead>
        <tbody>
          ${rows.map((r) => `
            <tr>
              <td>
                <div style="font-weight:560">${esc(r.canonical_name)}</div>
                ${englishHint(r)}
              </td>
              <td class="muted">${esc(r.category_en || r.category || "—")}</td>
              <td class="num">${num(r.kcal_per_100g)}</td>
              <td class="num">${num(r.protein_g_per_100g, 1)} g</td>
              <td>${halalBadge(r)}</td>
              <td><div class="badges">
                ${r.is_vegan ? '<span class="badge ok">vegan</span>'
                  : r.is_vegetarian ? '<span class="badge ok">veg</span>' : ""}
                ${r.contains_gluten ? '<span class="badge warn">gluten</span>' : ""}
                ${r.contains_lactose ? '<span class="badge warn">lactose</span>' : ""}
              </div></td>
              <td class="num">${r.price_eur ? "€" + num(r.price_eur, 2) : "—"}</td>
              <td>${r.price_source
                ? `<span class="badge">${esc(r.price_source.replace("_", " "))}</span>
                   <div class="faint" style="font-size:11px">${esc(r.store_name || "")}</div>`
                : '<span class="faint">no price</span>'}</td>
            </tr>`).join("")}
        </tbody>
      </table>
    </div>
    <p class="faint" style="font-size:12px;margin-top:10px">
      Showing ${rows.length} ingredients. Prices are estimates from the capture dates
      shown — verify at the till.
    </p>`;
}

$("#btn-search").addEventListener("click", loadCatalog);
$("#ingredient-search").addEventListener("keydown", (e) => {
  if (e.key === "Enter") loadCatalog();
});

/* ------------------------------------------- halal confirmation dialog */

let halalIngredient = null;

$("#catalog-body").addEventListener("click", (e) => {
  const badge = e.target.closest("[data-halal]");
  if (!badge) return;

  halalIngredient = catalogRows.find(
    (r) => r.ingredient_id == badge.dataset.halal);
  if (!halalIngredient) return;

  const r = halalIngredient;
  $("#halal-product-name").textContent = r.canonical_name;
  $("#halal-note").value = r.halal_note || "";
  $$('#halal-form input[name="halal"]').forEach((i) => {
    i.checked = i.value === r.halal_status;
  });

  const confirmed = r.halal_source === "user_confirmed";
  $("#halal-derived").innerHTML = `
    <div class="note ${confirmed ? "info" : ""}">
      <span>${confirmed ? "🔒" : "🤖"}</span>
      <div>${confirmed
        ? "<strong>Confirmed by you.</strong> "
        : "<strong>Derived automatically.</strong> "}
        ${esc(r.halal_reason || "no reason recorded")}</div>
    </div>`;
  $("#halal-clear").classList.toggle("hidden", !confirmed);
  $("#halal-dialog").showModal();
});

$("#halal-form").addEventListener("submit", async () => {
  const choice = $('#halal-form input[name="halal"]:checked');
  if (!choice) return;
  try {
    await api(`/api/ingredients/${halalIngredient.ingredient_id}/halal`, {
      method: "PUT",
      body: { halal_status: choice.value, note: $("#halal-note").value },
    });
    toast(`Saved — ${halalIngredient.canonical_name}`);
    loadCatalog();
  } catch (err) {
    toast(err.message, "error");
  }
});

$("#halal-clear").addEventListener("click", async () => {
  try {
    await api(`/api/ingredients/${halalIngredient.ingredient_id}/halal`,
              { method: "DELETE" });
    toast("Confirmation cleared — re-run ingestion to re-derive");
    $("#halal-dialog").close();
    loadCatalog();
  } catch (err) {
    toast(err.message, "error");
  }
});

/* -------------------------------------------------------------- pipeline */

async function loadPipeline() {
  const body = $("#pipeline-body");
  const s = await api("/api/stats");
  const c = s.counts;

  const tiles = [
    ["Ingredients", c.ingredients, "open food facts"],
    ["Prices", c.prices, "all sources"],
    ["OFF raw products", c.off_products, "landing table"],
    ["Receipts", c.receipts, "vision extracted"],
    ["Receipt lines", c.receipt_lines, "line items"],
    ["Recipes", c.recipes, "stage 2"],
    ["Cooking log", c.cooking_log_entries, "stage 3"],
  ];

  const totalPrices = s.price_provenance.reduce((a, p) => a + Number(p.n), 0);
  const bar = totalPrices
    ? s.price_provenance.map((p) =>
        `<span style="width:${(p.n / totalPrices) * 100}%;background:${
          SOURCE_COLORS[p.source] || "var(--text-faint)"}"></span>`).join("")
    : "";
  const legend = s.price_provenance.map((p) => `
    <div class="legend-item">
      <span class="dot" style="background:${SOURCE_COLORS[p.source] || "var(--text-faint)"}"></span>
      ${esc(p.source.replace("_", " "))} · ${p.n}
      <span class="faint">(conf ${num(p.avg_confidence, 2)})</span>
    </div>`).join("");

  body.innerHTML = `
    <div class="grid grid-4">
      ${tiles.map(([label, value, hint]) => `
        <div class="card stat">
          <div class="stat-label">${label}</div>
          <div class="stat-value">${num(value)}</div>
          <div class="stat-hint">${hint}</div>
        </div>`).join("")}
    </div>

    <div class="section-head"><h2 class="section-title">Price provenance</h2></div>
    <div class="card">
      ${totalPrices ? `
        <div class="bar">${bar}</div>
        <div class="legend">${legend}</div>
        <p class="faint" style="font-size:12px;margin:12px 0 0">
          Four independent sources reconciled into one price table. The grocery list
          in Stage 3 reports this breakdown alongside the total.
        </p>`
        : `<div class="muted">No prices loaded yet. Run the receipt extraction
             notebook, or add prices manually from the catalog.</div>`}
    </div>

    <div class="section-head"><h2 class="section-title">Halal coverage</h2></div>
    <div class="card">
      ${s.halal_coverage.length ? `
        <div class="legend">
          ${s.halal_coverage.map((h) => `
            <div class="legend-item"><span class="dot" style="background:${
              h.halal_status === "certified" ? "var(--green)"
              : h.halal_status === "likely_ok" ? "var(--blue)"
              : h.halal_status === "contains_flagged" ? "var(--red)"
              : "var(--text-faint)"}"></span>
              ${esc(h.halal_status.replace("_", " "))} · ${h.n}</div>`).join("")}
        </div>
        <div class="note" style="margin-top:12px">
          <span>⚠️</span>
          <div>Derived from Open Food Facts labels plus an ingredient scan for pork,
          gelatine, alcohol and carmine. Anything below <em>certified</em> needs the
          packaging checked — the app never claims otherwise.</div>
        </div>`
        : `<div class="muted">No ingredients loaded yet.</div>`}
    </div>`;
}

/* ---------------------------------------------------------------- health */

async function checkHealth() {
  try {
    const h = await api("/api/health");
    const ok = h.status === "ok";
    $("#health-dot").className = `pulse ${ok ? "ok" : "bad"}`;
    $("#health-text").textContent = ok ? "Lakebase connected" : "database degraded";
    if (!ok) $("#health-text").title = h.database || "";
  } catch (err) {
    $("#health-dot").className = "pulse bad";
    $("#health-text").textContent = "app unreachable";
    $("#health-text").title = err.message;
  }
}

/* ------------------------------------------------------------------ boot */

checkHealth();
loadHousehold();
