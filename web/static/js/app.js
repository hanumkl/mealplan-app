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
    ["household", "catalog", "pipeline", "recipes", "plan"].forEach((v) =>
      $(`#view-${v}`).classList.toggle("hidden", v !== view));
    if (view === "catalog") loadCatalog();
    if (view === "pipeline") loadPipeline();
    if (view === "recipes") loadRecipes();
    if (view === "plan") loadPlan();
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

/* --------------------------------------------------------------- recipes */

let recipeRows = [];

// A video description can prove a recipe contains pork, but never that its
// meat was slaughtered halal - so a clean recipe reads "halal unverified", not
// "halal ok". Saying nothing at all would let absence read as approval.
const halalRecipeBadge = (r) => {
  if (r.halal_status === "contains_flagged" || r.contains_pork)
    return '<span class="badge strict">not halal</span>';
  if (r.halal_status === "certified" || r.halal_status === "likely_ok")
    return '<span class="badge ok">halal ok</span>';
  return `<span class="badge" title="No pork or alcohol found in the ingredient
list, but nothing confirms how the meat was sourced. Check before cooking."
          >halal unverified</span>`;
};

const dietBadges = (r) => `
  ${r.is_vegan ? '<span class="badge ok">vegan</span>'
    : r.is_vegetarian ? '<span class="badge ok">veg</span>' : ""}
  ${r.contains_pork ? '<span class="badge strict">pork</span>' : ""}
  ${r.contains_gluten ? '<span class="badge warn">gluten</span>' : ""}
  ${r.contains_lactose ? '<span class="badge warn">lactose</span>' : ""}
  ${halalRecipeBadge(r)}`;

// LLM extraction is imperfect and the UI says so rather than hiding it, so a
// low-confidence parse gets reviewed instead of silently planned around.
const confidenceBadge = (r) => {
  const c = r.extraction_confidence;
  if (c === null || c === undefined) return "";
  const tone = c >= 0.8 ? "ok" : c >= 0.5 ? "warn" : "strict";
  return `<span class="badge ${tone}" title="LLM extraction confidence">
            parse ${Math.round(c * 100)}%</span>`;
};

async function loadRecipes() {
  const q = $("#recipe-search").value.trim();
  const body = $("#recipes-body");
  const mode = $("#recipe-mode");
  body.innerHTML = `<div class="empty"><div class="empty-sub">Searching…</div></div>`;
  mode.textContent = "";

  const params = new URLSearchParams({ limit: "24" });
  if ($("#rf-household").checked) params.set("household_id", HOUSEHOLD_ID);
  if ($("#rf-approved").checked) params.set("approved_only", "true");

  let data;
  try {
    if (q) {
      params.set("q", q);
      data = await api(`/api/recipes/search?${params}`);
    } else {
      // No query means browse, which needs no embeddings at all.
      data = await api(`/api/recipes?${params}`);
      data.mode = "browse";
    }
  } catch (err) {
    body.innerHTML = `<div class="empty">
        <div class="empty-icon">⚠️</div>
        <div class="empty-title">Search failed</div>
        <div class="empty-sub">${esc(err.message)}</div>
      </div>`;
    return;
  }

  recipeRows = data.results || [];

  if (data.mode === "semantic") mode.textContent = `semantic · ${data.model || ""}`;
  else if (data.mode === "keyword-fallback") mode.textContent = "keyword fallback";
  else if (data.mode === "browse") mode.textContent = "browsing all recipes";

  const notes = [];
  if (data.warning) notes.push(`<div class="note warn"><span>⚠️</span><div>${esc(data.warning)}</div></div>`);
  if (data.message) notes.push(`<div class="note info"><span>ℹ️</span><div>${esc(data.message)}</div></div>`);
  // Be explicit about restrictions we can't enforce in SQL, so an empty
  // warning isn't read as "this list is safe".
  if (data.unenforced_restrictions?.length) {
    notes.push(`<div class="note info"><span>ℹ️</span><div>
      Filtered on what the recipe data supports. Not enforced here:
      <strong>${data.unenforced_restrictions.map((r) =>
        esc(RESTRICTION_LABELS[r] || r)).join(", ")}</strong> — check the
      ingredients before cooking.</div></div>`);
  }

  if (!recipeRows.length) {
    body.innerHTML = notes.join("") + `
      <div class="empty">
        <div class="empty-icon">📺</div>
        <div class="empty-title">${q ? "No matches" : "No recipes yet"}</div>
        <div class="empty-sub">${q
          ? "Try describing the meal differently, or clear the filters."
          : "Run <span class='mono'>notebooks/harvest_youtube_recipes.py</span> then <span class='mono'>notebooks/embed_content.py</span>."}</div>
      </div>`;
    return;
  }

  body.innerHTML = notes.join("") + `
    <div class="recipe-grid">
      ${recipeRows.map((r) => `
        <article class="recipe-card" data-recipe="${r.recipe_id}">
          <div class="recipe-thumb">
            ${r.thumbnail_url
              ? `<img src="${esc(r.thumbnail_url)}" alt="" loading="lazy">`
              : `<div class="recipe-thumb-fallback">🍲</div>`}
            ${r.duration_min ? `<span class="recipe-duration">${r.duration_min} min</span>` : ""}
            ${r.similarity !== null && r.similarity !== undefined
              ? `<span class="recipe-match" title="cosine similarity">${Math.round(r.similarity * 100)}% match</span>`
              : ""}
          </div>
          <div class="recipe-body">
            <div class="recipe-title">${esc(r.title)}</div>
            <div class="faint recipe-meta">
              ${esc(r.channel_title || "")}${r.cuisine ? ` · ${esc(r.cuisine)}` : ""}
            </div>
            <div class="badges">
              ${dietBadges(r)}
              ${confidenceBadge(r)}
              ${r.review_status === "approved"
                ? '<span class="badge ok">approved</span>'
                : r.review_status === "rejected"
                ? '<span class="badge strict">rejected</span>'
                : '<span class="badge">needs review</span>'}
            </div>
          </div>
        </article>`).join("")}
    </div>
    <p class="faint" style="font-size:12px;margin-top:12px">
      Showing ${recipeRows.length} recipes. Ingredient lists come from an LLM reading
      the video description — check them before shopping.
    </p>`;
}

$("#btn-recipe-search").addEventListener("click", loadRecipes);
$("#recipe-search").addEventListener("keydown", (e) => {
  if (e.key === "Enter") loadRecipes();
});
$("#rf-household").addEventListener("change", loadRecipes);
$("#rf-approved").addEventListener("change", loadRecipes);

/* -------------------------------------------------- recipe detail dialog */

let openRecipeId = null;
let openRecipeServings = null;
// Taken from the detail payload, not the search results - the search SELECT
// doesn't carry video_id, so deep-links would silently lose their timestamps.
let openRecipeVideoId = null;
let recipeIngredients = [];

$("#recipes-body").addEventListener("click", (e) => {
  const card = e.target.closest("[data-recipe]");
  if (card) openRecipe(Number(card.dataset.recipe));
});

async function openRecipe(recipeId, servings = null) {
  openRecipeId = recipeId;
  const dlg = $("#recipe-dialog");
  const body = $("#recipe-dialog-body");

  if (!dlg.open) {
    body.innerHTML = `<div class="empty"><div class="empty-sub">Loading…</div></div>`;
    dlg.showModal();
  }

  // Default to cooking for everyone in the household rather than whatever the
  // video assumed - that's the number the family actually needs.
  const wanted = servings ?? openRecipeServings ?? (members.length || null);
  const url = `/api/recipes/${recipeId}` + (wanted ? `?servings=${wanted}` : "");

  let data;
  try {
    data = await api(url);
  } catch (err) {
    body.innerHTML = `<div class="note warn"><span>⚠️</span><div>${esc(err.message)}</div></div>`;
    return;
  }

  const r = data.recipe;
  openRecipeServings = data.servings;
  openRecipeVideoId = r.video_id || null;
  recipeIngredients = data.ingredients;
  $("#recipe-dialog-title").textContent = r.title;

  const fmtQty = (row) => {
    if (row.scaled_quantity === null || row.scaled_quantity === undefined) return "";
    const q = row.scaled_quantity;
    const shown = q >= 10 ? Math.round(q) : Math.round(q * 100) / 100;
    return `${shown}${row.unit ? " " + esc(row.unit) : ""}`;
  };

  const scalingHint = (row) =>
    row.scaling_class === "sublinear"
      ? `<span class="badge" title="Spices, salt and oil don't scale linearly - tripling the chilli makes it inedible">^0.8</span>`
      : row.scaling_class === "fixed"
      ? `<span class="badge" title="Doesn't scale - one bay leaf is one bay leaf">fixed</span>`
      : "";

  const proteins = data.ingredients.filter((i) => i.is_protein_component);
  const base = data.ingredients.filter((i) => !i.is_protein_component);

  const ingredientTable = (rows, caption) => !rows.length ? "" : `
    <div class="ingr-group">
      <div class="ingr-caption">${caption}</div>
      <table class="ingr-table">
        <tbody>
          ${rows.map((row) => `
            <tr class="${row.is_optional ? "optional" : ""}">
              <td class="ingr-qty">${fmtQty(row)}</td>
              <td>
                <div>${esc(row.ingredient_name || row.raw_text)}</div>
                ${row.canonical_name
                  ? `<div class="faint" style="font-size:11.5px">
                       → ${esc(row.canonical_name)}
                       ${row.match_method === "manual"
                         ? '<span class="lock">🔒</span>'
                         : row.match_confidence
                         ? `<span title="match confidence">${Math.round(row.match_confidence * 100)}%</span>`
                         : ""}
                     </div>`
                  : ""}
              </td>
              <td class="num">
                ${row.line_cost_eur !== null && row.line_cost_eur !== undefined
                  ? `€${num(row.line_cost_eur, 2)}` : ""}
              </td>
              <td class="num">${scalingHint(row)}
                ${row.is_optional ? '<span class="badge">optional</span>' : ""}
                ${row.grams_quality === "approximate"
                  ? '<span class="badge" title="Volume or spoon measure converted at water density — an estimate">≈</span>' : ""}
                ${row.ingredient_id
                  ? ""
                  : `<span class="badge warn badge-action" data-match="${row.ri_id}"
                           title="No catalog match — click to pick one. Not counted in nutrition or cost.">unmatched</span>`}
              </td>
            </tr>`).join("")}
        </tbody>
      </table>
    </div>`;

  // Totals are only as good as the matching, so coverage is shown next to them
  // rather than buried. A partial total presented as complete is how someone
  // ends up bulking on a plan that is 800 kcal short.
  const t = data.totals;
  const ps = data.per_serving;
  const nutritionPanel = `
    <div class="nutri-panel">
      <div class="nutri-row">
        <div class="nutri-cell">
          <div class="nutri-value">${t.ingredients_counted ? num(ps.kcal) : "—"}</div>
          <div class="nutri-label">kcal / serving</div>
        </div>
        <div class="nutri-cell">
          <div class="nutri-value">${t.ingredients_counted ? num(ps.protein_g, 1) + " g" : "—"}</div>
          <div class="nutri-label">protein / serving</div>
        </div>
        <div class="nutri-cell">
          <div class="nutri-value">${ps.cost_eur !== null && ps.cost_eur !== undefined
            ? "€" + num(ps.cost_eur, 2) : "—"}</div>
          <div class="nutri-label">cost / serving</div>
        </div>
        <div class="nutri-cell">
          <div class="nutri-value">${t.cost_eur !== null && t.cost_eur !== undefined
            ? "€" + num(t.cost_eur, 2) : "—"}</div>
          <div class="nutri-label">cost to cook</div>
        </div>
      </div>
      ${t.is_complete && !t.is_approximate ? "" : `
        <div class="nutri-note">
          ${t.ingredients_counted === 0
            ? `⚠️ Nothing matched the catalog yet, so there are no numbers to show.
               Run <span class="mono">notebooks/match_recipe_ingredients.py</span>.`
            : `Based on ${t.ingredients_counted} of ${t.ingredients_total} ingredients${
                t.ingredients_priced ? `, ${t.ingredients_priced} priced` : ", none priced"}.
               ${t.is_approximate ? "Spoon and volume measures are estimates. " : ""}
               ${t.missing.length ? `Not counted: ${t.missing.map(esc).join(", ")}.` : ""}`}
        </div>`}
    </div>`;

  const memberFit = !data.member_fit.length ? "" : `
    <div class="ingr-group">
      <div class="ingr-caption">Portions for your goals</div>
      <table class="ingr-table">
        <tbody>
          ${data.member_fit.map((m) => `
            <tr>
              <td><div>${esc(m.name)}</div>
                  <div class="faint" style="font-size:11.5px">${esc(m.goal_type)} ·
                    ${num(m.target_kcal)} kcal/day</div></td>
              <td class="num ingr-qty">${num(m.servings_needed, 2)}×</td>
              <td class="faint" style="font-size:11.5px">
                serving${m.servings_needed === 1 ? "" : "s"} to cover lunch + dinner
              </td>
            </tr>`).join("")}
        </tbody>
      </table>
      <div class="faint" style="font-size:11.5px">
        Assumes this dish covers lunch and dinner — about 65% of the day's
        calories. Breakfast and snacks are outside the plan.
      </div>
      ${data.member_fit_reliable ? "" : `
        <div class="note warn" style="margin-top:8px"><span>⚠️</span><div>
          These portions are unreliable — the recipe's calories are based on
          only ${data.totals.ingredients_counted} of
          ${data.totals.ingredients_total} ingredients, so the per-serving
          figure is too low and the multipliers come out too high. Match the
          missing ingredients to fix it.
        </div></div>`}
    </div>`;

  const videoId = r.video_id || "";
  body.innerHTML = `
    ${videoId ? `
      <div class="video-wrap">
        <iframe id="recipe-video" src="https://www.youtube-nocookie.com/embed/${esc(videoId)}"
                title="${esc(r.title)}" loading="lazy" allowfullscreen
                referrerpolicy="strict-origin-when-cross-origin"></iframe>
      </div>
      <a class="faint" style="font-size:12px" target="_blank" rel="noopener"
         href="${esc(r.video_url || `https://www.youtube.com/watch?v=${videoId}`)}">
        Open on YouTube ↗</a>` : ""}

    <div class="servings-bar">
      <div>
        <div class="label">Cooking for</div>
        <div class="faint" style="font-size:12px">
          Video serves ${num(data.base_servings, 0)} · scaled ×${num(data.scale_factor, 2)}
        </div>
      </div>
      <div class="stepper">
        <button class="btn btn-sm" data-servings="-1">−</button>
        <span class="stepper-value">${num(data.servings, 0)}</span>
        <button class="btn btn-sm" data-servings="1">+</button>
      </div>
    </div>

    <div class="badges" style="margin:12px 0">${dietBadges(r)} ${confidenceBadge(r)}</div>

    ${nutritionPanel}

    ${ingredientTable(base, "Base dish — cooked once, covers lunch and dinner")}
    ${ingredientTable(proteins, "Protein add-ons — per member, cooked separately")}

    ${memberFit}

    ${data.ingredients.length ? "" :
      `<div class="note warn"><span>⚠️</span><div>No ingredients were extracted
        for this recipe. Reject it, or open the video and add them by hand.</div></div>`}

    ${r.instructions ? `
      <div class="ingr-group">
        <div class="ingr-caption">Steps</div>
        <div class="form-row" style="grid-template-columns:1fr auto;margin-bottom:8px">
          <input type="text" id="step-search" placeholder="Ask a step — when do I add the coconut milk?">
          <button class="btn btn-sm" id="btn-step-search">Find</button>
        </div>
        <div id="step-results"></div>
        <div class="instructions">${esc(r.instructions)}</div>
      </div>` : ""}`;

  $("#recipe-approve").textContent =
    r.review_status === "approved" ? "Approved ✓" : "Approve for planning";
}

$("#recipe-dialog-body").addEventListener("click", async (e) => {
  const step = e.target.closest("[data-servings]");
  if (step) {
    const next = Math.max(1, (openRecipeServings || 4) + Number(step.dataset.servings));
    await openRecipe(openRecipeId, next);
    return;
  }
  if (e.target.closest("#btn-step-search")) runStepSearch();

  const unmatched = e.target.closest("[data-match]");
  if (unmatched) openMatchDialog(Number(unmatched.dataset.match));
});

/* --------------------------------------------- manual ingredient matching */

let matchingRiId = null;

function openMatchDialog(riId) {
  matchingRiId = riId;
  const row = (recipeIngredients || []).find((r) => r.ri_id === riId);
  $("#match-raw-text").textContent = row?.ingredient_name || row?.raw_text || "";
  $("#match-search").value = row?.ingredient_name || "";
  $("#match-results").innerHTML = "";
  $("#match-dialog").showModal();
  if ($("#match-search").value) runMatchSearch();
}

async function runMatchSearch() {
  const q = $("#match-search").value.trim();
  const out = $("#match-results");
  if (!q) return;
  out.innerHTML = `<div class="faint" style="font-size:12px">Searching…</div>`;

  // Semantic first so "chicken" finds "Broilerin fileesuikale"; the plain
  // catalog search is the fallback when embeddings aren't built yet.
  let rows = [];
  try {
    const sem = await api(`/api/search/ingredients?q=${encodeURIComponent(q)}&limit=12`);
    rows = sem.results || [];
    if (!rows.length) rows = await api(`/api/ingredients?q=${encodeURIComponent(q)}&limit=12`);
  } catch {
    rows = await api(`/api/ingredients?q=${encodeURIComponent(q)}&limit=12`);
  }

  if (!rows.length) {
    out.innerHTML = `<div class="faint" style="font-size:12px">No catalog matches.</div>`;
    return;
  }

  out.innerHTML = `
    <table class="ingr-table"><tbody>
      ${rows.map((r) => `
        <tr class="match-option" data-pick="${r.ingredient_id}">
          <td>
            <div>${esc(r.canonical_name)}</div>
            <div class="faint" style="font-size:11.5px">
              ${r.name_en ? esc(r.name_en) + " · " : ""}
              ${r.kcal_per_100g ? num(r.kcal_per_100g) + " kcal/100g"
                : '<span style="color:var(--amber)">no nutrition data</span>'}
            </div>
          </td>
          <td class="num faint" style="font-size:11.5px">
            ${r.similarity ? Math.round(r.similarity * 100) + "%" : ""}
          </td>
        </tr>`).join("")}
    </tbody></table>`;
}

$("#btn-match-search").addEventListener("click", runMatchSearch);
$("#match-search").addEventListener("keydown", (e) => {
  if (e.key === "Enter") { e.preventDefault(); runMatchSearch(); }
});

$("#match-results").addEventListener("click", async (e) => {
  const pick = e.target.closest("[data-pick]");
  if (!pick) return;
  await saveMatch(Number(pick.dataset.pick));
});

$("#match-clear").addEventListener("click", () => saveMatch(null));

async function saveMatch(ingredientId) {
  try {
    await api(`/api/recipe-ingredients/${matchingRiId}/match`, {
      method: "PUT",
      body: { ingredient_id: ingredientId },
    });
    toast(ingredientId ? "Matched — nutrition and cost updated" : "Match cleared");
    $("#match-dialog").close();
    await openRecipe(openRecipeId);      // refresh totals
  } catch (err) {
    toast(err.message, "bad");
  }
}

$("#recipe-dialog-body").addEventListener("keydown", (e) => {
  if (e.target.id === "step-search" && e.key === "Enter") {
    e.preventDefault();
    runStepSearch();
  }
});

async function runStepSearch() {
  const q = $("#step-search")?.value.trim();
  const out = $("#step-results");
  if (!q || !out) return;
  out.innerHTML = `<div class="faint" style="font-size:12px">Searching steps…</div>`;

  const data = await api(
    `/api/recipes/${openRecipeId}/steps?q=${encodeURIComponent(q)}&limit=3`);

  if (!data.results.length) {
    out.innerHTML = `<div class="faint" style="font-size:12px">${
      esc(data.message || "No matching step. The recipe may not be chunked yet.")
    }</div>`;
    return;
  }

  const videoId = openRecipeVideoId;

  out.innerHTML = data.results.map((s) => `
    <div class="step-hit">
      <div class="step-text">${esc(s.chunk_text)}</div>
      <div class="step-meta">
        ${s.similarity ? `${Math.round(s.similarity * 100)}% match` : ""}
        ${s.start_second !== null && s.start_second !== undefined && videoId
          ? ` · <a href="https://www.youtube.com/watch?v=${esc(videoId)}&t=${s.start_second}s"
                   target="_blank" rel="noopener">jump to ${
                     Math.floor(s.start_second / 60)}:${
                     String(s.start_second % 60).padStart(2, "0")} ↗</a>`
          : ""}
      </div>
    </div>`).join("");
}

async function setReview(status) {
  try {
    await api(`/api/recipes/${openRecipeId}/review`, {
      method: "PUT",
      body: { review_status: status },
    });
    toast(status === "approved" ? "Recipe approved — it can be planned now"
                                : "Recipe rejected", status === "approved" ? "ok" : "warn");
    $("#recipe-dialog").close();
    loadRecipes();
  } catch (err) {
    toast(err.message, "bad");
  }
}

$("#recipe-approve").addEventListener("click", () => setReview("approved"));
$("#recipe-reject").addEventListener("click", () => setReview("rejected"));

/* ------------------------------------------------------------ the agent */

const chatHistory = [];
let chatBusy = false;

const TOOL_LABELS = {
  get_household: "read your household",
  search_recipes: "searched recipes",
  get_recipe: "read a recipe",
  get_cooking_history: "read cooking history",
  create_meal_plan: "saved the week's plan",
  log_cooked: "logged what you cooked",
  build_grocery_list: "built the grocery list",
};

function addChatMessage(role, html) {
  const el = document.createElement("div");
  el.className = `chat-msg ${role}`;
  el.innerHTML = `<div class="chat-bubble">${html}</div>`;
  $("#chat-log").appendChild(el);
  $("#chat-log").scrollTop = $("#chat-log").scrollHeight;
  return el;
}

// Writes are shown explicitly. The user shouldn't have to take the model's
// word for it that something was saved.
const traceHtml = (trace) => !trace?.length ? "" : `
  <div class="tool-trace">
    ${trace.map((t) => `
      <span class="tool-chip ${t.is_write ? "write" : ""} ${t.ok ? "" : "failed"}">
        ${t.is_write ? "✎" : "🔎"} ${esc(TOOL_LABELS[t.tool] || t.tool)}
        ${t.ok ? "" : " (failed)"}
      </span>`).join("")}
  </div>`;

async function sendAgentMessage(text) {
  if (chatBusy || !text.trim()) return;
  chatBusy = true;
  $("#chat-input").value = "";
  addChatMessage("user", esc(text));
  chatHistory.push({ role: "user", content: text });

  const thinking = addChatMessage("assistant",
    `<span class="thinking">Thinking… this can take a few seconds per step.</span>`);

  try {
    const res = await api("/api/agent/chat", {
      method: "POST",
      body: { messages: chatHistory },
    });
    thinking.remove();
    addChatMessage("assistant",
      esc(res.reply).replace(/\n/g, "<br>") + traceHtml(res.trace));
    chatHistory.push({ role: "assistant", content: res.reply });

    // A write means the plan on the right is now stale.
    if (res.trace?.some((t) => t.is_write && t.ok)) loadPlan();
  } catch (err) {
    thinking.remove();
    addChatMessage("assistant",
      `<span style="color:var(--red)">${esc(err.message)}</span>`);
  } finally {
    chatBusy = false;
  }
}

$("#btn-chat-send").addEventListener("click", () => sendAgentMessage($("#chat-input").value));
$("#chat-input").addEventListener("keydown", (e) => {
  if (e.key === "Enter") sendAgentMessage($("#chat-input").value);
});
$("#chat-log").addEventListener("click", (e) => {
  const chip = e.target.closest("[data-ask]");
  if (chip) sendAgentMessage(chip.dataset.ask);
});

async function loadPlan() {
  const body = $("#plan-body");
  let data;
  try {
    data = await api("/api/plans/current");
  } catch (err) {
    body.innerHTML = `<div class="note warn"><span>⚠️</span><div>${esc(err.message)}</div></div>`;
    return;
  }

  if (!data.plan) {
    body.innerHTML = `
      <div class="empty">
        <div class="empty-icon">📅</div>
        <div class="empty-title">No plan yet</div>
        <div class="empty-sub">Ask the planner to build one.</div>
      </div>`;
    return;
  }

  const dayName = (d) => new Date(d).toLocaleDateString("en-GB",
    { weekday: "short", day: "numeric", month: "short" });

  const g = data.grocery;
  body.innerHTML = `
    <div class="card">
      <div class="section-head" style="margin-top:0">
        <h2 class="section-title">Week of ${esc(String(data.plan.week_start))}</h2>
        <span class="badge ${data.plan.status === "active" ? "ok" : ""}">${esc(data.plan.status)}</span>
      </div>
      ${data.plan.rationale
        ? `<div class="note info"><span>🤖</span><div>${esc(data.plan.rationale)}</div></div>`
        : ""}

      <table class="ingr-table">
        <tbody>
          ${data.days.map((d) => `
            <tr>
              <td class="ingr-qty" style="white-space:nowrap">${esc(dayName(d.plan_date))}</td>
              <td>
                <div>${esc(d.title || "—")}</div>
                <div class="faint" style="font-size:11.5px">
                  ${d.cuisine ? esc(d.cuisine) : ""}${d.duration_min ? ` · ${d.duration_min} min` : ""}
                  ${d.notes ? ` · ${esc(d.notes)}` : ""}
                </div>
                ${d.log_id ? `
                  <div class="faint" style="font-size:11.5px;color:${d.was_planned ? "var(--green)" : "var(--amber)"}">
                    ${d.was_planned ? "✓ cooked as planned"
                      : `↺ actually made ${esc(d.actually_cooked || "something else")}${
                          d.deviation_reason ? ` — “${esc(d.deviation_reason)}”` : ""}`}
                  </div>` : ""}
              </td>
              <td class="num">
                ${d.recipe_id ? `<button class="btn btn-sm" data-recipe="${d.recipe_id}">open</button>` : ""}
              </td>
            </tr>`).join("")}
        </tbody>
      </table>
    </div>

    ${g ? `
      <div class="card" style="margin-top:14px">
        <div class="section-head" style="margin-top:0">
          <h2 class="section-title">Grocery list</h2>
          <span class="nutri-value">€${num(g.total_eur, 2)}</span>
        </div>
        <div class="faint" style="font-size:12px;margin-bottom:8px">
          ${g.items} items. Prices come from your own receipts — an estimate, and
          a floor: anything we couldn't price isn't in this total.
        </div>
        <table class="ingr-table"><tbody>
          ${data.grocery_items.map((i) => `
            <tr>
              <td>${esc(i.display_name)}</td>
              <td class="faint" style="font-size:11.5px">${esc(i.store_name || "—")}</td>
              <td class="num ingr-qty">${i.est_price_eur !== null && i.est_price_eur !== undefined
                ? "€" + num(i.est_price_eur, 2)
                : '<span class="badge warn">no price</span>'}</td>
            </tr>`).join("")}
        </tbody></table>
      </div>` : ""}`;
}

$("#plan-body").addEventListener("click", (e) => {
  const btn = e.target.closest("[data-recipe]");
  if (btn) openRecipe(Number(btn.dataset.recipe));
});

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
