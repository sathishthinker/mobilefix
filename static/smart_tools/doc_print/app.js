/* global pdfjsLib */

const CARD_W_MM = 85.5;
const CARD_H_MM = 54;
const CARD_ASPECT = CARD_W_MM / CARD_H_MM;
const PAGE_MARGIN_MM = 12;
const CORNER_MM = 3;
const HINTS = {
  aadhaar: "e-Aadhaar PDF (password: first 4 letters of name + birth year) or photos of the PVC card.",
  pan: "e-PAN / DigiLocker PDF, or a clear photo of the front (and back if needed).",
  voter: "Voter ID / EPIC PDF or photos of both sides.",
  dl: "Driving licence PDF or photos of both sides.",
};

const state = {
  type: "aadhaar",
  pdfFile: null,
  frontFile: null,
  backFile: null,
  front: null,
  back: null,
  exportDpi: 400,
  cardGapMm: 4,
  adjust: { front: null, back: null },
};

const typeHint = document.getElementById("typeHint");
const pdfDrop = document.getElementById("pdfDrop");
const pdfInput = document.getElementById("pdfInput");
const pdfHint = document.getElementById("pdfHint");
const frontDrop = document.getElementById("frontDrop");
const frontInput = document.getElementById("frontInput");
const frontHint = document.getElementById("frontHint");
const backDrop = document.getElementById("backDrop");
const backInput = document.getElementById("backInput");
const backHint = document.getElementById("backHint");
const passwordInput = document.getElementById("passwordInput");
const cutGuides = document.getElementById("cutGuides");
const solidGuides = document.getElementById("solidGuides");
const roundedCorners = document.getElementById("roundedCorners");
const showBoth = document.getElementById("showBoth");
const sideBySide = document.getElementById("sideBySide");
const dpiOptions = document.getElementById("dpiOptions");
const sizePreview = document.getElementById("sizePreview");
const cardGapRange = document.getElementById("cardGapRange");
const cardGapInput = document.getElementById("cardGapInput");
const clearBtn = document.getElementById("clearBtn");
const extractBtn = document.getElementById("extractBtn");
const printBtn = document.getElementById("printBtn");
const downloadPdfBtn = document.getElementById("downloadPdfBtn");
const downloadFrontPngBtn = document.getElementById("downloadFrontPngBtn");
const downloadBackPngBtn = document.getElementById("downloadBackPngBtn");
const statusEl = document.getElementById("status");
const previewWrap = document.getElementById("previewWrap");
const adjustGrid = document.getElementById("adjustGrid");
const previewSheet = document.getElementById("previewSheet");
const printRoot = document.getElementById("printRoot");

if (typeof pdfjsLib !== "undefined") {
  pdfjsLib.GlobalWorkerOptions.workerSrc =
    "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";
}

function renderDpi() {
  return window.innerWidth < 768 ? 280 : 400;
}

function exportPx(mm) {
  return Math.round((mm * state.exportDpi) / 25.4);
}

function mmToPt(mm) {
  return (mm * 72) / 25.4;
}

function setStatus(text, kind) {
  statusEl.textContent = text || "";
  statusEl.className = kind ? `status ${kind}` : "status";
}

function formatBytes(n) {
  if (!n) return "—";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(0)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function dataUrlBytes(url) {
  const b64 = (url || "").split(",")[1] || "";
  return Math.floor((b64.length * 3) / 4);
}

function updateSizePreview() {
  const w = exportPx(CARD_W_MM);
  const h = exportPx(CARD_H_MM);
  const n = (state.front ? 1 : 0) + (state.back ? 1 : 0);
  let extra = `~${formatBytes(Math.round(w * h * 1.2))} PNG each`;
  if (n) extra = `${formatBytes((dataUrlBytes(state.front) + dataUrlBytes(state.back)) / n)} PNG each`;
  sizePreview.innerHTML = `<strong>${w} × ${h} px</strong> · ${extra} · ${CARD_W_MM} × ${CARD_H_MM} mm @ ${state.exportDpi} DPI`;
}

function maxGap() {
  if (sideBySide.checked) return Math.max(0, 210 - 2 * PAGE_MARGIN_MM - 2 * CARD_W_MM);
  return Math.max(0, 297 - 2 * PAGE_MARGIN_MM - 2 * CARD_H_MM);
}

function getGap() {
  return Math.max(0, Math.min(Math.min(15, maxGap()), Number(state.cardGapMm) || 0));
}

function setGap(raw, render) {
  const max = Math.min(15, maxGap());
  state.cardGapMm = Math.max(0, Math.min(max, Math.round((Number(raw) || 0) * 2) / 2));
  cardGapRange.max = String(max);
  cardGapRange.value = String(state.cardGapMm);
  cardGapInput.max = String(max);
  cardGapInput.value = String(state.cardGapMm);
  document.documentElement.style.setProperty("--card-gap", `${state.cardGapMm}mm`);
  if (render !== false && (state.front || state.back)) renderPreview();
}

function hasSource() {
  return Boolean(state.pdfFile || state.frontFile || state.backFile);
}

function updateButtons() {
  const cards = Boolean(state.front || state.back);
  clearBtn.disabled = !hasSource() && !cards;
  extractBtn.disabled = !hasSource();
  printBtn.disabled = !cards;
  downloadPdfBtn.disabled = !cards;
  downloadFrontPngBtn.disabled = !state.front;
  downloadBackPngBtn.disabled = !state.back;
}

function loadImage(src) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = () => reject(new Error("Could not read that image."));
    img.src = src;
  });
}

function fileToDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(new Error("Could not read file."));
    reader.readAsDataURL(file);
  });
}

function coverRect(w, h) {
  let cw;
  let ch;
  if (w / h > CARD_ASPECT) {
    ch = h;
    cw = h * CARD_ASPECT;
  } else {
    cw = w;
    ch = w / CARD_ASPECT;
  }
  return { x: (w - cw) / 2, y: (h - ch) / 2, w: cw, h: ch };
}

function makeAdjust(img, rect) {
  return {
    img,
    w: img.naturalWidth || img.width,
    h: img.naturalHeight || img.height,
    rect: { ...rect },
    zoom: 1,
    panX: 0,
    panY: 0,
  };
}

function visibleCrop(adj) {
  const base = coverRect(adj.rect.w, adj.rect.h);
  const zoom = Math.max(0.5, Math.min(3, adj.zoom || 1));
  const cropW = base.w / zoom;
  const cropH = base.h / zoom;
  let sx = adj.rect.x + base.x + base.w / 2 - cropW / 2 - (adj.panX || 0);
  let sy = adj.rect.y + base.y + base.h / 2 - cropH / 2 - (adj.panY || 0);
  if (cropW >= adj.w) sx = 0;
  else sx = Math.max(0, Math.min(adj.w - cropW, sx));
  if (cropH >= adj.h) sy = 0;
  else sy = Math.max(0, Math.min(adj.h - cropH, sy));
  return { sx, sy, cropW, cropH };
}

function cropSide(adj) {
  if (!adj || !adj.img) return null;
  const tw = exportPx(CARD_W_MM);
  const th = exportPx(CARD_H_MM);
  const crop = visibleCrop(adj);
  const canvas = document.createElement("canvas");
  canvas.width = tw;
  canvas.height = th;
  const ctx = canvas.getContext("2d");
  ctx.fillStyle = "#fff";
  ctx.fillRect(0, 0, tw, th);
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = "high";
  ctx.drawImage(adj.img, crop.sx, crop.sy, crop.cropW, crop.cropH, 0, 0, tw, th);
  if (!roundedCorners.checked) return canvas.toDataURL("image/png");
  const r = Math.max(1, Math.round(exportPx(CORNER_MM)));
  const out = document.createElement("canvas");
  out.width = tw;
  out.height = th;
  const octx = out.getContext("2d");
  octx.fillStyle = "#fff";
  octx.fillRect(0, 0, tw, th);
  octx.beginPath();
  if (typeof octx.roundRect === "function") octx.roundRect(0, 0, tw, th, r);
  else {
    octx.moveTo(r, 0);
    octx.arcTo(tw, 0, tw, th, r);
    octx.arcTo(tw, th, 0, th, r);
    octx.arcTo(0, th, 0, 0, r);
    octx.arcTo(0, 0, tw, 0, r);
  }
  octx.clip();
  octx.drawImage(canvas, 0, 0);
  return out.toDataURL("image/png");
}

function rebuildPrint() {
  state.front = cropSide(state.adjust.front);
  state.back = state.adjust.back ? cropSide(state.adjust.back) : null;
  renderPreview();
  updateSizePreview();
  updateButtons();
}

function inkBounds(data, W, H, x0, y0, x1, y1) {
  let minX = x1;
  let minY = y1;
  let maxX = x0;
  let maxY = y0;
  let n = 0;
  for (let y = y0; y <= y1; y += 2) {
    for (let x = x0; x <= x1; x += 2) {
      const i = (y * W + x) * 4;
      if (data[i] < 248 || data[i + 1] < 248 || data[i + 2] < 248) {
        n += 1;
        if (x < minX) minX = x;
        if (y < minY) minY = y;
        if (x > maxX) maxX = x;
        if (y > maxY) maxY = y;
      }
    }
  }
  if (n < 80) return null;
  return { x: minX, y: minY, w: maxX - minX + 1, h: maxY - minY + 1 };
}

function isRed(r, g, b) {
  return r > 140 && r > g * 1.2 && r > b * 1.2 && r - Math.max(g, b) > 20;
}

function detectRedPair(canvas) {
  const W = canvas.width;
  const H = canvas.height;
  const { data } = canvas.getContext("2d").getImageData(0, 0, W, H);
  const y0 = Math.floor(H * 0.5);
  let red = 0;
  for (let y = y0; y < H; y += 3) {
    for (let x = 0; x < W; x += 3) {
      const i = (y * W + x) * 4;
      if (isRed(data[i], data[i + 1], data[i + 2])) red += 1;
    }
  }
  if (red < 80) return null;
  const mid = Math.floor(W / 2);
  const left = inkBounds(data, W, H, 0, y0, mid, H - 1);
  const right = inkBounds(data, W, H, mid, y0, W - 1, H - 1);
  if (!left || !right) return null;
  return { front: left, back: right };
}

function detectLayoutPair(canvas) {
  const W = canvas.width;
  const H = canvas.height;
  const { data } = canvas.getContext("2d").getImageData(0, 0, W, H);
  const box = inkBounds(data, W, H, 0, Math.floor(H * 0.52), W - 1, H - 1);
  if (!box || box.w < W * 0.4) return null;
  const gap = Math.max(4, Math.round(box.w * 0.02));
  const half = Math.floor((box.w - gap) / 2);
  return {
    front: { x: box.x, y: box.y, w: half, h: box.h },
    back: { x: box.x + half + gap, y: box.y, w: box.w - half - gap, h: box.h },
  };
}

async function renderPdfPage(page, dpi) {
  const viewport = page.getViewport({ scale: dpi / 72 });
  const canvas = document.createElement("canvas");
  canvas.width = Math.floor(viewport.width);
  canvas.height = Math.floor(viewport.height);
  const ctx = canvas.getContext("2d", { willReadFrequently: true });
  ctx.fillStyle = "#fff";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  await page.render({ canvasContext: ctx, viewport }).promise;
  return canvas;
}

function canvasToImage(canvas) {
  return loadImage(canvas.toDataURL("image/jpeg", 0.92));
}

async function openPdf(file, password) {
  const data = await file.arrayBuffer();
  try {
    return await pdfjsLib.getDocument({ data, password: password || "" }).promise;
  } catch (err) {
    if (err && err.name === "PasswordException") throw new Error("This PDF needs a password.");
    throw new Error(err.message || "Could not open the PDF.");
  }
}

function formatAadhaarPassword(raw) {
  let out = "";
  for (const ch of String(raw || "")) {
    if (out.length >= 8) break;
    if (out.length < 4) {
      if (/[a-zA-Z]/.test(ch)) out += ch.toUpperCase();
    } else if (/\d/.test(ch)) out += ch;
  }
  return out;
}

function setType(type) {
  state.type = type;
  document.querySelectorAll(".type-chip, .type-chip").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.type === type);
  });
  typeHint.textContent = HINTS[type];
  passwordInput.placeholder = type === "aadhaar" ? "ABCD1990" : "If the PDF is locked";
  passwordInput.maxLength = type === "aadhaar" ? 8 : 32;
}

function markDrop(el, file, hintEl, fallback) {
  if (file) {
    el.classList.add("has-file");
    hintEl.textContent = file.name;
  } else {
    el.classList.remove("has-file");
    hintEl.textContent = fallback;
  }
}

function bindDrop(drop, input, onFile) {
  ["dragenter", "dragover"].forEach((evt) => {
    drop.addEventListener(evt, (e) => {
      e.preventDefault();
      drop.classList.add("dragover");
    });
  });
  ["dragleave", "drop"].forEach((evt) => {
    drop.addEventListener(evt, (e) => {
      e.preventDefault();
      drop.classList.remove("dragover");
    });
  });
  drop.addEventListener("drop", (e) => {
    const file = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
    if (file) onFile(file);
  });
  input.addEventListener("change", () => {
    if (input.files && input.files[0]) onFile(input.files[0]);
  });
}

function setPdf(file) {
  if (file && file.type !== "application/pdf" && !/\.pdf$/i.test(file.name)) {
    setStatus("Choose a PDF for that slot.", "error");
    return;
  }
  state.pdfFile = file || null;
  markDrop(pdfDrop, file, pdfHint, "DigiLocker / e-document");
  updateButtons();
  if (file) setStatus("Add photos if needed, then Extract.");
}

function setFront(file) {
  if (file && !file.type.startsWith("image/")) {
    setStatus("Front slot needs a photo.", "error");
    return;
  }
  state.frontFile = file || null;
  markDrop(frontDrop, file, frontHint, "Camera or gallery");
  updateButtons();
}

function setBack(file) {
  if (file && !file.type.startsWith("image/")) {
    setStatus("Back slot needs a photo.", "error");
    return;
  }
  state.backFile = file || null;
  markDrop(backDrop, file, backHint, "Optional");
  updateButtons();
}

function syncAdjustViewport(side) {
  const adj = state.adjust[side];
  const panel = adjustGrid.querySelector(`[data-side="${side}"]`);
  if (!adj || !panel) return;
  const viewport = panel.querySelector(".adjust-viewport");
  const img = panel.querySelector("img");
  const vw = viewport.clientWidth;
  if (!vw) return;
  const crop = visibleCrop(adj);
  const scale = vw / crop.cropW;
  img.style.width = `${adj.w * scale}px`;
  img.style.height = `${adj.h * scale}px`;
  img.style.transform = `translate(${-crop.sx * scale}px, ${-crop.sy * scale}px)`;
}

function buildAdjustUI() {
  adjustGrid.replaceChildren();
  const sides = [];
  if (state.adjust.front) sides.push(["front", "Front"]);
  if (state.adjust.back) sides.push(["back", "Back"]);
  sides.forEach(([key, label]) => {
    const adj = state.adjust[key];
    const panel = document.createElement("div");
    panel.className = "adjust-panel";
    panel.dataset.side = key;
    panel.innerHTML = `
      <h3>${label}</h3>
      <div class="adjust-viewport">
        <div class="stage"><img alt="${label}" draggable="false" /></div>
      </div>
      <div class="adjust-controls">
        <label class="zoom-label">
          <span>Zoom</span>
          <input type="range" min="50" max="300" step="1" value="${Math.round(adj.zoom * 100)}" />
          <span class="zoom-input-wrap">
            <input type="number" class="zoom-val" min="50" max="300" value="${Math.round(adj.zoom * 100)}" />
            <span>%</span>
          </span>
        </label>
        <div class="nudge-pad">
          <button type="button" class="nudge nudge-up" data-dx="0" data-dy="-1">▲</button>
          <button type="button" class="nudge nudge-left" data-dx="-1" data-dy="0">◀</button>
          <span class="nudge-label">1px</span>
          <button type="button" class="nudge nudge-right" data-dx="1" data-dy="0">▶</button>
          <button type="button" class="nudge nudge-down" data-dx="0" data-dy="1">▼</button>
        </div>
        <button type="button" class="btn secondary reset-btn">Reset</button>
      </div>
    `;
    adjustGrid.appendChild(panel);
    const viewport = panel.querySelector(".adjust-viewport");
    const img = panel.querySelector("img");
    const rangeEl = panel.querySelector("input[type=range]");
    const percentEl = panel.querySelector(".zoom-val");
    img.src = adj.img.src;
    img.onload = () => syncAdjustViewport(key);

    function applyZoom(raw) {
      const pct = Math.max(50, Math.min(300, Math.round(Number(raw) || 100)));
      adj.zoom = pct / 100;
      rangeEl.value = String(pct);
      percentEl.value = String(pct);
      syncAdjustViewport(key);
      rebuildPrint();
    }
    function nudge(dx, dy) {
      adj.panX += dx;
      adj.panY += dy;
      syncAdjustViewport(key);
      rebuildPrint();
    }

    panel.querySelectorAll(".nudge").forEach((btn) => {
      let hold;
      const step = () => nudge(Number(btn.dataset.dx), Number(btn.dataset.dy));
      const stop = () => clearInterval(hold);
      btn.addEventListener("pointerdown", (e) => {
        e.preventDefault();
        step();
        hold = setInterval(step, 40);
      });
      btn.addEventListener("pointerup", stop);
      btn.addEventListener("pointercancel", stop);
    });

    let dragging = false;
    let lastX = 0;
    let lastY = 0;
    let pinch0 = 0;
    viewport.addEventListener("pointerdown", (e) => {
      dragging = true;
      lastX = e.clientX;
      lastY = e.clientY;
      viewport.classList.add("dragging");
      viewport.setPointerCapture(e.pointerId);
    });
    viewport.addEventListener("pointermove", (e) => {
      if (!dragging) return;
      const crop = visibleCrop(adj);
      const scale = viewport.clientWidth / crop.cropW;
      adj.panX += (e.clientX - lastX) / (scale || 1);
      adj.panY += (e.clientY - lastY) / (scale || 1);
      lastX = e.clientX;
      lastY = e.clientY;
      syncAdjustViewport(key);
      rebuildPrint();
    });
    function endDrag(e) {
      dragging = false;
      viewport.classList.remove("dragging");
      try { viewport.releasePointerCapture(e.pointerId); } catch (err) { /* ignore */ }
    }
    viewport.addEventListener("pointerup", endDrag);
    viewport.addEventListener("pointercancel", endDrag);
    viewport.addEventListener("wheel", (e) => {
      e.preventDefault();
      applyZoom((adj.zoom + (e.deltaY > 0 ? -0.08 : 0.08)) * 100);
    }, { passive: false });
    viewport.addEventListener("touchstart", (e) => {
      if (e.touches.length === 2) {
        const a = e.touches[0];
        const b = e.touches[1];
        pinch0 = Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY);
      }
    }, { passive: true });
    viewport.addEventListener("touchmove", (e) => {
      if (e.touches.length !== 2 || !pinch0) return;
      e.preventDefault();
      const a = e.touches[0];
      const b = e.touches[1];
      const dist = Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY);
      applyZoom(adj.zoom * (dist / pinch0) * 100);
      pinch0 = dist;
    }, { passive: false });
    rangeEl.addEventListener("input", (e) => applyZoom(e.target.value));
    percentEl.addEventListener("change", () => applyZoom(percentEl.value));
    panel.querySelector(".reset-btn").addEventListener("click", () => {
      adj.panX = 0;
      adj.panY = 0;
      applyZoom(100);
    });
  });
}

function makeCard(src, label, guide, solid, round) {
  const card = document.createElement("div");
  card.className = round ? "id-card rounded" : "id-card";
  const img = document.createElement("img");
  img.src = src;
  img.alt = label;
  card.appendChild(img);
  if (guide || solid) {
    const g = document.createElement("div");
    g.className = solid && !guide ? "guide solid" : "guide";
    card.appendChild(g);
  }
  const cap = document.createElement("span");
  cap.className = "card-caption";
  cap.textContent = label;
  card.appendChild(cap);
  return card;
}

function renderPreview() {
  const guide = cutGuides.checked;
  const solid = solidGuides.checked && !guide;
  const round = roundedCorners.checked;
  const both = showBoth.checked;
  const row = sideBySide.checked;
  document.documentElement.style.setProperty("--card-gap", `${getGap()}mm`);
  const sides = [];
  if (state.front) sides.push({ src: state.front, label: "Front" });
  if (state.back) sides.push({ src: state.back, label: "Back" });
  previewSheet.replaceChildren();
  printRoot.replaceChildren();
  if (!sides.length) {
    previewWrap.hidden = true;
    updateButtons();
    return;
  }
  previewWrap.hidden = false;
  function append(parent, stacked) {
    if (!stacked && sides.length === 2) {
      const rowEl = document.createElement("div");
      rowEl.className = "card-row";
      sides.forEach((s) => rowEl.appendChild(makeCard(s.src, s.label, guide, solid, round)));
      parent.appendChild(rowEl);
    } else {
      sides.forEach((s) => parent.appendChild(makeCard(s.src, s.label, guide, solid, round)));
    }
  }
  append(previewSheet, !row);
  if (both || sides.length === 1) {
    const page = document.createElement("div");
    page.className = "print-page";
    append(page, !row);
    printRoot.appendChild(page);
  } else {
    sides.forEach((s) => {
      const page = document.createElement("div");
      page.className = "print-page";
      page.appendChild(makeCard(s.src, s.label, guide, solid, round));
      printRoot.appendChild(page);
    });
  }
  updateButtons();
}

function pdfRoundedPath(x, y, w, h, r) {
  const radius = Math.max(0, Math.min(r, w / 2, h / 2));
  if (radius < 0.2) return [`${x.toFixed(2)} ${y.toFixed(2)} ${w.toFixed(2)} ${h.toFixed(2)} re`];
  const k = radius * 0.5522847498;
  const x1 = x + w;
  const y1 = y + h;
  return [
    `${(x + radius).toFixed(2)} ${y.toFixed(2)} m`,
    `${(x1 - radius).toFixed(2)} ${y.toFixed(2)} l`,
    `${(x1 - radius + k).toFixed(2)} ${y.toFixed(2)} ${x1.toFixed(2)} ${(y + k).toFixed(2)} ${x1.toFixed(2)} ${(y + radius).toFixed(2)} c`,
    `${x1.toFixed(2)} ${(y1 - radius).toFixed(2)} l`,
    `${x1.toFixed(2)} ${(y1 - radius + k).toFixed(2)} ${(x1 - radius + k).toFixed(2)} ${y1.toFixed(2)} ${(x1 - radius).toFixed(2)} ${y1.toFixed(2)} c`,
    `${(x + radius).toFixed(2)} ${y1.toFixed(2)} l`,
    `${(x + radius - k).toFixed(2)} ${y1.toFixed(2)} ${x.toFixed(2)} ${(y1 - radius + k).toFixed(2)} ${x.toFixed(2)} ${(y1 - radius).toFixed(2)} c`,
    `${x.toFixed(2)} ${(y + radius).toFixed(2)} l`,
    `${x.toFixed(2)} ${(y + radius - k).toFixed(2)} ${(x + radius - k).toFixed(2)} ${y.toFixed(2)} ${(x + radius).toFixed(2)} ${y.toFixed(2)} c`,
    "h",
  ];
}

function jpegFromDataUrl(dataUrl) {
  return loadImage(dataUrl).then((img) => {
    const canvas = document.createElement("canvas");
    canvas.width = img.naturalWidth || img.width;
    canvas.height = img.naturalHeight || img.height;
    const ctx = canvas.getContext("2d");
    ctx.fillStyle = "#fff";
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(img, 0, 0);
    const jpeg = canvas.toDataURL("image/jpeg", 0.95);
    const bin = atob(jpeg.split(",")[1]);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return { bytes, width: canvas.width, height: canvas.height };
  });
}

function buildA4Pdf(pages) {
  const enc = new TextEncoder();
  const chunks = [];
  let pos = 0;
  function write(part) {
    const bytes = typeof part === "string" ? enc.encode(part) : part;
    chunks.push(bytes);
    pos += bytes.length;
  }
  write("%PDF-1.4\n");
  const starts = [];
  function begin(id) {
    starts[id] = pos;
    write(`${id} 0 obj\n`);
  }
  function end() {
    write("\nendobj\n");
  }
  let next = 3;
  const pageObjs = pages.map((layout) => {
    const pageId = next;
    next += 1;
    const contentId = next;
    next += 1;
    const imageIds = layout.items.map(() => {
      const id = next;
      next += 1;
      return id;
    });
    return { pageId, contentId, imageIds, layout };
  });
  begin(1);
  write("<< /Type /Catalog /Pages 2 0 R >>");
  end();
  begin(2);
  write(`<< /Type /Pages /Kids [${pageObjs.map((p) => `${p.pageId} 0 R`).join(" ")}] /Count ${pageObjs.length} >>`);
  end();
  const pageW = mmToPt(210);
  const pageH = mmToPt(297);
  pageObjs.forEach((obj) => {
    begin(obj.pageId);
    write(
      `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${pageW.toFixed(2)} ${pageH.toFixed(2)}] /Resources << /XObject << ${obj.imageIds
        .map((id, i) => `/Im${i} ${id} 0 R`)
        .join(" ")} >> >> /Contents ${obj.contentId} 0 R >>`
    );
    end();
    const ops = [];
    obj.layout.items.forEach((item, i) => {
      const x = mmToPt(item.xMm);
      const y = pageH - mmToPt(item.yMm) - mmToPt(item.hMm);
      const w = mmToPt(item.wMm);
      const h = mmToPt(item.hMm);
      const rr = item.rounded ? mmToPt(CORNER_MM) : 0;
      ops.push("q");
      if (rr > 0) ops.push(...pdfRoundedPath(x, y, w, h, rr), "W n");
      ops.push(`${w.toFixed(2)} 0 0 ${h.toFixed(2)} ${x.toFixed(2)} ${y.toFixed(2)} cm`, `/Im${i} Do`, "Q");
      if (item.guide || item.border) {
        ops.push("q", item.guide ? "0.4 w" : "1 w", item.guide ? "[2 2] 0 d" : "[] 0 d", "0 0 0 RG");
        if (rr > 0) ops.push(...pdfRoundedPath(x, y, w, h, rr), "S");
        else ops.push(`${x.toFixed(2)} ${y.toFixed(2)} ${w.toFixed(2)} ${h.toFixed(2)} re S`);
        ops.push("Q");
      }
    });
    const content = enc.encode(ops.join("\n"));
    begin(obj.contentId);
    write(`<< /Length ${content.length} >>\nstream\n`);
    write(content);
    write("\nendstream");
    end();
    obj.layout.items.forEach((item, i) => {
      begin(obj.imageIds[i]);
      write(
        `<< /Type /XObject /Subtype /Image /Width ${item.jpeg.width} /Height ${item.jpeg.height} /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode /Length ${item.jpeg.bytes.length} >>\nstream\n`
      );
      write(item.jpeg.bytes);
      write("\nendstream");
      end();
    });
  });
  const xref = pos;
  const maxId = next - 1;
  write(`xref\n0 ${maxId + 1}\n0000000000 65535 f \n`);
  for (let id = 1; id <= maxId; id++) write(`${String(starts[id]).padStart(10, "0")} 00000 n \n`);
  write(`trailer\n<< /Size ${maxId + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF`);
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const out = new Uint8Array(total);
  let o = 0;
  chunks.forEach((c) => {
    out.set(c, o);
    o += c.length;
  });
  return new Blob([out], { type: "application/pdf" });
}

async function buildPrintPdf() {
  const sides = [state.front, state.back].filter(Boolean);
  if (!sides.length) throw new Error("Nothing to export.");
  const jpegs = await Promise.all(sides.map((src) => jpegFromDataUrl(src)));
  const guide = cutGuides.checked;
  const border = solidGuides.checked && !guide;
  const rounded = roundedCorners.checked;
  const gap = getGap();
  const margin = PAGE_MARGIN_MM;
  function card(jpeg, xMm, yMm) {
    return { jpeg, xMm, yMm, wMm: CARD_W_MM, hMm: CARD_H_MM, guide, border, rounded };
  }
  if (jpegs.length === 1) return buildA4Pdf([{ items: [card(jpegs[0], margin, margin)] }]);
  if (showBoth.checked && sideBySide.checked) {
    return buildA4Pdf([{ items: [card(jpegs[0], margin, margin), card(jpegs[1], margin + CARD_W_MM + gap, margin)] }]);
  }
  if (showBoth.checked) {
    return buildA4Pdf([{ items: [card(jpegs[0], margin, margin), card(jpegs[1], margin, margin + CARD_H_MM + gap)] }]);
  }
  return buildA4Pdf([{ items: [card(jpegs[0], margin, margin)] }, { items: [card(jpegs[1], margin, margin)] }]);
}

function downloadBlob(blob, name) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1500);
}

function dataUrlToBlob(url) {
  const parts = url.split(",");
  const mime = /data:(.*?);/.exec(parts[0]);
  const bin = atob(parts[1]);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return new Blob([bytes], { type: mime ? mime[1] : "image/png" });
}

function slug() {
  const map = { aadhaar: "aadhaar", pan: "pan", voter: "voter-id", dl: "driving-licence" };
  return map[state.type] || "id";
}

async function extract() {
  if (!hasSource()) return;
  extractBtn.disabled = true;
  extractBtn.classList.add("busy");
  setStatus("Working…");
  try {
    state.adjust.front = null;
    state.adjust.back = null;
    if (state.frontFile) {
      const img = await loadImage(await fileToDataUrl(state.frontFile));
      state.adjust.front = makeAdjust(img, { x: 0, y: 0, w: img.naturalWidth, h: img.naturalHeight });
    }
    if (state.backFile) {
      const img = await loadImage(await fileToDataUrl(state.backFile));
      state.adjust.back = makeAdjust(img, { x: 0, y: 0, w: img.naturalWidth, h: img.naturalHeight });
    }
    if (state.pdfFile && !(state.frontFile && state.backFile)) {
      if (typeof pdfjsLib === "undefined") throw new Error("PDF library failed to load. Check the network.");
      const password = state.type === "aadhaar" ? formatAadhaarPassword(passwordInput.value) : passwordInput.value.trim();
      if (state.type === "aadhaar") passwordInput.value = password;
      const pdf = await openPdf(state.pdfFile, password);
      const dpi = renderDpi();
      if (pdf.numPages >= 2 && state.type !== "aadhaar") {
        if (!state.adjust.front) {
          const img = await canvasToImage(await renderPdfPage(await pdf.getPage(1), dpi));
          state.adjust.front = makeAdjust(img, { x: 0, y: 0, w: img.naturalWidth, h: img.naturalHeight });
        }
        if (!state.adjust.back) {
          const img = await canvasToImage(await renderPdfPage(await pdf.getPage(2), dpi));
          state.adjust.back = makeAdjust(img, { x: 0, y: 0, w: img.naturalWidth, h: img.naturalHeight });
        }
      } else if (!state.adjust.front) {
        const canvas = await renderPdfPage(await pdf.getPage(1), dpi);
        const img = await canvasToImage(canvas);
        const pair = state.type === "aadhaar" ? detectRedPair(canvas) || detectLayoutPair(canvas) : detectLayoutPair(canvas);
        if (pair && pair.front) {
          state.adjust.front = makeAdjust(img, pair.front);
          if (pair.back && !state.adjust.back) state.adjust.back = makeAdjust(img, pair.back);
        } else {
          state.adjust.front = makeAdjust(img, { x: 0, y: 0, w: img.naturalWidth, h: img.naturalHeight });
        }
      }
    }
    if (!state.adjust.front && !state.adjust.back) throw new Error("Add a PDF or a photo, then Extract.");
    buildAdjustUI();
    requestAnimationFrame(() => {
      if (state.adjust.front) syncAdjustViewport("front");
      if (state.adjust.back) syncAdjustViewport("back");
    });
    rebuildPrint();
    setStatus("Ready — adjust the crop if needed, then Print.", "ok");
  } catch (err) {
    console.error(err);
    setStatus(err.message || "Extract failed.", "error");
  } finally {
    extractBtn.classList.remove("busy");
    updateButtons();
  }
}

function clearAll() {
  state.pdfFile = null;
  state.frontFile = null;
  state.backFile = null;
  state.front = null;
  state.back = null;
  state.adjust.front = null;
  state.adjust.back = null;
  pdfInput.value = "";
  frontInput.value = "";
  backInput.value = "";
  passwordInput.value = "";
  markDrop(pdfDrop, null, pdfHint, "DigiLocker / e-document");
  markDrop(frontDrop, null, frontHint, "Camera or gallery");
  markDrop(backDrop, null, backHint, "Optional");
  adjustGrid.replaceChildren();
  previewSheet.replaceChildren();
  printRoot.replaceChildren();
  previewWrap.hidden = true;
  setStatus("");
  updateSizePreview();
  updateButtons();
}

document.querySelectorAll(".type-chip, .type-chip").forEach((btn) => {
  btn.addEventListener("click", () => setType(btn.dataset.type));
});
bindDrop(pdfDrop, pdfInput, setPdf);
bindDrop(frontDrop, frontInput, setFront);
bindDrop(backDrop, backInput, setBack);
passwordInput.addEventListener("input", () => {
  if (state.type !== "aadhaar") return;
  const next = formatAadhaarPassword(passwordInput.value);
  if (passwordInput.value !== next) passwordInput.value = next;
});
passwordInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") extract();
});
cutGuides.addEventListener("change", () => {
  if (cutGuides.checked) solidGuides.checked = false;
  solidGuides.disabled = cutGuides.checked;
  renderPreview();
});
solidGuides.addEventListener("change", () => {
  if (solidGuides.checked) cutGuides.checked = false;
  renderPreview();
});
roundedCorners.addEventListener("change", rebuildPrint);
showBoth.addEventListener("change", () => setGap(state.cardGapMm));
sideBySide.addEventListener("change", () => setGap(state.cardGapMm));
cardGapRange.addEventListener("input", (e) => setGap(e.target.value));
cardGapInput.addEventListener("change", () => setGap(cardGapInput.value));
dpiOptions.addEventListener("click", (e) => {
  const btn = e.target.closest(".dpi-btn, .dpi-btn");
  if (!btn) return;
  state.exportDpi = Number(btn.dataset.dpi) || 400;
  dpiOptions.querySelectorAll(".dpi-btn").forEach((b) => b.classList.toggle("active", b === btn));
  if (state.adjust.front || state.adjust.back) rebuildPrint();
  else updateSizePreview();
});
clearBtn.addEventListener("click", clearAll);
extractBtn.addEventListener("click", extract);
printBtn.addEventListener("click", () => {
  renderPreview();
  window.print();
});
downloadPdfBtn.addEventListener("click", async () => {
  try {
    setStatus("Building PDF…");
    downloadBlob(await buildPrintPdf(), `${slug()}-pvc-a4.pdf`);
    setStatus("PDF saved.", "ok");
  } catch (err) {
    setStatus(err.message || "PDF export failed.", "error");
  }
});
downloadFrontPngBtn.addEventListener("click", () => {
  if (state.front) downloadBlob(dataUrlToBlob(state.front), `${slug()}-front.png`);
});
downloadBackPngBtn.addEventListener("click", () => {
  if (state.back) downloadBlob(dataUrlToBlob(state.back), `${slug()}-back.png`);
});
window.addEventListener("resize", () => {
  if (state.adjust.front) syncAdjustViewport("front");
  if (state.adjust.back) syncAdjustViewport("back");
});

setType("aadhaar");
setGap(4, false);
updateSizePreview();
updateButtons();
solidGuides.disabled = cutGuides.checked;
