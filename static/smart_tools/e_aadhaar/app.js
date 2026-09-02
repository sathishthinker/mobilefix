/* global pdfjsLib */

/**
 * e-Aadhaar Print â€” AI extract (same approach as BusinessMate E-Way Bills):
 *  1) Unlock PDF (pdf.js)
 *  2) Read structured page content (text landmarks + painted images)
 *  3) Score / merge detectors (image â†’ red frame â†’ bottom layout)
 *  4) Fit front & back into exact 85.5 Ã— 54 mm print boxes (2 mm margin)
 */

const CARD_W_MM = 85.5;
const CARD_H_MM = 54;
const CARD_ASPECT = CARD_W_MM / CARD_H_MM;
const MARGIN_MM = 2;
const MARGIN_BOTTOM_MM = 3.5;
const PAGE_W_MM = 210;
const PAGE_H_MM = 297;
const PAGE_MARGIN_MM = 12;
const GAP_MIN_MM = 0;
const GAP_MAX_MM = 15;
const CORNER_RADIUS_MM = 3;
/** Page render DPI for detection (high enough for 600 DPI export). */
const RENDER_DPI = 600;
const DPI_OPTIONS = [200, 300, 400, 600];

const state = {
  file: null,
  front: null,
  back: null,
  meta: null,
  cardName: null,
  aadhaarNumber: null,
  cardGapMm: 4,
  exportDpi: 400,
  pageUrl: null,
  pageW: 0,
  pageH: 0,
  pageImg: null,
  adjust: {
    front: null, // { rect, zoom, panX, panY }
    back: null,
  },
};

const pdfInput = document.getElementById("pdfInput");
const pdfDrop = document.getElementById("pdfDrop");
const pdfHint = document.getElementById("pdfHint");
const passwordInput = document.getElementById("passwordInput");
const cutGuides = document.getElementById("cutGuides");
const solidGuides = document.getElementById("solidGuides");
const roundedCorners = document.getElementById("roundedCorners");
const showBoth = document.getElementById("showBoth");
const cardGapRange = document.getElementById("cardGapRange");
const cardGapInput = document.getElementById("cardGapInput");
const clearBtn = document.getElementById("clearBtn");
const extractBtn = document.getElementById("extractBtn");
const printBtn = document.getElementById("printBtn");
const downloadPdfBtn = document.getElementById("downloadPdfBtn");
const downloadFrontPngBtn = document.getElementById("downloadFrontPngBtn");
const downloadBackPngBtn = document.getElementById("downloadBackPngBtn");
const previewWrap = document.getElementById("previewWrap");
const previewSheet = document.getElementById("previewSheet");
const printRoot = document.getElementById("printRoot");
const statusEl = document.getElementById("status");
const adjustGrid = document.getElementById("adjustGrid");
const dpiOptions = document.getElementById("dpiOptions");
const sizePreview = document.getElementById("sizePreview");

if (typeof pdfjsLib !== "undefined") {
  pdfjsLib.GlobalWorkerOptions.workerSrc =
    "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";
}

function mmPx(mm, dpi = RENDER_DPI) {
  return (mm * dpi) / 25.4;
}

function exportPx(mm) {
  return Math.round(mmPx(mm, state.exportDpi));
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) return "—";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function dataUrlByteLength(dataUrl) {
  if (!dataUrl) return 0;
  const b64 = dataUrl.split(",")[1] || "";
  return Math.floor((b64.length * 3) / 4);
}

function updateSizePreview() {
  if (!sizePreview) return;
  const dpi = state.exportDpi;
  const w = exportPx(CARD_W_MM);
  const h = exportPx(CARD_H_MM);
  let sizeNote;
  if (state.front) {
    const n = (state.front ? 1 : 0) + (state.back ? 1 : 0);
    const bytes = dataUrlByteLength(state.front) + dataUrlByteLength(state.back);
    sizeNote = `${formatBytes(bytes / n)} PNG each`;
  } else {
    // Rough PNG estimate for photo-like content
    const est = Math.round(w * h * 3 * 0.4);
    sizeNote = `~${formatBytes(est)} PNG each`;
  }
  sizePreview.innerHTML = `<strong>${w} × ${h} px</strong> · ${sizeNote} · ${CARD_W_MM} × ${CARD_H_MM} mm @ ${dpi} DPI`;
}

function setExportDpi(dpi) {
  const next = DPI_OPTIONS.includes(dpi) ? dpi : 400;
  state.exportDpi = next;
  dpiOptions?.querySelectorAll(".dpi-btn").forEach((btn) => {
    btn.classList.toggle("active", Number(btn.dataset.dpi) === next);
  });
  if (state.adjust.front || state.adjust.back) {
    rebuildPrintFromAdjust();
  } else {
    updateSizePreview();
  }
}

function maxCardGapMm() {
  const sideBySide = document.getElementById("sideBySide")?.checked;
  if (sideBySide) {
    return Math.max(GAP_MIN_MM, PAGE_W_MM - 2 * PAGE_MARGIN_MM - 2 * CARD_W_MM);
  }
  return Math.max(GAP_MIN_MM, PAGE_H_MM - 2 * PAGE_MARGIN_MM - 2 * CARD_H_MM);
}

function getCardGapMm() {
  const max = Math.min(GAP_MAX_MM, maxCardGapMm());
  return Math.max(GAP_MIN_MM, Math.min(max, Number(state.cardGapMm) || 0));
}

function setCardGapMm(raw, { render = true } = {}) {
  const max = Math.min(GAP_MAX_MM, maxCardGapMm());
  const gap = Math.round((Number(raw) || 0) * 2) / 2; // 0.5 mm steps
  state.cardGapMm = Math.max(GAP_MIN_MM, Math.min(max, gap));
  if (cardGapRange) {
    cardGapRange.max = String(max);
    cardGapRange.value = String(state.cardGapMm);
  }
  if (cardGapInput) {
    cardGapInput.max = String(max);
    cardGapInput.value = String(state.cardGapMm);
  }
  document.documentElement.style.setProperty("--card-gap", `${state.cardGapMm}mm`);
  if (render && (state.front || state.back)) renderPreview();
}

function setStatus(text, kind = "") {
  statusEl.textContent = text || "";
  statusEl.className = `status${kind ? ` ${kind}` : ""}`;
}

function revoke(url) {
  if (url && String(url).startsWith("blob:")) URL.revokeObjectURL(url);
}

function updateButtons() {
  const hasFile = Boolean(state.file);
  const hasCards = Boolean(state.front || state.back);
  clearBtn.disabled = !hasFile && !hasCards;
  extractBtn.disabled = !hasFile;
  printBtn.disabled = !hasCards;
  if (downloadPdfBtn) downloadPdfBtn.disabled = !hasCards;
  if (downloadFrontPngBtn) downloadFrontPngBtn.disabled = !state.front;
  if (downloadBackPngBtn) downloadBackPngBtn.disabled = !state.back;
}

function mmToPt(mm) {
  return (mm * 72) / 25.4;
}

/** PDF path ops for a rounded rectangle (y = bottom-left origin). */
function pdfRoundedRectPath(x, y, w, h, r) {
  const radius = Math.max(0, Math.min(r, w / 2, h / 2));
  if (radius <= 0.01) {
    return [`${x.toFixed(2)} ${y.toFixed(2)} ${w.toFixed(2)} ${h.toFixed(2)} re`];
  }
  const k = radius * 0.5522847498;
  const x0 = x;
  const y0 = y;
  const x1 = x + w;
  const y1 = y + h;
  return [
    `${(x0 + radius).toFixed(2)} ${y0.toFixed(2)} m`,
    `${(x1 - radius).toFixed(2)} ${y0.toFixed(2)} l`,
    `${(x1 - radius + k).toFixed(2)} ${y0.toFixed(2)} ${x1.toFixed(2)} ${(y0 + k).toFixed(2)} ${x1.toFixed(2)} ${(y0 + radius).toFixed(2)} c`,
    `${x1.toFixed(2)} ${(y1 - radius).toFixed(2)} l`,
    `${x1.toFixed(2)} ${(y1 - radius + k).toFixed(2)} ${(x1 - radius + k).toFixed(2)} ${y1.toFixed(2)} ${(x1 - radius).toFixed(2)} ${y1.toFixed(2)} c`,
    `${(x0 + radius).toFixed(2)} ${y1.toFixed(2)} l`,
    `${(x0 + radius - k).toFixed(2)} ${y1.toFixed(2)} ${x0.toFixed(2)} ${(y1 - radius + k).toFixed(2)} ${x0.toFixed(2)} ${(y1 - radius).toFixed(2)} c`,
    `${x0.toFixed(2)} ${(y0 + radius).toFixed(2)} l`,
    `${x0.toFixed(2)} ${(y0 + radius - k).toFixed(2)} ${(x0 + radius - k).toFixed(2)} ${y0.toFixed(2)} ${(x0 + radius).toFixed(2)} ${y0.toFixed(2)} c`,
    "h",
  ];
}

function roundRectPath(ctx, x, y, w, h, r) {
  const radius = Math.max(0, Math.min(r, w / 2, h / 2));
  if (typeof ctx.roundRect === "function") {
    ctx.roundRect(x, y, w, h, radius);
    return;
  }
  ctx.moveTo(x + radius, y);
  ctx.arcTo(x + w, y, x + w, y + h, radius);
  ctx.arcTo(x + w, y + h, x, y + h, radius);
  ctx.arcTo(x, y + h, x, y, radius);
  ctx.arcTo(x, y, x + w, y, radius);
  ctx.closePath();
}

/** Apply PVC-style rounded corners for PNG/PDF export. */
async function applyRoundedCorners(dataUrl, { transparent = false } = {}) {
  if (!roundedCorners?.checked) return dataUrl;
  const img = await loadImage(dataUrl);
  const w = img.naturalWidth || img.width;
  const h = img.naturalHeight || img.height;
  const r = Math.max(1, Math.round(exportPx(CORNER_RADIUS_MM)));
  const canvas = document.createElement("canvas");
  canvas.width = w;
  canvas.height = h;
  const ctx = canvas.getContext("2d");
  if (!transparent) {
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, w, h);
  } else {
    ctx.clearRect(0, 0, w, h);
  }
  ctx.beginPath();
  roundRectPath(ctx, 0, 0, w, h, r);
  ctx.clip();
  ctx.drawImage(img, 0, 0);
  return canvas.toDataURL("image/png");
}

function dataUrlToJpegBytes(dataUrl, quality = 0.95) {
  return loadImage(dataUrl).then((img) => {
    const canvas = document.createElement("canvas");
    canvas.width = img.naturalWidth || img.width;
    canvas.height = img.naturalHeight || img.height;
    const ctx = canvas.getContext("2d");
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(img, 0, 0);
    const jpegUrl = canvas.toDataURL("image/jpeg", quality);
    const bin = atob(jpegUrl.split(",")[1]);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return { bytes, width: canvas.width, height: canvas.height };
  });
}

/** Minimal A4 PDF writer (no CDN) â€” embeds JPEGs at exact mm sizes. */
function buildA4Pdf(pages) {
  const encoder = new TextEncoder();
  const chunks = [];
  let pos = 0;

  const write = (part) => {
    const bytes = typeof part === "string" ? encoder.encode(part) : part;
    chunks.push(bytes);
    pos += bytes.length;
  };

  write("%PDF-1.4\n");
  write(new Uint8Array([0x25, 0xe2, 0xe3, 0xcf, 0xd3, 0x0a]));

  const objStarts = [];
  const beginObj = (id) => {
    objStarts[id] = pos;
    write(`${id} 0 obj\n`);
  };
  const endObj = () => write("\nendobj\n");

  const pageLayouts = pages.map((page) => ({
    items: page.items,
  }));

  let nextId = 3;
  const pageObjs = [];
  for (const layout of pageLayouts) {
    const pageId = nextId++;
    const contentId = nextId++;
    const imageIds = layout.items.map(() => nextId++);
    pageObjs.push({ pageId, contentId, imageIds, layout });
  }

  beginObj(1);
  write("<< /Type /Catalog /Pages 2 0 R >>");
  endObj();

  beginObj(2);
  write(`<< /Type /Pages /Kids [${pageObjs.map((p) => `${p.pageId} 0 R`).join(" ")}] /Count ${pageObjs.length} >>`);
  endObj();

  const pageW = mmToPt(210);
  const pageH = mmToPt(297);

  for (const { pageId, contentId, imageIds, layout } of pageObjs) {
    const xObjects = imageIds.map((id, i) => `/Im${i} ${id} 0 R`).join(" ");
    beginObj(pageId);
    write(
      `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${pageW.toFixed(2)} ${pageH.toFixed(2)}] ` +
        `/Resources << /XObject << ${xObjects} >> >> /Contents ${contentId} 0 R >>`
    );
    endObj();

    // Content stream: PDF y grows up; our layout uses top-left mm
    const ops = [];
    layout.items.forEach((item, i) => {
      const x = mmToPt(item.xMm);
      const y = pageH - mmToPt(item.yMm) - mmToPt(item.hMm);
      const w = mmToPt(item.wMm);
      const h = mmToPt(item.hMm);
      const rr = item.rounded ? mmToPt(CORNER_RADIUS_MM) : 0;
      ops.push("q");
      if (rr > 0) {
        ops.push(...pdfRoundedRectPath(x, y, w, h, rr));
        ops.push("W n");
      }
      ops.push(`${w.toFixed(2)} 0 0 ${h.toFixed(2)} ${x.toFixed(2)} ${y.toFixed(2)} cm`);
      ops.push(`/Im${i} Do`);
      ops.push("Q");
      if (item.guide) {
        ops.push("q");
        ops.push("0.4 w");
        ops.push("[2 2] 0 d");
        ops.push("0.25 0.25 0.25 RG");
        if (rr > 0) ops.push(...pdfRoundedRectPath(x, y, w, h, rr), "S");
        else ops.push(`${x.toFixed(2)} ${y.toFixed(2)} ${w.toFixed(2)} ${h.toFixed(2)} re S`);
        ops.push("Q");
      }
      if (item.border) {
        ops.push("q");
        ops.push("1 w");
        ops.push("[] 0 d");
        ops.push("0 0 0 RG");
        if (rr > 0) ops.push(...pdfRoundedRectPath(x, y, w, h, rr), "S");
        else ops.push(`${x.toFixed(2)} ${y.toFixed(2)} ${w.toFixed(2)} ${h.toFixed(2)} re S`);
        ops.push("Q");
      }
    });
    const content = ops.join("\n");
    const contentBytes = encoder.encode(content);
    beginObj(contentId);
    write(`<< /Length ${contentBytes.length} >>\nstream\n`);
    write(contentBytes);
    write("\nendstream");
    endObj();

    layout.items.forEach((item, i) => {
      const imgId = imageIds[i];
      const { bytes, width, height } = item.jpeg;
      beginObj(imgId);
      write(
        `<< /Type /XObject /Subtype /Image /Width ${width} /Height ${height} ` +
          `/ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode /Length ${bytes.length} >>\nstream\n`
      );
      write(bytes);
      write("\nendstream");
      endObj();
    });
  }

  const xrefPos = pos;
  const maxId = nextId - 1;
  write(`xref\n0 ${maxId + 1}\n`);
  write("0000000000 65535 f \n");
  for (let id = 1; id <= maxId; id++) {
    write(`${String(objStarts[id]).padStart(10, "0")} 00000 n \n`);
  }
  write(`trailer\n<< /Size ${maxId + 1} /Root 1 0 R >>\n`);
  write(`startxref\n${xrefPos}\n%%EOF`);

  // Concatenate
  let total = 0;
  for (const c of chunks) total += c.length;
  const out = new Uint8Array(total);
  let o = 0;
  for (const c of chunks) {
    out.set(c, o);
    o += c.length;
  }
  return new Blob([out], { type: "application/pdf" });
}

async function buildPrintPdfBlob() {
  if (!state.front && !state.back) throw new Error("No cards to export.");

  const bothOnOne = showBoth.checked;
  const sideBySide = document.getElementById("sideBySide")?.checked;
  const withGuide = cutGuides.checked;
  const withSolid = Boolean(solidGuides?.checked) && !withGuide;
  const withRound = Boolean(roundedCorners?.checked);
  const margin = PAGE_MARGIN_MM;
  const gap = getCardGapMm();

  const sides = [];
  if (state.front) sides.push(state.front);
  if (state.back) sides.push(state.back);

  const rounded = await Promise.all(sides.map((src) => applyRoundedCorners(src, { transparent: false })));
  const jpegs = await Promise.all(rounded.map((src) => dataUrlToJpegBytes(src)));

  const card = (jpeg, xMm, yMm) => ({
    jpeg,
    xMm,
    yMm,
    wMm: CARD_W_MM,
    hMm: CARD_H_MM,
    guide: withGuide,
    border: withSolid,
    rounded: withRound,
  });

  let pages;
  if (jpegs.length === 1) {
    pages = [{ items: [card(jpegs[0], margin, margin)] }];
  } else if (bothOnOne && sideBySide) {
    const x1 = Math.min(PAGE_W_MM - margin - CARD_W_MM, margin + CARD_W_MM + gap);
    pages = [
      {
        items: [
          card(jpegs[0], margin, margin),
          card(jpegs[1], x1, margin),
        ],
      },
    ];
  } else if (bothOnOne) {
    const y1 = Math.min(PAGE_H_MM - margin - CARD_H_MM, margin + CARD_H_MM + gap);
    pages = [
      {
        items: [
          card(jpegs[0], margin, margin),
          card(jpegs[1], margin, y1),
        ],
      },
    ];
  } else {
    pages = [
      { items: [card(jpegs[0], margin, margin)] },
      { items: [card(jpegs[1], margin, margin)] },
    ];
  }

  return buildA4Pdf(pages);
}

function pdfFileName() {
  const base =
    (state.file?.name || "e-aadhaar").replace(/\.pdf$/i, "").replace(/[^\w\-]+/g, "_") ||
    "e-aadhaar";
  return `${base}-print-A4.pdf`;
}

function pngFileName(side) {
  const id =
    (state.aadhaarNumber && String(state.aadhaarNumber).replace(/\D/g, "")) ||
    sanitizeFileNamePart(state.cardName) ||
    "aadhaar";
  return `${id}-${side}.png`;
}

function crc32(bytes) {
  let c = ~0;
  for (let i = 0; i < bytes.length; i++) {
    c ^= bytes[i];
    for (let k = 0; k < 8; k++) c = (c >>> 1) ^ (0xedb88320 & -(c & 1));
  }
  return ~c >>> 0;
}

function u32be(n) {
  return new Uint8Array([(n >>> 24) & 255, (n >>> 16) & 255, (n >>> 8) & 255, n & 255]);
}

/** Insert pHYs so editors treat the PNG as 85.5Ã—54 mm at our export DPI. */
function pngWithPhysicalSize(pngBytes, dpi = state.exportDpi) {
  if (pngBytes.length < 33) return pngBytes;
  // Skip if pHYs already present
  for (let i = 8; i < Math.min(pngBytes.length - 4, 200); i++) {
    if (
      pngBytes[i] === 0x70 &&
      pngBytes[i + 1] === 0x48 &&
      pngBytes[i + 2] === 0x59 &&
      pngBytes[i + 3] === 0x73
    ) {
      return pngBytes;
    }
  }

  const ppm = Math.round(dpi / 0.0254);
  const type = new TextEncoder().encode("pHYs");
  const data = new Uint8Array(9);
  data.set(u32be(ppm), 0);
  data.set(u32be(ppm), 4);
  data[8] = 1; // meter
  const crcIn = new Uint8Array(13);
  crcIn.set(type, 0);
  crcIn.set(data, 4);
  const chunk = new Uint8Array(4 + 4 + 9 + 4);
  chunk.set(u32be(9), 0);
  chunk.set(type, 4);
  chunk.set(data, 8);
  chunk.set(u32be(crc32(crcIn)), 17);

  // Signature (8) + IHDR (4+4+13+4 = 25) = 33
  const ihdrEnd = 8 + 25;
  const out = new Uint8Array(pngBytes.length + chunk.length);
  out.set(pngBytes.subarray(0, ihdrEnd), 0);
  out.set(chunk, ihdrEnd);
  out.set(pngBytes.subarray(ihdrEnd), ihdrEnd + chunk.length);
  return out;
}

function dataUrlToBytes(dataUrl) {
  const bin = atob(dataUrl.split(",")[1]);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

function triggerDownload(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 2000);
}

async function downloadCardPng(side) {
  const src = side === "back" ? state.back : state.front;
  if (!src) {
    setStatus(`No ${side} card to download.`, "error");
    return;
  }
  try {
    const rounded = await applyRoundedCorners(src, { transparent: true });
    const raw = dataUrlToBytes(rounded);
    const withPhys = pngWithPhysicalSize(raw, state.exportDpi);
    const blob = new Blob([withPhys], { type: "image/png" });
    triggerDownload(blob, pngFileName(side));
    setStatus(`${side === "back" ? "Back" : "Front"} PNG saved`, "ok");
  } catch (err) {
    console.error(err);
    setStatus(err.message || "PNG download failed.", "error");
  }
}

async function downloadPdf() {
  if (!state.front && !state.back) return;
  try {
    setStatus("Building PDFâ€¦");
    if (downloadPdfBtn) downloadPdfBtn.disabled = true;
    const blob = await buildPrintPdfBlob();
    triggerDownload(blob, pdfFileName());
    setStatus("PDF downloaded", "ok");
  } catch (err) {
    console.error(err);
    setStatus(err.message || "PDF download failed.", "error");
  } finally {
    updateButtons();
  }
}

async function printClean() {
  if (!state.front && !state.back) return;
  try {
    setStatus("Preparing printâ€¦");
    printBtn.disabled = true;
    const blob = await buildPrintPdfBlob();
    const url = URL.createObjectURL(blob);

    // Open/print the PDF blob â€” no HTML date/title header
    const win = window.open(url, "_blank");
    if (!win) {
      // Popup blocked â€” fall back to download
      const a = document.createElement("a");
      a.href = url;
      a.download = pdfFileName();
      document.body.appendChild(a);
      a.click();
      a.remove();
      setStatus("Popup blocked â€” PDF downloaded instead", "ok");
      setTimeout(() => URL.revokeObjectURL(url), 60000);
      updateButtons();
      return;
    }

    const tryPrint = () => {
      try {
        win.focus();
        win.print();
      } catch {
        /* user can print from the tab */
      }
    };
    // PDF viewer needs a moment to load
    setTimeout(tryPrint, 600);
    setStatus("Print ready", "ok");
    setTimeout(() => URL.revokeObjectURL(url), 120000);
  } catch (err) {
    console.error(err);
    setStatus(err.message || "Print failed.", "error");
  } finally {
    updateButtons();
  }
}

function addMargin(rect, W, H, marginPx, bottomExtraPx = 0) {
  const x = Math.max(0, Math.floor(rect.x - marginPx));
  const y = Math.max(0, Math.floor(rect.y - marginPx));
  const x2 = Math.min(W - 1, Math.ceil(rect.x + rect.w - 1 + marginPx));
  const y2 = Math.min(H - 1, Math.ceil(rect.y + rect.h - 1 + marginPx + bottomExtraPx));
  return { x, y, w: x2 - x + 1, h: y2 - y + 1 };
}

function cropFillPrintBox(canvas, rect) {
  const targetW = Math.round(mmPx(CARD_W_MM));
  const targetH = Math.round(mmPx(CARD_H_MM));
  const sx = Math.max(0, Math.floor(rect.x));
  const sy = Math.max(0, Math.floor(rect.y));
  const sw = Math.min(Math.floor(rect.w), canvas.width - sx);
  const sh = Math.min(Math.floor(rect.h), canvas.height - sy);
  if (sw <= 0 || sh <= 0) return null;

  const out = document.createElement("canvas");
  out.width = targetW;
  out.height = targetH;
  const ctx = out.getContext("2d");
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, targetW, targetH);
  ctx.drawImage(canvas, sx, sy, sw, sh, 0, 0, targetW, targetH);
  return out.toDataURL("image/png");
}

async function renderPageCanvas(page) {
  const scale = RENDER_DPI / 72;
  const viewport = page.getViewport({ scale });
  const canvas = document.createElement("canvas");
  canvas.width = Math.floor(viewport.width);
  canvas.height = Math.floor(viewport.height);
  const ctx = canvas.getContext("2d", { willReadFrequently: true });
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  await page.render({ canvasContext: ctx, viewport }).promise;
  return { canvas, ctx, viewport };
}

async function openPdf(file, password) {
  const data = new Uint8Array(await file.arrayBuffer());
  try {
    return await pdfjsLib.getDocument({ data, password: password || "" }).promise;
  } catch (err) {
    const msg = String(err?.message || err);
    if (/password/i.test(msg) || err?.name === "PasswordException") {
      if (err?.code === pdfjsLib.PasswordResponses?.INCORRECT_PASSWORD || /incorrect/i.test(msg)) {
        throw new Error("Incorrect password. Use first 4 letters of name (CAPS) + birth year.");
      }
      throw new Error("This PDF is password-protected. Enter the UIDAI password above.");
    }
    throw err;
  }
}

function multiply(m1, m2) {
  return [
    m1[0] * m2[0] + m1[2] * m2[1],
    m1[1] * m2[0] + m1[3] * m2[1],
    m1[0] * m2[2] + m1[2] * m2[3],
    m1[1] * m2[2] + m1[3] * m2[3],
    m1[0] * m2[4] + m1[2] * m2[5] + m1[4],
    m1[1] * m2[4] + m1[3] * m2[5] + m1[5],
  ];
}

function isCardAspect(a) {
  return Number.isFinite(a) && a > 1.35 && a < 1.85;
}

function isBorderRed(r, g, b) {
  if (r < 140) return false;
  if (g > 110 || b > 110) return false;
  if (r - g < 40 || r - b < 40) return false;
  if (g > 85 && r - g < 70) return false;
  return true;
}

function clusterIndices(indices, maxGap = 3) {
  if (!indices.length) return [];
  const sorted = [...indices].sort((a, b) => a - b);
  const bands = [];
  let start = sorted[0];
  let end = sorted[0];
  let sum = sorted[0];
  let n = 1;
  for (let i = 1; i < sorted.length; i++) {
    const v = sorted[i];
    if (v - end <= maxGap) {
      end = v;
      sum += v;
      n++;
    } else {
      bands.push({ start, end, center: Math.round(sum / n), count: n });
      start = end = v;
      sum = v;
      n = 1;
    }
  }
  bands.push({ start, end, center: Math.round(sum / n), count: n });
  return bands;
}

// â”€â”€ AI Layer 1: text landmarks â€” ONLY in bottom cuttable strip â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function extractTextLandmarks(page, viewport) {
  const tc = await page.getTextContent();
  const items = [];
  for (const item of tc.items) {
    const str = (item.str || "").trim();
    if (!str) continue;
    const m = pdfjsLib.Util.transform(viewport.transform, item.transform);
    const x = m[4];
    const y = viewport.height - m[5];
    items.push({
      str,
      x,
      y,
      w: Math.abs(item.width * viewport.scale) || 8,
      h: Math.abs(item.height * viewport.scale) || 10,
    });
  }
  return items;
}

/** Reject letter-body hits; only use cues in the bottom card strip. */
function parseCardLandmarks(items, W, H) {
  const expectW = mmPx(CARD_W_MM);
  const expectH = mmPx(CARD_H_MM);
  // Cuttable cards are always in the lowest part of the e-Aadhaar page
  const bandMinY = H * 0.55;
  const lower = items.filter((it) => it.y > bandMinY);

  const hit = (re) => lower.filter((it) => re.test(it.str));

  // Prefer cues unique to the PVC faces (not the letter body)
  const frontHits = [
    ...hit(/aadhaar\s*no\.?\s*issued/i),
    ...hit(/government\s*of\s*india/i),
  ];
  const backHits = [
    ...hit(/details\s*as\s*on/i),
    ...hit(/help@uidai\.gov\.in/i),
    ...hit(/www\.uidai\.gov\.in/i),
    ...hit(/unique\s*identification\s*authority/i),
  ];

  // Ignore "proof of identity" â€” it appears in the LETTER and caused wrong crops
  const midX = W / 2;
  const leftHits = frontHits.filter((h) => h.x < midX);
  const rightHits = backHits.filter((h) => h.x >= midX);

  const cutHits = items.filter((it) => /cut\s*along|scissors/i.test(it.str) && it.y > H * 0.4);
  let bandTop = H - expectH;
  if (cutHits.length) {
    bandTop = Math.max(bandMinY, Math.min(...cutHits.map((h) => h.y)) + 2);
  }

  // Must have bottom-strip cues; otherwise skip this layer entirely
  if (!leftHits.length && !rightHits.length && !cutHits.length) return null;

  const front = {
    x: Math.max(0, Math.round(midX - expectW - 6)),
    y: Math.round(Math.min(H - expectH, Math.max(bandTop, H - expectH - mmPx(2)))),
    w: Math.round(expectW),
    h: Math.round(expectH),
  };
  const back = {
    x: Math.min(W - expectW, Math.round(midX + 6)),
    y: front.y,
    w: Math.round(expectW),
    h: Math.round(expectH),
  };

  // Only accept if landmarks sit inside these bottom boxes
  const inBox = (h, b) => h.x >= b.x - 20 && h.x <= b.x + b.w + 20 && h.y >= b.y - 20 && h.y <= b.y + b.h + 20;
  const frontOk = leftHits.some((h) => inBox(h, front)) || cutHits.length > 0;
  const backOk = rightHits.some((h) => inBox(h, back)) || cutHits.length > 0;
  if (!frontOk && !backOk) return null;

  return {
    front,
    back,
    mode: "ai-text-bottom",
    score: 4 + leftHits.length + rightHits.length,
  };
}

/** English cardholder name from PDF text (for download filenames). */
function extractCardholderName(items, frontRect) {
  const reject =
    /government|india|aadhaar|uidai|male|female|dob|gender|issued|vid|www\.|help@|cut\s*along|unique|identification|authority|details\s*as|enrolment|download|mobile|email|address|s\/o|d\/o|c\/o|w\/o|proof|identity|resident|verify|qr\s*code|virtual|offline|xml|phone|1947|tamil|nadu|pincode|pin\s*code|district|state|to\b/i;

  const isName = (raw) => {
    const s = String(raw || "").trim().replace(/\s+/g, " ");
    if (s.length < 2 || s.length > 48) return false;
    if (reject.test(s)) return false;
    if (/\d/.test(s)) return false;
    // Latin letters / spaces / .' - only (skip Tamil lines)
    if (!/^[A-Za-z][A-Za-z .'\-]*$/.test(s)) return false;
    // At least one 2+ letter word
    if (!/[A-Za-z]{2,}/.test(s)) return false;
    return true;
  };

  const inRect = (it, r) =>
    !r ||
    (it.x >= r.x - 12 &&
      it.x <= r.x + r.w + 12 &&
      it.y >= r.y - 12 &&
      it.y <= r.y + r.h + 12);

  const frontItems = items
    .filter((it) => inRect(it, frontRect))
    .sort((a, b) => a.y - b.y || a.x - b.x);

  const dobIdx = frontItems.findIndex(
    (it) =>
      /^\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4}$/.test(it.str.trim()) || /^DOB\b/i.test(it.str.trim())
  );

  if (dobIdx > 0) {
    for (let i = dobIdx - 1; i >= Math.max(0, dobIdx - 6); i--) {
      if (isName(frontItems[i].str)) return frontItems[i].str.trim().replace(/\s+/g, " ");
    }
  }

  for (const it of frontItems) {
    if (isName(it.str)) return it.str.trim().replace(/\s+/g, " ");
  }

  // Letter body fallback (often above the cut strip)
  const upper = items
    .filter((it) => !frontRect || it.y < frontRect.y)
    .sort((a, b) => a.y - b.y);
  for (const it of upper) {
    if (isName(it.str) && it.str.trim().split(/\s+/).length <= 4) {
      return it.str.trim().replace(/\s+/g, " ");
    }
  }
  return null;
}

/** 12-digit Aadhaar number from PDF text (for PNG filenames). */
function extractAadhaarNumber(items, frontRect) {
  const pickFromText = (text) => {
    const s = String(text || "");
    // Prefer spaced form as printed on the card
    let m = s.match(/\b(\d{4})\s+(\d{4})\s+(\d{4})\b/);
    if (m) return `${m[1]}${m[2]}${m[3]}`;
    // Compact 12 digits (avoid 16-digit VID)
    m = s.match(/(?<!\d)(\d{12})(?!\d)/);
    if (m) return m[1];
    return null;
  };

  const inRect = (it, r) =>
    !r ||
    (it.x >= r.x - 12 &&
      it.x <= r.x + r.w + 12 &&
      it.y >= r.y - 12 &&
      it.y <= r.y + r.h + 12);

  // 1) Single text items inside front card
  const frontItems = items.filter((it) => inRect(it, frontRect));
  for (const it of frontItems) {
    const n = pickFromText(it.str);
    if (n) return n;
  }

  // 2) Digits sometimes split across nearby items — join by line
  const byLine = new Map();
  for (const it of frontItems) {
    const key = Math.round(it.y / 8);
    if (!byLine.has(key)) byLine.set(key, []);
    byLine.get(key).push(it);
  }
  for (const row of byLine.values()) {
    row.sort((a, b) => a.x - b.x);
    const n = pickFromText(row.map((it) => it.str).join(" "));
    if (n) return n;
  }

  // 3) Anywhere on the page (letter often repeats the number)
  for (const it of items) {
    const n = pickFromText(it.str);
    if (n) return n;
  }
  const n = pickFromText(items.map((it) => it.str).join(" "));
  return n || null;
}

function sanitizeFileNamePart(name) {
  return String(name || "")
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s.\-]+/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 60);
}

/** True if rect looks like a QR block (dense B/W square) — required for BACK card. */
function hasQrLikeSquare(data, W, H, rect) {
  if (!rect) return false;
  const x0 = Math.floor(rect.x + rect.w * 0.42);
  const x1 = Math.floor(rect.x + rect.w * 0.98);
  const y0 = Math.floor(rect.y + rect.h * 0.18);
  const y1 = Math.floor(rect.y + rect.h * 0.72);
  if (x1 <= x0 || y1 <= y0) return false;

  const size = Math.min(x1 - x0, y1 - y0);
  const step = Math.max(2, Math.floor(size / 40));
  let dark = 0;
  let light = 0;
  let total = 0;
  for (let y = y0; y < y0 + size; y += step) {
    for (let x = x0; x < x0 + size; x += step) {
      if (x >= W || y >= H) continue;
      const i = (y * W + x) * 4;
      const v = (data[i] + data[i + 1] + data[i + 2]) / 3;
      total++;
      if (v < 90) dark++;
      else if (v > 200) light++;
    }
  }
  if (total < 40) return false;
  const darkR = dark / total;
  const lightR = light / total;
  // QR codes are roughly half dark modules on white
  return darkR > 0.22 && darkR < 0.72 && lightR > 0.15;
}

/** Front card usually has a photo block on the left (skin/gray tones, not QR). */
function hasPhotoBlock(data, W, H, rect) {
  if (!rect) return false;
  const x0 = Math.floor(rect.x + rect.w * 0.04);
  const x1 = Math.floor(rect.x + rect.w * 0.38);
  const y0 = Math.floor(rect.y + rect.h * 0.22);
  const y1 = Math.floor(rect.y + rect.h * 0.72);
  let ink = 0;
  let total = 0;
  const step = 3;
  for (let y = y0; y < y1; y += step) {
    for (let x = x0; x < x1; x += step) {
      if (x >= W || y >= H) continue;
      const i = (y * W + x) * 4;
      total++;
      if (data[i] < 245 || data[i + 1] < 245 || data[i + 2] < 245) ink++;
    }
  }
  return total > 30 && ink / total > 0.35;
}

/** Hard rule: cuttable cards must sit near the bottom of the page (not letter body). */
function isBottomCardZone(rect, H) {
  if (!rect) return false;
  const bottom = rect.y + rect.h;
  return rect.y >= H * 0.48 && bottom >= H * 0.78;
}

function scoreDetection(det, data, W, H) {
  if (!det?.front) return -999;
  let s = det.score || 0;
  if (!isBottomCardZone(det.front, H)) s -= 40;
  if (det.back && !isBottomCardZone(det.back, H)) s -= 40;
  // Aligned side-by-side pair
  if (det.back && Math.abs(det.front.y - det.back.y) < det.front.h * 0.25) s += 6;
  if (det.back && det.back.x > det.front.x + det.front.w * 0.5) s += 4;
  if (det.back && hasQrLikeSquare(data, W, H, det.back)) s += 14;
  else if (det.back) s -= 8;
  if (hasPhotoBlock(data, W, H, det.front)) s += 8;
  return s;
}

// â”€â”€ AI Layer 2: painted image rectangles (pdf operator list) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function extractImageRects(page, viewport) {
  const ops = await page.getOperatorList();
  const OPS = pdfjsLib.OPS;
  const stack = [];
  let ctm = [1, 0, 0, 1, 0, 0];
  const found = [];

  for (let i = 0; i < ops.fnArray.length; i++) {
    const fn = ops.fnArray[i];
    const args = ops.argsArray[i];
    if (fn === OPS.save) stack.push(ctm.slice());
    else if (fn === OPS.restore) ctm = stack.pop() || [1, 0, 0, 1, 0, 0];
    else if (fn === OPS.transform) ctm = multiply(ctm, args);
    else if (
      fn === OPS.paintImageXObject ||
      fn === OPS.paintInlineImageXObject ||
      fn === OPS.paintImageXObjectRepeat
    ) {
      const corners = [
        [ctm[4], ctm[5]],
        [ctm[4] + ctm[0], ctm[5] + ctm[1]],
        [ctm[4] + ctm[2], ctm[5] + ctm[3]],
        [ctm[4] + ctm[0] + ctm[2], ctm[5] + ctm[1] + ctm[3]],
      ];
      const xs = corners.map((c) => c[0]);
      const ys = corners.map((c) => c[1]);
      const pdfRect = {
        x: Math.min(...xs),
        y: Math.min(...ys),
        w: Math.max(...xs) - Math.min(...xs),
        h: Math.max(...ys) - Math.min(...ys),
      };
      if (pdfRect.w < 20 || pdfRect.h < 20) continue;
      const [x1, y1, x2, y2] = viewport.convertToViewportRectangle([
        pdfRect.x,
        pdfRect.y,
        pdfRect.x + pdfRect.w,
        pdfRect.y + pdfRect.h,
      ]);
      const rect = {
        x: Math.min(x1, x2),
        y: Math.min(y1, y2),
        w: Math.abs(x2 - x1),
        h: Math.abs(y2 - y1),
      };
      rect.area = rect.w * rect.h;
      rect.aspect = rect.w / rect.h;
      found.push(rect);
    }
  }
  return found;
}

function pickCardsFromImages(rects, H) {
  const expectW = mmPx(CARD_W_MM);
  const expectH = mmPx(CARD_H_MM);
  const cards = rects
    .filter((r) => isCardAspect(r.aspect) || isCardAspect(r.w / r.h))
    .filter((r) => r.w > expectW * 0.5 && r.h > expectH * 0.5)
    .filter((r) => isBottomCardZone(r, H))
    .sort((a, b) => b.area - a.area);

  if (cards.length >= 2) {
    const pair = cards.slice(0, 2).sort((a, b) => a.x - b.x);
    return { front: pair[0], back: pair[1], mode: "ai-images", score: 10 };
  }

  const strips = rects
    .filter((r) => r.aspect > 2.4 && r.aspect < 3.6 && isBottomCardZone(r, H))
    .sort((a, b) => b.area - a.area);
  if (strips[0]) {
    const s = strips[0];
    const gap = Math.max(4, Math.round(s.w * 0.015));
    const half = Math.floor((s.w - gap) / 2);
    return {
      front: { x: s.x, y: s.y, w: half, h: s.h },
      back: { x: s.x + half + gap, y: s.y, w: s.w - half - gap, h: s.h },
      mode: "ai-image-strip",
      score: 8,
    };
  }
  return null;
}

// â”€â”€ AI Layer 3: red UIDAI border (visual) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function detectRedBorderCards(canvas, ctx) {
  const W = canvas.width;
  const H = canvas.height;
  const { data } = ctx.getImageData(0, 0, W, H);
  const expectW = mmPx(CARD_W_MM);
  const expectH = mmPx(CARD_H_MM);
  const pad = Math.round(mmPx(1.5));

  const redAt = (x, y) => {
    if (x < 0 || y < 0 || x >= W || y >= H) return false;
    const i = (y * W + x) * 4;
    return isBorderRed(data[i], data[i + 1], data[i + 2]);
  };

  // Only search the bottom cuttable strip â€” never the letter body
  const y0 = Math.floor(H * 0.52);
  const hHits = [];
  for (let y = y0; y < H; y++) {
    let run = 0;
    let best = 0;
    let count = 0;
    for (let x = 0; x < W; x++) {
      if (redAt(x, y)) {
        run++;
        count++;
        if (run > best) best = run;
      } else run = 0;
    }
    if (best >= expectW * 0.5 || count >= expectW * 0.7) hHits.push(y);
  }
  const hBands = clusterIndices(hHits, 4);
  if (hBands.length < 2) return null;

  let bestPair = null;
  let bestScore = Infinity;
  for (let i = 0; i < hBands.length; i++) {
    for (let j = i + 1; j < hBands.length; j++) {
      const top = hBands[i].start;
      const bot = hBands[j].end;
      const h = bot - top + 1;
      if (h < expectH * 0.85 || h > expectH * 1.25) continue;
      const score = Math.abs(h - expectH) * 2 - h * 0.02 + (H - bot) * 0.01;
      if (score < bestScore) {
        bestScore = score;
        bestPair = { top, bot };
      }
    }
  }
  if (!bestPair) {
    const last = hBands.slice(-2);
    if (last.length < 2) return null;
    bestPair = { top: last[0].start, bot: last[1].end };
  }

  const searchTop = Math.max(y0, bestPair.top - Math.round(expectH * 0.12));
  const searchBot = Math.min(H - 1, bestPair.bot + Math.round(expectH * 0.12));
  const midX = Math.floor(W / 2);
  const overlap = Math.round(expectW * 0.1);

  const frameIn = (xMin, xMax) => {
    const bandH = searchBot - searchTop + 1;
    const minVert = Math.max(6, Math.floor(bandH * 0.28));
    const vCols = [];
    for (let x = xMin; x <= xMax; x++) {
      let run = 0;
      let best = 0;
      let count = 0;
      for (let y = searchTop; y <= searchBot; y++) {
        if (redAt(x, y)) {
          run++;
          count++;
          if (run > best) best = run;
        } else run = 0;
      }
      if (best >= minVert || count >= minVert * 1.1) vCols.push(x);
    }
    if (vCols.length < 2) return null;

    let best = null;
    let err = Infinity;
    for (let i = 0; i < vCols.length; i++) {
      for (let j = vCols.length - 1; j > i; j--) {
        const w = vCols[j] - vCols[i] + 1;
        if (w < expectW * 0.8 || w > expectW * 1.25) continue;
        const e = Math.abs(w - expectW) - w * 0.01;
        if (e < err) {
          err = e;
          best = { L: vCols[i], R: vCols[j] };
        }
        break;
      }
    }
    if (!best) best = { L: vCols[0], R: vCols[vCols.length - 1] };

    const minHoriz = Math.max(6, Math.floor(expectW * 0.28));
    const hRows = [];
    for (let y = searchTop; y <= searchBot; y++) {
      let run = 0;
      let bestRun = 0;
      let count = 0;
      for (let x = best.L; x <= best.R; x++) {
        if (redAt(x, y)) {
          run++;
          count++;
          if (run > bestRun) bestRun = run;
        } else run = 0;
      }
      if (bestRun >= minHoriz || count >= minHoriz * 1.1) hRows.push(y);
    }
    if (hRows.length < 2) {
      return { x: best.L, y: searchTop, w: best.R - best.L + 1, h: bandH };
    }
    return {
      x: best.L,
      y: hRows[0],
      w: best.R - best.L + 1,
      h: hRows[hRows.length - 1] - hRows[0] + 1,
    };
  };

  const front = frameIn(0, midX + overlap);
  const back = frameIn(midX - overlap, W - 1);
  if (!front && !back) return null;

  const expand = (r) => {
    if (!r) return null;
    return addMargin(r, W, H, pad, Math.round(mmPx(1.5)));
  };

  return { front: expand(front), back: expand(back), mode: "ai-red", score: 7 };
}

// â”€â”€ AI Layer 4: bottom-anchored layout fallback â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function detectCardsByLayout(canvas, ctx) {
  const W = canvas.width;
  const H = canvas.height;
  const { data } = ctx.getImageData(0, 0, W, H);
  const expectW = mmPx(CARD_W_MM);
  const expectH = mmPx(CARD_H_MM);
  const pad = Math.round(mmPx(1.2));

  const isInk = (x, y) => {
    if (x < 0 || y < 0 || x >= W || y >= H) return false;
    const i = (y * W + x) * 4;
    return data[i] < 250 || data[i + 1] < 250 || data[i + 2] < 250;
  };

  // Always start from the physical bottom of the page (PVC strip â€” NOT letter body)
  let pageBottom = -1;
  for (let y = H - 1; y >= Math.floor(H * 0.5); y--) {
    for (let x = 0; x < W; x += 2) {
      if (isInk(x, y)) {
        pageBottom = y;
        break;
      }
    }
    if (pageBottom >= 0) break;
  }
  if (pageBottom < 0) return null;

  let bandBot = pageBottom;
  let bandTop = Math.max(Math.floor(H * 0.52), Math.round(bandBot - expectH - mmPx(3)));

  for (let y = bandTop - 1; y >= Math.floor(H * 0.48); y--) {
    let ink = 0;
    for (let x = 0; x < W; x += 2) if (isInk(x, y)) ink++;
    if (ink < W * 0.025) break;
    bandTop = y;
  }

  let minX = W;
  let maxX = 0;
  for (let y = bandTop; y <= bandBot; y++) {
    for (let x = 0; x < W; x++) {
      if (!isInk(x, y)) continue;
      if (x < minX) minX = x;
      if (x > maxX) maxX = x;
    }
  }
  if (maxX <= minX) return null;

  minX = Math.max(0, minX - pad);
  maxX = Math.min(W - 1, maxX + pad);
  bandTop = Math.max(0, bandTop - pad);
  bandBot = Math.min(H - 1, bandBot + pad);

  const boxW = maxX - minX + 1;
  const boxH = bandBot - bandTop + 1;
  const gap = Math.max(4, Math.round(expectW * 0.02));
  const half = Math.floor((boxW - gap) / 2);

  return {
    front: { x: minX, y: bandTop, w: half, h: boxH },
    back: { x: minX + half + gap, y: bandTop, w: boxW - half - gap, h: boxH },
    mode: "ai-bottom-strip",
    score: 12,
  };
}

function tightenToRedCard(data, W, H, rough) {
  if (!rough) return null;
  const marginPx = Math.round(mmPx(MARGIN_MM));
  const bottomExtra = Math.round(mmPx(MARGIN_BOTTOM_MM - MARGIN_MM));
  const expectW = mmPx(CARD_W_MM);
  const expectH = mmPx(CARD_H_MM);

  const x0 = Math.max(0, Math.floor(rough.x));
  const y0 = Math.max(0, Math.floor(rough.y));
  const x1 = Math.min(W - 1, Math.ceil(rough.x + rough.w - 1));
  const y1 = Math.min(H - 1, Math.ceil(rough.y + rough.h - 1));

  let minX = x1;
  let minY = y1;
  let maxX = x0;
  let maxY = y0;
  let count = 0;
  for (let y = y0; y <= y1; y++) {
    for (let x = x0; x <= x1; x++) {
      const i = (y * W + x) * 4;
      if (!isBorderRed(data[i], data[i + 1], data[i + 2])) continue;
      count++;
      if (x < minX) minX = x;
      if (y < minY) minY = y;
      if (x > maxX) maxX = x;
      if (y > maxY) maxY = y;
    }
  }
  if (count < 100) return null;
  const w = maxX - minX + 1;
  const h = maxY - minY + 1;
  if (w < expectW * 0.55 || h < expectH * 0.55) return null;
  if (w / h < 1.25 || w / h > 1.95) return null;

  let foot = maxY;
  for (let y = maxY + 1; y <= Math.min(y1, maxY + Math.round(expectH * 0.12)); y++) {
    let red = 0;
    for (let x = minX; x <= maxX; x += 2) {
      const i = (y * W + x) * 4;
      if (isBorderRed(data[i], data[i + 1], data[i + 2])) red++;
    }
    if (red < (maxX - minX) * 0.15) break;
    foot = y;
  }
  maxY = foot;
  return addMargin({ x: minX, y: minY, w: maxX - minX + 1, h: maxY - minY + 1 }, W, H, marginPx, bottomExtra);
}

function trimSideGutters(isInk, W, H, rect) {
  if (!rect) return null;
  const expectW = mmPx(CARD_W_MM);
  const expectH = mmPx(CARD_H_MM);
  const marginPx = Math.round(mmPx(MARGIN_MM));
  const bottomExtra = Math.round(mmPx(MARGIN_BOTTOM_MM - MARGIN_MM));
  const threshold = Math.max(4, Math.floor(rect.h * 0.12));

  const colInk = (x) => {
    let c = 0;
    for (let y = rect.y; y < rect.y + rect.h; y++) if (isInk(x, y)) c++;
    return c;
  };

  let left = rect.x;
  let right = rect.x + rect.w - 1;
  while (left < right - expectW * 0.7 && colInk(left) < threshold) left++;
  while (right > left + expectW * 0.7 && colInk(right) < threshold) right--;

  const rowThresh = Math.max(4, Math.floor((right - left) * 0.08));
  const rowInk = (y) => {
    let c = 0;
    for (let x = left; x <= right; x += 2) if (isInk(x, y)) c++;
    return c;
  };
  let top = rect.y;
  const bot = rect.y + rect.h - 1;
  while (top < bot - expectH * 0.75 && rowInk(top) < rowThresh) top++;

  return addMargin(
    { x: left, y: top, w: right - left + 1, h: bot - top + 1 },
    W,
    H,
    marginPx,
    bottomExtra
  );
}

function bumpBottom(r, H) {
  if (!r) return null;
  const extra = Math.round(mmPx(2.5));
  return { ...r, h: Math.min(H - r.y, r.h + extra) };
}

function pickBestDetection(candidates, data, W, H) {
  const valid = candidates.filter((c) => c?.front);
  if (!valid.length) return null;
  valid.sort((a, b) => scoreDetection(b, data, W, H) - scoreDetection(a, data, W, H));
  const best = valid[0];
  if (!isBottomCardZone(best.front, H)) {
    const bottomOnly = valid.find((c) => isBottomCardZone(c.front, H));
    if (!bottomOnly) return null;
    return bottomOnly;
  }
  if (best.back && !hasQrLikeSquare(data, W, H, best.back)) {
    const withQr = valid.find(
      (c) => isBottomCardZone(c.front, H) && c.back && hasQrLikeSquare(data, W, H, c.back)
    );
    if (withQr) return withQr;
  }
  return best;
}

// â”€â”€ Main AI extract (E-Way Bill style pipeline) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function extractCards() {
  if (!state.file) return;
  if (typeof pdfjsLib === "undefined") {
    setStatus("PDF library failed to load. Check your network and reload.", "error");
    return;
  }

  const password = formatUidaiPassword(passwordInput.value);
  passwordInput.value = password;
  extractBtn.disabled = true;
  extractBtn.classList.add("busy");
  setStatus("Reading PDFâ€¦");

  try {
    const pdf = await openPdf(state.file, password);
    setStatus("Locating cardsâ€¦");
    const page = await pdf.getPage(1);
    const { canvas, ctx, viewport } = await renderPageCanvas(page);
    const { data } = ctx.getImageData(0, 0, canvas.width, canvas.height);
    const W = canvas.width;
    const H = canvas.height;

    const isInk = (x, y) => {
      if (x < 0 || y < 0 || x >= W || y >= H) return false;
      const i = (y * W + x) * 4;
      return data[i] < 250 || data[i + 1] < 250 || data[i + 2] < 250;
    };

    // Layered detectors â€” same multi-pattern approach as E-Way Bill parsers
    const textItems = await extractTextLandmarks(page, viewport);
    const textDet = parseCardLandmarks(textItems, W, H);

    const imageRects = await extractImageRects(page, viewport);
    const imageDet = pickCardsFromImages(imageRects, H);

    const redDet = detectRedBorderCards(canvas, ctx);
    const layoutDet = detectCardsByLayout(canvas, ctx);

    // Prefer bottom PVC strip first (letter body caused the wrong crop)
    let detected = pickBestDetection(
      [layoutDet, redDet, imageDet, textDet],
      data,
      W,
      H
    );
    if (!detected?.front) {
      throw new Error("Could not locate Aadhaar card faces in this PDF.");
    }

    setStatus("Refining cropâ€¦");

    let frontRect = detected.front;
    let backRect = detected.back;

    frontRect = tightenToRedCard(data, W, H, frontRect) || trimSideGutters(isInk, W, H, frontRect);
    backRect = backRect
      ? tightenToRedCard(data, W, H, backRect) || trimSideGutters(isInk, W, H, backRect)
      : null;

    frontRect = bumpBottom(frontRect, H);
    backRect = bumpBottom(backRect, H);

    // Keep full page for manual zoom/drag adjust
    state.pageUrl = canvas.toDataURL("image/png");
    state.pageW = W;
    state.pageH = H;
    state.pageImg = await loadImage(state.pageUrl);

    state.adjust.front = makeAdjustState(frontRect);
    state.adjust.back = backRect ? makeAdjustState(backRect) : null;
    state.cardName = extractCardholderName(textItems, frontRect);
    state.aadhaarNumber = extractAadhaarNumber(textItems, frontRect);
    state.meta = {
      ...detected,
      front: frontRect,
      back: backRect,
      cardName: state.cardName,
      aadhaarNumber: state.aadhaarNumber,
    };

    rebuildPrintFromAdjust();
    buildAdjustUI();
    requestAnimationFrame(() => {
      if (state.adjust.front) syncAdjustViewport("front");
      if (state.adjust.back) syncAdjustViewport("back");
    });
    renderPreview();
    const hint = state.aadhaarNumber
      ? ` · ${state.aadhaarNumber.replace(/(\d{4})(\d{4})(\d{4})/, "$1 $2 $3")}`
      : state.cardName
        ? ` · ${state.cardName}`
        : "";
    setStatus(`Ready${hint}`, "ok");
  } catch (err) {
    console.error(err);
    setStatus(err.message || "Extraction failed.", "error");
  } finally {
    extractBtn.classList.remove("busy");
    updateButtons();
  }
}

function makeAdjustState(rect) {
  return {
    rect: { ...rect },
    zoom: 1,
    // pan in page (source) pixels â€” same space as PDF render
    panX: 0,
    panY: 0,
  };
}

function loadImage(url) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = reject;
    img.src = url;
  });
}

/** Card-aspect window that covers the AI rect (clips gutters / overflow). */
function coverCardAspect(rect) {
  let w;
  let h;
  if (rect.w / rect.h > CARD_ASPECT) {
    h = rect.h;
    w = h * CARD_ASPECT;
  } else {
    w = rect.w;
    h = w / CARD_ASPECT;
  }
  return {
    x: rect.x + (rect.w - w) / 2,
    y: rect.y + (rect.h - h) / 2,
    w,
    h,
  };
}

/**
 * Exact page region shown in the adjust viewport (and used for print).
 * Always 85.5∶54 — WYSIWYG with the red adjust frame.
 */
function getVisibleCrop(adj) {
  const base = coverCardAspect(adj.rect);
  const zoom = Math.max(0.5, Math.min(3, adj.zoom || 1));
  const cropW = base.w / zoom;
  const cropH = base.h / zoom;
  let sx = base.x + base.w / 2 - cropW / 2 - (adj.panX || 0);
  let sy = base.y + base.h / 2 - cropH / 2 - (adj.panY || 0);

  if (cropW >= state.pageW) sx = 0;
  else sx = Math.max(0, Math.min(state.pageW - cropW, sx));
  if (cropH >= state.pageH) sy = 0;
  else sy = Math.max(0, Math.min(state.pageH - cropH, sy));

  return { sx, sy, cropW, cropH, zoom };
}

/** Map zoom/pan into a source crop, then fill the 85.5×54 mm print box. */
function cropFromAdjust(adj) {
  if (!adj || !state.pageImg || !state.pageW || !state.pageH) return null;
  const targetW = exportPx(CARD_W_MM);
  const targetH = exportPx(CARD_H_MM);
  const { sx, sy, cropW, cropH } = getVisibleCrop(adj);

  const out = document.createElement("canvas");
  out.width = targetW;
  out.height = targetH;
  const ctx = out.getContext("2d");
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, targetW, targetH);
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = "high";
  ctx.drawImage(state.pageImg, sx, sy, cropW, cropH, 0, 0, targetW, targetH);
  return out.toDataURL("image/png");
}

function rebuildPrintFromAdjust() {
  revoke(state.front);
  revoke(state.back);
  state.front = cropFromAdjust(state.adjust.front);
  state.back = state.adjust.back ? cropFromAdjust(state.adjust.back) : null;
  renderPreview();
  updateSizePreview();
}

function buildAdjustUI() {
  adjustGrid.replaceChildren();
  const sides = [];
  if (state.adjust.front) sides.push(["front", "Front"]);
  if (state.adjust.back) sides.push(["back", "Back"]);

  for (const [key, label] of sides) {
    const adj = state.adjust[key];
    const panel = document.createElement("div");
    panel.className = "adjust-panel";
    panel.dataset.side = key;

    panel.innerHTML = `
      <h3>${label}</h3>
      <div class="adjust-viewport" data-side="${key}">
        <div class="stage"><img alt="${label}" draggable="false" /></div>
      </div>
      <div class="adjust-controls">
        <div class="zoom-label">
          <span>Zoom</span>
          <input type="range" min="50" max="300" step="1" value="${Math.round(adj.zoom * 100)}" data-zoom="${key}" />
          <span class="zoom-input-wrap">
            <input
              type="number"
              class="zoom-val"
              data-zoom-val="${key}"
              min="50"
              max="300"
              step="1"
              value="${Math.round(adj.zoom * 100)}"
              aria-label="${label} zoom percent"
            />
            <span class="zoom-suffix">%</span>
          </span>
        </div>
        <div class="nudge-pad" title="Nudge 1 px">
          <button type="button" class="nudge nudge-up" data-nudge="0,-1" aria-label="${label} nudge up">▲</button>
          <button type="button" class="nudge nudge-left" data-nudge="-1,0" aria-label="${label} nudge left">◀</button>
          <span class="nudge-label">1px</span>
          <button type="button" class="nudge nudge-right" data-nudge="1,0" aria-label="${label} nudge right">▶</button>
          <button type="button" class="nudge nudge-down" data-nudge="0,1" aria-label="${label} nudge down">▼</button>
        </div>
        <button type="button" class="btn secondary" data-reset="${key}">Reset</button>
        <button type="button" class="btn secondary" data-png="${key}">PNG</button>
      </div>
    `;
    adjustGrid.appendChild(panel);

    const viewport = panel.querySelector(".adjust-viewport");
    const img = panel.querySelector("img");
    const rangeEl = panel.querySelector(`[data-zoom="${key}"]`);
    const percentEl = panel.querySelector(`[data-zoom-val="${key}"]`);
    img.src = state.pageUrl;
    img.onload = () => syncAdjustViewport(key);

    const applyZoomPercent = (raw) => {
      const pct = Math.max(50, Math.min(300, Math.round(Number(raw) || 100)));
      adj.zoom = pct / 100;
      rangeEl.value = String(pct);
      percentEl.value = String(pct);
      syncAdjustViewport(key);
      rebuildPrintFromAdjust();
      return pct;
    };

    const nudgeBy = (dx, dy) => {
      adj.panX += dx;
      adj.panY += dy;
      syncAdjustViewport(key);
      rebuildPrintFromAdjust();
    };

    // Hold to repeat 1px nudges
    panel.querySelectorAll("[data-nudge]").forEach((btn) => {
      let timer = null;
      let interval = null;
      const step = () => {
        const [dx, dy] = btn.dataset.nudge.split(",").map(Number);
        nudgeBy(dx, dy);
      };
      const stop = () => {
        clearTimeout(timer);
        clearInterval(interval);
        timer = null;
        interval = null;
      };
      btn.addEventListener("pointerdown", (e) => {
        e.preventDefault();
        btn.setPointerCapture(e.pointerId);
        step();
        timer = setTimeout(() => {
          interval = setInterval(step, 40);
        }, 350);
      });
      btn.addEventListener("pointerup", stop);
      btn.addEventListener("pointercancel", stop);
      btn.addEventListener("lostpointercapture", stop);
    });

    // Drag â€” pan in page pixels so print matches viewport
    let dragging = false;
    let lastX = 0;
    let lastY = 0;
    viewport.addEventListener("pointerdown", (e) => {
      dragging = true;
      lastX = e.clientX;
      lastY = e.clientY;
      viewport.classList.add("dragging");
      viewport.setPointerCapture(e.pointerId);
    });
    viewport.addEventListener("pointermove", (e) => {
      if (!dragging) return;
      const dx = e.clientX - lastX;
      const dy = e.clientY - lastY;
      lastX = e.clientX;
      lastY = e.clientY;
      const { cropW } = getVisibleCrop(adj);
      const scale = viewport.clientWidth / cropW;
      if (scale > 0) {
        adj.panX += dx / scale;
        adj.panY += dy / scale;
      }
      syncAdjustViewport(key);
      rebuildPrintFromAdjust();
    });
    const endDrag = (e) => {
      dragging = false;
      viewport.classList.remove("dragging");
      try {
        viewport.releasePointerCapture(e.pointerId);
      } catch {
        /* ignore */
      }
    };
    viewport.addEventListener("pointerup", endDrag);
    viewport.addEventListener("pointercancel", endDrag);

    // Wheel zoom (zoom toward viewport center; pan stays in page space)
    viewport.addEventListener(
      "wheel",
      (e) => {
        e.preventDefault();
        const delta = e.deltaY > 0 ? -0.08 : 0.08;
        applyZoomPercent((adj.zoom + delta) * 100);
      },
      { passive: false }
    );

    rangeEl.addEventListener("input", (e) => {
      applyZoomPercent(e.target.value);
    });

    percentEl.addEventListener("change", () => {
      applyZoomPercent(percentEl.value);
    });
    percentEl.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        applyZoomPercent(percentEl.value);
        percentEl.blur();
      }
    });
    percentEl.addEventListener("blur", () => {
      applyZoomPercent(percentEl.value);
    });

    panel.querySelector(`[data-reset="${key}"]`).addEventListener("click", () => {
      adj.panX = 0;
      adj.panY = 0;
      applyZoomPercent(100);
      setStatus(`${label} reset`, "ok");
    });

    panel.querySelector(`[data-png="${key}"]`).addEventListener("click", () => {
      downloadCardPng(key);
    });
  }
}

function syncAdjustViewport(side) {
  const adj = state.adjust[side];
  if (!adj || !state.pageImg || !state.pageW) return;
  const panel = adjustGrid.querySelector(`.adjust-panel[data-side="${side}"]`);
  if (!panel) return;
  const viewport = panel.querySelector(".adjust-viewport");
  const img = panel.querySelector("img");
  const vw = viewport.clientWidth;
  if (!vw) return;

  // Same crop as print â€” fill the 85.5âˆ¶54 viewport exactly
  const { sx, sy, cropW, cropH } = getVisibleCrop(adj);
  const scale = vw / cropW;

  // Keep pan consistent with clamped crop (stops drag runaway at edges)
  const base = coverCardAspect(adj.rect);
  adj.panX = base.x + base.w / 2 - cropW / 2 - sx;
  adj.panY = base.y + base.h / 2 - cropH / 2 - sy;

  img.style.width = `${state.pageW * scale}px`;
  img.style.height = `${state.pageH * scale}px`;
  img.style.transform = `translate(${-sx * scale}px, ${-sy * scale}px)`;
  img.style.left = "0";
  img.style.top = "0";
}

function makeCard(src, label, withGuide, withSolid, withRound) {
  const card = document.createElement("div");
  card.className = "aadhaar-card";
  if (withRound) card.classList.add("rounded");

  const img = document.createElement("img");
  img.src = src;
  img.alt = `Aadhaar ${label}`;
  card.appendChild(img);

  if (withGuide || withSolid) {
    const guide = document.createElement("div");
    guide.className = withSolid ? "guide solid" : "guide";
    card.appendChild(guide);
  }

  const caption = document.createElement("span");
  caption.className = "card-caption";
  caption.textContent = label;
  card.appendChild(caption);
  return card;
}

function renderPreview() {
  const withGuide = cutGuides.checked;
  const withSolid = Boolean(solidGuides?.checked) && !withGuide;
  const withRound = Boolean(roundedCorners?.checked);
  const bothOnOne = showBoth.checked;
  const sideBySide = document.getElementById("sideBySide")?.checked;
  const gap = getCardGapMm();
  document.documentElement.style.setProperty("--card-gap", `${gap}mm`);
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

  const appendCards = (parent, stacked) => {
    if (!stacked && sides.length === 2) {
      const row = document.createElement("div");
      row.className = "card-row";
      for (const side of sides) {
        row.appendChild(makeCard(side.src, side.label, withGuide, withSolid, withRound));
      }
      parent.appendChild(row);
    } else {
      for (const side of sides) {
        parent.appendChild(makeCard(side.src, side.label, withGuide, withSolid, withRound));
      }
    }
  };

  previewSheet.classList.toggle("side-by-side", Boolean(sideBySide));
  appendCards(previewSheet, !sideBySide);

  if (bothOnOne || sides.length === 1) {
    const page = document.createElement("div");
    page.className = `print-page${sideBySide ? " side-by-side" : ""}`;
    appendCards(page, !sideBySide);
    printRoot.appendChild(page);
  } else {
    for (const side of sides) {
      const page = document.createElement("div");
      page.className = "print-page";
      page.appendChild(makeCard(side.src, side.label, withGuide, withSolid, withRound));
      printRoot.appendChild(page);
    }
  }

  updateButtons();
}

function resetAdjustState() {
  state.pageUrl = null;
  state.pageW = 0;
  state.pageH = 0;
  state.pageImg = null;
  state.cardName = null;
  state.aadhaarNumber = null;
  state.adjust.front = null;
  state.adjust.back = null;
  adjustGrid.replaceChildren();
}

function setFile(file) {
  if (!file) return;
  if (!/\.pdf$/i.test(file.name) && file.type !== "application/pdf") {
    setStatus("Please choose an e-Aadhaar PDF file.", "error");
    return;
  }
  state.file = file;
  revoke(state.front);
  revoke(state.back);
  state.front = null;
  state.back = null;
  state.meta = null;
  resetAdjustState();
  pdfHint.textContent = file.name;
  pdfDrop.classList.add("has-file");
  previewWrap.hidden = true;
  previewSheet.replaceChildren();
  printRoot.replaceChildren();
  setStatus("Enter password, then Extract");
  updateButtons();
}

function clearAll() {
  revoke(state.front);
  revoke(state.back);
  state.file = null;
  state.front = null;
  state.back = null;
  state.meta = null;
  resetAdjustState();
  pdfInput.value = "";
  passwordInput.value = "";
  pdfHint.textContent = "Drop file or click to browse";
  pdfDrop.classList.remove("has-file");
  previewWrap.hidden = true;
  previewSheet.replaceChildren();
  printRoot.replaceChildren();
  setStatus("");
  updateButtons();
  updateSizePreview();
}

pdfInput.addEventListener("change", () => setFile(pdfInput.files?.[0]));

/** UIDAI password: first 4 = A–Z only, last 4 = digits only, max 8. */
function formatUidaiPassword(raw) {
  let out = "";
  for (const ch of String(raw || "")) {
    if (out.length >= 8) break;
    if (out.length < 4) {
      if (/[a-zA-Z]/.test(ch)) out += ch.toUpperCase();
    } else if (/\d/.test(ch)) {
      out += ch;
    }
  }
  return out;
}

function syncPasswordField() {
  const formatted = formatUidaiPassword(passwordInput.value);
  if (passwordInput.value !== formatted) {
    const pos = passwordInput.selectionStart;
    passwordInput.value = formatted;
    // Keep caret near end when filtering
    const next = Math.min(formatted.length, pos ?? formatted.length);
    try {
      passwordInput.setSelectionRange(next, next);
    } catch {
      /* ignore */
    }
  }
}

passwordInput.addEventListener("input", syncPasswordField);
passwordInput.addEventListener("blur", syncPasswordField);
passwordInput.addEventListener("paste", () => {
  requestAnimationFrame(syncPasswordField);
});
passwordInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    syncPasswordField();
    extractCards();
  }
});

["dragenter", "dragover"].forEach((evt) => {
  pdfDrop.addEventListener(evt, (e) => {
    e.preventDefault();
    pdfDrop.classList.add("dragover");
  });
});
["dragleave", "drop"].forEach((evt) => {
  pdfDrop.addEventListener(evt, (e) => {
    e.preventDefault();
    pdfDrop.classList.remove("dragover");
  });
});
pdfDrop.addEventListener("drop", (e) => {
  const file = e.dataTransfer?.files?.[0];
  if (file) setFile(file);
});

function syncGuideOptions(source) {
  if (!cutGuides || !solidGuides) return;
  if (source === "cut" && cutGuides.checked) {
    solidGuides.checked = false;
  } else if (source === "solid" && solidGuides.checked) {
    cutGuides.checked = false;
  }
  // When cut guides is on, solid guides cannot be used
  solidGuides.disabled = cutGuides.checked;
  renderPreview();
}

cutGuides.addEventListener("change", () => syncGuideOptions("cut"));
solidGuides?.addEventListener("change", () => syncGuideOptions("solid"));
roundedCorners?.addEventListener("change", renderPreview);
showBoth.addEventListener("change", () => {
  setCardGapMm(state.cardGapMm, { render: true });
});
document.getElementById("sideBySide")?.addEventListener("change", () => {
  setCardGapMm(state.cardGapMm, { render: true });
});

cardGapRange?.addEventListener("input", (e) => {
  setCardGapMm(e.target.value);
});
cardGapInput?.addEventListener("change", () => {
  setCardGapMm(cardGapInput.value);
});
cardGapInput?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    e.preventDefault();
    setCardGapMm(cardGapInput.value);
    cardGapInput.blur();
  }
});
cardGapInput?.addEventListener("blur", () => {
  setCardGapMm(cardGapInput.value);
});
clearBtn.addEventListener("click", clearAll);
extractBtn.addEventListener("click", extractCards);
downloadPdfBtn.addEventListener("click", () => {
  downloadPdf();
});
downloadFrontPngBtn.addEventListener("click", () => downloadCardPng("front"));
downloadBackPngBtn.addEventListener("click", () => downloadCardPng("back"));
printBtn.addEventListener("click", () => {
  printClean();
});
dpiOptions?.addEventListener("click", (e) => {
  const btn = e.target.closest(".dpi-btn");
  if (!btn) return;
  setExportDpi(Number(btn.dataset.dpi));
});
window.addEventListener("resize", () => {
  if (state.adjust.front) syncAdjustViewport("front");
  if (state.adjust.back) syncAdjustViewport("back");
});

updateButtons();
updateSizePreview();
setCardGapMm(4, { render: false });
if (solidGuides) solidGuides.disabled = Boolean(cutGuides?.checked);

