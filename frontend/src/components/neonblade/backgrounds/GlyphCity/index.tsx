"use client";

import { useEffect, useRef, useCallback } from "react";

export type GlyphCityVariant = "downtown" | "megacity" | "district" | "ruins";
export type OutlineCityVariant = "sparse" | "dense" | "layered" | "horizon";
export type CityType = "solid" | "outline";
export type CharSet = "blocks" | "custom";

export interface GlyphCityProps {
  cityType?: CityType;
  variant?: GlyphCityVariant;
  outlineVariant?: OutlineCityVariant;
  colorPrimary?: string;
  colorSecondary?: string;
  colorTertiary?: string;
  bgColor?: string;
  fontSize?: number;
  speed?: number;
  showVehicles?: boolean;
  blinkingLights?: boolean;
  opacity?: number;
  charSet?: CharSet;
  customChars?: string;
  className?: string;
}

const BUILDING_CHARS = {
  wall: ["█", "▓", "▒", "░", "┃", "│", "║", "|"],
  window_on: ["▪", "■", "□", "▫", "◼", "◻", "▮"],
  window_off: ["▫", "░", "·", " ", " ", " "],
  roof: ["▀", "▄", "▬", "▭", "─", "═", "▔"],
  antenna: ["╻", "┃", "│", "╷", "╿", "┿"],
  peak: ["▲", "△", "◬", "⋀"],
  decor: ["╬", "╫", "╪", "┼", "╋", "╂"],
};

const NEON_CHARS = "▓▒░█▪■│║┃╬╪┼╋◼▮▬╻╷╿┿";
const SPARSE_CHARS = "░▒·│┃|║";
const STRUCTURAL_CHARS = "─═╔╗╚╝╠╣╦╩╬║│┌┐└┘├┤┬┴┼";

interface Building {
  x: number;
  width: number;
  height: number;
  hasAntenna: boolean;
  antennaHeight: number;
  hasPeakLight: boolean;
  lightOn: boolean;
  lightTimer: number;
  lightInterval: number;
  colorIdx: number;
  windowPhase: number;
  style: "dense" | "glass" | "grid" | "minimal";
}

interface Vehicle {
  x: number;
  y: number;
  speed: number;
  direction: 1 | -1;
  type: "speeder" | "transport" | "fighter";
  trail: Array<{ x: number; y: number; alpha: number }>;
  colorIdx: number;
}

function mulberry32(seed: number) {
  return function () {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function buildCityLayout(cols: number, rows: number, variant: GlyphCityVariant): Building[] {
  const buildings: Building[] = [];
  const rng = mulberry32(42 + variant.charCodeAt(0));
  const configs: Record<GlyphCityVariant, { minW: number; maxW: number; minH: number; maxH: number; density: number; antennaRate: number }> = {
    downtown: { minW: 3, maxW: 7, minH: 12, maxH: Math.floor(rows * 0.85), density: 0.05, antennaRate: 0.5 },
    megacity: { minW: 2, maxW: 5, minH: 8, maxH: Math.floor(rows * 0.92), density: 0.02, antennaRate: 0.7 },
    district: { minW: 3, maxW: 9, minH: 6, maxH: Math.floor(rows * 0.65), density: 0.1, antennaRate: 0.3 },
    ruins: { minW: 2, maxW: 6, minH: 4, maxH: Math.floor(rows * 0.55), density: 0.15, antennaRate: 0.2 },
  };
  const cfg = configs[variant];
  let x = 0;
  while (x < cols) {
    if (rng() < cfg.density && x > 0) { x += 1; continue; }
    const w = Math.max(cfg.minW, Math.floor(rng() * (cfg.maxW - cfg.minW + 1)) + cfg.minW);
    if (x + w > cols) break;
    const h = Math.max(cfg.minH, Math.floor(rng() * (cfg.maxH - cfg.minH + 1)) + cfg.minH);
    const hasAntenna = rng() < cfg.antennaRate;
    const antennaH = hasAntenna ? Math.floor(rng() * 4) + 2 : 0;
    const hasPeakLight = hasAntenna && rng() < 0.7;
    const styles: Building["style"][] = ["dense", "glass", "grid", "minimal"];
    const style = styles[Math.floor(rng() * styles.length)];
    buildings.push({ x, width: w, height: Math.min(h, rows - 2), hasAntenna, antennaHeight: antennaH, hasPeakLight, lightOn: rng() > 0.5, lightTimer: 0, lightInterval: Math.floor(rng() * 60) + 20, colorIdx: Math.floor(rng() * 3), windowPhase: Math.floor(rng() * 100), style });
    x += w + (rng() < 0.3 ? 0 : 0);
  }
  return buildings;
}

function spawnVehicles(cols: number, rows: number, variant: GlyphCityVariant): Vehicle[] {
  const count = variant === "megacity" ? 5 : variant === "downtown" ? 3 : variant === "ruins" ? 1 : 2;
  const vehicleRows = [Math.floor(rows * 0.15), Math.floor(rows * 0.22), Math.floor(rows * 0.1)];
  return Array.from({ length: count }, (_, i) => ({
    x: Math.floor(Math.random() * cols),
    y: vehicleRows[i % vehicleRows.length],
    speed: 0.3 + Math.random() * 0.5,
    direction: (Math.random() > 0.5 ? 1 : -1) as 1 | -1,
    type: (["speeder", "transport", "fighter"] as const)[i % 3],
    trail: [],
    colorIdx: i % 3,
  }));
}

const VEHICLE_CHARS: Record<Vehicle["type"], { body: string; left: string; right: string }> = {
  speeder: { body: "◈", left: "◂", right: "▸" },
  transport: { body: "▬", left: "◀", right: "▶" },
  fighter: { body: "◆", left: "◁", right: "▷" },
};

function buildOutlineCityLayout(cols: number, rows: number, variant: OutlineCityVariant): Building[] {
  const rng = mulberry32(99 + variant.charCodeAt(0));
  const configs: Record<OutlineCityVariant, { minW: number; maxW: number; minH: number; maxH: number; density: number; antennaRate: number }> = {
    sparse: { minW: 3, maxW: 9, minH: 10, maxH: Math.floor(rows * 0.88), density: 0.08, antennaRate: 0.55 },
    dense: { minW: 2, maxW: 5, minH: 8, maxH: Math.floor(rows * 0.93), density: 0.01, antennaRate: 0.65 },
    layered: { minW: 3, maxW: 8, minH: 7, maxH: Math.floor(rows * 0.8), density: 0.05, antennaRate: 0.45 },
    horizon: { minW: 4, maxW: 12, minH: 5, maxH: Math.floor(rows * 0.6), density: 0.1, antennaRate: 0.3 },
  };
  const cfg = configs[variant];
  const buildings: Building[] = [];
  let x = 0;
  while (x < cols) {
    if (rng() < cfg.density && x > 0) { x += 1; continue; }
    const w = Math.max(cfg.minW, Math.floor(rng() * (cfg.maxW - cfg.minW + 1)) + cfg.minW);
    if (x + w > cols) break;
    const h = Math.max(cfg.minH, Math.floor(rng() * (cfg.maxH - cfg.minH + 1)) + cfg.minH);
    const hasAntenna = rng() < cfg.antennaRate;
    const antennaH = hasAntenna ? Math.floor(rng() * 5) + 2 : 0;
    const hasPeakLight = hasAntenna && rng() < 0.7;
    buildings.push({ x, width: w, height: Math.min(h, rows - 2), hasAntenna, antennaHeight: antennaH, hasPeakLight, lightOn: rng() > 0.5, lightTimer: 0, lightInterval: Math.floor(rng() * 60) + 20, colorIdx: Math.floor(rng() * 3), windowPhase: 0, style: "dense" });
    x += w;
  }
  return buildings;
}

function drawOutlineCity(ctx: CanvasRenderingContext2D, W: number, H: number, buildings: Building[], vehicles: Vehicle[], cols: number, rows: number, cw: number, ch: number, palette: string[], bgColor: string, blinkingLights: boolean, showVehicles: boolean, frame: number) {
  ctx.fillStyle = bgColor;
  ctx.fillRect(0, 0, W, H);
  ctx.save();
  ctx.strokeStyle = palette[0];
  ctx.globalAlpha = 0.4;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, H - 1);
  ctx.lineTo(W, H - 1);
  ctx.stroke();
  ctx.restore();
  for (const b of buildings) {
    const color = palette[b.colorIdx];
    if (blinkingLights && b.hasPeakLight) {
      b.lightTimer++;
      if (b.lightTimer >= b.lightInterval) { b.lightOn = !b.lightOn; b.lightTimer = 0; }
    }
    const pxLeft = b.x * cw;
    const pxRight = (b.x + b.width) * cw;
    const pxGround = H - 1;
    const pxTop = pxGround - b.height * ch;
    ctx.save();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.shadowColor = color;
    ctx.shadowBlur = 8;
    ctx.globalAlpha = 0.9;
    ctx.beginPath();
    ctx.moveTo(pxLeft, pxGround);
    ctx.lineTo(pxLeft, pxTop);
    ctx.lineTo(pxRight, pxTop);
    ctx.lineTo(pxRight, pxGround);
    ctx.stroke();
    if (b.hasAntenna) {
      const antennaX = pxLeft + (b.width * cw) / 2;
      const antennaTopY = pxTop - b.antennaHeight * ch;
      ctx.strokeStyle = palette[2];
      ctx.shadowColor = palette[2];
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(antennaX, pxTop);
      ctx.lineTo(antennaX, antennaTopY);
      ctx.stroke();
      if (b.hasPeakLight) {
        ctx.beginPath();
        ctx.arc(antennaX, antennaTopY - 2, 2, 0, Math.PI * 2);
        ctx.fillStyle = palette[2];
        ctx.shadowBlur = b.lightOn ? 12 : 2;
        ctx.globalAlpha = b.lightOn ? 1 : 0.25;
        ctx.fill();
      }
    }
    ctx.restore();
  }
  if (showVehicles) {
    for (const v of vehicles) {
      v.x += v.speed * v.direction;
      if (v.x > cols + 5) v.x = -5;
      if (v.x < -5) v.x = cols + 5;
      const vColor = palette[v.colorIdx];
      const vPxX = v.x * cw;
      const vPxY = v.y * ch + (H - rows * ch);
      v.trail.push({ x: v.x, y: v.y, alpha: 0.4 });
      if (v.trail.length > 8) v.trail.shift();
      ctx.save();
      ctx.strokeStyle = vColor;
      ctx.shadowColor = vColor;
      ctx.shadowBlur = 8;
      for (let ti = 0; ti < v.trail.length - 1; ti++) {
        const tx = v.trail[ti].x * cw;
        const ty = v.trail[ti].y * ch;
        ctx.globalAlpha = (ti / v.trail.length) * 0.2;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(tx, ty);
        ctx.lineTo(tx + v.direction * cw * 0.5, ty);
        ctx.stroke();
      }
      ctx.globalAlpha = 0.88;
      ctx.lineWidth = 1.5;
      const dir = v.direction;
      ctx.beginPath();
      ctx.moveTo(vPxX, vPxY);
      ctx.lineTo(vPxX + dir * cw * 0.8, vPxY - ch * 0.25);
      ctx.lineTo(vPxX + dir * cw * 1.4, vPxY);
      ctx.lineTo(vPxX + dir * cw * 0.8, vPxY + ch * 0.25);
      ctx.closePath();
      ctx.stroke();
      ctx.restore();
    }
  }
  if (frame % 4 === 0) {
    const noiseY = Math.floor(Math.random() * H * 0.55);
    ctx.save();
    ctx.strokeStyle = palette[Math.floor(Math.random() * 3)];
    ctx.globalAlpha = 0.04;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, noiseY);
    ctx.lineTo(W, noiseY);
    ctx.stroke();
    ctx.restore();
  }
}

export function GlyphCity({
  cityType = "solid",
  variant = "downtown",
  outlineVariant = "sparse",
  colorPrimary = "#00ffff",
  colorSecondary = "#ff00ff",
  colorTertiary = "#ffff00",
  bgColor = "#000000",
  fontSize = 12,
  speed = 80,
  showVehicles = true,
  blinkingLights = true,
  opacity = 90,
  charSet = "blocks",
  customChars,
  className,
}: GlyphCityProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const stateRef = useRef<{ buildings: Building[]; vehicles: Vehicle[]; frame: number; cols: number; rows: number } | null>(null);
  const outlineStateRef = useRef<{ buildings: Building[]; vehicles: Vehicle[]; frame: number; cols: number; rows: number } | null>(null);

  const colors = useCallback(() => [colorPrimary, colorSecondary, colorTertiary], [colorPrimary, colorSecondary, colorTertiary]);

  useEffect(() => {
    if (cityType !== "outline") return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const palette = colors();
    const resize = () => {
      canvas.width = canvas.offsetWidth;
      canvas.height = canvas.offsetHeight;
      const cols = Math.floor(canvas.width / (fontSize * 0.6));
      const rows = Math.floor(canvas.height / fontSize);
      outlineStateRef.current = { buildings: buildOutlineCityLayout(cols, rows, outlineVariant), vehicles: showVehicles ? spawnVehicles(cols, rows, "downtown") : [], frame: 0, cols, rows };
    };
    resize();
    window.addEventListener("resize", resize);
    const draw = () => {
      const state = outlineStateRef.current;
      if (!state) return;
      state.frame++;
      const cw = canvas.width / state.cols;
      const ch = fontSize;
      drawOutlineCity(ctx, canvas.width, canvas.height, state.buildings, state.vehicles, state.cols, state.rows, cw, ch, palette, bgColor, blinkingLights, showVehicles, state.frame);
    };
    const interval = setInterval(draw, speed);
    return () => { clearInterval(interval); window.removeEventListener("resize", resize); };
  }, [cityType, outlineVariant, colorPrimary, colorSecondary, colorTertiary, bgColor, fontSize, speed, showVehicles, blinkingLights, colors]);

  useEffect(() => {
    if (cityType !== "solid") return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const resize = () => {
      canvas.width = canvas.offsetWidth;
      canvas.height = canvas.offsetHeight;
      const cols = Math.floor(canvas.width / (fontSize * 0.6));
      const rows = Math.floor(canvas.height / fontSize);
      stateRef.current = { buildings: buildCityLayout(cols, rows, variant), vehicles: showVehicles ? spawnVehicles(cols, rows, variant) : [], frame: 0, cols, rows };
    };
    resize();
    window.addEventListener("resize", resize);
    const palette = colors();
    const CHAR_POOLS: Record<"neon" | "sparse" | "structural" | "windowOn" | "windowOff" | "wall", string> = (() => {
      const resolved = charSet === "custom" && customChars && customChars.length > 0 ? customChars : null;
      if (resolved === null) {
        return {
          neon: "▓▒░█▪■│║┃╬╪┼╋◼▮▬╻╷╿┿",
          sparse: "░▒·│┃|║",
          structural: "─═╔╗╚╝╠╣╦╩╬║│┌┐└┘├┤┬┴┼",
          windowOn: "▪■□▫◼◻▮",
          windowOff: "▫░·  ",
          wall: "█┃║│|",
        };
      }
      return { neon: resolved, sparse: resolved, structural: resolved, windowOn: resolved, windowOff: resolved.slice(0, Math.max(1, Math.floor(resolved.length / 3))), wall: resolved };
    })();
    const charAt = (pool: string, idx: number) => pool[Math.abs(idx) % pool.length];
    const draw = () => {
      const state = stateRef.current;
      if (!state) return;
      const { buildings, vehicles, cols, rows } = state;
      state.frame++;
      const cw = canvas.width / cols;
      const ch = fontSize;
      const yOffset = canvas.height - rows * ch;
      ctx.fillStyle = bgColor;
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.font = `${fontSize}px monospace`;
      ctx.textBaseline = "top";
      const drawChar = (col: number, row: number, char: string, color: string, alpha = 1) => {
        if (col < 0 || col >= cols || row < 0 || row >= rows) return;
        ctx.globalAlpha = alpha;
        ctx.fillStyle = color;
        ctx.fillText(char, col * cw, row * ch + yOffset);
        ctx.globalAlpha = 1;
      };
      for (let c = 0; c < cols; c++) { drawChar(c, rows - 1, charAt(CHAR_POOLS.structural, c), palette[0], 0.4); }
      for (const b of buildings) {
        const color = palette[b.colorIdx];
        const dimColor = palette[(b.colorIdx + 1) % 3];
        if (blinkingLights && b.hasPeakLight) {
          b.lightTimer++;
          if (b.lightTimer >= b.lightInterval) { b.lightOn = !b.lightOn; b.lightTimer = 0; }
        }
        const topRow = rows - 1 - b.height;
        for (let row = rows - 1; row >= topRow; row--) {
          const relRow = rows - 1 - row;
          for (let col = b.x; col < b.x + b.width; col++) {
            const relCol = col - b.x;
            let char = "│";
            let charColor = color;
            if (row === topRow) {
              if (relCol === 0) char = charSet === "blocks" ? "╔" : charAt(CHAR_POOLS.structural, 0);
              else if (relCol === b.width - 1) char = charSet === "blocks" ? "╗" : charAt(CHAR_POOLS.structural, 1);
              else char = charSet === "blocks" ? "═" : charAt(CHAR_POOLS.structural, 2);
              charColor = color;
            } else if (row === rows - 1) {
              char = relCol === 0 ? (charSet === "blocks" ? "╚" : charAt(CHAR_POOLS.structural, 3)) : relCol === b.width - 1 ? (charSet === "blocks" ? "╝" : charAt(CHAR_POOLS.structural, 4)) : (charSet === "blocks" ? "═" : charAt(CHAR_POOLS.structural, 2));
              charColor = color;
            } else {
              if (relCol === 0 || relCol === b.width - 1) {
                char = b.style === "glass" ? "║" : b.style === "minimal" ? "│" : "█";
                charColor = color;
              } else {
                const windowRow = Math.floor((relRow + b.windowPhase) / 2) % 2 === 0;
                const windowCol = relCol % 2 === 1;
                if (windowRow && windowCol) {
                  const flickerSeed = (col * 7 + row * 13 + state.frame * 0) % 97;
                  const isOn = flickerSeed > 20;
                  char = isOn ? BUILDING_CHARS.window_on[relCol % BUILDING_CHARS.window_on.length] : BUILDING_CHARS.window_off[relCol % BUILDING_CHARS.window_off.length];
                  charColor = isOn ? dimColor : color;
                  if (char === " ") continue;
                } else {
                  switch (b.style) {
                    case "dense": char = NEON_CHARS[relCol % NEON_CHARS.length]; break;
                    case "glass": char = SPARSE_CHARS[relCol % SPARSE_CHARS.length]; charColor = color; break;
                    case "grid": char = STRUCTURAL_CHARS[relRow % STRUCTURAL_CHARS.length]; break;
                    case "minimal": char = relCol % 3 === 0 ? "│" : " "; break;
                  }
                  if (char === " ") continue;
                }
              }
            }
            drawChar(col, row, char, charColor, 0.85 + (relRow / b.height) * 0.15);
          }
        }
        if (b.hasAntenna) {
          const antennaCol = b.x + Math.floor(b.width / 2);
          for (let ar = 0; ar < b.antennaHeight; ar++) {
            const antennaChar = ar === b.antennaHeight - 1 ? (charSet === "blocks" ? BUILDING_CHARS.peak[0] : charAt(CHAR_POOLS.neon, ar)) : (charSet === "blocks" ? BUILDING_CHARS.antenna[ar % BUILDING_CHARS.antenna.length] : charAt(CHAR_POOLS.neon, ar + 1));
            drawChar(antennaCol, topRow - 1 - ar, antennaChar, palette[2], 0.9);
          }
          if (b.hasPeakLight) {
            const lightChar = b.lightOn ? "●" : "○";
            drawChar(antennaCol, topRow - b.antennaHeight - 1, lightChar, palette[2], b.lightOn ? 1 : 0.3);
          }
        }
      }
      if (showVehicles) {
        for (const v of vehicles) {
          v.x += v.speed * v.direction;
          if (v.x > cols + 5) v.x = -5;
          if (v.x < -5) v.x = cols + 5;
          const vColor = palette[v.colorIdx];
          const vcDef = VEHICLE_CHARS[v.type];
          const col = Math.round(v.x);
          v.trail.push({ x: col, y: v.y, alpha: 0.5 });
          if (v.trail.length > 6) v.trail.shift();
          for (let ti = 0; ti < v.trail.length; ti++) {
            const t = v.trail[ti];
            drawChar(t.x - v.direction, t.y, "·", vColor, t.alpha * (ti / v.trail.length) * 0.4);
          }
          if (v.direction === 1) {
            drawChar(col, v.y, vcDef.left, vColor);
            drawChar(col + 1, v.y, vcDef.body, vColor);
            drawChar(col + 2, v.y, vcDef.right, vColor);
          } else {
            drawChar(col, v.y, vcDef.right, vColor);
            drawChar(col + 1, v.y, vcDef.body, vColor);
            drawChar(col + 2, v.y, vcDef.left, vColor);
          }
        }
      }
      if (state.frame % 3 === 0) {
        const noiseRow = Math.floor(Math.random() * rows);
        for (let c = 0; c < cols; c++) {
          if (Math.random() < 0.03) {
            const nc = charAt(CHAR_POOLS.neon, Math.floor(Math.random() * CHAR_POOLS.neon.length));
            drawChar(c, noiseRow, nc, palette[Math.floor(Math.random() * 3)], 0.12);
          }
        }
      }
    };
    const interval = setInterval(draw, speed);
    return () => { clearInterval(interval); window.removeEventListener("resize", resize); };
  }, [cityType, variant, colorPrimary, colorSecondary, colorTertiary, bgColor, fontSize, speed, showVehicles, blinkingLights, charSet, customChars, colors]);

  return (
    <div className={`absolute inset-0 z-0 overflow-hidden ${className ?? ""}`} style={{ background: bgColor }}>
      <canvas ref={canvasRef} className="w-full h-full pointer-events-none" style={{ opacity: opacity / 100 }} />
    </div>
  );
}
