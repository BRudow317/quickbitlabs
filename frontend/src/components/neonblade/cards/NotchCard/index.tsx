"use client";

import React, { HTMLAttributes, ReactNode } from "react";
import "./notch-card.css";

export type NCNotchSide = "top" | "bottom" | "left" | "right";
export type NCColor = "cyan" | "pink" | "green" | (string & {});
export type NCSize = "sm" | "md" | "lg" | "xl";
export type NCBeamVariant = "none" | "single" | "dual" | "gradient-sweep" | "rainbow" | "pulse";
export type NCHoverEffect = "glow" | "scan" | "pulse" | "lift" | "none";
export type NCGlowIntensity = "none" | "low" | "medium" | "high";

const COLOR_PRESETS: Record<string, string> = {
  cyan: "#00f3ff",
  pink: "#ff00ff",
  green: "#39ff14",
};

function resolveColor(c: NCColor): string {
  return COLOR_PRESETS[c] ?? c;
}

function buildClipPath(
  notchSides: NCNotchSide[],
  notchSize: number,
  notchWidth: number,
  notchWidthV: number,
  notchSkew: number,
): string {
  const hasTop = notchSides.includes("top");
  const hasRight = notchSides.includes("right");
  const hasBottom = notchSides.includes("bottom");
  const hasLeft = notchSides.includes("left");

  const d = notchSize;
  const wH = notchWidth;
  const wV = notchWidthV;
  const sk = notchSkew;
  const halfH = wH / 2 + sk;
  const halfV = wV / 2 + sk;

  const pts: string[] = [];
  const pt = (x: string, y: string) => pts.push(`${x} ${y}`);

  const topNotch = () => {
    pt(`calc(50% - ${halfH}px)`, "0");
    pt(`calc(50% - ${wH / 2}px)`, `${d}px`);
    pt(`calc(50% + ${wH / 2}px)`, `${d}px`);
    pt(`calc(50% + ${halfH}px)`, "0");
  };

  const rightNotch = () => {
    pt("100%", `calc(50% - ${halfV}px)`);
    pt(`calc(100% - ${d}px)`, `calc(50% - ${wV / 2}px)`);
    pt(`calc(100% - ${d}px)`, `calc(50% + ${wV / 2}px)`);
    pt("100%", `calc(50% + ${halfV}px)`);
  };

  const bottomNotch = () => {
    pt(`calc(50% + ${halfH}px)`, "100%");
    pt(`calc(50% + ${wH / 2}px)`, `calc(100% - ${d}px)`);
    pt(`calc(50% - ${wH / 2}px)`, `calc(100% - ${d}px)`);
    pt(`calc(50% - ${halfH}px)`, "100%");
  };

  const leftNotch = () => {
    pt("0", `calc(50% + ${halfV}px)`);
    pt(`${d}px`, `calc(50% + ${wV / 2}px)`);
    pt(`${d}px`, `calc(50% - ${wV / 2}px)`);
    pt("0", `calc(50% - ${halfV}px)`);
  };

  pt("0", "0");
  if (hasTop) topNotch();
  pt("100%", "0");
  if (hasRight) rightNotch();
  pt("100%", "100%");
  if (hasBottom) bottomNotch();
  pt("0", "100%");
  if (hasLeft) leftNotch();

  return `polygon(${pts.join(", ")})`;
}

const INNER_PADDING: Record<NCSize, string> = {
  sm: "p-4",
  md: "p-6",
  lg: "p-8",
  xl: "p-10",
};
const TITLE_SIZE: Record<NCSize, string> = {
  sm: "text-sm",
  md: "text-lg",
  lg: "text-xl",
  xl: "text-2xl",
};
const DESC_SIZE: Record<NCSize, string> = {
  sm: "text-xs",
  md: "text-sm",
  lg: "text-base",
  xl: "text-lg",
};
const ICON_BOX_SIZE: Record<NCSize, string> = {
  sm: "w-9 h-9",
  md: "w-12 h-12",
  lg: "w-14 h-14",
  xl: "w-16 h-16",
};
const ICON_SIZE: Record<NCSize, string> = {
  sm: "w-4 h-4",
  md: "w-6 h-6",
  lg: "w-8 h-8",
  xl: "w-9 h-9",
};

export interface NotchCardProps extends HTMLAttributes<HTMLDivElement> {
  children?: ReactNode;
  icon?: ReactNode;
  title?: string;
  description?: string;
  notchSides?: NCNotchSide[];
  notchSize?: number;
  notchWidth?: number;
  notchWidthV?: number;
  notchSkew?: number;
  borderWidth?: number | string;
  borderColor?: NCColor;
  borderColorB?: NCColor;
  borderGradient?: boolean;
  beamVariant?: NCBeamVariant;
  beamColor?: NCColor;
  beamColorB?: NCColor;
  beamDuration?: number;
  beamDurationB?: number;
  cardColor?: string;
  textColor?: string;
  accentColor?: NCColor;
  glowIntensity?: NCGlowIntensity;
  hoverEffect?: NCHoverEffect;
  size?: NCSize;
  align?: "start" | "center";
  innerClassName?: string;
}

export const NotchCard: React.FC<NotchCardProps> = ({
  children,
  icon,
  title,
  description,
  notchSides = ["top", "bottom"],
  notchSize = 12,
  notchWidth = 50,
  notchWidthV,
  notchSkew = 12,
  borderWidth = "2px",
  borderColor = "cyan",
  borderColorB = "pink",
  borderGradient = false,
  beamVariant = "none",
  beamColor = "cyan",
  beamColorB = "pink",
  beamDuration = 4,
  beamDurationB = 6,
  cardColor,
  textColor,
  accentColor,
  glowIntensity = "medium",
  hoverEffect = "glow",
  size = "md",
  align = "start",
  className = "",
  innerClassName = "",
  style,
  ...props
}) => {
  const validSides: NCNotchSide[] = notchSides.length > 0 ? notchSides : ["top", "bottom"];

  const resolvedBorderColor = resolveColor(borderColor);
  const resolvedBorderColorB = resolveColor(borderColorB);
  const resolvedBeamColor = resolveColor(beamColor);
  const resolvedBeamColorB = resolveColor(beamColorB);
  const resolvedAccent = resolveColor(accentColor ?? (beamVariant !== "none" ? beamColor : borderColor));

  const bwValue = typeof borderWidth === "number" ? `${borderWidth}px` : borderWidth;

  const clipPath = buildClipPath(validSides, notchSize, notchWidth, notchWidthV ?? notchWidth, notchSkew);

  const hasBeam = beamVariant !== "none";

  let outerBg: string;
  if (hasBeam) {
    outerBg = "transparent";
  } else if (borderGradient) {
    outerBg = `linear-gradient(135deg, ${resolvedBorderColor}, ${resolvedBorderColorB})`;
  } else {
    outerBg = resolvedBorderColor;
  }

  const outerClasses = [
    "nc-wrapper",
    "relative overflow-hidden",
    hasBeam ? `nc-beam-${beamVariant}` : "",
    glowIntensity !== "none" ? `nc-glow-${glowIntensity}` : "",
    `nc-hover-${hoverEffect}`,
    className,
  ].filter(Boolean).join(" ");

  const innerClasses = [
    "nc-inner",
    "relative z-10 w-full h-full flex flex-col",
    align === "center" ? "items-center text-center" : "",
    INNER_PADDING[size],
    innerClassName,
  ].filter(Boolean).join(" ");

  return (
    <div
      className={outerClasses}
      style={{
        "--nc-accent": resolvedAccent,
        "--nc-border-color": resolvedBorderColor,
        "--nc-border-color-b": resolvedBorderColorB,
        "--nc-beam-color": resolvedBeamColor,
        "--nc-beam-color-b": resolvedBeamColorB,
        "--nc-duration": `${beamDuration}s`,
        "--nc-duration-b": `${beamDurationB}s`,
        background: outerBg,
        padding: bwValue,
        clipPath,
        ...style,
      } as React.CSSProperties}
      {...props}
    >
      {hasBeam && <div className="nc-beam" aria-hidden="true" />}
      {beamVariant === "dual" && <div className="nc-beam-b" aria-hidden="true" />}
      <div
        className={innerClasses}
        style={{
          backgroundColor: cardColor ?? "var(--background, #050505)",
          color: textColor ?? undefined,
          clipPath,
        }}
      >
        {icon && (
          <div className={["nc-icon-box", ICON_BOX_SIZE[size], align === "center" ? "self-center" : "", "mb-4 flex shrink-0 items-center justify-center border border-white/20 bg-white/[0.04]"].filter(Boolean).join(" ")}>
            <div className={["nc-icon", ICON_SIZE[size], "text-white/60"].join(" ")}>
              {icon}
            </div>
          </div>
        )}
        {title && (
          <h3 className={["nc-title", "font-orbitron font-semibold text-white", TITLE_SIZE[size], icon || children ? "mb-1" : ""].join(" ")}>
            {title}
          </h3>
        )}
        {description && (
          <p className={["nc-desc", "text-white/55 leading-relaxed", DESC_SIZE[size], children ? "mb-3" : ""].join(" ")}>
            {description}
          </p>
        )}
        {children}
      </div>
      {hoverEffect === "scan" && <div className="nc-scan-line" aria-hidden="true" />}
    </div>
  );
};

export default NotchCard;
