"use client";

import React, { ButtonHTMLAttributes, ReactNode } from "react";
import "./corner-cut-button.css";

export type CCBColor = "cyan" | "pink" | "green" | (string & {});
export type CCBSize = "xs" | "sm" | "md" | "lg" | "xl";
export type CCBVariant = "solid" | "outline" | "ghost";
export type CCBCorner = "bottom-right" | "bottom-left" | "top-right" | "top-left" | "all";
export type CCBHoverEffect = "glow" | "shift" | "shine" | "pulse" | "scan" | "flicker" | "default" | "none";
export type CCBGlowIntensity = "low" | "medium" | "high";

const COLOR_PRESETS: Record<string, string> = {
  cyan: "#00f3ff",
  pink: "#ff00ff",
  green: "#39ff14",
};

const SIZE_CLASSES: Record<CCBSize, string> = {
  xs: "px-4 py-2 text-xs",
  sm: "px-6 py-3 text-xs",
  md: "px-8 py-4 text-sm",
  lg: "px-10 py-5 text-base",
  xl: "px-12 py-6 text-lg",
};

const CORNER_CLASSES: Record<CCBCorner, string> = {
  "bottom-right": "ccb-clip-br",
  "bottom-left": "ccb-clip-bl",
  "top-right": "ccb-clip-tr",
  "top-left": "ccb-clip-tl",
  all: "ccb-clip-all",
};

const HOVER_CLASSES: Record<CCBHoverEffect, string> = {
  glow: "ccb-hover-glow",
  shift: "ccb-hover-shift",
  shine: "ccb-hover-shine",
  pulse: "ccb-hover-pulse",
  scan: "ccb-hover-scan",
  flicker: "ccb-hover-flicker",
  default: "ccb-hover-default",
  none: "",
};

const GLOW_SIZES: Record<CCBGlowIntensity, number> = {
  low: 8,
  medium: 15,
  high: 28,
};

export interface CornerCutButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode;
  color?: CCBColor;
  size?: CCBSize;
  variant?: CCBVariant;
  corner?: CCBCorner;
  cornerSize?: number;
  hoverEffect?: CCBHoverEffect;
  glowIntensity?: CCBGlowIntensity;
  showArrow?: boolean;
  hoverColor?: CCBColor;
  hoverOutlined?: boolean;
  textColor?: CCBColor;
}

export const CornerCutButton: React.FC<CornerCutButtonProps> = ({
  children,
  color = "cyan",
  size = "md",
  variant = "solid",
  corner = "bottom-right",
  cornerSize = 20,
  hoverEffect = "default",
  glowIntensity = "medium",
  showArrow = false,
  hoverColor,
  hoverOutlined = false,
  textColor,
  className = "",
  style,
  ...props
}) => {
  const resolvedColor = COLOR_PRESETS[color] ?? color;
  const resolvedHoverColor = hoverColor ? (COLOR_PRESETS[hoverColor] ?? hoverColor) : undefined;
  const resolvedTextColor = textColor ? (COLOR_PRESETS[textColor] ?? textColor) : undefined;
  const glowSize = GLOW_SIZES[glowIntensity];

  const ghostStyle = variant === "ghost" ? { backgroundColor: "color-mix(in srgb, var(--ccb-color) 12%, #000)", color: "var(--ccb-color)" } : undefined;

  return (
    <div
      className={[
        "relative inline-flex p-px group/ccb",
        `ccb-wrapper-${hoverEffect}`,
        hoverEffect === "flicker" ? "ccb-wrapper" : "",
        className,
      ].filter(Boolean).join(" ")}
      style={{
        "--ccb-color": resolvedColor,
        "--ccb-hover-color": resolvedHoverColor ?? resolvedColor,
        "--ccb-hover-bg": resolvedHoverColor ?? "#ffffff",
        "--ccb-corner-size": `${cornerSize}px`,
        "--ccb-glow-size": `${glowSize}px`,
        ...(resolvedTextColor ? { "--ccb-text-color": resolvedTextColor } : {}),
        ...style,
      } as React.CSSProperties}
    >
      <div
        className={[
          "absolute inset-0 pointer-events-none z-0 transition-[background,opacity] duration-300",
          CORNER_CLASSES[corner],
          variant === "outline" ? "bg-[var(--ccb-color)]" : "bg-white/[0.08]",
          variant === "solid" && hoverOutlined && hoverEffect === "shift" ? "group-hover/ccb:bg-[var(--ccb-hover-color)]" : "",
        ].filter(Boolean).join(" ")}
        aria-hidden="true"
      />
      <button
        className={[
          "flex-1 relative group font-orbitron font-bold tracking-wider uppercase transition-all overflow-hidden cursor-pointer",
          SIZE_CLASSES[size],
          CORNER_CLASSES[corner],
          HOVER_CLASSES[hoverEffect],
          `ccb-${variant}`,
          hoverOutlined ? "ccb-hover-outlined" : "",
          variant === "solid" ? `bg-[var(--ccb-color)] ${resolvedTextColor ? "text-[var(--ccb-text-color)]" : "text-black"}` : "",
          variant === "outline" ? `bg-black ${resolvedTextColor ? "text-[var(--ccb-text-color)]" : "text-[var(--ccb-color)]"}` : "",
        ].filter(Boolean).join(" ")}
        style={{ ...ghostStyle, ...(resolvedTextColor && variant === "ghost" ? { color: resolvedTextColor } : {}) }}
        {...props}
      >
        {hoverEffect === "shine" && <span className="ccb-shine-layer" aria-hidden="true" />}
        {hoverEffect === "scan" && <span className="ccb-scan-layer" aria-hidden="true" />}
        <span className="relative z-10 flex items-center gap-2">
          {children}
          {showArrow && (
            <span className="group-hover:translate-x-1 transition-transform inline-block" aria-hidden="true">→</span>
          )}
        </span>
      </button>
    </div>
  );
};

export default CornerCutButton;
