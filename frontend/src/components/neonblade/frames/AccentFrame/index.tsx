"use client";

import React, { HTMLAttributes } from "react";
import "./accent-frame.css";

export type AFColor = "cyan" | "pink" | "green" | (string & {});
export type AFHoverEffect = "expand" | "glow" | "pulse" | "flicker" | "trace" | "none";
export type AFGlowIntensity = "low" | "medium" | "high";
export type AFBgVariant = "none" | "subtle" | "solid";
export type AFCornerStyle = "square" | "rounded";

const COLOR_PRESETS: Record<string, string> = {
  cyan: "#00f3ff",
  pink: "#ff00ff",
  green: "#39ff14",
};

export interface AccentFrameProps extends HTMLAttributes<HTMLDivElement> {
  children?: React.ReactNode;
  /** @deprecated use children */
  text?: React.ReactNode;
  color?: AFColor;
  colorB?: AFColor;
  cornerLength?: number;
  cornerThickness?: number;
  hoverLength?: number;
  transitionDuration?: number;
  cornerStyle?: AFCornerStyle;
  mode?: "duo" | "quad";
  hoverEffect?: AFHoverEffect;
  glowIntensity?: AFGlowIntensity;
  animated?: boolean;
  bgVariant?: AFBgVariant;
}

export const AccentFrame: React.FC<AccentFrameProps> = ({
  children,
  text,
  className = "",
  color = "cyan",
  colorB,
  cornerLength = 16,
  cornerThickness = 2,
  hoverLength = 32,
  transitionDuration = 300,
  cornerStyle = "square",
  mode = "duo",
  hoverEffect = "expand",
  glowIntensity = "medium",
  animated = false,
  bgVariant = "none",
  style,
  ...props
}) => {
  const resolvedA = COLOR_PRESETS[color] ?? color;
  const resolvedB = colorB ? (COLOR_PRESETS[colorB] ?? colorB) : resolvedA;

  const wrapperClasses = [
    "px-6 py-4 relative group",
    hoverEffect !== "expand" && hoverEffect !== "none" ? `af-hover-${hoverEffect}` : "",
    `af-glow-${glowIntensity}`,
    animated ? "af-animated" : "",
    bgVariant === "subtle" ? "af-bg-subtle" : "",
    bgVariant === "solid" ? "bg-[#0a0a0a]" : "",
    className,
  ].filter(Boolean).join(" ");

  const shouldExpand = hoverEffect === "expand";
  const off = `-${cornerThickness / 2}px`;

  const cornerBase = [
    "af-corner absolute",
    "transition-[width,height,box-shadow,opacity] duration-[var(--af-duration)]",
  ].join(" ");

  const H = (pos: string, isB = false) =>
    [
      cornerBase,
      isB ? "af-corner-b" : "",
      pos,
      "h-[var(--af-thickness)] w-[var(--af-corner-length)]",
      shouldExpand ? "group-hover:w-[var(--af-hover-length)]" : "",
      isB ? "bg-[var(--af-color-b,var(--af-color-a,#00f3ff))]" : "bg-[var(--af-color-a,#00f3ff)]",
      cornerStyle === "rounded" ? "rounded-[2px]" : "",
    ].filter(Boolean).join(" ");

  const V = (pos: string, isB = false) =>
    [
      cornerBase,
      isB ? "af-corner-b" : "",
      pos,
      "w-[var(--af-thickness)] h-[var(--af-corner-length)]",
      shouldExpand ? "group-hover:h-[var(--af-hover-length)]" : "",
      isB ? "bg-[var(--af-color-b,var(--af-color-a,#00f3ff))]" : "bg-[var(--af-color-a,#00f3ff)]",
      cornerStyle === "rounded" ? "rounded-[2px]" : "",
    ].filter(Boolean).join(" ");

  return (
    <div
      className={wrapperClasses}
      style={{
        "--af-color-a": resolvedA,
        "--af-color-b": resolvedB,
        "--af-corner-length": `${cornerLength}px`,
        "--af-hover-length": `${hoverLength}px`,
        "--af-thickness": `${cornerThickness}px`,
        "--af-duration": `${transitionDuration}ms`,
        ...style,
      } as React.CSSProperties}
      {...props}
    >
      <div className={H("top-0 left-0")} style={{ marginTop: off, marginLeft: off }} />
      <div className={V("top-0 left-0")} style={{ marginTop: off, marginLeft: off }} />
      <div className={H("bottom-0 right-0", true)} style={{ marginBottom: off, marginRight: off }} />
      <div className={V("bottom-0 right-0", true)} style={{ marginBottom: off, marginRight: off }} />
      {mode === "quad" && (
        <>
          <div className={H("top-0 right-0", true)} style={{ marginTop: off, marginRight: off }} />
          <div className={V("top-0 right-0", true)} style={{ marginTop: off, marginRight: off }} />
          <div className={H("bottom-0 left-0")} style={{ marginBottom: off, marginLeft: off }} />
          <div className={V("bottom-0 left-0")} style={{ marginBottom: off, marginLeft: off }} />
        </>
      )}
      <div className="relative z-10">{text ?? children}</div>
    </div>
  );
};

export default AccentFrame;
