"use client";
import React, { HTMLAttributes, ReactNode, useEffect, useState } from "react";
import "./glitch-text.css";

export type GTColor = "cyan" | "pink" | "green" | (string & {});

export type GTIntensity = "subtle" | "normal" | "heavy" | "chaos";

export type GTSpeed = "slow" | "normal" | "fast" | "frenzy";

const COLOR_PRESETS: Record<string, string> = {
  cyan: "#00f3ff",
  pink: "#ff00ff",
  green: "#39ff14",
};

const SPEED_MAP: Record<GTSpeed, string> = {
  slow: "2s",
  normal: "1s",
  fast: "0.45s",
  frenzy: "0.2s",
};

const CHAOS_DEFAULT_SPEED = "0.8s";

export interface GlitchTextProps extends HTMLAttributes<HTMLSpanElement> {
  children: ReactNode;
  text?: string;
  mode?: "active" | "hover";
  colorA?: GTColor;
  colorB?: GTColor;
  intensity?: GTIntensity;
  speed?: GTSpeed;
  customSpeed?: string;
  offset?: number;
  neon?: boolean;
  neonFlicker?: boolean;
  glowColor?: GTColor;
  /** @deprecated use glitchDuration or speed instead */
  glitchDuration?: number;
}

export const GlitchText: React.FC<GlitchTextProps> = ({
  children,
  text,
  mode = "hover",
  colorA = "pink",
  colorB = "cyan",
  intensity = "normal",
  speed = "normal",
  customSpeed,
  offset = 2,
  neon = false,
  neonFlicker = false,
  glowColor,
  glitchDuration,
  className = "",
  style,
  ...props
}) => {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  const resolvedText = text ?? (typeof children === "string" ? children : "");

  const resolvedA = COLOR_PRESETS[colorA] ?? colorA;
  const resolvedB = COLOR_PRESETS[colorB] ?? colorB;
  const resolvedGlow = glowColor
    ? (COLOR_PRESETS[glowColor] ?? glowColor)
    : resolvedB;

  let resolvedSpeed: string;
  if (customSpeed) {
    resolvedSpeed = customSpeed;
  } else if (glitchDuration !== undefined) {
    resolvedSpeed = `${glitchDuration}s`;
  } else if (intensity === "chaos" && speed === "normal") {
    resolvedSpeed = CHAOS_DEFAULT_SPEED;
  } else {
    resolvedSpeed = SPEED_MAP[speed];
  }

  const classes = [
    "glitch-wrapper",
    "relative inline-block",
    mode === "active" ? "activeglitch" : "hoverglitch",
    intensity !== "normal" ? `gt-${intensity}` : "",
    neon ? "gt-neon" : "",
    neon && neonFlicker ? "gt-neon-flicker" : "",
    className,
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <span
      className={classes}
      data-text={resolvedText}
      style={
        {
          "--gt-color-a": resolvedA,
          "--gt-color-b": resolvedB,
          "--gt-offset": `${offset}px`,
          "--gt-speed": resolvedSpeed,
          "--gt-glow-color": resolvedGlow,
          ...style,
        } as React.CSSProperties
      }
      {...props}
    >
      {mounted ? children : <span className="invisible">{children}</span>}
      {!mounted && <span className="absolute inset-0">{children}</span>}
    </span>
  );
};

export default GlitchText;
