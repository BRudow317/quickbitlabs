"use client";
import React, { CSSProperties, HTMLAttributes, ReactNode } from "react";
import "./neon-glow-text.css";

export type NGColor =
  | "cyan"
  | "pink"
  | "green"
  | "purple"
  | "orange"
  | "yellow"
  | (string & {});

const COLOR_PRESETS: Record<string, string> = {
  cyan: "#00f3ff",
  pink: "#ff00ff",
  green: "#39ff14",
  purple: "#bf00ff",
  orange: "#ff6600",
  yellow: "#ffe000",
};

function resolveColor(color: NGColor): string {
  return COLOR_PRESETS[color as string] ?? color;
}

export type NGGradientDirection =
  | "left-right"
  | "right-left"
  | "top-bottom"
  | "bottom-top"
  | "diagonal-tl-br"
  | "diagonal-tr-bl"
  | "radial"
  | "conic";

const LINEAR_ANGLES: Record<string, string> = {
  "left-right": "90deg",
  "right-left": "270deg",
  "top-bottom": "180deg",
  "bottom-top": "0deg",
  "diagonal-tl-br": "135deg",
  "diagonal-tr-bl": "45deg",
};

export type NGGlowIntensity =
  | "none"
  | "subtle"
  | "normal"
  | "strong"
  | "intense";

function buildSingleGlow(color: string, intensity: NGGlowIntensity): string {
  switch (intensity) {
    case "none":
      return "";
    case "subtle":
      return `0 0 5px ${color}99`;
    case "normal":
      return `0 0 6px ${color}, 0 0 14px ${color}66`;
    case "strong":
      return `0 0 8px ${color}, 0 0 20px ${color}, 0 0 40px ${color}55`;
    case "intense":
      return `0 0 10px ${color}, 0 0 20px ${color}, 0 0 40px ${color}, 0 0 80px ${color}44`;
    default:
      return "";
  }
}

function buildGradientGlowFilter(
  colors: string[],
  intensity: NGGlowIntensity,
): string {
  if (intensity === "none") return "";
  const perColor: Record<NGGlowIntensity, (c: string) => string> = {
    none: () => "",
    subtle: (c) => `drop-shadow(0 0 4px ${c}88)`,
    normal: (c) => `drop-shadow(0 0 6px ${c}99) drop-shadow(0 0 14px ${c}44)`,
    strong: (c) => `drop-shadow(0 0 8px ${c}) drop-shadow(0 0 20px ${c}66)`,
    intense: (c) =>
      `drop-shadow(0 0 12px ${c}) drop-shadow(0 0 28px ${c}77) drop-shadow(0 0 48px ${c}33)`,
  };
  return colors.map(perColor[intensity]).join(" ");
}

export type NGAnimationType = "auto" | "shift" | "pulse";
export type NGAnimationSpeed = "slow" | "normal" | "fast";

export interface NeonGlowProps extends Omit<
  HTMLAttributes<HTMLSpanElement>,
  "style"
> {
  children: ReactNode;
  colors?: NGColor | NGColor[];
  gradientDirection?: NGGradientDirection;
  glowIntensity?: NGGlowIntensity;
  glowColor?: NGColor;
  gradientGlow?: boolean;
  animate?: boolean;
  animationType?: NGAnimationType;
  animationSpeed?: NGAnimationSpeed;
  className?: string;
}

export default function NeonGlow({
  children,
  colors = "cyan",
  gradientDirection = "left-right",
  glowIntensity = "normal",
  glowColor,
  gradientGlow = false,
  animate = false,
  animationType = "auto",
  animationSpeed = "normal",
  className,
  ...rest
}: NeonGlowProps) {
  const colorArray = (Array.isArray(colors) ? colors : [colors]).slice(0, 4);
  const resolvedColors = colorArray.map(resolveColor);
  const isMultiColor = resolvedColors.length > 1;
  const primaryColor = resolvedColors[0];
  const isLinearGradient =
    isMultiColor && !["radial", "conic"].includes(gradientDirection);

  const resolvedAnimType: "shift" | "pulse" =
    animationType === "auto"
      ? isLinearGradient
        ? "shift"
        : "pulse"
      : animationType;

  const computedStyle: CSSProperties = {};

  if (isMultiColor) {
    if (gradientDirection === "radial") {
      computedStyle.backgroundImage = `radial-gradient(ellipse at center, ${resolvedColors.join(", ")})`;
    } else if (gradientDirection === "conic") {
      computedStyle.backgroundImage = `conic-gradient(from 0deg, ${resolvedColors.join(", ")}, ${resolvedColors[0]})`;
    } else {
      const angle = LINEAR_ANGLES[gradientDirection] ?? "90deg";
      if (animate && resolvedAnimType === "shift") {
        const extended = [
          ...resolvedColors,
          ...resolvedColors.slice().reverse(),
          resolvedColors[0],
        ];
        computedStyle.backgroundImage = `linear-gradient(${angle}, ${extended.join(", ")})`;
        computedStyle.backgroundSize = "300% 300%";
      } else {
        computedStyle.backgroundImage = `linear-gradient(${angle}, ${resolvedColors.join(", ")})`;
      }
    }

    computedStyle.WebkitBackgroundClip = "text";
    computedStyle.backgroundClip = "text";
    computedStyle.WebkitTextFillColor = "transparent";
    computedStyle.color = "transparent";

    if (gradientGlow && glowIntensity !== "none") {
      computedStyle.filter = buildGradientGlowFilter(resolvedColors, glowIntensity);
    } else if (glowColor) {
      const singleGlow = buildSingleGlow(resolveColor(glowColor), glowIntensity);
      if (singleGlow) {
        computedStyle.filter = singleGlow
          .split(", ")
          .map((s) => `drop-shadow(${s})`)
          .join(" ");
      }
    }
  } else {
    computedStyle.color = primaryColor;
    const glow = buildSingleGlow(
      glowColor ? resolveColor(glowColor) : primaryColor,
      glowIntensity,
    );
    if (glow) computedStyle.textShadow = glow;
  }

  const animClasses: string[] = [];
  if (animate) {
    if (resolvedAnimType === "shift") {
      animClasses.push("ng-shift", `ng-shift--${animationSpeed}`);
    } else {
      animClasses.push("ng-pulse", `ng-pulse--${animationSpeed}`);
    }
  }

  const allClasses =
    [...animClasses, className].filter(Boolean).join(" ") || undefined;

  return (
    <span className={allClasses} style={computedStyle} {...rest}>
      {children}
    </span>
  );
}
