import React, { HTMLAttributes, ReactNode } from "react";
import "./border-beam.css";

export type BBCColor = "cyan" | "pink" | "green" | (string & {});
export type BBCSize = "sm" | "md" | "lg" | "xl";
export type BBCCorner = "bottom-right" | "bottom-left" | "top-right" | "top-left" | "all";
export type BBCVariant = "single" | "dual" | "gradient-sweep" | "rainbow" | "pulse";
export type BBCGlowIntensity = "none" | "low" | "medium" | "high";

const COLOR_PRESETS: Record<string, string> = {
  cyan: "#00f3ff",
  pink: "#ff00ff",
  green: "#39ff14",
};

const CORNER_CLASSES: Record<BBCCorner, string> = {
  "bottom-right": "bbc-clip-br",
  "bottom-left": "bbc-clip-bl",
  "top-right": "bbc-clip-tr",
  "top-left": "bbc-clip-tl",
  all: "bbc-clip-all",
};

export interface BorderBeamCornerCutCardProps extends HTMLAttributes<HTMLDivElement> {
  children?: ReactNode;
  icon?: ReactNode;
  title?: string;
  description?: string;
  beamColor?: BBCColor;
  beamColorB?: BBCColor;
  variant?: BBCVariant;
  duration?: number;
  durationB?: number;
  borderWidth?: number | string;
  size?: BBCSize;
  corner?: BBCCorner;
  cornerSize?: number;
  glowIntensity?: BBCGlowIntensity;
  bgColor?: string;
  innerClassName?: string;
}

const INNER_PADDING: Record<BBCSize, string> = {
  sm: "p-5",
  md: "p-6",
  lg: "p-8",
  xl: "p-10",
};
const ICON_BOX_SIZE: Record<BBCSize, string> = {
  sm: "w-9 h-9",
  md: "w-12 h-12",
  lg: "w-14 h-14",
  xl: "w-16 h-16",
};
const ICON_SIZE: Record<BBCSize, string> = {
  sm: "w-4 h-4",
  md: "w-6 h-6",
  lg: "w-8 h-8",
  xl: "w-9 h-9",
};
const TITLE_SIZE: Record<BBCSize, string> = {
  sm: "text-sm",
  md: "text-lg",
  lg: "text-[1.375rem]",
  xl: "text-[1.625rem]",
};
const DESC_SIZE: Record<BBCSize, string> = {
  sm: "text-xs",
  md: "text-sm",
  lg: "text-base",
  xl: "text-lg",
};

export const BorderBeamCornerCutCard: React.FC<BorderBeamCornerCutCardProps> = ({
  children,
  icon,
  title,
  description,
  beamColor = "pink",
  beamColorB = "cyan",
  variant = "single",
  duration = 4,
  durationB = 6,
  borderWidth = "2px",
  size = "md",
  corner = "bottom-right",
  cornerSize = 20,
  glowIntensity = "medium",
  bgColor,
  className = "",
  innerClassName = "",
  style,
  ...props
}) => {
  const resolvedA = COLOR_PRESETS[beamColor] ?? beamColor;
  const resolvedB = COLOR_PRESETS[beamColorB] ?? beamColorB;
  const borderWidthValue = typeof borderWidth === "number" ? `${borderWidth}px` : borderWidth;
  const cornerClass = CORNER_CLASSES[corner];

  const outerClasses = [
    "bbc-wrapper",
    "relative w-full overflow-hidden bg-white/[0.05]",
    `bbc-variant-${variant}`,
    glowIntensity !== "none" ? `bbc-glow-${glowIntensity}` : "",
    cornerClass,
    className,
  ].filter(Boolean).join(" ");

  return (
    <div
      className={outerClasses}
      style={{
        "--bbc-beam-color": resolvedA,
        "--bbc-beam-color-b": resolvedB,
        "--bbc-corner-size": `${cornerSize}px`,
        "--bbc-duration": `${duration}s`,
        "--bbc-duration-b": `${durationB}s`,
        padding: borderWidthValue,
        ...style,
      } as React.CSSProperties}
      {...props}
    >
      <div className="bbc-beam" aria-hidden="true" />
      {variant === "dual" && <div className="bbc-beam-b" aria-hidden="true" />}
      <div
        className={[
          "bbc-inner",
          "relative z-10 w-full h-full flex flex-col",
          cornerClass,
          INNER_PADDING[size],
          innerClassName,
        ].filter(Boolean).join(" ")}
        style={{ backgroundColor: bgColor ?? "var(--background, #050505)" }}
      >
        {icon && (
          <div className={["bbc-icon-box", "border border-white/10 bg-black rounded-[4px] flex items-center justify-center shrink-0 mb-6 transition-[border-color,box-shadow] duration-300", ICON_BOX_SIZE[size]].join(" ")}>
            <span className={["bbc-icon", "text-white/70 flex items-center justify-center transition-colors duration-300", ICON_SIZE[size]].join(" ")}>
              {icon}
            </span>
          </div>
        )}
        {title && (
          <h3 className={["bbc-title", "font-orbitron font-bold text-white mb-3 leading-[1.3] transition-[text-shadow] duration-300", TITLE_SIZE[size]].join(" ")}>
            {title}
          </h3>
        )}
        {description && (
          <p className={["text-white/60 leading-[1.65] flex-grow", DESC_SIZE[size]].join(" ")}>
            {description}
          </p>
        )}
        {children}
      </div>
    </div>
  );
};

export default BorderBeamCornerCutCard;
