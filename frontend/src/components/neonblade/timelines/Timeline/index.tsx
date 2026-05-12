"use client";

import React, { HTMLAttributes, ReactNode } from "react";
import "./timeline.css";

export type TimelineColor = "cyan" | "pink" | "green" | (string & {});

export type TimelineVariant = "default" | "glow" | "minimal" | "stepped";

export type TimelineLineStyle = "solid" | "dashed" | "glow" | "none";

export type TimelineDotStyle = "circle" | "square" | "diamond";

export type TimelineDotAnim = "none" | "pulse" | "ping";

export type TimelineAlign = "left" | "right" | "alternate";

export interface TimelineItemData {
  date?: string;
  title: string;
  description?: string;
  badge?: string;
  icon?: ReactNode;
  active?: boolean;
}

export interface TimelineProps extends Omit<
  HTMLAttributes<HTMLDivElement>,
  "children"
> {
  items: TimelineItemData[];
  color?: TimelineColor;
  variant?: TimelineVariant;
  lineStyle?: TimelineLineStyle;
  dotStyle?: TimelineDotStyle;
  dotAnim?: TimelineDotAnim;
  align?: TimelineAlign;
  animate?: boolean;
}

const COLOR_PRESETS: Record<string, string> = {
  cyan: "#00f3ff",
  pink: "#ff00ff",
  green: "#39ff14",
};

function resolveColor(color: TimelineColor): string {
  return COLOR_PRESETS[color as string] ?? color;
}

export function Timeline({
  items,
  color = "cyan",
  variant = "default",
  lineStyle = "solid",
  dotStyle = "circle",
  dotAnim = "none",
  align = "left",
  animate = false,
  className = "",
  style,
  ...rest
}: TimelineProps) {
  const resolvedColor = resolveColor(color);

  const rootClasses = [
    "tl-root",
    "relative flex flex-col",
    `tl-variant-${variant}`,
    animate ? "tl-animate" : "",
    className,
  ]
    .filter(Boolean)
    .join(" ");

  const lineClasses = ["tl-line", `tl-line-${lineStyle}`].join(" ");

  return (
    <div
      {...rest}
      className={rootClasses}
      data-align={align}
      style={{ "--tl-color": resolvedColor, ...style } as React.CSSProperties}
    >
      {lineStyle !== "none" && <div className={lineClasses} />}

      {items.map((item, idx) => {
        const dotClasses = [
          "tl-dot",
          "border-2 text-[var(--tl-color)] border-[var(--tl-color)] bg-[#0a0a0a] flex items-center justify-center transition-[box-shadow] duration-300",
          "w-[var(--tl-dot-size)] h-[var(--tl-dot-size)] text-[11px]",
          dotStyle === "circle"
            ? "rounded-full"
            : dotStyle === "square"
              ? "rounded-[2px]"
              : "rounded-[2px] rotate-45",
          item.active ? "tl-dot-active" : "",
        ]
          .filter(Boolean)
          .join(" ");

        const dotWrapClasses = [
          dotAnim === "pulse" ? "tl-dot-pulse" : "",
          "shrink-0 flex items-center justify-center relative z-[1]",
          align === "alternate"
            ? "absolute left-1/2 -translate-x-1/2 mt-0.5"
            : "mt-0.5",
        ]
          .filter(Boolean)
          .join(" ");

        const itemClasses = [
          "tl-item relative flex items-start gap-4 pb-8 last:pb-0",
          align === "right" ? "flex-row-reverse" : "",
        ]
          .filter(Boolean)
          .join(" ");

        const isAlternate = align === "alternate";
        const contentClasses = [
          "flex-1 flex flex-col gap-1 min-w-0 pt-px",
          isAlternate && idx % 2 === 0 ? "pr-[calc(50%+24px)]" : "",
          isAlternate && idx % 2 !== 0
            ? "pl-[calc(50%+24px)] text-right items-end"
            : "",
        ]
          .filter(Boolean)
          .join(" ");

        const titleClasses = [
          "font-semibold text-white leading-[1.3] font-orbitron tracking-[0.04em]",
          variant === "minimal" ? "text-[0.8rem]" : "text-sm",
        ].join(" ");

        const descClasses = [
          "text-white/45 leading-[1.55]",
          variant === "minimal" ? "text-[0.73rem]" : "text-[0.8rem]",
        ].join(" ");

        return (
          <div key={idx} className={itemClasses}>
            <div className={dotWrapClasses}>
              <div className={dotClasses}>
                {item.icon && (
                  <span
                    style={{
                      transform:
                        dotStyle === "diamond" ? "rotate(-45deg)" : undefined,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                    }}
                  >
                    {item.icon}
                  </span>
                )}
                {dotAnim === "ping" && (
                  <span
                    className="tl-dot-ping-ring"
                    style={{
                      borderRadius:
                        dotStyle === "circle" ? "50%" : "2px",
                    }}
                  />
                )}
              </div>
            </div>

            <div className={contentClasses}>
              <div
                className={
                  variant === "glow" || variant === "stepped" ? "tl-card" : ""
                }
              >
                {item.date && (
                  <p className="tl-date font-orbitron text-[0.65rem] tracking-[0.1em] uppercase text-[var(--tl-color)] opacity-70 mb-0.5">
                    {item.date}
                  </p>
                )}
                <p className={titleClasses}>{item.title}</p>
                {item.description && (
                  <p className={descClasses}>{item.description}</p>
                )}
                {item.badge && <span className="tl-badge">{item.badge}</span>}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

export default Timeline;
