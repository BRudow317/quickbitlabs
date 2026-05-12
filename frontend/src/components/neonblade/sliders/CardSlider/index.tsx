"use client";

import React, { Children, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";
import "./card-slider.css";

export type CardSliderProgressStyle = "bar" | "dots" | "counter";
export type CardSliderCornerAccentStyle = "frame" | "plus";
export type CardSliderButtonPosition = "sides" | "bottom";
export type CardSliderButtonVisibility = "always" | "hover";
export type CardSliderButtonCorner = "top-right" | "top-left" | "bottom-right" | "bottom-left";
export type CardSliderVisibleCount = number | { sm?: number; md?: number; lg?: number; xl?: number };

function CsPlusIcon({ className = "", color }: { className?: string; color: string }) {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor" style={{ color }} className={`absolute h-6 w-6 pointer-events-none z-20 ${className}`}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v12m6-6H6" />
    </svg>
  );
}

export interface CardSliderProps {
  children: ReactNode;
  visibleCount?: CardSliderVisibleCount;
  gap?: number;
  showButtons?: boolean;
  buttonPosition?: CardSliderButtonPosition;
  buttonVisibility?: CardSliderButtonVisibility;
  buttonCornerSize?: number;
  prevButtonCorner?: CardSliderButtonCorner;
  nextButtonCorner?: CardSliderButtonCorner;
  enableSwipe?: boolean;
  swipeThreshold?: number;
  showProgress?: boolean;
  progressStyle?: CardSliderProgressStyle;
  loop?: boolean;
  autoPlay?: boolean;
  autoPlayInterval?: number;
  accentColor?: string;
  edgeFadeColor?: string;
  showEdgeFades?: boolean;
  showCornerAccents?: boolean;
  cornerAccentStyle?: CardSliderCornerAccentStyle;
  scanLines?: boolean;
  className?: string;
  viewportClassName?: string;
  itemClassName?: string;
}

const BUTTON_CORNER_CLASSES: Record<CardSliderButtonCorner, string> = {
  "top-right": "cs-btn-clip-tr",
  "top-left": "cs-btn-clip-tl",
  "bottom-right": "cs-btn-clip-br",
  "bottom-left": "cs-btn-clip-bl",
};

function getVisible(containerWidth: number, vc: CardSliderVisibleCount): number {
  if (typeof vc === "number") return Math.max(1, vc);
  const { sm = 1, md, lg, xl } = vc;
  if (xl !== undefined && containerWidth >= 1280) return Math.max(1, xl);
  if (lg !== undefined && containerWidth >= 1024) return Math.max(1, lg);
  if (md !== undefined && containerWidth >= 768) return Math.max(1, md);
  return Math.max(1, sm);
}

function zeroPad(n: number): string {
  return String(n).padStart(2, "0");
}

export function CardSlider({
  children,
  visibleCount = 1,
  gap = 16,
  showButtons = true,
  buttonPosition = "sides",
  buttonVisibility = "always",
  buttonCornerSize = 10,
  prevButtonCorner = "bottom-left",
  nextButtonCorner = "bottom-right",
  enableSwipe = true,
  swipeThreshold = 50,
  showProgress = true,
  progressStyle = "bar",
  loop = false,
  autoPlay = false,
  autoPlayInterval = 3000,
  accentColor = "#00f3ff",
  edgeFadeColor = "#000000",
  showEdgeFades = false,
  showCornerAccents = true,
  cornerAccentStyle = "frame",
  scanLines = false,
  className = "",
  viewportClassName = "",
  itemClassName = "",
}: CardSliderProps) {
  const items = Children.toArray(children);
  const totalCount = items.length;

  const viewportRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(0);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [isHovered, setIsHovered] = useState(false);
  const [isDragging, setIsDragging] = useState(false);

  const dragStartXRef = useRef(0);
  const dragCurrentXRef = useRef(0);
  const autoPlayTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const visible = useMemo(() => getVisible(containerWidth, visibleCount), [containerWidth, visibleCount]);

  const maxIndex = Math.max(0, totalCount - visible);
  const totalPages = visible > 0 ? Math.ceil(totalCount / visible) : 1;
  const currentPage = visible > 0 ? Math.floor(currentIndex / visible) : 0;
  const progressPercent = maxIndex > 0 ? (currentIndex / maxIndex) * 100 : 100;
  const isPrevDisabled = !loop && currentIndex === 0;
  const isNextDisabled = !loop && currentIndex >= maxIndex;

  const itemWidth = containerWidth > 0 ? (containerWidth - gap * (visible - 1)) / visible : 0;
  const trackOffset = -(currentIndex * (itemWidth + gap));

  useEffect(() => {
    const el = viewportRef.current;
    if (!el) return;
    const ro = new ResizeObserver(([entry]) => setContainerWidth(entry.contentRect.width));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    setCurrentIndex((prev) => Math.min(prev, maxIndex));
  }, [maxIndex]);

  const goTo = useCallback((index: number) => {
    if (totalCount <= visible) return;
    let next: number;
    if (loop) {
      const range = maxIndex + 1;
      next = ((index % range) + range) % range;
    } else {
      next = Math.max(0, Math.min(index, maxIndex));
    }
    setCurrentIndex(next);
  }, [loop, maxIndex, totalCount, visible]);

  const prev = useCallback(() => goTo(currentIndex - 1), [currentIndex, goTo]);
  const next = useCallback(() => goTo(currentIndex + 1), [currentIndex, goTo]);

  const stopAutoPlay = useCallback(() => {
    if (autoPlayTimerRef.current) { clearInterval(autoPlayTimerRef.current); autoPlayTimerRef.current = null; }
  }, []);

  const startAutoPlay = useCallback(() => {
    stopAutoPlay();
    if (!autoPlay || maxIndex <= 0) return;
    autoPlayTimerRef.current = setInterval(() => {
      setCurrentIndex((prev) => { if (prev >= maxIndex) return loop ? 0 : prev; return prev + 1; });
    }, autoPlayInterval);
  }, [autoPlay, autoPlayInterval, maxIndex, loop, stopAutoPlay]);

  useEffect(() => { startAutoPlay(); return stopAutoPlay; }, [startAutoPlay, stopAutoPlay]);

  useEffect(() => {
    if (autoPlay) { if (isHovered) stopAutoPlay(); else startAutoPlay(); }
  }, [isHovered, autoPlay, startAutoPlay, stopAutoPlay]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (!isHovered) return;
      if (e.key === "ArrowLeft") { stopAutoPlay(); prev(); startAutoPlay(); }
      if (e.key === "ArrowRight") { stopAutoPlay(); next(); startAutoPlay(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [prev, next, isHovered, startAutoPlay, stopAutoPlay]);

  const handleDragStart = (x: number) => {
    if (!enableSwipe) return;
    dragStartXRef.current = x;
    dragCurrentXRef.current = x;
    setIsDragging(true);
    stopAutoPlay();
  };

  const handleDragMove = (x: number) => {
    if (!enableSwipe || !isDragging) return;
    dragCurrentXRef.current = x;
  };

  const handleDragEnd = () => {
    if (!enableSwipe || !isDragging) return;
    const diff = dragStartXRef.current - dragCurrentXRef.current;
    if (Math.abs(diff) > swipeThreshold) { if (diff > 0) next(); else prev(); }
    setIsDragging(false);
    startAutoPlay();
  };

  const cssVars = { "--cs-accent": accentColor, "--cs-corner-size": `${buttonCornerSize}px`, "--cs-edge-color": edgeFadeColor } as React.CSSProperties;
  const navBtnInlineStyle: React.CSSProperties = { background: `color-mix(in srgb, ${accentColor} 10%, rgba(5,5,5,0.8))`, color: accentColor, backdropFilter: "blur(6px)" };
  const buttonsVisible = buttonVisibility === "always" || (buttonVisibility === "hover" && isHovered);

  const NavBtn = ({ direction, size = "md" }: { direction: "prev" | "next"; size?: "sm" | "md" }) => {
    const disabled = direction === "prev" ? isPrevDisabled : isNextDisabled;
    const wh = size === "sm" ? "w-9 h-8" : "w-10 h-10";
    const iconClass = size === "sm" ? "w-3.5 h-3.5" : "w-4 h-4";
    const corner = direction === "prev" ? prevButtonCorner : nextButtonCorner;
    const clipClass = BUTTON_CORNER_CLASSES[corner];
    return (
      <div className={`cs-btn-wrapper${disabled ? " cs-btn-disabled" : ""}`}>
        <div className={`p-px ${clipClass}`} style={{ background: `color-mix(in srgb, ${accentColor} 45%, transparent)` }}>
          <button
            className={`cs-btn ${clipClass} ${wh} flex items-center justify-center active:scale-95 transition-all duration-200 ${disabled ? "opacity-20 cursor-not-allowed" : "cursor-pointer"}`}
            style={navBtnInlineStyle}
            onClick={() => { stopAutoPlay(); if (direction === "prev") prev(); else next(); startAutoPlay(); }}
            disabled={disabled}
            aria-label={direction === "prev" ? "Previous slide" : "Next slide"}
          >
            {direction === "prev" ? <ChevronLeft className={iconClass} /> : <ChevronRight className={iconClass} />}
          </button>
        </div>
      </div>
    );
  };

  const ProgressIndicator = () => {
    if (!showProgress) return null;
    if (progressStyle === "bar") {
      return (
        <div className="cs-progress-track flex-1 h-[3px]" style={{ background: `color-mix(in srgb, ${accentColor} 10%, rgba(255,255,255,0.04))` }}>
          <div className="cs-progress-fill" style={{ width: `${progressPercent}%`, background: accentColor, boxShadow: `0 0 7px color-mix(in srgb, ${accentColor} 70%, transparent)` }} />
        </div>
      );
    }
    if (progressStyle === "dots") {
      return (
        <div className="flex-1 flex justify-center items-center gap-2">
          {Array.from({ length: totalPages }).map((_, i) => {
            const active = i === currentPage;
            return (
              <div key={i} className="flex p-px cs-dot-clip" style={{ background: `color-mix(in srgb, ${accentColor} ${active ? 100 : 35}%, transparent)` }}>
                <button
                  onClick={() => { stopAutoPlay(); goTo(i * visible); startAutoPlay(); }}
                  className={`cs-dot cs-dot-clip w-2.5 h-2.5${active ? " cs-dot-active" : ""}`}
                  style={{ background: active ? accentColor : `color-mix(in srgb, ${accentColor} 8%, transparent)` }}
                  aria-label={`Go to slide ${i * visible + 1}`}
                />
              </div>
            );
          })}
        </div>
      );
    }
    if (progressStyle === "counter") {
      return (
        <div className="flex-1 flex justify-center items-center">
          <span className="font-orbitron text-xs tracking-[0.25em] select-none" style={{ color: `color-mix(in srgb, ${accentColor} 50%, rgba(255,255,255,0.5))` }}>
            <span className="cs-counter-current" style={{ color: accentColor }}>{zeroPad(currentIndex + 1)}</span>
            <span className="mx-2 opacity-30">/</span>
            <span>{zeroPad(totalCount)}</span>
          </span>
        </div>
      );
    }
    return null;
  };

  const showSideButtons = showButtons && buttonPosition === "sides";
  const showBottomButtons = showButtons && buttonPosition === "bottom";
  const showFooter = showProgress || showBottomButtons;

  return (
    <div className={`relative select-none ${className}`} style={cssVars} onMouseEnter={() => setIsHovered(true)} onMouseLeave={() => setIsHovered(false)}>
      <div className="relative">
        {showSideButtons && (
          <>
            <div className={`absolute left-2 top-1/2 -translate-y-1/2 z-20 transition-opacity duration-300 ${buttonsVisible ? "opacity-100" : "opacity-0 pointer-events-none"}`}>
              <NavBtn direction="prev" size="md" />
            </div>
            <div className={`absolute right-2 top-1/2 -translate-y-1/2 z-20 transition-opacity duration-300 ${buttonsVisible ? "opacity-100" : "opacity-0 pointer-events-none"}`}>
              <NavBtn direction="next" size="md" />
            </div>
          </>
        )}
        <div
          ref={viewportRef}
          className={`overflow-hidden relative py-3 -my-3 p-2 ${viewportClassName}`}
          onTouchStart={(e) => handleDragStart(e.touches[0].clientX)}
          onTouchMove={(e) => handleDragMove(e.touches[0].clientX)}
          onTouchEnd={handleDragEnd}
          onMouseDown={(e) => handleDragStart(e.clientX)}
          onMouseMove={(e) => handleDragMove(e.clientX)}
          onMouseUp={handleDragEnd}
          onMouseLeave={handleDragEnd}
          style={{ cursor: enableSwipe ? (isDragging ? "grabbing" : "grab") : "default" }}
        >
          {showEdgeFades && (
            <>
              <div className="cs-edge-fade-left absolute inset-y-0 left-0 w-16 z-10" />
              <div className="cs-edge-fade-right absolute inset-y-0 right-0 w-16 z-10" />
            </>
          )}
          {scanLines && <div className="cs-scanlines z-10" />}
          <div className={`cs-track flex${isDragging ? " cs-dragging" : ""}`} style={{ transform: `translateX(${trackOffset}px)`, gap: `${gap}px` }}>
            {items.map((item, i) => (
              <div
                key={i}
                className={`relative flex-shrink-0 ${itemClassName}`}
                style={{ width: itemWidth > 0 ? `${itemWidth}px` : `calc(${100 / visible}% - ${(gap * (visible - 1)) / visible}px)` }}
              >
                {showCornerAccents && cornerAccentStyle === "frame" && (
                  <>
                    <div className="cs-corner-tl" />
                    <div className="cs-corner-tr" />
                    <div className="cs-corner-bl" />
                    <div className="cs-corner-br" />
                  </>
                )}
                {showCornerAccents && cornerAccentStyle === "plus" && (
                  <>
                    <CsPlusIcon color={accentColor} className="-top-3 -left-3" />
                    <CsPlusIcon color={accentColor} className="-bottom-3 -left-3" />
                    <CsPlusIcon color={accentColor} className="-top-3 -right-3" />
                    <CsPlusIcon color={accentColor} className="-bottom-3 -right-3" />
                  </>
                )}
                {item}
              </div>
            ))}
          </div>
        </div>
      </div>
      {showFooter && (
        <div className="flex items-center gap-3 mt-3.5 px-0.5">
          {showBottomButtons && (
            <div className={`flex-shrink-0 transition-opacity duration-300 ${buttonsVisible ? "opacity-100" : "opacity-0 pointer-events-none"}`}>
              <NavBtn direction="prev" size="sm" />
            </div>
          )}
          <ProgressIndicator />
          {showBottomButtons && (
            <div className={`flex-shrink-0 transition-opacity duration-300 ${buttonsVisible ? "opacity-100" : "opacity-0 pointer-events-none"}`}>
              <NavBtn direction="next" size="sm" />
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default CardSlider;
