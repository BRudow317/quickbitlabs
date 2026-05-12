"use client";

import React, { useState, useEffect, useRef, ReactNode } from "react";
import "./navbar.css";

export type NavBarColor = "cyan" | "pink" | "green" | (string & {});
export type NavBarVariant = "standard" | "dock";
export type NavAlign = "left" | "center" | "right";
export type DropdownAlign = "left" | "center" | "right";
export type NavBarPosition = "fixed" | "sticky" | "floating" | "static";
export type NavBarTransparency = "transparent" | "glass" | "solid";
export type DockPosition = "top" | "bottom";

export interface NavItem {
  label: string;
  href?: string;
  icon?: ReactNode;
  onClick?: () => void;
  children?: NavItem[];
  active?: boolean;
}

export interface ProfileMenuItem {
  key: string;
  label?: string;
  icon?: ReactNode;
  onClick?: () => void;
  divider?: boolean;
}

const COLOR_PRESETS: Record<string, string> = {
  cyan: "#00f3ff",
  pink: "#ff00ff",
  green: "#39ff14",
};

const ALIGN_CLASSES: Record<NavAlign, string> = {
  center: "justify-center",
  left: "justify-start",
  right: "justify-end",
};

export const PRESET_PROFILE_ITEMS: ProfileMenuItem[] = [
  { key: "profile", label: "Profile" },
  { key: "settings", label: "Settings" },
  { key: "divider", divider: true },
  { key: "logout", label: "Logout" },
];

export interface NavBarProps {
  variant?: NavBarVariant;
  position?: NavBarPosition;
  transparency?: NavBarTransparency;
  color?: NavBarColor;
  logo?: string | ReactNode;
  logoText?: string;
  logoHref?: string;
  items?: NavItem[];
  showProfile?: boolean;
  profileAvatar?: string | ReactNode;
  profileName?: string;
  profileItems?: ProfileMenuItem[];
  onProfileAction?: (key: string) => void;
  scrollEffect?: boolean;
  scrollThreshold?: number;
  dockPosition?: DockPosition;
  dockShowLabels?: boolean;
  navAlign?: NavAlign;
  dropdownAlign?: DropdownAlign;
  className?: string;
}

function SubMenuDropdown({ items, color, align = "center" }: { items: NavItem[]; color: string; align?: DropdownAlign }) {
  const alignClass = align === "left" ? "nbr-submenu--left" : align === "right" ? "nbr-submenu--right" : "nbr-submenu--center";
  return (
    <div className={`nbr-submenu ${alignClass}`} style={{ "--nbr-color": color } as React.CSSProperties}>
      {items.map((item, idx) =>
        item.href ? (
          <a key={idx} href={item.href} className="nbr-submenu-item">
            {item.icon && <span className="inline-flex items-center w-[0.8rem] h-[0.8rem]">{item.icon}</span>}
            {item.label}
          </a>
        ) : (
          <button key={idx} className="nbr-submenu-item" onClick={item.onClick}>
            {item.icon && <span className="inline-flex items-center w-[0.8rem] h-[0.8rem]">{item.icon}</span>}
            {item.label}
          </button>
        ),
      )}
    </div>
  );
}

function NavDesktopItem({ item, color, dropdownAlign = "center" }: { item: NavItem; color: string; dropdownAlign?: DropdownAlign }) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function handleOutside(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handleOutside);
    return () => document.removeEventListener("mousedown", handleOutside);
  }, [open]);

  const hasChildren = !!item.children?.length;
  const style = { "--nbr-color": color } as React.CSSProperties;

  if (!hasChildren) {
    return item.href ? (
      <a href={item.href} className="nbr-nav-item" style={style} onClick={item.onClick}>
        {item.icon && <span className="inline-flex items-center w-[0.9rem] h-[0.9rem]">{item.icon}</span>}
        {item.label}
      </a>
    ) : (
      <button className="nbr-nav-item" style={style} onClick={item.onClick}>
        {item.icon && <span className="inline-flex items-center w-[0.9rem] h-[0.9rem]">{item.icon}</span>}
        {item.label}
      </button>
    );
  }

  return (
    <div ref={wrapRef} className="relative">
      <button className="nbr-nav-item nbr-nav-item--has-sub" style={style} onClick={() => setOpen((v) => !v)} aria-expanded={open} aria-haspopup="true">
        {item.icon && <span className="inline-flex items-center w-[0.9rem] h-[0.9rem]">{item.icon}</span>}
        {item.label}
        <span className={`nbr-chevron${open ? " nbr-chevron--open" : ""}`}>▾</span>
      </button>
      {open && <SubMenuDropdown items={item.children!} color={color} align={dropdownAlign} />}
    </div>
  );
}

function ProfileDropdown({ avatar, name, items, color, onAction }: { avatar?: string | ReactNode; name?: string; items: ProfileMenuItem[]; color: string; onAction?: (key: string) => void }) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function handleOutside(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handleOutside);
    return () => document.removeEventListener("mousedown", handleOutside);
  }, [open]);

  const style = { "--nbr-color": color } as React.CSSProperties;

  const avatarContent = typeof avatar === "string" ? (
    <img src={avatar} alt="User avatar" className="w-full h-full object-cover" />
  ) : avatar !== undefined ? avatar : (
    <span className="nbr-avatar-initials">{name ? name.charAt(0).toUpperCase() : "U"}</span>
  );

  return (
    <div ref={wrapRef} className="relative">
      <button className="nbr-avatar-btn" style={style} onClick={() => setOpen((v) => !v)} aria-label="User account menu" aria-expanded={open} aria-haspopup="true">
        {avatarContent}
      </button>
      {open && (
        <div className="nbr-profile-menu" style={style}>
          {name && <div className="nbr-profile-menu-header">{name}</div>}
          {items.map((item, idx) => {
            if (item.divider) return <div key={idx} className="nbr-profile-divider" />;
            return (
              <button key={item.key} className="nbr-profile-menu-item" onClick={() => { item.onClick?.(); onAction?.(item.key); setOpen(false); }}>
                {item.icon && <span className="inline-flex items-center w-[0.8rem] h-[0.8rem]">{item.icon}</span>}
                {item.label}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

export const NavBar: React.FC<NavBarProps> = ({
  variant = "standard",
  position = "fixed",
  transparency = "glass",
  color = "cyan",
  logo,
  logoText,
  logoHref = "/",
  items = [],
  showProfile = false,
  profileAvatar,
  profileName,
  profileItems = PRESET_PROFILE_ITEMS,
  onProfileAction,
  scrollEffect = true,
  scrollThreshold = 20,
  dockPosition = "bottom",
  dockShowLabels = true,
  navAlign = "center",
  dropdownAlign = "center",
  className = "",
}) => {
  const [scrolled, setScrolled] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [openMobileIdx, setOpenMobileIdx] = useState<number | null>(null);

  const resolvedColor = COLOR_PRESETS[color] ?? color;
  const cssVars = { "--nbr-color": resolvedColor } as React.CSSProperties;

  useEffect(() => {
    if (!scrollEffect || variant === "dock") return;
    const onScroll = () => setScrolled(window.scrollY > scrollThreshold);
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, [scrollEffect, scrollThreshold, variant]);

  useEffect(() => {
    const onResize = () => { if (window.innerWidth >= 768) setMobileOpen(false); };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  const logoNode = (
    <a href={logoHref} className="nbr-logo inline-flex items-center gap-2.5 shrink-0 no-underline cursor-pointer">
      {typeof logo === "string" ? <img src={logo} alt="Logo" className="nbr-logo-img w-9 h-9 object-contain" /> : logo}
      {logoText && <span className="nbr-logo-text font-orbitron font-bold text-[1.05rem] tracking-[0.08em] text-white transition-[color,text-shadow] duration-[280ms]">{logoText}</span>}
    </a>
  );

  if (variant === "dock") {
    const dockPosClass = position === "static" ? "nbr-pos-static" : position === "sticky" ? "nbr-pos-sticky" : "nbr-pos-fixed";
    return (
      <nav className={`nbr-dock nbr-dock--${dockPosition} ${dockPosClass} ${className}`} style={cssVars} aria-label="Navigation">
        <div className="nbr-dock-inner">
          {items.map((item, idx) => (
            <a key={idx} href={item.href} className={`nbr-dock-item${item.active ? " nbr-dock-item--active" : ""}`} onClick={item.onClick} title={item.label} aria-label={item.label}>
              {item.icon && <span className="flex items-center justify-center w-[1.1rem] h-[1.1rem]">{item.icon}</span>}
              {dockShowLabels && <span className="nbr-dock-item-label">{item.label}</span>}
            </a>
          ))}
          {showProfile && (
            <>
              <div className="nbr-dock-profile-sep" aria-hidden />
              <div className="nbr-dock-item">
                <ProfileDropdown avatar={profileAvatar} name={profileName} items={profileItems} color={resolvedColor} onAction={onProfileAction} />
                {dockShowLabels && <span className="nbr-dock-item-label">Account</span>}
              </div>
            </>
          )}
        </div>
      </nav>
    );
  }

  const posClass = position === "floating" ? "nbr-pos-floating" : position === "sticky" ? "nbr-pos-sticky" : position === "static" ? "nbr-pos-static" : "nbr-pos-fixed";
  const bgClass = scrollEffect && transparency === "transparent" && scrolled ? "nbr-bg-glass nbr-scrolled" : `nbr-bg-${transparency}`;

  return (
    <>
      <nav className={`nbr-root ${posClass} ${bgClass} w-full z-[100] ${className}`} style={cssVars} aria-label="Navigation">
        <div className="flex items-center justify-between py-[0.875rem] px-6 max-w-[1280px] mx-auto gap-4">
          {logoNode}
          <div className={`hidden md:flex items-center gap-0 flex-1 ${ALIGN_CLASSES[navAlign]}`}>
            {items.map((item, idx) => <NavDesktopItem key={idx} item={item} color={resolvedColor} dropdownAlign={dropdownAlign} />)}
          </div>
          <div className="flex items-center gap-2.5 shrink-0">
            {showProfile && <ProfileDropdown avatar={profileAvatar} name={profileName} items={profileItems} color={resolvedColor} onAction={onProfileAction} />}
            <button
              className="nbr-hamburger flex md:hidden items-center justify-center w-9 h-9 bg-transparent border-none cursor-pointer p-1.5 text-white/65 transition-colors duration-[280ms] shrink-0"
              onClick={() => setMobileOpen((v) => !v)}
              aria-label={mobileOpen ? "Close navigation" : "Open navigation"}
              aria-expanded={mobileOpen}
            >
              <span className={`nbr-ham-icon${mobileOpen ? " nbr-ham-icon--open" : ""}`} />
            </button>
          </div>
        </div>
        {mobileOpen && (
          <div className="nbr-mobile-menu" style={cssVars}>
            {items.map((item, idx) => {
              const hasChildren = !!item.children?.length;
              const isOpen = openMobileIdx === idx;
              return (
                <div key={idx} className="nbr-mobile-item-wrap">
                  {hasChildren ? (
                    <>
                      <button className="nbr-mobile-item nbr-mobile-item--has-sub" onClick={() => setOpenMobileIdx(isOpen ? null : idx)} aria-expanded={isOpen}>
                        {item.icon && <span className="inline-flex items-center w-[0.9rem] h-[0.9rem]">{item.icon}</span>}
                        <span>{item.label}</span>
                        <span className={`nbr-chevron${isOpen ? " nbr-chevron--open" : ""}`}>▾</span>
                      </button>
                      {isOpen && (
                        <div className="nbr-mobile-submenu">
                          {item.children!.map((child, cidx) =>
                            child.href ? (
                              <a key={cidx} href={child.href} className="nbr-mobile-submenu-item" onClick={() => { child.onClick?.(); setMobileOpen(false); }}>
                                {child.icon && <span className="inline-flex items-center w-[0.9rem] h-[0.9rem]">{child.icon}</span>}
                                {child.label}
                              </a>
                            ) : (
                              <button key={cidx} className="nbr-mobile-submenu-item" onClick={() => { child.onClick?.(); setMobileOpen(false); }}>
                                {child.icon && <span className="inline-flex items-center w-[0.9rem] h-[0.9rem]">{child.icon}</span>}
                                {child.label}
                              </button>
                            ),
                          )}
                        </div>
                      )}
                    </>
                  ) : item.href ? (
                    <a href={item.href} className="nbr-mobile-item" onClick={() => { item.onClick?.(); setMobileOpen(false); }}>
                      {item.icon && <span className="inline-flex items-center w-[0.9rem] h-[0.9rem]">{item.icon}</span>}
                      {item.label}
                    </a>
                  ) : (
                    <button className="nbr-mobile-item" onClick={() => { item.onClick?.(); setMobileOpen(false); }}>
                      {item.icon && <span className="inline-flex items-center w-[0.9rem] h-[0.9rem]">{item.icon}</span>}
                      {item.label}
                    </button>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </nav>
      {mobileOpen && <div className="nbr-mobile-backdrop" onClick={() => setMobileOpen(false)} aria-hidden />}
    </>
  );
};

export default NavBar;
