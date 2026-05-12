"use client";

import React, { ReactNode, useState } from "react";
import "./footer.css";

export type FooterColor = "cyan" | "pink" | "green" | (string & {});
export type FooterVariant = "minimal" | "columns" | "centered" | "mega";

export interface FooterLink {
  label: string;
  href?: string;
  onClick?: () => void;
  external?: boolean;
}

export interface FooterLinkGroup {
  title: string;
  links: FooterLink[];
}

export interface FooterSocialLink {
  label: string;
  href: string;
  icon: ReactNode;
}

const COLOR_PRESETS: Record<string, string> = {
  cyan: "#00f3ff",
  pink: "#ff00ff",
  green: "#39ff14",
};

export const GithubIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-full h-full" aria-hidden="true">
    <path d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0 1 12 6.844a9.59 9.59 0 0 1 2.504.337c1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.02 10.02 0 0 0 22 12.017C22 6.484 17.522 2 12 2z" />
  </svg>
);

export const TwitterIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-full h-full" aria-hidden="true">
    <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z" />
  </svg>
);

export const DiscordIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-full h-full" aria-hidden="true">
    <path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057c.002.022.015.043.032.054a19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028c.462-.63.874-1.295 1.226-1.994a.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.956-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.956 2.418-2.157 2.418zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.946 2.418-2.157 2.418z" />
  </svg>
);

export const LinkedInIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-full h-full" aria-hidden="true">
    <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 0 1-2.063-2.065 2.064 2.064 0 1 1 2.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z" />
  </svg>
);

export interface FooterProps {
  variant?: FooterVariant;
  color?: FooterColor;
  logo?: string | ReactNode;
  logoText?: string;
  logoHref?: string;
  description?: string;
  linkGroups?: FooterLinkGroup[];
  navLinks?: FooterLink[];
  socialLinks?: FooterSocialLink[];
  copyright?: string;
  showNewsletter?: boolean;
  newsletterPlaceholder?: string;
  newsletterButtonLabel?: string;
  onNewsletterSubmit?: (email: string) => void;
  background?: "transparent" | "glass" | "solid";
  topBorder?: boolean;
  className?: string;
}

function resolveCopyright(text: string): string {
  return text.replace("{year}", String(new Date().getFullYear()));
}

function FooterLinkNode({ link }: { link: FooterLink }) {
  const props = {
    className: "ftc-link",
    href: link.href ?? "#",
    ...(link.external ? { target: "_blank", rel: "noopener noreferrer" } : {}),
    onClick: link.onClick,
  };
  return <a {...props}>{link.label}</a>;
}

function SocialBtn({ s }: { s: FooterSocialLink }) {
  return (
    <a
      href={s.href}
      className="ftc-social-btn flex items-center justify-center w-8 h-8"
      target="_blank"
      rel="noopener noreferrer"
      aria-label={s.label}
      title={s.label}
    >
      <span className="flex items-center justify-center w-[0.95rem] h-[0.95rem]">{s.icon}</span>
    </a>
  );
}

function Newsletter({
  placeholder,
  buttonLabel,
  color,
  onSubmit,
}: {
  placeholder: string;
  buttonLabel: string;
  color: string;
  onSubmit?: (email: string) => void;
}) {
  const [email, setEmail] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (email.trim()) {
      onSubmit?.(email.trim());
      setEmail("");
    }
  };

  return (
    <form className="ftc-newsletter" onSubmit={handleSubmit} style={{ "--ftc-color": color } as React.CSSProperties}>
      <input
        type="email"
        className="ftc-newsletter-input"
        placeholder={placeholder}
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        required
        aria-label="Email for newsletter"
      />
      <button type="submit" className="ftc-newsletter-btn">{buttonLabel}</button>
    </form>
  );
}

export const Footer: React.FC<FooterProps> = ({
  variant = "columns",
  color = "cyan",
  logo,
  logoText,
  logoHref = "/",
  description,
  linkGroups = [],
  navLinks = [],
  socialLinks = [],
  copyright = "© {year} All rights reserved.",
  showNewsletter = false,
  newsletterPlaceholder = "Enter your email",
  newsletterButtonLabel = "Subscribe",
  onNewsletterSubmit,
  background = "solid",
  topBorder = true,
  className = "",
}) => {
  const resolvedColor = COLOR_PRESETS[color] ?? color;
  const cssVars = { "--ftc-color": resolvedColor } as React.CSSProperties;
  const copyrightText = resolveCopyright(copyright);

  const bgClass =
    background === "glass"
      ? "bg-[rgba(4,4,4,0.82)] backdrop-blur-[14px]"
      : background === "transparent"
        ? "bg-transparent"
        : "bg-[#060606]";

  const borderClass = topBorder ? "ftc-border-top" : "";

  const logoNode = logo || logoText ? (
    <a href={logoHref} className="ftc-logo inline-flex items-center gap-2.5 no-underline cursor-pointer shrink-0">
      {typeof logo === "string" ? (
        <img src={logo} alt="Logo" className="ftc-logo-img w-8 h-8 object-contain" />
      ) : (logo)}
      {logoText && (
        <span className="ftc-logo-text font-orbitron font-bold text-base tracking-[0.1em] text-white transition-[color,text-shadow] duration-[240ms]">
          {logoText}
        </span>
      )}
    </a>
  ) : null;

  const socialRow = socialLinks.length > 0 ? (
    <div className="flex items-center gap-2 flex-wrap">
      {socialLinks.map((s, i) => <SocialBtn key={i} s={s} />)}
    </div>
  ) : null;

  if (variant === "minimal") {
    return (
      <footer className={`w-full font-orbitron py-4 ${bgClass} ${borderClass} ${className}`} style={cssVars} aria-label="Site footer">
        <div className="flex flex-col sm:flex-row items-center sm:justify-between flex-wrap gap-3 sm:gap-4 py-3 px-5 sm:px-8 max-w-[1280px] mx-auto text-center sm:text-left">
          <div className="flex items-center justify-center sm:justify-start gap-3 shrink-0 w-full sm:w-auto">
            {logoNode}
            {!logo && !logoText && (
              <span className="font-orbitron text-[0.58rem] tracking-[0.06em] uppercase text-white/25 whitespace-nowrap">{copyrightText}</span>
            )}
          </div>
          {navLinks.length > 0 && (
            <nav className="flex items-center gap-5 flex-wrap flex-1 justify-center w-full sm:w-auto" aria-label="Footer navigation">
              {navLinks.map((link, i) => <FooterLinkNode key={i} link={link} />)}
            </nav>
          )}
          <div className="flex sm:flex-row flex-col items-center gap-4 shrink-0 w-full sm:w-auto justify-center sm:justify-end">
            {socialRow}
            {(logo || logoText) && (
              <span className="font-orbitron text-[0.58rem] tracking-[0.06em] uppercase text-white/25 whitespace-nowrap">{copyrightText}</span>
            )}
          </div>
        </div>
      </footer>
    );
  }

  if (variant === "centered") {
    return (
      <footer className={`w-full font-orbitron py-10 px-8 ${bgClass} ${borderClass} ${className}`} style={cssVars} aria-label="Site footer">
        <div className="flex flex-col items-center gap-6 max-w-[1280px] mx-auto text-center">
          {logoNode && <div className="flex justify-center">{logoNode}</div>}
          {navLinks.length > 0 && (
            <nav className="flex items-center justify-center gap-7 flex-wrap" aria-label="Footer navigation">
              {navLinks.map((link, i) => <FooterLinkNode key={i} link={link} />)}
            </nav>
          )}
          {socialRow && <div className="flex justify-center">{socialRow}</div>}
          <span className="font-orbitron text-[0.58rem] tracking-[0.06em] uppercase text-white/25 whitespace-nowrap">{copyrightText}</span>
        </div>
      </footer>
    );
  }

  if (variant === "columns") {
    return (
      <footer className={`w-full font-orbitron ${bgClass} ${borderClass} ${className}`} style={cssVars} aria-label="Site footer">
        <div className="grid grid-cols-1 md:grid-cols-[1.6fr_repeat(auto-fill,minmax(9rem,1fr))] gap-8 md:gap-12 py-8 md:py-14 px-5 md:px-8 max-w-[1280px] mx-auto w-full box-border">
          <div className="flex flex-col gap-5">
            {logoNode}
            {description && <p className="font-sans text-[0.78rem] leading-[1.65] text-white/[0.38] max-w-[22rem] m-0">{description}</p>}
            {socialRow}
          </div>
          {linkGroups.map((group, gi) => (
            <div key={gi} className="flex flex-col gap-2.5">
              <p className="ftc-link-group-title font-orbitron text-[0.6rem] font-bold tracking-[0.12em] uppercase text-[var(--ftc-color)] m-0 mb-1">{group.title}</p>
              {group.links.map((link, li) => <FooterLinkNode key={li} link={link} />)}
            </div>
          ))}
        </div>
        <div className="flex items-center justify-between flex-wrap gap-3 py-4 px-8 border-t border-white/[0.07] max-w-[1280px] mx-auto w-full box-border">
          <span className="font-orbitron text-[0.58rem] tracking-[0.06em] uppercase text-white/25 whitespace-nowrap">{copyrightText}</span>
          {navLinks.length > 0 && (
            <nav className="flex items-center gap-5 flex-wrap" aria-label="Bottom footer navigation">
              {navLinks.map((link, i) => <FooterLinkNode key={i} link={link} />)}
            </nav>
          )}
        </div>
      </footer>
    );
  }

  return (
    <footer className={`w-full font-orbitron ${bgClass} ${borderClass} ${className}`} style={cssVars} aria-label="Site footer">
      <div className="grid grid-cols-1 md:grid-cols-[1.75fr_1fr] gap-8 md:gap-12 py-8 md:py-14 px-5 md:px-8 pb-6 md:pb-10 max-w-[1280px] mx-auto">
        <div className="flex flex-col gap-5">
          {logoNode}
          {description && <p className="font-sans text-[0.78rem] leading-[1.65] text-white/[0.38] max-w-[22rem] m-0">{description}</p>}
          {showNewsletter && (
            <div className="flex flex-col gap-1.5">
              <p className="ftc-newsletter-label">Stay in the loop</p>
              <Newsletter placeholder={newsletterPlaceholder} buttonLabel={newsletterButtonLabel} color={resolvedColor} onSubmit={onNewsletterSubmit} />
            </div>
          )}
          {socialRow}
        </div>
        <div className="grid grid-cols-2 md:grid-cols-[repeat(auto-fill,minmax(9rem,1fr))] gap-6 md:gap-y-8 md:gap-x-10 content-start">
          {linkGroups.map((group, gi) => (
            <div key={gi} className="flex flex-col gap-2.5">
              <p className="ftc-link-group-title font-orbitron text-[0.6rem] font-bold tracking-[0.12em] uppercase text-[var(--ftc-color)] m-0 mb-1">{group.title}</p>
              {group.links.map((link, li) => <FooterLinkNode key={li} link={link} />)}
            </div>
          ))}
        </div>
      </div>
      <div className="flex items-center justify-between flex-wrap gap-3 py-4 px-8 border-t border-white/[0.07] max-w-[1280px] mx-auto w-full box-border">
        <span className="font-orbitron text-[0.58rem] tracking-[0.06em] uppercase text-white/25 whitespace-nowrap">{copyrightText}</span>
        {navLinks.length > 0 && (
          <nav className="flex items-center gap-5 flex-wrap" aria-label="Bottom footer navigation">
            {navLinks.map((link, i) => <FooterLinkNode key={i} link={link} />)}
          </nav>
        )}
      </div>
    </footer>
  );
};

export default Footer;
