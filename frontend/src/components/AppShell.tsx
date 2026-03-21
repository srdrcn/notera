import type { PropsWithChildren, ReactNode } from "react";
import { Link } from "react-router-dom";


type AppShellProps = PropsWithChildren<{
  title: string;
  subtitle?: string;
  aboveTitle?: ReactNode;
  navSlot?: ReactNode;
  actions?: ReactNode;
}>;


export function AppShell({
  title,
  subtitle,
  aboveTitle,
  navSlot,
  actions,
  children,
}: AppShellProps) {
  return (
    <div className="nt-app">
      <div className="nt-glow-line" />
      <div className="nt-bg-gradient" />
      <div className="nt-shell">
        <header className="nt-nav">
          <div className="nt-nav-inner">
            <Link className="nt-brand" to="/dashboard">
              <img className="nt-brand-logo" src="/brand-mark.svg" alt="" />
              <span className="nt-brand-name">Notera</span>
            </Link>
            <div className="nt-nav-slot">{navSlot}</div>
          </div>
        </header>
        <main className="nt-container nt-page">
          <section className="nt-page-head">
            <div>
              {aboveTitle ? <div className="nt-page-context">{aboveTitle}</div> : null}
              <p className="nt-page-kicker">Teams transcript operations</p>
              <h1 className="nt-page-title">{title}</h1>
              {subtitle ? <p className="nt-page-subtitle">{subtitle}</p> : null}
            </div>
            {actions ? <div className="nt-page-actions">{actions}</div> : null}
          </section>
          {children}
        </main>
      </div>
    </div>
  );
}
