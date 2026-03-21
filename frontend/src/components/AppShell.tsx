import type { PropsWithChildren, ReactNode } from "react";
import { Link } from "react-router-dom";


type AppShellProps = PropsWithChildren<{
  title: string;
  subtitle?: string;
  aboveTitle?: ReactNode;
  titleAction?: ReactNode;
  navSlot?: ReactNode;
  actions?: ReactNode;
}>;


export function AppShell({
  title,
  subtitle,
  aboveTitle,
  titleAction,
  navSlot,
  actions,
  children,
}: AppShellProps) {
  return (
    <div className="nt-app">
      <div className="nt-glow-line" />
      <div className="nt-bg-gradient" />
      <div className="nt-shell">
        <main className="nt-container nt-page">
          <section className="nt-page-toolbar">
            <Link className="nt-brand nt-page-brand" to="/dashboard">
              <img className="nt-brand-logo" src="/brand-mark.svg" alt="" />
              <span className="nt-brand-name">Notera</span>
            </Link>
            {aboveTitle || navSlot ? (
              <div className="nt-page-toolbar-actions">
                {aboveTitle ? <div>{aboveTitle}</div> : null}
                {navSlot}
              </div>
            ) : null}
          </section>
          <section className="nt-page-head">
            <div>
              <p className="nt-page-kicker">Teams transcript operations</p>
              <div className="nt-page-title-row">
                <h1 className="nt-page-title">{title}</h1>
                {titleAction ? <div className="nt-page-title-action">{titleAction}</div> : null}
              </div>
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
