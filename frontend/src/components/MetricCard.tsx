import type { ReactNode } from "react";


type MetricCardProps = {
  label: string;
  value: ReactNode;
  hint?: string | null;
  accent?: "primary" | "teal" | "warning";
  extra?: ReactNode;
  className?: string;
};


export function MetricCard({
  label,
  value,
  hint,
  accent = "primary",
  extra,
  className = "",
}: MetricCardProps) {
  return (
    <article className={`nt-card nt-card-padded nt-metric-card nt-metric-${accent} ${className}`}>
      <span className="nt-card-label">{label}</span>
      <strong className="nt-card-value">{value}</strong>
      {hint ? <p className="nt-card-hint">{hint}</p> : null}
      {extra ? <div className="nt-card-extra">{extra}</div> : null}
    </article>
  );
}
