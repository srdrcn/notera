type StatusPillProps = {
  children: string;
  tone?: "default" | "success" | "warning" | "danger" | "primary" | "teal";
};


export function StatusPill({ children, tone = "default" }: StatusPillProps) {
  const toneClass =
    tone === "success"
      ? "is-success"
      : tone === "danger"
        ? "is-danger"
        : tone === "warning"
          ? "is-warning"
          : tone === "primary"
            ? "is-review"
            : tone === "teal"
              ? "is-teal"
              : "is-muted";

  return <span className={`nt-chip ${toneClass}`}>{children}</span>;
}
