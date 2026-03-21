type LoadingViewProps = {
  label: string;
};


export function LoadingView({ label }: LoadingViewProps) {
  return (
    <div className="nt-app">
      <div className="nt-glow-line" />
      <div className="nt-bg-gradient" />
      <main className="nt-shell nt-page-center">
        <div className="nt-loading-card">
          <div className="nt-loading-dot" />
          <strong>{label}</strong>
        </div>
      </main>
    </div>
  );
}
