import { useEffect, useRef } from "react";
import { BrowserRouter } from "react-router-dom";
import { useLocation } from "react-router-dom";

import { AppRouter } from "../routes/AppRouter";
import { logEvent, setLogContext } from "../lib/logging/logger";


function RouteLogger() {
  const location = useLocation();
  const previousPathRef = useRef<string | null>(null);

  useEffect(() => {
    const nextPath = `${location.pathname}${location.search}${location.hash}`;
    setLogContext({ path: nextPath });
    if (previousPathRef.current !== null && previousPathRef.current !== nextPath) {
      logEvent("info", "router.navigation", "Route changed", {
        path: nextPath,
        from_path: previousPathRef.current,
      });
    }
    previousPathRef.current = nextPath;
  }, [location.hash, location.pathname, location.search]);

  return null;
}


export function App() {
  return (
    <BrowserRouter>
      <RouteLogger />
      <AppRouter />
    </BrowserRouter>
  );
}
