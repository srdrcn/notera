import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClientProvider } from "@tanstack/react-query";

import { App } from "./app/App";
import { SessionProvider } from "./app/session";
import { queryClient } from "./lib/api/queryClient";
import { initializeClientLogging } from "./lib/logging/logger";
import "./styles/index.css";

initializeClientLogging();

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <SessionProvider>
        <App />
      </SessionProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
