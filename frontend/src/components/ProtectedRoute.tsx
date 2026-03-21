import type { PropsWithChildren } from "react";
import { Navigate, useLocation } from "react-router-dom";

import { useSession } from "../app/session";


export function ProtectedRoute({ children }: PropsWithChildren) {
  const session = useSession();
  const location = useLocation();

  if (!session.isAuthenticated) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }

  return <>{children}</>;
}
