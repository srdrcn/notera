import { Navigate, Route, Routes } from "react-router-dom";

import { LoadingView } from "../components/LoadingView";
import { ProtectedRoute } from "../components/ProtectedRoute";
import { useSession } from "../app/session";
import { LoginPage } from "../features/auth/LoginPage";
import { DashboardPage } from "../features/dashboard/DashboardPage";
import { TranscriptPage } from "../features/transcripts/TranscriptPage";


export function AppRouter() {
  const session = useSession();

  if (session.isLoading) {
    return <LoadingView label="Oturum doğrulanıyor" />;
  }

  return (
    <Routes>
      <Route
        path="/"
        element={
          <Navigate
            to={session.isAuthenticated ? "/dashboard" : "/login"}
            replace
          />
        }
      />
      <Route
        path="/login"
        element={
          session.isAuthenticated ? (
            <Navigate to="/dashboard" replace />
          ) : (
            <LoginPage />
          )
        }
      />
      <Route
        path="/dashboard"
        element={
          <ProtectedRoute>
            <DashboardPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="/transcripts/:meetingId"
        element={
          <ProtectedRoute>
            <TranscriptPage />
          </ProtectedRoute>
        }
      />
      <Route
        path="*"
        element={<Navigate to={session.isAuthenticated ? "/dashboard" : "/login"} replace />}
      />
    </Routes>
  );
}
