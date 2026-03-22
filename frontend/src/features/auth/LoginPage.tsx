import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { useLocation, useNavigate } from "react-router-dom";
import { z } from "zod";

import { useSession } from "../../app/session";
import { useLogin, useRegister } from "./useAuthMutations";

const authSchema = z.object({
  email: z.string().trim().email("Geçerli bir e-posta adresi girin."),
});

type AuthForm = z.infer<typeof authSchema>;


export function LoginPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const session = useSession();
  const [mode, setMode] = useState<"login" | "register">("login");
  const login = useLogin();
  const register = useRegister();
  const form = useForm<AuthForm>({
    resolver: zodResolver(authSchema),
    defaultValues: {
      email: "",
    },
  });

  useEffect(() => {
    if (!session.isAuthenticated) {
      return;
    }
    const state = location.state as { from?: string } | null;
    navigate(state?.from ?? "/dashboard", { replace: true });
  }, [location.state, navigate, session.isAuthenticated]);

  const activeMutation = mode === "login" ? login : register;

  async function onSubmit(values: AuthForm) {
    await activeMutation.mutateAsync({
      email: values.email.trim().toLowerCase(),
    });
    const state = location.state as { from?: string } | null;
    navigate(state?.from ?? "/dashboard", { replace: true });
  }

  return (
    <div className="nt-app">
      <div className="nt-glow-line" />
      <div className="nt-bg-gradient" />
      <main className="nt-shell nt-auth-shell nt-auth-page">
        <section className="nt-auth-card nt-auth-main">
          <img className="nt-auth-logo" src="/brand-mark.svg" alt="" />
          <p className="nt-eyebrow">Quiet intelligence for Teams calls</p>
          <h1 className="nt-auth-title">Toplantıyı bot yönetir, son metni sen netleştirirsin.</h1>
          <p className="nt-auth-desc">
            Participant registry, meeting audio, speaker binding ve inline review tek akışta.
          </p>
          <div className="nt-auth-tabs">
            <button
              className={mode === "login" ? "is-active" : ""}
              onClick={() => setMode("login")}
              type="button"
            >
              Giriş yap
            </button>
            <button
              className={mode === "register" ? "is-active" : ""}
              onClick={() => setMode("register")}
              type="button"
            >
              Kayıt ol
            </button>
          </div>
          <form className="nt-form" onSubmit={form.handleSubmit(onSubmit)}>
            <label className="nt-field">
              <span>E-posta</span>
              <input
                autoComplete="email"
                className="nt-input"
                placeholder="ornek@alan.com"
                {...form.register("email")}
              />
              {form.formState.errors.email ? (
                <small>{form.formState.errors.email.message}</small>
              ) : null}
            </label>

            {activeMutation.error ? (
              <div className="nt-alert">
                {activeMutation.error.message}
              </div>
            ) : null}

            <button className="nt-btn nt-btn-primary nt-auth-btn" disabled={activeMutation.isPending} type="submit">
              {activeMutation.isPending
                ? "İşleniyor"
                : mode === "login"
                  ? "Devam et"
                  : "Hesabı oluştur"}
            </button>
          </form>
          <p className="nt-auth-footnote">
            İlk sürümde parola yok. Aynı e-postayla tekrar giriş yaparak oturumu devam ettirebilirsin.
          </p>
        </section>
      </main>
    </div>
  );
}
