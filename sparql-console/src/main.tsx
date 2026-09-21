import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Route, Routes } from "react-router";
import { AuthProvider } from "react-oidc-context";
import { oidcConfig, onSigninCallback } from "./auth";
import { Shell } from "./components/Shell";
import { QueryPage } from "./routes/QueryPage";
import { CallbackPage } from "./routes/CallbackPage";
import { AboutPage } from "./routes/AboutPage";

import "normalize.css";
import "@blueprintjs/core/lib/css/blueprint.css";
import "@blueprintjs/icons/lib/css/blueprint-icons.css";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <AuthProvider {...oidcConfig} onSigninCallback={onSigninCallback}>
      <BrowserRouter>
        <Routes>
          <Route element={<Shell />}>
            <Route index element={<QueryPage />} />
            <Route path="auth/callback" element={<CallbackPage />} />
            <Route path="about" element={<AboutPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  </StrictMode>,
);
