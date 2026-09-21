import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The dev server is bound to localhost on purpose, and so is the published container port.
// Signing in uses PKCE S256, which needs crypto.subtle, which browsers only expose in a secure
// context -- HTTPS, or plain HTTP on localhost. See index.html and docker/entrypoint.sh.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3002,
    strictPort: true,
  },
  preview: {
    port: 3002,
    strictPort: true,
  },

  css: {
    lightningcss: {
      // YASGUI bundles DataTables' stylesheet, which still carries an Internet Explorer star
      // hack (`*cursor:hand`). Modern browsers ignore it; LightningCSS, which Vite 8 minifies
      // with, refuses to parse it and fails the build. Recovering strips the offending
      // declarations and leaves the rest alone.
      errorRecovery: true,
    },
  },
});
