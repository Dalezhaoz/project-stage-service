import { createRouter, createWebHistory } from "vue-router";
import OverviewPage from "./views/OverviewPage.vue";
import PlaygroundPage from "./views/PlaygroundPage.vue";
import LoginPage from "./views/LoginPage.vue";
import ChangePasswordPage from "./views/ChangePasswordPage.vue";
import { useAuthState } from "./stores/auth";

function resolveRuntimeBase(): string {
  if (typeof window === "undefined") {
    return import.meta.env.BASE_URL;
  }

  const marker = "/new";
  const pathname = window.location.pathname.replace(/\/+$/, "");
  const markerIndex = pathname.indexOf(marker);

  if (markerIndex >= 0) {
    return `${pathname.slice(0, markerIndex + marker.length)}/`;
  }

  return import.meta.env.BASE_URL;
}

const router = createRouter({
  history: createWebHistory(resolveRuntimeBase()),
  routes: [
    {
      path: "/login",
      component: LoginPage,
      meta: { guestOnly: true }
    },
    {
      path: "/change-password",
      component: ChangePasswordPage,
      meta: { requiresAuth: true, passwordChangeOnly: true }
    },
    {
      path: "/",
      component: OverviewPage,
      meta: { requiresAuth: true }
    },
    {
      path: "/playground",
      component: PlaygroundPage,
      meta: { requiresAuth: true }
    }
  ]
});

router.beforeEach(async (to) => {
  const auth = useAuthState();
  const status = await auth.ensureAuthStatus();
  const authenticated = status?.authenticated === true;
  const needsPasswordChange = status?.forcePasswordChange === true;
  const hasAccount = status?.hasAccount === true;

  if (!hasAccount && to.path !== "/login") {
    return "/login";
  }

  if (to.meta.requiresAuth && !authenticated) {
    return "/login";
  }

  if (to.meta.guestOnly && authenticated && !needsPasswordChange) {
    return "/";
  }

  if (authenticated && needsPasswordChange && to.path !== "/change-password") {
    return "/change-password";
  }

  if (to.meta.passwordChangeOnly && !needsPasswordChange) {
    return authenticated ? "/" : "/login";
  }

  return true;
});

export default router;
