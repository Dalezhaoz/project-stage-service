import { computed, ref } from "vue";
import type { AuthStatus, ChangePasswordPayload, LoginPayload, SetupPayload } from "@/services/api";
import { changePassword, fetchAuthStatus, login, logout, setupAccount } from "@/services/api";

const authStatus = ref<AuthStatus | null>(null);
const loading = ref(false);
const initialized = ref(false);

export function useAuthState() {
  const isAuthenticated = computed(() => authStatus.value?.authenticated === true);
  const needsPasswordChange = computed(() => authStatus.value?.forcePasswordChange === true);
  const hasAccount = computed(() => authStatus.value?.hasAccount === true);

  async function refreshAuthStatus() {
    loading.value = true;

    try {
      authStatus.value = await fetchAuthStatus();
      initialized.value = true;
      return authStatus.value;
    } finally {
      loading.value = false;
    }
  }

  async function ensureAuthStatus() {
    if (!initialized.value) {
      return refreshAuthStatus();
    }

    return authStatus.value;
  }

  async function loginWithPassword(payload: LoginPayload) {
    await login(payload);
    return refreshAuthStatus();
  }

  async function initializeAccount(payload: SetupPayload) {
    await setupAccount(payload);
    return refreshAuthStatus();
  }

  async function changeOwnPassword(payload: ChangePasswordPayload) {
    await changePassword(payload);
    return refreshAuthStatus();
  }

  async function logoutCurrentUser() {
    await logout();
    authStatus.value = await fetchAuthStatus();
    initialized.value = true;
    return authStatus.value;
  }

  return {
    authStatus,
    loading,
    initialized,
    isAuthenticated,
    needsPasswordChange,
    hasAccount,
    refreshAuthStatus,
    ensureAuthStatus,
    loginWithPassword,
    initializeAccount,
    changeOwnPassword,
    logoutCurrentUser
  };
}
