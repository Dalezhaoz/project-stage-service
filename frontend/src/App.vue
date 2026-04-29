<template>
  <div class="app-shell">
    <div class="app-main">
      <header class="app-topbar">
        <div>
          <p class="topbar-kicker">Modern Admin Console</p>
          <h2>项目阶段汇总服务</h2>
        </div>
        <div class="topbar-meta">
          <span>并行前端</span>
          <span>Vue 3 + Vite + TypeScript</span>
          <button v-if="auth.authStatus.value?.authenticated" class="topbar-logout" type="button" @click="handleLogout">
            退出登录
          </button>
        </div>
      </header>

      <main class="app-content">
        <RouterView />
      </main>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useRouter } from "vue-router";
import { RouterView } from "vue-router";
import { useAuthState } from "./stores/auth";

const router = useRouter();
const auth = useAuthState();

async function handleLogout() {
  await auth.logoutCurrentUser();
  await router.replace("/login");
}
</script>
