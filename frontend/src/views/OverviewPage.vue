<template>
  <section class="page">
    <header class="hero">
      <div>
        <p class="eyebrow">Modern Frontend Sandbox</p>
        <h2>先并行，再迁移</h2>
        <p class="hero-copy">
          旧版 <code>wwwroot</code> 继续提供生产页面，新版前端在 <code>/new</code> 路径独立开发。
          这里先放基础路由、状态管理和 API 连通性，后面可以逐页把老页面功能迁过来。
        </p>
      </div>
      <div class="hero-actions">
        <a class="button primary" href="/" target="_blank" rel="noreferrer">查看旧版首页</a>
        <RouterLink class="button secondary" to="/playground">进入试验区</RouterLink>
      </div>
    </header>

    <div class="grid">
      <article class="panel">
        <p class="panel-label">后端健康检查</p>
        <h3>{{ healthLabel }}</h3>
        <p class="panel-copy">{{ healthHint }}</p>
        <button class="button secondary" type="button" @click="loadHealth" :disabled="loadingHealth">
          {{ loadingHealth ? "检测中..." : "重新检测" }}
        </button>
      </article>

      <article class="panel">
        <p class="panel-label">登录态 API</p>
        <h3>{{ authLabel }}</h3>
        <p class="panel-copy">{{ authHint }}</p>
        <button class="button secondary" type="button" @click="loadAuth" :disabled="loadingAuth">
          {{ loadingAuth ? "读取中..." : "读取状态" }}
        </button>
      </article>

      <article class="panel">
        <p class="panel-label">迁移建议</p>
        <h3>按页面切，不一次推翻</h3>
        <p class="panel-copy">
          先把公共壳子、登录态、筛选条件、看板卡片拆成组件，再迁复杂表格和配置弹窗。
        </p>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { RouterLink } from "vue-router";
import { fetchAuthStatus, fetchHealth } from "@/services/api";

const loadingHealth = ref(false);
const loadingAuth = ref(false);
const healthStatus = ref<string>("待检测");
const healthError = ref<string>("");
const authStatus = ref<string>("未读取");
const authError = ref<string>("");

const healthLabel = computed(() => healthError.value || healthStatus.value);
const authLabel = computed(() => authError.value || authStatus.value);
const healthHint = computed(() =>
  healthError.value
    ? "说明新前端还没连到后端，优先看 Vite 代理或 ASP.NET Core 是否启动。"
    : "这里验证 /health 是否可用，方便确认新旧前后端能并行工作。"
);
const authHint = computed(() =>
  authError.value
    ? "如果这里报错，通常是未登录或开发代理配置还没连上后端。"
    : "这里读取 /api/auth/status，确认 cookie 和 API 结构能被新前端复用。"
);

async function loadHealth() {
  loadingHealth.value = true;
  healthError.value = "";

  try {
    const result = await fetchHealth();
    healthStatus.value = result.status === "ok" ? "后端正常" : JSON.stringify(result);
  } catch (error) {
    healthError.value = error instanceof Error ? error.message : "health request failed";
  } finally {
    loadingHealth.value = false;
  }
}

async function loadAuth() {
  loadingAuth.value = true;
  authError.value = "";

  try {
    const result = await fetchAuthStatus();
    authStatus.value = JSON.stringify(result, null, 2);
  } catch (error) {
    authError.value = error instanceof Error ? error.message : "auth request failed";
  } finally {
    loadingAuth.value = false;
  }
}

onMounted(() => {
  void loadHealth();
});
</script>
