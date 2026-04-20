<template>
  <div class="shell">
    <aside class="sidebar">
      <div class="brand">
        <span class="brand-mark">PS</span>
        <div>
          <p class="brand-kicker">Parallel Frontend</p>
          <h1>项目阶段汇总</h1>
        </div>
      </div>

      <nav class="nav">
        <RouterLink to="/">概览</RouterLink>
        <RouterLink to="/playground">迁移试验区</RouterLink>
      </nav>

      <div class="legacy-card">
        <p class="legacy-title">旧版入口保留中</p>
        <a href="/" target="_blank" rel="noreferrer">打开当前生产版</a>
        <a href="/manage.html" target="_blank" rel="noreferrer">打开管理台</a>
      </div>
    </aside>

    <main class="content">
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
import { RouterLink, RouterView } from "vue-router";
</script>
