<template>
  <section class="page">
    <header class="subhero">
      <p class="eyebrow">Migration Playground</p>
      <h2>新前端试验区</h2>
      <p class="hero-copy">
        这里先做低风险组件实验，确认视觉、交互和数据流，再决定替换旧版哪个模块。
      </p>
    </header>

    <div class="playground-grid">
      <article class="panel accent">
        <p class="panel-label">建议顺序</p>
        <ol class="ordered">
          <li>先抽登录态和用户信息。</li>
          <li>再抽筛选区和摘要卡片。</li>
          <li>最后迁主表格、分组详情和设置弹窗。</li>
        </ol>
      </article>

      <article class="panel">
        <p class="panel-label">当前接入方式</p>
        <ul class="plain-list">
          <li>开发环境通过 Vite 代理转发到 ASP.NET Core。</li>
          <li>生产构建默认输出到 <code>frontend/dist</code>。</li>
          <li>后端检测到构建产物后，会把它挂到 <code>/new</code>。</li>
        </ul>
      </article>
    </div>
  </section>
</template>
