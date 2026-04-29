<template>
  <section class="dashboard-page">
    <section class="hero-banner">
      <div class="hero-banner-copy">
        <p class="eyebrow">Operations Overview</p>
        <h3>项目阶段查询与监控</h3>
        <p>
          这一版首页按现代管理后台的方式组织信息：先看总体指标，再通过筛选工作台收敛范围，
          最后在结果区切换卡片视图和表格视图，并通过详情抽屉查看完整阶段明细。
        </p>
      </div>

      <div class="hero-banner-side">
        <div class="identity-card" :class="{ danger: authError, muted: !authStatus?.authenticated }">
          <span class="identity-label">当前会话</span>
          <strong>{{ authTitle }}</strong>
          <p>{{ authSubtitle }}</p>
        </div>

        <div class="hero-banner-actions">
          <button class="button secondary" type="button" @click="refreshAuth" :disabled="loadingAuth">
            {{ loadingAuth ? "读取中..." : "刷新登录态" }}
          </button>
          <button class="button primary" type="button" @click="runQuery" :disabled="loadingQuery || !authStatus?.authenticated">
            {{ loadingQuery ? "查询中..." : "立即查询" }}
          </button>
        </div>
      </div>
    </section>

    <section class="metrics-strip">
      <article class="metric-card metric-card-accent">
        <span>项目总数</span>
        <strong>{{ summary?.groups.length ?? 0 }}</strong>
        <small>当前筛选条件下命中的项目数</small>
      </article>
      <article class="metric-card">
        <span>阶段记录</span>
        <strong>{{ summary?.records.length ?? 0 }}</strong>
        <small>查询返回的阶段明细数量</small>
      </article>
      <article class="metric-card">
        <span>启用服务器</span>
        <strong>{{ summary?.enabledServers ?? 0 }}</strong>
        <small>本次查询参与的服务器数</small>
      </article>
      <article class="metric-card">
        <span>匹配业务库</span>
        <strong>{{ summary?.matchedDatabases ?? 0 }}</strong>
        <small>命中的数据库数量</small>
      </article>
      <article class="metric-card">
        <span>正在进行</span>
        <strong>{{ summary?.ongoingCount ?? 0 }}</strong>
        <small>优先关注的活动项目</small>
      </article>
      <article class="metric-card">
        <span>即将开始</span>
        <strong>{{ summary?.upcomingCount ?? 0 }}</strong>
        <small>临近开始的项目数量</small>
      </article>
    </section>

    <section class="active-filters-bar">
      <div class="active-filters-copy">
        <p class="panel-label">Active Filters</p>
        <h4>当前筛选条件</h4>
      </div>
      <div class="active-filters-list">
        <span v-for="chip in activeFilterChips" :key="chip" class="filter-chip">{{ chip }}</span>
        <span v-if="activeFilterChips.length === 0" class="filter-chip filter-chip-muted">默认条件</span>
      </div>
      <button class="button secondary active-filters-clear" type="button" @click="resetFilters">一键清空</button>
    </section>

    <section class="dashboard-layout">
      <aside class="workbench-panel">
        <div class="workbench-panel-head">
          <div>
            <p class="panel-label">Workbench</p>
            <h4>筛选工作台</h4>
          </div>
          <span class="soft-badge">{{ summaryStatusLine }}</span>
        </div>

        <div class="workbench-group">
          <p class="workbench-group-title">关键字</p>
          <label class="field">
            <span>项目关键字</span>
            <input v-model.trim="filters.projectKeyword" type="text" placeholder="项目名称 / 模糊匹配" />
          </label>
          <label class="field">
            <span>数据库关键字</span>
            <input v-model.trim="filters.databaseKeyword" type="text" placeholder="数据库名 / 模糊匹配" />
          </label>
          <label class="field">
            <span>考试代码</span>
            <input v-model.trim="filters.examCodeKeyword" type="text" placeholder="examCode / 模糊匹配" />
          </label>
        </div>

        <div class="workbench-group">
          <p class="workbench-group-title">状态筛选</p>
          <div class="pill-grid">
            <label
              v-for="item in statusOptions"
              :key="item.value"
              class="toggle-pill"
              :class="{ active: filters.statusFilters.includes(item.value) }"
            >
              <input v-model="filters.statusFilters" type="checkbox" :value="item.value" />
              <span>{{ item.label }}</span>
            </label>
          </div>
        </div>

        <div class="workbench-group">
          <p class="workbench-group-title">服务器范围</p>
          <div v-if="serverOptions.length > 0" class="pill-grid">
            <label
              v-for="serverName in serverOptions"
              :key="serverName"
              class="toggle-pill"
              :class="{ active: filters.serverNames.includes(serverName) }"
            >
              <input v-model="filters.serverNames" type="checkbox" :value="serverName" />
              <span>{{ serverName }}</span>
            </label>
          </div>
          <div v-else class="empty-inline">先查询一次后，这里会出现服务器筛选项。</div>
        </div>

        <div class="workbench-footer">
          <div class="toolbar-copy">
            <strong>执行状态</strong>
            <span>{{ queryMessage }}</span>
          </div>
          <div class="toolbar-actions toolbar-actions-column">
            <button class="button secondary" type="button" @click="resetFilters">重置条件</button>
            <button class="button primary" type="button" @click="runQuery" :disabled="loadingQuery || !authStatus?.authenticated">
              {{ loadingQuery ? "查询中..." : "应用筛选" }}
            </button>
          </div>
        </div>
      </aside>

      <section class="result-stack">
        <article class="result-panel status-panel">
          <div class="panel-head">
            <div>
              <p class="panel-label">Status Snapshot</p>
              <h4>阶段状态分布</h4>
            </div>
          </div>

          <div class="summary-mini-grid summary-mini-grid-inline">
            <div class="summary-mini-card">
              <span>正在进行</span>
              <strong>{{ summary?.ongoingCount ?? 0 }}</strong>
            </div>
            <div class="summary-mini-card">
              <span>即将开始</span>
              <strong>{{ summary?.upcomingCount ?? 0 }}</strong>
            </div>
            <div class="summary-mini-card">
              <span>已结束</span>
              <strong>{{ summary?.endedCount ?? 0 }}</strong>
            </div>
          </div>

          <div class="insight-strip">
            <div class="insight-card">
              <span>主要关注</span>
              <strong>{{ primaryFocusLabel }}</strong>
            </div>
            <div class="insight-card">
              <span>服务器覆盖</span>
              <strong>{{ serverCoverageLabel }}</strong>
            </div>
            <div class="insight-card">
              <span>查询条件</span>
              <strong>{{ activeFilterSummary }}</strong>
            </div>
          </div>
        </article>

        <article class="result-panel">
          <div class="panel-head">
            <div>
              <p class="panel-label">Project Feed</p>
              <h4>项目看板</h4>
            </div>
            <div class="panel-head-actions">
              <div class="view-toggle">
                <button class="view-toggle-btn" :class="{ active: viewMode === 'cards' }" type="button" @click="viewMode = 'cards'">
                  卡片视图
                </button>
                <button class="view-toggle-btn" :class="{ active: viewMode === 'table' }" type="button" @click="viewMode = 'table'">
                  表格视图
                </button>
              </div>
              <div v-if="viewMode === 'table'" class="table-sorter">
                <label>
                  <span>排序</span>
                  <select v-model="tableSortKey">
                    <option value="projectName">项目名称</option>
                    <option value="serverName">服务器</option>
                    <option value="registrationCount">报名人数</option>
                    <option value="admissionTicketCount">准考证人数</option>
                    <option value="stageCount">阶段数量</option>
                  </select>
                </label>
                <button class="table-sort-order" type="button" @click="toggleSortOrder">
                  {{ tableSortOrder === "asc" ? "升序" : "降序" }}
                </button>
              </div>
              <span class="soft-badge">{{ sortedGroups.length }} 个项目</span>
            </div>
          </div>

          <div v-if="sortedGroups.length > 0 && viewMode === 'cards'" class="project-grid">
            <article v-for="group in sortedGroups" :key="buildGroupKey(group)" class="project-card project-card-modern">
              <div class="project-card-head">
                <div>
                  <h5>{{ group.projectName || "未命名项目" }}</h5>
                </div>
                <div class="status-badges">
                  <span v-for="status in group.statuses" :key="status" class="soft-badge">{{ status }}</span>
                </div>
              </div>

              <div class="project-card-meta">
                <div>
                  <p>{{ group.serverName }} / {{ group.databaseName }}</p>
                  <small>考试代码：{{ group.examCode || "-" }}</small>
                </div>
              </div>

              <div class="project-kpis">
                <div>
                  <span>报名</span>
                  <strong>{{ group.registrationCount || 0 }}</strong>
                </div>
                <div>
                  <span>准考证</span>
                  <strong>{{ group.admissionTicketCount || 0 }}</strong>
                </div>
                <div>
                  <span>阶段</span>
                  <strong>{{ group.stages?.length || 0 }}</strong>
                </div>
              </div>

              <ul class="stage-list">
                <li v-for="stage in group.stages.slice(0, 4)" :key="`${stage.stageName}-${stage.startTime}`">
                  <div class="stage-list-title">
                    <strong>{{ stage.stageName }}</strong>
                    <span class="stage-status">{{ stage.status }}</span>
                  </div>
                  <span>{{ formatDateTime(stage.startTime) }} - {{ formatDateTime(stage.endTime) }}</span>
                </li>
              </ul>

              <button class="button secondary project-open-btn" type="button" @click="openDrawer(group)">查看项目详情</button>
            </article>
          </div>

          <div v-else-if="sortedGroups.length > 0" class="table-shell">
            <table class="project-table">
              <thead>
                <tr>
                  <th class="col-project">项目</th>
                  <th class="col-server">服务器 / 数据库</th>
                  <th class="col-code">考试代码</th>
                  <th class="col-status">状态</th>
                  <th class="col-number">报名</th>
                  <th class="col-number">准考证</th>
                  <th class="col-number">阶段数</th>
                  <th class="col-action">操作</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="group in sortedGroups" :key="buildGroupKey(group)">
                  <td>
                    <div class="table-title-cell">
                      <strong>{{ group.projectName || "未命名项目" }}</strong>
                    </div>
                  </td>
                  <td>{{ group.serverName }} / {{ group.databaseName }}</td>
                  <td>{{ group.examCode || "-" }}</td>
                  <td>
                    <div class="table-status-list">
                      <span v-for="status in group.statuses" :key="status" class="soft-badge">{{ status }}</span>
                    </div>
                  </td>
                  <td>{{ group.registrationCount || 0 }}</td>
                  <td>{{ group.admissionTicketCount || 0 }}</td>
                  <td>{{ group.stages?.length || 0 }}</td>
                  <td>
                    <button class="table-open-btn" type="button" @click="openDrawer(group)">详情</button>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <div v-else class="empty-state">
            {{ summary ? "当前筛选条件下没有结果。" : "先点“立即查询”加载数据。" }}
          </div>
        </article>
      </section>
    </section>

    <div v-if="selectedGroup" class="drawer-backdrop" @click="closeDrawer"></div>
    <aside class="detail-drawer" :class="{ open: !!selectedGroup }">
      <div v-if="selectedGroup" class="detail-drawer-inner">
        <div class="detail-drawer-head">
          <div>
            <p class="panel-label">Project Detail</p>
            <h4>{{ selectedGroup.projectName || "未命名项目" }}</h4>
            <p class="detail-drawer-meta">
              {{ selectedGroup.serverName }} / {{ selectedGroup.databaseName }} / {{ selectedGroup.examCode || "-" }}
            </p>
          </div>
          <div class="drawer-head-actions">
            <button class="drawer-nav-btn" type="button" @click="openPreviousGroup" :disabled="!hasPreviousGroup">上一项目</button>
            <button class="drawer-nav-btn" type="button" @click="openNextGroup" :disabled="!hasNextGroup">下一项目</button>
            <button class="drawer-close-btn" type="button" @click="closeDrawer">关闭</button>
          </div>
        </div>

        <div class="detail-kpis">
          <div>
            <span>报名人数</span>
            <strong>{{ selectedGroup.registrationCount || 0 }}</strong>
          </div>
          <div>
            <span>准考证人数</span>
            <strong>{{ selectedGroup.admissionTicketCount || 0 }}</strong>
          </div>
          <div>
            <span>阶段数量</span>
            <strong>{{ selectedGroup.stages?.length || 0 }}</strong>
          </div>
        </div>

        <div class="detail-section">
          <div class="detail-section-head detail-section-head-actions">
            <h5>快捷操作</h5>
            <div class="drawer-quick-actions">
              <button class="drawer-nav-btn" type="button" @click="copySelectedProjectInfo">复制项目信息</button>
              <button class="drawer-nav-btn" type="button" @click="exportSelectedProject">导出当前项目</button>
            </div>
          </div>
          <div class="empty-inline">可快速复制当前项目关键信息，或将当前项目阶段明细导出为 CSV 文件。</div>
        </div>

        <div class="detail-section">
          <div class="detail-section-head">
            <h5>当前状态</h5>
          </div>
          <div class="status-badges">
            <span v-for="status in selectedGroup.statuses" :key="status" class="soft-badge">{{ status }}</span>
          </div>
        </div>

        <div class="detail-section">
          <div class="detail-section-head">
            <h5>阶段时间线</h5>
          </div>
          <ul class="timeline-list">
            <li v-for="stage in selectedGroup.stages" :key="`${stage.stageName}-${stage.startTime}-${stage.endTime}`">
              <div class="timeline-dot"></div>
              <div class="timeline-card">
                <div class="timeline-head">
                  <strong>{{ stage.stageName }}</strong>
                  <span class="stage-status">{{ stage.status }}</span>
                </div>
                <div class="timeline-time">{{ formatDateTime(stage.startTime) }} - {{ formatDateTime(stage.endTime) }}</div>
                <div class="timeline-meta">
                  <span>报名：{{ stage.registrationCount || 0 }}</span>
                  <span>准考证：{{ stage.admissionTicketCount || 0 }}</span>
                </div>
              </div>
            </li>
          </ul>
        </div>
      </div>
    </aside>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import type { AuthStatus, ProjectStageGroup, ProjectStageQueryRequest, ProjectStageSummary } from "@/services/api";
import { fetchAuthStatus, queryProjectStages } from "@/services/api";

const statusOptions = [
  { label: "正在进行", value: "正在进行" },
  { label: "即将开始", value: "即将开始" },
  { label: "已结束", value: "已结束" }
];

const loadingAuth = ref(false);
const loadingQuery = ref(false);
const authStatus = ref<AuthStatus | null>(null);
const authError = ref("");
const queryMessage = ref("读取登录态后即可查询。");
const summary = ref<ProjectStageSummary | null>(null);
const serverOptions = ref<string[]>([]);
const viewMode = ref<"cards" | "table">("cards");
const selectedGroup = ref<ProjectStageGroup | null>(null);
const tableSortKey = ref<"projectName" | "serverName" | "registrationCount" | "admissionTicketCount" | "stageCount">("projectName");
const tableSortOrder = ref<"asc" | "desc">("asc");

const filters = reactive({
  statusFilters: ["正在进行", "即将开始"] as string[],
  serverNames: [] as string[],
  projectKeyword: "",
  databaseKeyword: "",
  examCodeKeyword: ""
});

const displayGroups = computed(() => summary.value?.groups ?? []);
const sortedGroups = computed(() => {
  const groups = [...displayGroups.value];
  const factor = tableSortOrder.value === "asc" ? 1 : -1;

  return groups.sort((a, b) => {
    switch (tableSortKey.value) {
      case "serverName":
        return a.serverName.localeCompare(b.serverName, "zh-CN") * factor;
      case "registrationCount":
        return ((a.registrationCount || 0) - (b.registrationCount || 0)) * factor;
      case "admissionTicketCount":
        return ((a.admissionTicketCount || 0) - (b.admissionTicketCount || 0)) * factor;
      case "stageCount":
        return ((a.stages?.length || 0) - (b.stages?.length || 0)) * factor;
      case "projectName":
      default:
        return (a.projectName || "").localeCompare(b.projectName || "", "zh-CN") * factor;
    }
  });
});

const selectedGroupIndex = computed(() => {
  if (!selectedGroup.value) {
    return -1;
  }

  return sortedGroups.value.findIndex((group) => buildGroupKey(group) === buildGroupKey(selectedGroup.value!));
});

const hasPreviousGroup = computed(() => selectedGroupIndex.value > 0);
const hasNextGroup = computed(() => selectedGroupIndex.value >= 0 && selectedGroupIndex.value < sortedGroups.value.length - 1);

const authTitle = computed(() => {
  if (authError.value) return "登录态读取失败";
  if (!authStatus.value?.authenticated) return "当前未登录";
  return `${authStatus.value.username ?? "未知用户"} 已登录`;
});

const authSubtitle = computed(() => {
  if (authError.value) return authError.value;
  if (!authStatus.value?.authenticated) return "请先在旧版页面登录，再回来用新前端。";

  const roleLabel =
    authStatus.value.role === "admin"
      ? "管理员"
      : authStatus.value.role === "internal"
        ? "内部用户"
        : "外部用户";

  return `${roleLabel}${authStatus.value.forcePasswordChange ? "，首次登录需修改密码" : ""}`;
});

const summaryStatusLine = computed(() => {
  if (!summary.value) return "未查询";
  return `服务器 ${summary.value.enabledServers} 台 · 数据库 ${summary.value.matchedDatabases} 个`;
});

const primaryFocusLabel = computed(() => {
  if (!summary.value) return "等待查询";
  if ((summary.value.ongoingCount ?? 0) > 0) return `${summary.value.ongoingCount} 个项目正在进行`;
  if ((summary.value.upcomingCount ?? 0) > 0) return `${summary.value.upcomingCount} 个项目即将开始`;
  return "当前结果以已结束项目为主";
});

const serverCoverageLabel = computed(() => {
  if (!summary.value) return "尚未统计";
  return `${summary.value.enabledServers} 台服务器 / ${summary.value.matchedDatabases} 个业务库`;
});

const activeFilterSummary = computed(() => {
  const parts: string[] = [];
  if (filters.projectKeyword) parts.push(`项目:${filters.projectKeyword}`);
  if (filters.databaseKeyword) parts.push(`数据库:${filters.databaseKeyword}`);
  if (filters.examCodeKeyword) parts.push(`考试代码:${filters.examCodeKeyword}`);
  if (filters.serverNames.length > 0) parts.push(`服务器:${filters.serverNames.length}项`);
  if (filters.statusFilters.length > 0) parts.push(`状态:${filters.statusFilters.join("/")}`);
  return parts.length > 0 ? parts.join(" · ") : "当前为默认查询条件";
});

const activeFilterChips = computed(() => {
  const chips: string[] = [];
  if (filters.projectKeyword) chips.push(`项目：${filters.projectKeyword}`);
  if (filters.databaseKeyword) chips.push(`数据库：${filters.databaseKeyword}`);
  if (filters.examCodeKeyword) chips.push(`考试代码：${filters.examCodeKeyword}`);
  if (filters.serverNames.length > 0) chips.push(`服务器 ${filters.serverNames.length} 项`);
  if (filters.statusFilters.length > 0) chips.push(`状态 ${filters.statusFilters.join(" / ")}`);
  return chips;
});

function createPayload(): ProjectStageQueryRequest {
  return {
    servers: [],
    statusFilters: [...filters.statusFilters],
    timeMatchMode: "overlap",
    stageKeyword: "",
    stageNames: [],
    serverNames: [...filters.serverNames],
    projectKeyword: filters.projectKeyword,
    serverKeyword: "",
    databaseKeyword: filters.databaseKeyword,
    examCodeKeyword: filters.examCodeKeyword,
    rangeStart: null,
    rangeEnd: null,
    dayOffsets: []
  };
}

function collectServerOptions(groups: ProjectStageGroup[]): string[] {
  return [...new Set(groups.map((item) => item.serverName?.trim()).filter(Boolean))]
    .sort((a, b) => a.localeCompare(b, "zh-CN"));
}

function formatDateTime(value: string): string {
  if (!value) return "-";
  return value.replace("T", " ").slice(0, 16);
}

function buildGroupKey(group: ProjectStageGroup): string {
  return `${group.serverName}|${group.databaseName}|${group.examCode}`;
}

function openDrawer(group: ProjectStageGroup) {
  selectedGroup.value = group;
}

function closeDrawer() {
  selectedGroup.value = null;
}

function openPreviousGroup() {
  if (selectedGroupIndex.value > 0) {
    selectedGroup.value = sortedGroups.value[selectedGroupIndex.value - 1] ?? null;
  }
}

function openNextGroup() {
  if (selectedGroupIndex.value >= 0 && selectedGroupIndex.value < sortedGroups.value.length - 1) {
    selectedGroup.value = sortedGroups.value[selectedGroupIndex.value + 1] ?? null;
  }
}

function toggleSortOrder() {
  tableSortOrder.value = tableSortOrder.value === "asc" ? "desc" : "asc";
}

async function copySelectedProjectInfo() {
  if (!selectedGroup.value) return;

  const group = selectedGroup.value;
  const text = [
    `项目名称：${group.projectName || "未命名项目"}`,
    `服务器：${group.serverName}`,
    `数据库：${group.databaseName}`,
    `考试代码：${group.examCode || "-"}`,
    `状态：${group.statuses.join(" / ")}`,
    `报名人数：${group.registrationCount || 0}`,
    `准考证人数：${group.admissionTicketCount || 0}`,
    "阶段：",
    ...group.stages.map((stage) => `- ${stage.stageName} | ${stage.status} | ${formatDateTime(stage.startTime)} - ${formatDateTime(stage.endTime)}`)
  ].join("\n");

  try {
    await navigator.clipboard.writeText(text);
    queryMessage.value = "已复制当前项目信息。";
  } catch {
    queryMessage.value = "复制失败，请检查浏览器权限。";
  }
}

function exportSelectedProject() {
  if (!selectedGroup.value) return;

  const group = selectedGroup.value;
  const rows = [
    ["项目名称", "服务器", "数据库", "考试代码", "阶段名称", "状态", "开始时间", "结束时间", "报名人数", "准考证人数"],
    ...group.stages.map((stage) => [
      csvEscape(group.projectName || "未命名项目"),
      csvEscape(group.serverName),
      csvEscape(group.databaseName),
      csvEscape(group.examCode || "-"),
      csvEscape(stage.stageName),
      csvEscape(stage.status),
      csvEscape(formatDateTime(stage.startTime)),
      csvEscape(formatDateTime(stage.endTime)),
      String(stage.registrationCount || 0),
      String(stage.admissionTicketCount || 0)
    ])
  ];

  const csvContent = rows.map((row) => row.join(",")).join("\r\n");
  const blob = new Blob(["\uFEFF" + csvContent], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `${sanitizeFilename(group.projectName || "project-detail")}.csv`;
  link.click();
  URL.revokeObjectURL(url);
  queryMessage.value = "已导出当前项目 CSV。";
}

function csvEscape(value: string): string {
  const escaped = value.replaceAll("\"", "\"\"");
  return `"${escaped}"`;
}

function sanitizeFilename(value: string): string {
  return value.replace(/[\\/:*?"<>|]/g, "_");
}

async function refreshAuth() {
  loadingAuth.value = true;
  authError.value = "";

  try {
    authStatus.value = await fetchAuthStatus();
    queryMessage.value = authStatus.value.authenticated
      ? "已读取登录态，可以开始查询。"
      : "当前还没登录，请先在旧版页面登录。";
  } catch (error) {
    authError.value = error instanceof Error ? error.message : "auth status failed";
    authStatus.value = null;
    queryMessage.value = "登录态读取失败，请先确认旧版站点可登录。";
  } finally {
    loadingAuth.value = false;
  }
}

async function runQuery() {
  if (!authStatus.value?.authenticated) {
    queryMessage.value = "请先在旧版页面登录。";
    return;
  }

  loadingQuery.value = true;
  queryMessage.value = "正在查询项目数据...";

  try {
    const result = await queryProjectStages(createPayload());
    summary.value = result;
    serverOptions.value = collectServerOptions(result.groups);
    if (selectedGroup.value) {
      const current = result.groups.find((group) => buildGroupKey(group) === buildGroupKey(selectedGroup.value!));
      selectedGroup.value = current ?? null;
    }
    queryMessage.value = `查询完成：项目 ${result.groups.length} 个，阶段 ${result.records.length} 条。`;
  } catch (error) {
    queryMessage.value = error instanceof Error ? error.message : "query failed";
  } finally {
    loadingQuery.value = false;
  }
}

function resetFilters() {
  filters.statusFilters = ["正在进行", "即将开始"];
  filters.serverNames = [];
  filters.projectKeyword = "";
  filters.databaseKeyword = "";
  filters.examCodeKeyword = "";
  selectedGroup.value = null;
  queryMessage.value = "筛选条件已重置。";
}

onMounted(async () => {
  await refreshAuth();
  if (authStatus.value?.authenticated) {
    await runQuery();
  }
});
</script>
