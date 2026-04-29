export type ApiHealth = {
  status?: string;
};

export type AuthStatus = {
  authenticated: boolean;
  hasAccount: boolean;
  username: string | null;
  role: string | null;
  isAdmin: boolean;
  forcePasswordChange: boolean;
  allowUserRefresh: boolean;
  canAssign: boolean;
};

export type LoginPayload = {
  username: string;
  password: string;
};

export type SetupPayload = {
  username: string;
  password: string;
  confirmPassword: string;
};

export type ChangePasswordPayload = {
  currentPassword: string;
  newPassword: string;
  confirmPassword: string;
};

export type ProjectStageRecord = {
  serverName: string;
  databaseName: string;
  examCode: string;
  projectName: string;
  stageName: string;
  startTime: string;
  endTime: string;
  status: string;
  registrationCount: number;
  admissionTicketCount: number;
};

export type ProjectStageGroup = {
  serverName: string;
  databaseName: string;
  examCode: string;
  projectName: string;
  startTime: string;
  endTime: string;
  registrationCount: number;
  admissionTicketCount: number;
  statuses: string[];
  stages: ProjectStageRecord[];
};

export type ProjectStageSummary = {
  records: ProjectStageRecord[];
  groups: ProjectStageGroup[];
  enabledServers: number;
  visitedDatabases: number;
  matchedDatabases: number;
  endedCount: number;
  ongoingCount: number;
  upcomingCount: number;
};

export type ProjectStageQueryRequest = {
  servers: unknown[];
  statusFilters: string[];
  timeMatchMode: string;
  stageKeyword: string;
  stageNames: string[];
  serverNames: string[];
  projectKeyword: string;
  serverKeyword: string;
  databaseKeyword: string;
  examCodeKeyword: string;
  rangeStart: string | null;
  rangeEnd: string | null;
  dayOffsets: number[];
};

function resolveAppRoot(): string {
  if (typeof window === "undefined") {
    return "/";
  }

  const marker = "/new";
  const pathname = window.location.pathname.replace(/\/+$/, "");
  const markerIndex = pathname.indexOf(marker);

  if (markerIndex >= 0) {
    const root = pathname.slice(0, markerIndex);
    return root.length > 0 ? `${root}/` : "/";
  }

  return "/";
}

function buildAppUrl(path: string): string {
  const normalizedPath = path.replace(/^\/+/, "");
  return new URL(normalizedPath, window.location.origin + resolveAppRoot()).toString();
}

async function ensureOk(response: Response, fallback: string): Promise<Response> {
  if (response.ok) {
    return response;
  }

  const payload = await response.json().catch(() => ({}));
  const detail =
    typeof payload?.detail === "string" && payload.detail.trim().length > 0
      ? payload.detail
      : fallback;

  throw new Error(detail);
}

async function postJson<TInput, TOutput>(path: string, payload: TInput): Promise<TOutput> {
  const response = await fetch(buildAppUrl(path), {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    credentials: "include",
    body: JSON.stringify(payload)
  });

  await ensureOk(response, `${path} failed`);
  return response.json() as Promise<TOutput>;
}

export async function fetchHealth(): Promise<ApiHealth> {
  const response = await fetch(buildAppUrl("health"), {
    credentials: "include"
  });

  await ensureOk(response, "health request failed");
  return response.json() as Promise<ApiHealth>;
}

export async function fetchAuthStatus(): Promise<AuthStatus> {
  const response = await fetch(buildAppUrl("api/auth/status"), {
    credentials: "include"
  });

  await ensureOk(response, "auth status failed");
  return response.json() as Promise<AuthStatus>;
}

export async function login(payload: LoginPayload): Promise<{ username: string }> {
  return postJson<LoginPayload, { username: string }>("api/auth/login", payload);
}

export async function setupAccount(payload: SetupPayload): Promise<{ username: string }> {
  return postJson<SetupPayload, { username: string }>("api/auth/setup", payload);
}

export async function logout(): Promise<{ ok?: boolean }> {
  return postJson<Record<string, never>, { ok?: boolean }>("api/auth/logout", {});
}

export async function changePassword(payload: ChangePasswordPayload): Promise<{ ok: boolean }> {
  return postJson<ChangePasswordPayload, { ok: boolean }>("api/auth/change-password", payload);
}

export async function queryProjectStages(payload: ProjectStageQueryRequest): Promise<ProjectStageSummary> {
  return postJson<ProjectStageQueryRequest, ProjectStageSummary>("api/query", payload);
}
