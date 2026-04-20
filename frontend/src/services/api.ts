export type ApiHealth = {
  status?: string;
};

export async function fetchHealth(): Promise<ApiHealth> {
  const response = await fetch("/health", {
    credentials: "include"
  });

  if (!response.ok) {
    throw new Error(`health request failed: ${response.status}`);
  }

  return response.json() as Promise<ApiHealth>;
}

export async function fetchAuthStatus(): Promise<unknown> {
  const response = await fetch("/api/auth/status", {
    credentials: "include"
  });

  if (!response.ok) {
    throw new Error(`auth status failed: ${response.status}`);
  }

  return response.json() as Promise<unknown>;
}
