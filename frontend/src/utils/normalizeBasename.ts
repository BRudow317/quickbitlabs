// normalizes local paths for the router basename, ensuring it works correctly whether it's "/" or "/repo-name/"
export function normalizeBasename(baseUrl: string): string {
  // Vite sets import.meta.env.BASE_URL like "/" or "/repo-name/"
  if (!baseUrl) return "";
  const trimmed = baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
  return trimmed === "/" ? "" : trimmed;
}