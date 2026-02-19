const level = (process.env.DOCKER_TEST_LEVEL ?? "all").toLowerCase();

export function isSmokeEnabled(): boolean {
  return level === "all" || level === "smoke";
}

export function isFullEnabled(): boolean {
  return level === "all" || level === "full";
}

