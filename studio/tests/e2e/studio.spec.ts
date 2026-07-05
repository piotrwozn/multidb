import { expect, test } from "@playwright/test";

import {
  auditFixture,
  authFixture,
  catalogFixture,
  adviceFixture,
  configFixture,
  domainsFixture,
  extensionsFixture,
  loginFixture,
  manifestFixture,
  migrationPlanFixture,
  profilesFixture,
  rolesFixture,
  securityFixture,
  statusFixture,
  validationFixture,
} from "../../src/test/fixtures";

const envelope = (data: unknown) => ({ ok: true, data });

test("operator can inspect Studio data, validate config and build a dry run", async ({
  page,
}) => {
  let tableDeleteConfirmed = false;
  let documentDeleteConfirmed = false;
  let securitySaved = false;
  let adviceRejected = false;
  const topologyValidationFixture = {
    ...validationFixture,
    issues: [
      {
        code: "TOPOLOGY_CP_CLUSTER_QUORUM",
        severity: "error",
        path: "topology.replica_count",
        message: "cluster + CP requires an odd replica quorum of at least 3",
        suggestion: "Use 3, 5, 7... replicas for CP clusters.",
      },
    ],
  };

  await page.route("**/api/health", async (route) => {
    await route.fulfill({ json: { ok: true, status: "healthy" } });
  });
  await page.route("**/api/ready", async (route) => {
    await route.fulfill({ json: { ok: true, status: "ready" } });
  });
  await page.route("**/api/status", async (route) => {
    await route.fulfill({ json: envelope(statusFixture) });
  });
  await page.route("**/api/auth/login", async (route) => {
    await route.fulfill({ json: envelope(loginFixture) });
  });
  await page.route("**/api/auth/logout", async (route) => {
    await route.fulfill({ json: envelope({}) });
  });
  await page.route("**/api/auth/me", async (route) => {
    await route.fulfill({ json: envelope(authFixture) });
  });
  await page.route("**/api/catalog", async (route) => {
    await route.fulfill({ json: envelope(catalogFixture) });
  });
  await page.route("**/api/security", async (route) => {
    if (route.request().method() === "POST") {
      securitySaved = true;
      await route.fulfill({ json: envelope(route.request().postDataJSON()) });
      return;
    }
    await route.fulfill({ json: envelope(securityFixture) });
  });
  await page.route("**/api/audit", async (route) => {
    await route.fulfill({ json: envelope(auditFixture) });
  });
  await page.route("**/api/config", async (route) => {
    if (route.request().method() === "GET") {
      await route.fulfill({ json: envelope(configFixture) });
      return;
    }
    await route.fallback();
  });
  await page.route("**/api/studio", async (route) => {
    await route.fulfill({ json: envelope(manifestFixture) });
  });
  await page.route("**/api/profiles", async (route) => {
    await route.fulfill({ json: envelope(profilesFixture) });
  });
  await page.route("**/api/roles", async (route) => {
    await route.fulfill({ json: envelope(rolesFixture) });
  });
  await page.route("**/api/domains", async (route) => {
    await route.fulfill({ json: envelope(domainsFixture) });
  });
  await page.route("**/api/extensions", async (route) => {
    await route.fulfill({ json: envelope(extensionsFixture) });
  });
  await page.route("**/api/advice", async (route) => {
    await route.fulfill({ json: envelope(adviceFixture) });
  });
  await page.route("**/api/advice/plan", async (route) => {
    await route.fulfill({ json: envelope(migrationPlanFixture) });
  });
  await page.route("**/api/advice/decision", async (route) => {
    const body = route.request().postDataJSON();
    adviceRejected = body?.status === "rejected" && body?.reason === "defer until low traffic";
    await route.fulfill({
      json: envelope({
        advice_id: body?.advice_id,
        status: body?.status,
        reason: body?.reason,
        decided_by: "admin",
        decided_at_millis: 1_720_000_000_500,
      }),
    });
  });
  await page.route("**/api/data/tables/users/rows**", async (route) => {
    if (route.request().method() === "DELETE") {
      const body = route.request().postDataJSON();
      tableDeleteConfirmed = body?.confirm === "users";
      await route.fulfill({ json: envelope({}) });
      return;
    }
    await route.fulfill({
      json: envelope({
        table: "users",
        schema: catalogFixture.objects[0].schema,
        rows: [[1, "Ada"]],
        offset: 0,
        limit: 100,
        returned: 1,
        has_more: false,
        next_offset: null,
        capped: false,
      }),
    });
  });
  await page.route("**/api/data/collections/profiles/documents**", async (route) => {
    if (route.request().method() === "DELETE") {
      const body = route.request().postDataJSON();
      documentDeleteConfirmed = body?.confirm === "doc-1";
      await route.fulfill({ json: envelope({}) });
      return;
    }
    await route.fulfill({
      json: envelope({
        collection: "profiles",
        documents: [{ id: "doc-1", document: { name: "Ada" } }],
        offset: 0,
        limit: 100,
        returned: 1,
        has_more: false,
        next_offset: null,
        capped: false,
      }),
    });
  });
  await page.route("**/api/builder/table", async (route) => {
    await route.fulfill({ json: envelope({ created: true, kind: "table" }) });
  });
  await page.route("**/api/config/validate", async (route) => {
    const body = route.request().postDataJSON();
    await route.fulfill({
      json: envelope(
        body?.topology?.replica_count === 2 ? topologyValidationFixture : validationFixture,
      ),
    });
  });
  await page.route("**/api/config/plan", async (route) => {
    await route.fulfill({ json: envelope(migrationPlanFixture) });
  });
  await page.route("**/api/sql", async (route) => {
    await route.fulfill({
      json: envelope({ output: { kind: "affected_rows", affected_rows: 1 } }),
    });
  });

  await page.goto("/");
  await page.getByLabel("Password").fill("secret");
  await page.getByRole("button", { name: /sign in/i }).click();

  await expect(page.getByText("0.1.0-test")).toBeVisible();
  await expect(page.getByText("users")).toBeVisible();
  await expect(page.getByText("Health", { exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1)).toBeTruthy();

  await page.getByRole("button", { name: /data explorer/i }).click();
  await page.getByRole("button", { name: /load page/i }).click();
  await expect(page.getByText("Ada")).toBeVisible();
  await expect(page.getByRole("button", { name: /delete row/i })).toBeDisabled();
  await page.getByLabel("Confirm table name").fill("users");
  await page.getByRole("button", { name: /delete row/i }).click();
  expect(tableDeleteConfirmed).toBeTruthy();

  await page.getByRole("button", { name: /profiles collection/i }).click();
  await page.getByRole("button", { name: /load page/i }).click();
  await page.getByRole("button", { name: /doc-1/i }).click();
  await expect(page.getByRole("button", { name: /delete document/i })).toBeDisabled();
  await page.getByLabel("Confirm document id").fill("doc-1");
  await page.getByRole("button", { name: /delete document/i }).click();
  expect(documentDeleteConfirmed).toBeTruthy();

  await page.getByRole("button", { name: /^config$/i }).click();
  await page.getByLabel("Deployment mode").selectOption("cluster");
  await page.getByLabel("Replication mode").selectOption("cp");
  await page.getByLabel("Replica count").fill("3");
  await page.getByRole("button", { name: /build dry run/i }).click();
  await expect(page.getByText("plan-1")).toBeVisible();
  await expect(page.getByText("$.topology.replica_count")).toBeVisible();

  await page.getByLabel("Replica count").fill("2");
  await page.getByRole("button", { name: /^validate$/i }).click();
  await expect(page.getByText("Validation failed")).toBeVisible();
  await expect(page.getByText("cluster + CP requires an odd replica quorum of at least 3")).toBeVisible();

  await page.getByRole("button", { name: /sql console/i }).click();
  await page.getByRole("button", { name: /^run$/i }).click();
  await expect(page.getByText("affected_rows")).toBeVisible();

  await page.getByRole("button", { name: /builder/i }).click();
  await page.getByRole("button", { name: /^create$/i }).click();
  await expect(page.getByText('"created": true')).toBeVisible();

  await page.getByRole("button", { name: /security/i }).click();
  await expect(page.getByText("admin -> admin")).toBeVisible();
  await page.getByRole("button", { name: /add user/i }).click();
  await page.getByRole("button", { name: /save changes/i }).click();
  expect(securitySaved).toBeTruthy();

  await page.getByRole("button", { name: /audit/i }).click();
  await page.getByLabel("Action filter").fill("login");
  await expect(page.getByText("1 of 1 events")).toBeVisible();

  await page.getByRole("button", { name: /advice/i }).click();
  await expect(page.getByText("Create an index for users.age")).toBeVisible();
  await expect(page.getByText("advisor.index.create.users.age")).toBeVisible();
  await page.getByRole("button", { name: /load plan/i }).click();
  await expect(page.getByText("plan-1")).toBeVisible();
  await page.getByLabel("Reject reason").fill("defer until low traffic");
  await page.getByRole("button", { name: /^reject$/i }).click();
  await expect(page.getByText(/Decision recorded: rejected by admin/i)).toBeVisible();
  expect(adviceRejected).toBeTruthy();

  await page.getByRole("button", { name: /logout/i }).click();
  await expect(page.getByRole("button", { name: /sign in/i })).toBeVisible();
});
