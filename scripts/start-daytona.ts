// ══════════════════════════════════════════════════════════════════════════════
//  start-daytona.ts — `start:daytona`: CREATE a Daytona sandbox, register the SAME
//  anonymous payload in it as the app "provider", and report ONE origin
//
//      yarn workspace @typecrate/provider start:daytona                       # create + deploy
//      yarn workspace @typecrate/provider start:daytona -- --dry-run          # the plan, zero Daytona calls
//      yarn workspace @typecrate/provider start:daytona -- --private --ttl 120
//      yarn workspace @typecrate/provider start:daytona -- --cpu 2 --mem 4
// ══════════════════════════════════════════════════════════════════════════════
//
//  WHY THIS VERB EXISTS, AND WHY IT IS THE LOWEST-PRIORITY ONE. The endpoint the
//  provider ships to today is a Modal box (start-modal.ts): long-lived, stable URL, and
//  shared. A Daytona sandbox is the alternative for an operator who wants a machine per
//  deploy: `createSandbox` from @services/daytona/sandbox brings one up, and EVERYTHING
//  after that is the same shared client again — same tar.gz, same config, same
//  POST/PUT /apps, same poll (this file re-uses deploy-endpoint.ts's parser and payload
//  builder rather than carrying a second copy of either).
//
//  ONE ORIGIN, SO THERE IS NO SECOND PREVIEW LINK. v2 serves every app under
//  `/app/<name>/` on the runner's ONE public origin (spec §4.1/§4.2), so:
//    · the MANAGEMENT endpoint is the preview link createSandbox returns for the
//      RUNNER's port (`RUNNER_PORT` 7777) — leave its `port` unset, never pass the
//      provider's internal port; and
//    · the PROVIDER's public base is that same endpoint + the registry mount
//      `/app/provider/` (`result.path`), composed by the shared client. There is no
//      `getPreviewLink(<payload port>)` step any more: v2 has no per-port preview link,
//      and the preview token is the SAME origin credential — not a second one.
//  The internal port in the summary is the registry's own loopback port. It is never a
//  public port and there is no per-port advice to print.
//
//  THE ADMIN BEARER (spec §12.3) IS INDEPENDENT OF THE PREVIEW TOKEN. When
//  `--admin-token` / `--admin-token-file` (or the documented env names) is present it
//  is used three ways, and never printed: it configures the fresh sandbox's own admin
//  auth (`createSandbox({adminTokenFile})` — the file form is preferred), it rides on
//  every deploy/poll/log/delete call as `Authorization: Bearer`, and the summaries
//  report it by PRESENCE only.
//
//  LIFECYCLE / BILLING — the CALLER owns the sandbox. `createSandbox` deletes only a
//  sandbox whose OWN bring-up failed (setup-sandbox.ts). This verb:
//    · never deletes on SUCCESS (a deployed, serving provider is the thing you asked for);
//    · deletes on a FAILED deploy, because a sandbox holding a failed app is worthless
//      and still meters — `--keep-on-fail` keeps it for log-reading instead;
//    · prints the delete recipe on every path, and defaults `--ttl` to NOTHING (no
//      fence), exactly like the library does.
//
//  AUTH: `--private` makes the preview link token-authenticated. The deploy sends the
//  token createSandbox returned, and the provider app lives on that SAME origin, so the
//  compatibility field `providerToken` in the summary is that same token (there is no
//  second, per-port secret). `--public` (the default) leaves the origin tokenless, and a
//  public Daytona sandbox executes whatever is POSTed to it (setup-sandbox.ts states
//  the consequence).
//
//  IMPORT IS A NO-OP (entry-guard.ts): start-daytona.test.ts drives the parser, the create
//  options and the whole flow with a fake `createSandbox` and a fake probe — zero network,
//  zero cargo, zero Daytona API.
// ══════════════════════════════════════════════════════════════════════════════

import { CargoBuildError, PayloadError, RuntimeDeployError, createSandbox, deployPayload } from '@services/daytona/sandbox';
import type { CreateSandboxOptions, DeployResult } from '@services/daytona/sandbox';

import {
    APP_NAME,
    APP_PATH,
    DEFAULT_PROBE_PATH,
    PREFIX,
    PROVIDER_HOST_ENV,
    PROVIDER_HOST_LOOPBACK,
    ProviderDeployError,
    TARGET_PREFIX,
    buildDeployOptions,
    buildPayloadConfig,
    buildProviderPayload,
    createDefaultDeps,
    parseArgs,
    printPayloadPlan,
    resolveAdminToken,
    resolveAdminTokenFile,
} from './deploy-endpoint';
import type { ProviderDeps, ProviderFlags } from './deploy-endpoint';
import { entrySignals, isCliEntryPoint, modulePathOf, scriptsDirOf } from './entry-guard';

/** This verb's own file name (the entry guard's identity). */
export const ENTRY_SCRIPT_NAME_DAYTONA = 'start-daytona.ts';
const SCRIPTS_DIR = scriptsDirOf(import.meta.url);
const MODULE_FILE_PATH = modulePathOf(import.meta.url, SCRIPTS_DIR, ENTRY_SCRIPT_NAME_DAYTONA);

/**
 * Daytona's runner port — the port the MANAGEMENT endpoint is minted for
 * (setup-sandbox.ts `RUNNER_PORT`). Pinned here rather than imported because the barrel
 * curates the library API, not the CLI's constants.
 */
export const RUNNER_PORT = 7777;

/** The create knobs this verb adds on top of the deploy verb's flags. */
export type DaytonaFlags = {
    /** `--private` flips it; the library default is public/tokenless. */
    public: boolean;
    cpu?: number;
    mem?: number;
    disk?: number;
    /** Wall-clock fence in minutes; undefined = no fence (the library's own default). */
    ttlMinutes?: number;
    /** Sandbox name; undefined = the generated `noobscript-runtime-<UTCstamp>`. */
    name?: string;
    /** Keep a FAILED sandbox alive (the deploy failed, but the machine is worth reading). */
    keepOnFail: boolean;
};

export type ParsedDaytonaArgs = { daytona: DaytonaFlags; deploy: ProviderFlags; errors: string[] };

/** The create-only flags, mapped to the kind of value each takes (undefined = a boolean). */
const CREATE_FLAGS: Record<string, 'number' | 'string' | undefined> = { '--cpu': 'number', '--mem': 'number', '--disk': 'number', '--ttl': 'number', '--name': 'string', '--public': undefined, '--private': undefined, '--keep-on-fail': undefined };

export const USAGE_DAYTONA = `usage: yarn workspace @typecrate/provider start:daytona [flags]

  brings up a Daytona sandbox of its own and registers THIS package's anonymous payload
  in it as the app "${APP_NAME}" — the same bundle deploy:endpoint ships. Clients use
  <sandbox-preview>/app/${APP_NAME}/… ; there is ONE origin and no second preview link.
  Every flag below that is not a CREATE flag is a deploy flag, with the same meaning as
  there (see deploy:endpoint --help for --probe-path --env --vault --out --no-build
  --artifact --no-teardown --no-wait --logs --dry-run --admin-token --admin-token-file).

  the sandbox (create flags):
    --public            tokenless preview URL (the default)
    --private           token-authenticated preview URL (last of --public/--private wins)
    --cpu <n>           vCPU (default the library's 1)
    --mem <n>           GiB of RAM (default 1)
    --disk <n>          GiB of disk (default 10)
    --ttl <minutes>     Daytona's kill fence; default NONE, so the sandbox bills until deleted
    --name <name>       sandbox name; default the generated noobscript-runtime-<stamp>
    --keep-on-fail      leave a FAILED sandbox alone instead of deleting it (log reading)

  the payload (deploy flags, shared with deploy:endpoint):
    --port <n>          OPTIONAL internal loopback port for the provider (0/omitted = the
                        runner auto-allocates). It is NOT the runner's port and NEVER a
                        public port: the management endpoint stays on ${RUNNER_PORT}, and
                        clients use /app/${APP_NAME}/.
    --admin-token <t>   the runner's admin Bearer (spec §12.3): configures the new
    --admin-token-file <path>
                        sandbox's admin auth (the file form is preferred) and rides on
                        the deploy/log/delete calls. Never printed.
                        (env: TYPECRATE_PROVIDER_SANDBOX_ADMIN_TOKEN[_FILE], then
                        SANDBOX_ADMIN_TOKEN[_FILE])

  credentials: DAYTONA_API_KEY (or @config/vault) covers the Daytona API — this verb needs
  no endpoint and no token argument, because the sandbox hands both back. The sandbox is
  NOT deleted on success; --ttl or the printed recipe stops the meter.`;

/**
 * Splits the command line: the create knobs are consumed HERE, every other token is
 * forwarded verbatim to the deploy verb's parser, so `--port`, `--env`, `--dry-run`,
 * `--vault`, `--admin-token`… keep ONE meaning across both verbs. Order-independent,
 * never throws.
 */
export const parseArgsDaytona = (argv: readonly string[], env: Readonly<Record<string, string | undefined>> = process.env): ParsedDaytonaArgs => {
    const daytona: DaytonaFlags = { public: true, keepOnFail: false };
    const forwarded: string[] = [];
    const errors: string[] = [];
    /** `--ttl` alone allows 0 (an explicit "no fence"); the resource knobs start at 1. */
    const number = (flag: string, value: string, minimum: number): number | undefined => {
        const parsed = Number(value);
        if (!Number.isInteger(parsed) || parsed < minimum) {
            errors.push(`${flag} ${JSON.stringify(value)} is not an integer >= ${minimum}`);
            return undefined;
        }
        return parsed;
    };
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index]!;
        const kind = flag in CREATE_FLAGS ? CREATE_FLAGS[flag] : undefined;
        if (!(flag in CREATE_FLAGS)) {
            forwarded.push(flag);
            continue;
        }
        if (flag === '--public') daytona.public = true;
        else if (flag === '--private') daytona.public = false;
        else if (flag === '--keep-on-fail') daytona.keepOnFail = true;
        else {
            const value = argv[++index];
            if (value === undefined || value.startsWith('--')) {
                errors.push(`${flag} needs a value`);
                continue;
            }
            if (kind === 'string') daytona.name = value;
            else if (flag === '--ttl') daytona.ttlMinutes = number(flag, value, 0);
            else daytona[flag.slice(2) as 'cpu' | 'mem' | 'disk'] = number(flag, value, 1);
        }
    }
    // `requireEndpoint: false`: this verb MINTS its endpoint, so the deploy verb's
    // requirement does not apply here — and its error text would name a flag that this
    // verb refuses outright.
    const parsed = parseArgs(forwarded, env, { requireEndpoint: false });
    // A Daytona sandbox is created EMPTY, so `--teardown` (unregister an app and ship
    // nothing) is meaningless there, and an endpoint argument would be a second machine to
    // deploy into. Both are refused rather than silently ignored.
    if (parsed.flags.teardownOnly) errors.push(`start:daytona always creates a sandbox — --teardown (unregister the app, ship nothing) is deploy:endpoint's verb`);
    if (parsed.flags.endpoint) errors.push(`start:daytona mints its own endpoint — the endpoint argument/--endpoint ("${parsed.flags.endpoint}") belongs to deploy:endpoint`);
    // The endpoint is minted, so the deploy verb's "package only" posture is not this
    // verb's; --out is still honoured ALONGSIDE the deploy (it is a host-side write).
    return { daytona, deploy: { ...parsed.flags, endpoint: '' }, errors: [...errors, ...parsed.errors] };
};

/**
 * The createSandbox options those flags map onto. `port` is DELIBERATELY ABSENT: it
 * selects the RUNNER's port (and therefore the management endpoint), never the payload's.
 * Unset knobs are omitted rather than sent as undefined, so the library's defaults stay
 * authoritative (the same reason the deploy config omits an absent port).
 *
 * The admin credential is passed in its PREFERRED form: the file path when the operator
 * named one (the token is uploaded 0600 and never reaches the runner's command line),
 * otherwise the resolved value. It is never logged — the real run prints the same options
 * WITHOUT the credential and a separate presence line.
 */
export const buildCreateOptions = (daytona: DaytonaFlags, credentials: { adminToken?: string; adminTokenFile?: string } = {}): CreateSandboxOptions => ({
    public: daytona.public,
    ...(daytona.cpu === undefined ? {} : { cpu: daytona.cpu }),
    ...(daytona.mem === undefined ? {} : { mem: daytona.mem }),
    ...(daytona.disk === undefined ? {} : { disk: daytona.disk }),
    ...(daytona.ttlMinutes === undefined ? {} : { ttlMinutes: daytona.ttlMinutes }),
    ...(daytona.name === undefined ? {} : { name: daytona.name }),
    ...(credentials.adminTokenFile ? { adminTokenFile: credentials.adminTokenFile } : credentials.adminToken ? { adminToken: credentials.adminToken } : {}),
});

/**
 * The slice of `CreateSandboxResult` this verb reads, named as its own type so a test can
 * fake exactly what is used. `sandbox` is narrowed to the one call made here (stop the
 * billing) — the real SDK handle satisfies it structurally. There is NO `getPreviewLink`:
 * v2 has one origin and the app's URL is composed, not minted.
 */
export type CreatedSandbox = {
    endpoint: string;
    token?: string;
    sandboxId: string;
    name: string;
    port: number;
    public: boolean;
    sandbox: { delete: () => Promise<unknown> };
};

/** What this verb needs beyond the deploy deps: the two Daytona side effects. */
export type DaytonaDeps = ProviderDeps & {
    /** real: createSandbox from @services/daytona/sandbox (Daytona API + network). */
    create?: (options: CreateSandboxOptions) => Promise<CreatedSandbox>;
    /** real: the shared client's deployPayload, with this verb's flags mapped onto its options. */
    ship?: (endpoint: string, payload: Parameters<typeof deployPayload>[1], flags: ProviderFlags, deps: ProviderDeps) => Promise<DeployResult>;
};

/** The default `ship`: the flags this verb parsed, translated exactly as the verb does. */
const shipDefault: NonNullable<DaytonaDeps['ship']> = (endpoint, payload, flags, deps) => deployPayload(endpoint, payload, deps, buildDeployOptions(flags));

/**
 * The machine-readable LAST line (`tail -1 | jq`). It is NOT the client's `buildSummary`:
 * a Daytona run has fields the client's shape has no room for (`sandboxId`, the token),
 * and the client's `publicUrl` is already the app base, so this line names it explicitly.
 * The client's own summary is still printed, PREFIXED, one line above — so nothing is
 * lost and the unprefixed last line stays the single JSON object.
 *
 * `providerToken` is a COMPATIBILITY field: v2 has one origin, so it is the SAME preview
 * token reported on `endpoint`, never a second per-port secret. `port` is the internal
 * loopback port and is omitted entirely when it is not known yet.
 */
export const buildDaytonaSummary = (input: {
    sandboxId: string;
    name: string;
    management: string;
    status: string;
    /** INTERNAL loopback port (registry-assigned/requested); never a public port. */
    port?: number;
    public: boolean;
    /** The app base URL: management + `/app/provider/`, on the ONE origin. */
    providerUrl: string | null;
    providerToken?: string;
    notes: string[];
}): string =>
    JSON.stringify({
        mode: 'create+deploy',
        sandboxId: input.sandboxId,
        name: input.name,
        endpoint: input.management,
        status: input.status,
        ...(input.port === undefined ? {} : { port: input.port }),
        public: input.public,
        providerUrl: input.providerUrl,
        ...(input.providerToken ? { providerToken: input.providerToken, tokenScope: 'preview-proxy' } : {}),
        notes: input.notes,
    });

/** The billing line set, printed on every path that leaves a sandbox alive. */
export const deleteRecipe = (sandboxId: string, isPublic: boolean): string[] => [
    'the sandbox IS BILLING until it is deleted (this verb never deletes on success):',
    `  yarn workspace @services/daytona sandbox down -- --sandbox ${sandboxId}`,
    ...(isPublic ? [] : [`  (a private sandbox also loses its preview token when it goes)`]),
];

/**
 * The placeholder vocabulary for a run that never got a machine. ONE definition, shared by
 * the dry run and by a failed create, so a consumer can match on the same strings for both.
 */
export const NOT_CREATED = {
    sandboxId: '<not created>',
    name: '<not created>',
    management: `<Daytona preview link, runner port ${RUNNER_PORT}>`,
} as const;

/** The dry run's LAST line: the same shape as a real run's, with `status: "planned"`. */
export const buildPlannedSummary = (daytona: DaytonaFlags, flags: ProviderFlags, notes: string[]): string =>
    buildDaytonaSummary({ ...NOT_CREATED, status: 'planned', port: flags.port, public: daytona.public, providerUrl: null, notes });

/**
 * `start:daytona` end to end. Returns the exit code and never calls process.exit, so the
 * entry guard and the tests both own the process.
 */
export const runStartDaytona = async (daytona: DaytonaFlags, flags: ProviderFlags, deps: DaytonaDeps = createDefaultDeps()): Promise<number> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const error = deps.error ?? ((text: string) => console.error(text));
    const line = (text: string) => log(`${PREFIX} ${text}`);
    const create = deps.create ?? createSandbox;
    const ship = deps.ship ?? shipDefault;
    const config = buildPayloadConfig(flags);
    // Resolved before createSandbox so a bad token file costs no sandbox, and so the
    // SAME value configures the machine's admin auth and authenticates the deploy.
    const adminToken = resolveAdminToken(flags, deps);
    const adminLine = adminToken || flags.adminTokenFile ? 'admin Bearer configured (value never printed)' : 'no admin Bearer';

    // ── dry run: the plan, and provably no Daytona call, no cargo, no network ──
    if (flags.dryRun) {
        const built = await buildProviderPayload(flags, deps);
        printPayloadPlan(flags, built, config, ['no createSandbox — Daytona was never called'], log, `<a Daytona preview link for runner port ${RUNNER_PORT}>`);
        // The credential is NOT part of the printed options: this is the exact shape the
        // real run sends MINUS the admin credential, whose presence is named separately.
        line(`create:   ${JSON.stringify(buildCreateOptions(daytona))}  (the management endpoint is its ${RUNNER_PORT} preview link)`);
        line(`admin:    ${adminLine}`);
        line(`provider: <that endpoint>${APP_PATH} after the deploy — ONE origin, no per-port preview link to mint`);
        // BARE, unprefixed: the last line is the machine-readable one on every path, and
        // a prefixed summary cannot be parsed by `tail -1 | jq`.
        log(buildPlannedSummary(daytona, flags, ['no createSandbox — Daytona was never called']));
        return 0;
    }

    // ── create: the machine, and the management endpoint that comes back with it ──
    // The FILE path is the one resolveAdminToken read, anchored by the verb so the
    // sandbox uploads exactly the file whose value the deploy will send.
    const options = buildCreateOptions(daytona, { adminToken: adminToken || undefined, adminTokenFile: resolveAdminTokenFile(flags) });
    line(`createSandbox ${JSON.stringify(buildCreateOptions(daytona))} — the runner boots on ${RUNNER_PORT}; this deploy registers into that origin · ${adminLine}`);
    // A create failure is OPERATOR error (credentials, quota, snapshot), and there is no
    // sandbox to clean up: createSandbox deletes its own half-built machine.
    let created: CreatedSandbox;
    try {
        created = await create(options);
    } catch (failure) {
        error(`${PREFIX} ✗ createSandbox failed: ${failure instanceof Error ? failure.message : String(failure)}`);
        error(`${PREFIX} no sandbox was created, so nothing is billing (a half-built sandbox is deleted by createSandbox itself)`);
        log(buildDaytonaSummary({ ...NOT_CREATED, status: 'failed', port: flags.port, public: daytona.public, providerUrl: null, notes: [`createSandbox failed: ${failure instanceof Error ? failure.message : String(failure)}`] }));
        return 1;
    }
    line(`✓ sandbox ${created.sandboxId} (${created.name}) — management ${created.endpoint} (${created.public ? 'public, tokenless' : 'private, token header'})`);
    // The management link's own token: createSandbox hands it back, and a public sandbox
    // simply ignores it (no header is sent when it is empty). In v2 this is the ONLY
    // origin, so it is also the provider app's credential.
    const token = flags.token || created.token || '';

    // ── deploy: the same payload, the same client, the same app policy ──
    const built = await buildProviderPayload(flags, deps);
    const result = await ship(created.endpoint, built.payload, { ...flags, token, adminToken }, deps);
    const allNotes = [...result.notes];
    if (!result.ok) {
        error(`${PREFIX} ✗ ${result.status}: ${result.error ?? 'the app never became ready'}`);
        for (const note of allNotes) error(`${PREFIX} note: ${note}`);
        // The app log is the only view of WHY (bind failure, refused vault, ENOEXEC);
        // the shared client attaches it to a failed run.
        if (result.logs) {
            error(`${PREFIX} ── GET /apps/${APP_NAME}/logs (${result.logs.split('\n').length} lines) ──`);
            for (const entry of result.logs.split('\n')) error(`${PREFIX} ${entry}`);
        } else {
            error(`${PREFIX} no app log tail was attached — the reason above is all the endpoint said`);
        }
        if (daytona.keepOnFail) {
            error(`${PREFIX} --keep-on-fail: sandbox ${created.sandboxId} is left alive for log reading`);
            for (const entry of deleteRecipe(created.sandboxId, created.public)) error(`${PREFIX} ${entry}`);
        } else {
            try {
                await created.sandbox.delete();
                error(`${PREFIX} ✓ sandbox ${created.sandboxId} deleted — a failed app is not worth billing for (--keep-on-fail keeps it)`);
            } catch (failure) {
                error(`${PREFIX} ✗ sandbox delete failed (${failure instanceof Error ? failure.message : String(failure)}) — delete it yourself:`);
                for (const entry of deleteRecipe(created.sandboxId, created.public)) error(`${PREFIX} ${entry}`);
                allNotes.push('sandbox.delete() failed — the sandbox is still billing');
            }
        }
        log(buildDaytonaSummary({ sandboxId: created.sandboxId, name: created.name, management: created.endpoint, status: result.status, port: result.port, public: created.public, providerUrl: null, notes: allNotes }));
        return 1;
    }

    // ── the provider app's OWN url: the ONE origin + the registry mount, never a port ──
    // `result.publicUrl` is endpoint + `path` (composed by the client from the registry
    // record, not from the runner's Host heuristic). The literal fallback keeps the
    // documented mount even if a future client reports no composed URL.
    const providerUrl = result.publicUrl ?? `${created.endpoint}${result.path || APP_PATH}`;
    // COMPATIBILITY: v2 has one origin, so the preview token is the same credential for
    // the registry and for the app. There is no second, per-port token to mint or report.
    const providerToken = token || undefined;

    line(`✓ ${result.status} — app "${APP_NAME}" revision ${result.revision ?? '?'}, provider INTERNAL port ${result.port} (never public)`);
    line(`  provider (clients): ${providerUrl}`);
    line(`  mount:              ${result.path} → native ${TARGET_PREFIX}/… (targetPrefix)`);
    line(`  provider health:    the bin's GET ${DEFAULT_PROBE_PATH} on the container LOOPBACK (${PROVIDER_HOST_ENV}=${PROVIDER_HOST_LOOPBACK}) — NOT ${result.path}health`);
    line(`  management:         GET ${created.endpoint}/apps · PUT|DELETE ${created.endpoint}/apps/${APP_NAME} · GET ${created.endpoint}/apps/${APP_NAME}/logs`);
    line(`  artifact:           ${built.artifact.path} (${built.artifact.source}) + ${built.vaultPath} INSIDE the bundle — the app record exposes config metadata, never secrets`);
    line(`  client view:        ${result.summary}`);
    for (const entry of deleteRecipe(created.sandboxId, created.public)) line(entry);
    log(buildDaytonaSummary({ sandboxId: created.sandboxId, name: created.name, management: created.endpoint, status: result.status, port: result.port, public: created.public, providerUrl, ...(providerToken ? { providerToken } : {}), notes: allNotes }));
    return 0;
};

/**
 * Parses, checks, runs, maps onto exit codes: 0 planned/deployed, 1 the verb was told
 * something contradictory or the payload is not serving. Daytona is never called for a
 * parse error, so a typo can never cost money.
 */
export const mainDaytona = async (argv: readonly string[] = process.argv.slice(2), deps: DaytonaDeps = createDefaultDeps(), env: Readonly<Record<string, string | undefined>> = process.env): Promise<number> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const error = deps.error ?? ((text: string) => console.error(text));
    const { daytona, deploy, errors } = parseArgsDaytona(argv, env);
    if (deploy.help) {
        log(USAGE_DAYTONA);
        return 0;
    }
    if (errors.length) {
        for (const problem of errors) error(`${PREFIX} ✗ ${problem}`);
        error('');
        error(USAGE_DAYTONA);
        return 1;
    }
    try {
        return await runStartDaytona(daytona, deploy, deps);
    } catch (failure) {
        // The four NAMED failure classes all carry their own fix (operator error); an
        // unexpected one keeps its stack so a defect is not dressed up as one.
        if (failure instanceof ProviderDeployError || failure instanceof RuntimeDeployError || failure instanceof PayloadError || failure instanceof CargoBuildError) {
            error(`${PREFIX} ✗ ${failure.message}`);
            return 1;
        }
        error(`${PREFIX} ✗ unexpected failure: ${failure instanceof Error ? (failure.stack ?? failure.message) : String(failure)}`);
        return 1;
    }
};

// ── the entry point (guarded: importing this file must never create a sandbox) ──
if (isCliEntryPoint(entrySignals(MODULE_FILE_PATH))) {
    mainDaytona().then(
        (code) => {
            process.exitCode = code;
        },
        (failure) => {
            console.error(`${PREFIX} ✗ unexpected failure: ${failure instanceof Error ? (failure.stack ?? String(failure)) : String(failure)}`);
            process.exitCode = 1;
        },
    );
}
