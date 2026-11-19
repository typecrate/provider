// ─────────────────────────────────────────────────────────────────────────────
//  start-daytona.test.ts — create + deploy + mint, with NO Daytona API, NO cargo
//  spawn and NO network.
//
//  `start:daytona` is the only verb that BRINGS UP MACHINE, so the tests below are
//  built around the money-shaped rules:
//    · parser      — the CREATE knobs are consumed here and every other token is
//                    forwarded to the deploy verb unchanged (one meaning per flag
//                    across both verbs); a value knob that gets no value, a
//                    non-integer, a 0 where 1 is the floor, and the two combinations
//                    that belong to the OTHER verb (--teardown, an endpoint) are errors
//    · create opts — `port` is DELIBERATELY ABSENT. `createSandbox({port})` selects the
//                    RUNNER's port and therefore the management endpoint
//                    (setup-sandbox.ts:1565-1587 `createSandbox`, RUNNER_PORT at :234),
//                    never the payload's, so passing 8800 here would POST the bundle at a
//                    port nothing serves yet. Unset knobs are omitted, not sent as
//                    undefined.
//    · dry run     — provably zero Daytona calls (the injected `create`/`ship` THROW),
//                    zero cargo, zero probes, and the plan still shows the payload
//    · real run    — the deploy goes to the MANAGEMENT endpoint with the token
//                    createSandbox returned, and the PROVIDER's URL is a SECOND link,
//                    minted afterwards with getPreviewLink(<payload port>) — never
//                    derived by string surgery on the management URL
//    · lifecycle   — never deleted on success; deleted on a failed deploy, with
//                    --keep-on-fail as the opt-out and a failed delete reported in the
//                    notes as well as on stderr; the billing recipe is printed either way
//    · summary     — the LAST line is the create+deploy JSON with sandboxId and the
//                    minted link, and `publicUrl` is never echoed from the client (a
//                    Daytona runner cannot know its own preview URL, so echoing null
//                    would report a lie — runtime-deploy.ts:1579-1587 `buildPublicUrl`)
// ─────────────────────────────────────────────────────────────────────────────

import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

import type { CreateSandboxOptions, DeployResult, FsAccess, PreviewLink, Probe } from '@services/daytona/sandbox';

import { DEFAULT_VAULT_PATH, PACKAGE_ROOT, PREFIX, buildProviderPayload, parseArgs } from './deploy-endpoint';
import {
    ENTRY_SCRIPT_NAME_DAYTONA,
    RUNNER_PORT,
    USAGE_DAYTONA,
    type CreatedSandbox,
    type DaytonaDeps,
    buildCreateOptions,
    buildDaytonaSummary,
    buildPlannedSummary,
    deleteRecipe,
    mainDaytona,
    parseArgsDaytona,
    runStartDaytona,
} from './start-daytona';

// ══════════════════════════════════════════════════════════════════════════════
//  FAKES — a file table, a Daytona that records, and a ship that records
// ══════════════════════════════════════════════════════════════════════════════

const ELF = Buffer.concat([Buffer.from([0x7f, 0x45, 0x4c, 0x46]), Buffer.from('provider-server-image', 'utf8')]);
const VAULT_DOC = '{"consumers":["agentic-provider"],"keys":["sk-test"]}';
/** Absolute paths, derived from the verb's OWN constants so a rename breaks a test, not a deploy. */
const REPO = join(PACKAGE_ROOT, '..', '..', '..');
const BIN_PATH = join(REPO, '.rust', 'target', 'release', 'typecrate-provider-server');
const VAULT_PATH = DEFAULT_VAULT_PATH;

const hostFs = (): FsAccess => {
    const table = new Map<string, Buffer>([
        [BIN_PATH, ELF],
        [VAULT_PATH, Buffer.from(VAULT_DOC, 'utf8')],
        [join(REPO, '.cargo', 'config.toml'), Buffer.from('[build]\ntarget-dir = ".rust/target"\n', 'utf8')],
    ]);
    const bytesOf = (path: string): Buffer | undefined => table.get(path);
    return {
        exists: (path) => table.has(path),
        read: (path) => {
            const data = bytesOf(path);
            if (!data) throw new Error(`fake fs: no such file ${path}`);
            return data;
        },
        readHead: (path, bytes) => (bytesOf(path) ?? Buffer.alloc(0)).subarray(0, bytes),
        stat: (path) => {
            const data = bytesOf(path);
            return data ? { size: data.length, isFile: true } : null;
        },
    };
};

/** A dry run must never reach the network, so any probe is a loud failure. */
const neverProbed: Probe = async (method, url) => {
    throw new Error(`start:daytona touched the network: ${method} ${url}`);
};

/** The management endpoint and its token, exactly as `createSandbox` hands them back. */
const MANAGEMENT = 'https://7777-sb-1.preview.daytona.run';
const MANAGEMENT_TOKEN = 'mgmt-token-9e2c';
/** The provider's OWN link is a different URL with a different token, minted per port. */
const PROVIDER_LINK: PreviewLink = { url: 'https://8800-sb-1.preview.daytona.run', token: 'preview-token-4f1a' };

/** The sandbox handle: records the link mint and the delete, and can be told to fail each. */
const fakeSandbox = (options: { previewFails?: string; deleteFails?: string } = {}) => {
    const minted: number[] = [];
    const deletions: number[] = [];
    const sandbox = {
        getPreviewLink: async (port: number): Promise<PreviewLink> => {
            minted.push(port);
            if (options.previewFails) throw new Error(options.previewFails);
            return PROVIDER_LINK;
        },
        delete: async () => {
            deletions.push(1);
            if (options.deleteFails) throw new Error(options.deleteFails);
            return { deleted: true };
        },
    };
    const created: CreatedSandbox = { endpoint: MANAGEMENT, token: MANAGEMENT_TOKEN, sandboxId: 'sb-1', name: 'noobscript-runtime-20261119-090000', port: RUNNER_PORT, public: false, sandbox };
    return { created, sandbox, minted, deletions };
};

/** The client's own summary line, for a fake result the verb prints verbatim. */
const clientSummary = (status: string, endpoint: string, notes: string[]): string =>
    JSON.stringify({ mode: 'deploy', endpoint, status, publicUrl: null, port: 8800, profile: 'anonymous', filename: 'typecrate-provider.tar.gz', notes });

const readyResult = (notes: string[] = []): DeployResult => ({
    ok: true,
    status: 'ready',
    runtime: { profile: 'generic', port: 8800, ready: true, runtimeId: 'rt-1' },
    // A Daytona runner never learns its preview URL, so the client reports null HERE —
    // which is exactly why this verb mints its own link instead of echoing that field.
    publicUrl: null,
    notes,
    endpoint: MANAGEMENT,
    port: 8800,
    filename: 'typecrate-provider.tar.gz',
    profile: 'anonymous',
    slot: 'ready',
    replaced: null,
    recycled: null,
    summary: clientSummary('ready', MANAGEMENT, notes),
});

const failedResult = (): DeployResult => ({
    ...readyResult(['the runtime never reported ready']),
    ok: false,
    status: 'failed',
    error: 'the runtime never became ready',
    logs: 'booting ./typecrate-provider-server\nerror: vault document refused',
});

/** The whole Daytona world: recorded create/ship calls, sinks, and a cargo recorder. */
const daytonaHost = (options: { createFails?: string; previewFails?: string; deleteFails?: string; result?: DeployResult } = {}) => {
    const out: string[] = [];
    const err: string[] = [];
    const cargo: string[] = [];
    const sandbox = fakeSandbox(options);
    const creates: CreateSandboxOptions[] = [];
    const ships: Array<{ endpoint: string; token: string; filename: string; config: unknown; bufferBytes: number }> = [];
    const deps: DaytonaDeps = {
        probe: neverProbed,
        fs: hostFs(),
        runCommand: async (command, args) => {
            cargo.push(`${command} ${args.join(' ')}`);
            return { status: 0, stdout: '', stderr: '' };
        },
        env: {},
        sleep: async () => undefined,
        log: (text) => out.push(text),
        error: (text) => err.push(text),
        create: async (createOptions) => {
            creates.push(createOptions);
            if (options.createFails) throw new Error(options.createFails);
            return sandbox.created;
        },
        ship: async (endpoint, payload, flags) => {
            ships.push({ endpoint, token: flags.token, filename: payload.filename, config: payload.config, bufferBytes: payload.buffer.length });
            return options.result ?? readyResult();
        },
    };
    return { deps, out, err, cargo, creates, ships, sandbox };
};

/** Flags for the flow tests, parsed by the verb's own two-stage parser. */
const flagsFor = (argv: readonly string[]) => parseArgsDaytona(argv, {});

// ══════════════════════════════════════════════════════════════════════════════
//  THE CREATE KNOBS — parsed here, forwarded nowhere
// ══════════════════════════════════════════════════════════════════════════════

describe('parseArgsDaytona', () => {
    it('starts public, tokenless, unbudgeted, and with no endpoint at all', () => {
        const { daytona, deploy, errors } = flagsFor([]);
        expect(errors).toEqual([]);
        expect(daytona).toEqual({ public: true, keepOnFail: false });
        // The endpoint is MINTED by this verb, so the deploy verb's endpoint field stays
        // empty even though its parser would otherwise demand one.
        expect(deploy.endpoint).toBe('');
        expect(deploy.port).toBe(8800);
        expect(deploy.dryRun).toBe(false);
        expect(deploy.wait).toBe(true);
    });

    it('consumes the create knobs and forwards every deploy knob verbatim', () => {
        const { daytona, deploy, errors } = flagsFor(['--private', '--cpu', '2', '--mem', '4', '--disk', '20', '--ttl', '0', '--name', 'provider-box', '--keep-on-fail', '--port', '9000', '--env', 'RUST_LOG=debug', '--dry-run']);
        expect(errors).toEqual([]);
        expect(daytona).toEqual({ public: false, keepOnFail: true, cpu: 2, mem: 4, disk: 20, ttlMinutes: 0, name: 'provider-box' });
        // `--port` is the PROVIDER's port, and it stays with the deploy verb.
        expect(deploy.port).toBe(9000);
        expect(deploy.env).toEqual({ RUST_LOG: 'debug' });
        expect(deploy.dryRun).toBe(true);
    });

    it('lets the LAST of --public/--private win, like any other pair of flags', () => {
        expect(flagsFor(['--private', '--public']).daytona.public).toBe(true);
        expect(flagsFor(['--public', '--private']).daytona.public).toBe(false);
    });

    it('reports each bad value instead of creating the wrong machine', () => {
        const ttl = flagsFor(['--ttl', 'soon']);
        expect(ttl.errors).toEqual(['--ttl "soon" is not an integer >= 0']);
        expect(ttl.daytona).toEqual({ public: true, keepOnFail: false });

        const cpu = flagsFor(['--cpu', '0']);
        expect(cpu.errors).toEqual(['--cpu "0" is not an integer >= 1']);

        expect(flagsFor(['--name', '--private']).errors).toEqual(['--name needs a value']);
        expect(flagsFor(['--ttl']).errors).toEqual(['--ttl needs a value']);
    });

    it('refuses what belongs to deploy:endpoint, before Daytona is called', () => {
        // `--teardown` frees a slot on a machine that already exists; this verb has no
        // such machine yet, so the flag would be silently meaningless.
        expect(flagsFor(['--teardown']).errors).toEqual([`start:daytona always creates a sandbox — --teardown (free a slot, ship nothing) is deploy:endpoint's verb`]);
        // An endpoint here would be a SECOND machine to deploy into, next to the one this
        // verb creates — a contradiction, not a default.
        expect(flagsFor(['https://box.modal.run']).errors).toEqual(['start:daytona mints its own endpoint — the endpoint argument/--endpoint ("https://box.modal.run") belongs to deploy:endpoint']);
        expect(flagsFor(['--endpoint', 'https://box.modal.run']).errors).toEqual(['start:daytona mints its own endpoint — the endpoint argument/--endpoint ("https://box.modal.run") belongs to deploy:endpoint']);
        // …and the endpoint never leaks into the forwarded flags either.
        expect(flagsFor(['https://box.modal.run']).deploy.endpoint).toBe('');
    });

    it('keeps --out as a host-side write ALONGSIDE the deploy', () => {
        const { deploy, errors } = flagsFor(['--out', 'Temp/provider.tar.gz']);
        expect(errors).toEqual([]);
        expect(deploy.out).toBe('Temp/provider.tar.gz');
        expect(deploy.endpoint).toBe('');
    });
});

describe('buildCreateOptions', () => {
    it('names no port: the runner keeps 7777 and that is the management endpoint', () => {
        const options = buildCreateOptions({ public: true, keepOnFail: false });
        expect(options).toEqual({ public: true });
        // The trap itself, stated as an assertion: an unset knob is OMITTED, so the
        // library's own defaults stay authoritative.
        expect('port' in options).toBe(false);
        expect('cpu' in options).toBe(false);
        expect('ttlMinutes' in options).toBe(false);
    });

    it('maps every knob, including a ttl of 0', () => {
        const { daytona } = flagsFor(['--private', '--cpu', '2', '--mem', '4', '--disk', '20', '--ttl', '0', '--name', 'provider-box']);
        expect(buildCreateOptions(daytona)).toEqual({ public: false, cpu: 2, mem: 4, disk: 20, ttlMinutes: 0, name: 'provider-box' });
    });

    it('pins the runner port this verb refuses to override', () => {
        expect(RUNNER_PORT).toBe(7777);
        expect(ENTRY_SCRIPT_NAME_DAYTONA).toBe('start-daytona.ts');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  THE MACHINE-READABLE LINES
// ══════════════════════════════════════════════════════════════════════════════

describe('summaries and recipes', () => {
    it('prints the billing recipe, and the private-sandbox caveat only when it applies', () => {
        expect(deleteRecipe('sb-1', true)).toEqual(['the sandbox IS BILLING until it is deleted (this verb never deletes on success):', '  yarn workspace @services/daytona sandbox down -- --sandbox sb-1']);
        expect(deleteRecipe('sb-1', false)).toEqual([
            'the sandbox IS BILLING until it is deleted (this verb never deletes on success):',
            '  yarn workspace @services/daytona sandbox down -- --sandbox sb-1',
            '  (a private sandbox also loses its preview tokens when it goes)',
        ]);
    });

    it('reports the create+deploy shape, with the minted link and its token', () => {
        expect(buildDaytonaSummary({ sandboxId: 'sb-1', name: 'box', management: MANAGEMENT, status: 'ready', port: 8800, public: false, providerUrl: PROVIDER_LINK.url, providerToken: PROVIDER_LINK.token, notes: ['n'] })).toBe(
            `{"mode":"create+deploy","sandboxId":"sb-1","name":"box","endpoint":"${MANAGEMENT}","status":"ready","port":8800,"public":false,"providerUrl":"${PROVIDER_LINK.url}","providerToken":"${PROVIDER_LINK.token}","tokenScope":"preview-proxy","notes":["n"]}`,
        );
        // No token ⇒ no token fields at all: a consumer must not have to test for "".
        expect(buildDaytonaSummary({ sandboxId: 'sb-2', name: 'box', management: MANAGEMENT, status: 'ready', port: 8800, public: true, providerUrl: PROVIDER_LINK.url, notes: [] })).toBe(
            `{"mode":"create+deploy","sandboxId":"sb-2","name":"box","endpoint":"${MANAGEMENT}","status":"ready","port":8800,"public":true,"providerUrl":"${PROVIDER_LINK.url}","notes":[]}`,
        );
    });

    it('plans the same shape it will later produce', () => {
        const { daytona, deploy } = flagsFor(['--private', '--port', '9000']);
        expect(buildPlannedSummary(daytona, deploy, [])).toBe('{"mode":"create+deploy","sandboxId":"<not created>","name":"<not created>","endpoint":"<Daytona preview link, runner port 7777>","status":"planned","port":9000,"public":false,"providerUrl":null,"notes":[]}');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  THE VERB
// ══════════════════════════════════════════════════════════════════════════════

describe('runStartDaytona — dry run', () => {
    it('prints the whole plan while Daytona, cargo and the network are never touched', async () => {
        const { daytona, deploy } = flagsFor(['--dry-run']);
        // A throwing create/ship is stronger than a recorder: the exit code alone proves
        // they were never reached.
        const hostWorld = daytonaHost({ createFails: 'createSandbox must not run in a dry run' });
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        expect(hostWorld.cargo).toEqual([]);
        expect(hostWorld.err).toEqual([]);

        const printed = hostWorld.out.join('\n');
        expect(printed).toContain('DRY RUN — no probe, no DELETE, no POST, no cargo, no network');
        expect(printed).toContain('no createSandbox — Daytona was never called');
        expect(hostWorld.out).toContain(`${PREFIX} create:   {"public":true}  (the management endpoint is its 7777 preview link)`);
        expect(hostWorld.out).toContain(`${PREFIX} provider: the port-8800 preview link, minted with sandbox.getPreviewLink(8800) AFTER the deploy`);
        // The bundle the plan describes is the real one, not a stand-in.
        expect(printed).toContain('0755  typecrate-provider-server');
        expect(printed).toContain('0644  agentic-provider.json');
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toEqual({
            mode: 'create+deploy',
            sandboxId: '<not created>',
            name: '<not created>',
            endpoint: '<Daytona preview link, runner port 7777>',
            status: 'planned',
            port: 8800,
            public: true,
            providerUrl: null,
            notes: [],
        });
    });

    it('carries the provider port into the plan, and never into the create options', async () => {
        const { daytona, deploy } = flagsFor(['--dry-run', '--port', '9100']);
        const hostWorld = daytonaHost();
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        expect(hostWorld.out).toContain(`${PREFIX} provider: the port-9100 preview link, minted with sandbox.getPreviewLink(9100) AFTER the deploy`);
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toMatchObject({ port: 9100 });
        expect(hostWorld.creates).toEqual([]);
    });
});

describe('runStartDaytona — a real deploy', () => {
    it('deploys to the MANAGEMENT endpoint and mints the provider link afterwards', async () => {
        const { daytona, deploy } = flagsFor(['--private']);
        const hostWorld = daytonaHost();
        const built = await buildProviderPayload(parseArgs(['https://x', '--dry-run'], {}).flags, hostWorld.deps);

        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        // The create call gets exactly the knobs the operator named — no port.
        expect(hostWorld.creates).toEqual([{ public: false }]);
        // The POST target is the runner's link, with the token createSandbox returned.
        expect(hostWorld.ships).toEqual([{ endpoint: MANAGEMENT, token: MANAGEMENT_TOKEN, filename: 'typecrate-provider.tar.gz', config: built.payload.config, bufferBytes: built.payload.buffer.length }]);
        expect(hostWorld.ships[0]!.config).toEqual({
            runCmd: './typecrate-provider-server',
            port: 8800,
            artifactPath: 'typecrate-provider-server',
            probeTimeoutSec: 120,
            stopSignal: 'SIGTERM',
            env: { TYPECRATE_PROVIDER_VAULT_PATH: './agentic-provider.json', RUST_LOG: 'info' },
        });
        // The provider's URL is minted for the PAYLOAD's port, and nothing else.
        expect(hostWorld.sandbox.minted).toEqual([8800]);
        // Success never deletes: a serving provider is the thing that was asked for.
        expect(hostWorld.sandbox.deletions).toEqual([]);

        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toEqual({
            mode: 'create+deploy',
            sandboxId: 'sb-1',
            name: 'noobscript-runtime-20261119-090000',
            endpoint: MANAGEMENT,
            status: 'ready',
            port: 8800,
            public: false,
            providerUrl: PROVIDER_LINK.url,
            providerToken: PROVIDER_LINK.token,
            tokenScope: 'preview-proxy',
            notes: [],
        });
        expect(hostWorld.out).toContain(`${PREFIX} ✓ provider URL (clients): ${PROVIDER_LINK.url} (x-daytona-preview-token: ***)`);
        expect(hostWorld.out).toContain(`${PREFIX}   management:  ${MANAGEMENT}/health · /status · /logs · DELETE /`);
        expect(hostWorld.out).toContain(`${PREFIX}   yarn workspace @services/daytona sandbox down -- --sandbox sb-1`);
        // The credential-in-the-bundle rule is printed where the operator can see it.
        // The source is `candidate`, not `cargo-json`: a dry run never compiles, so the
        // artifact is the already-built file under the repo-root .rust/target that the
        // repo-root .cargo/config.toml declares (runtime-deploy.ts:874-898, the
        // `cargoArtifactCandidates` doc + body).
        expect(hostWorld.out).toContain(`${PREFIX}   artifact:    ${BIN_PATH} (candidate) + ${VAULT_PATH} INSIDE the bundle — GET /status echoes config to anonymous callers`);
    });

    it('takes --token over the created sandbox token when the operator names one', async () => {
        const { daytona, deploy } = flagsFor(['--token', 'operator-token']);
        const hostWorld = daytonaHost();
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        expect(hostWorld.ships[0]!.token).toBe('operator-token');
    });

    it('deletes a sandbox whose deploy FAILED, and still reports the client summary', async () => {
        const { daytona, deploy } = flagsFor([]);
        const hostWorld = daytonaHost({ result: failedResult() });
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(1);
        expect(hostWorld.sandbox.deletions).toEqual([1]);
        expect(hostWorld.sandbox.minted).toEqual([]);
        expect(hostWorld.err).toEqual([
            `${PREFIX} ✗ failed: the runtime never became ready`,
            `${PREFIX} note: the runtime never reported ready`,
            `${PREFIX} ── GET /logs (2 lines) ──`,
            `${PREFIX} booting ./typecrate-provider-server`,
            `${PREFIX} error: vault document refused`,
            `${PREFIX} ✓ sandbox sb-1 deleted — a failed payload is not worth billing for (--keep-on-fail keeps it)`,
        ]);
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toEqual({
            mode: 'create+deploy',
            sandboxId: 'sb-1',
            name: 'noobscript-runtime-20261119-090000',
            endpoint: MANAGEMENT,
            status: 'failed',
            port: 8800,
            public: false,
            providerUrl: null,
            notes: ['the runtime never reported ready'],
        });
    });

    it('leaves the machine alone with --keep-on-fail, and prints how to delete it', async () => {
        const { daytona, deploy } = flagsFor(['--keep-on-fail']);
        const hostWorld = daytonaHost({ result: failedResult() });
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(1);
        expect(hostWorld.sandbox.deletions).toEqual([]);
        expect(hostWorld.err.slice(-4)).toEqual([
            `${PREFIX} --keep-on-fail: sandbox sb-1 is left alive for log reading`,
            `${PREFIX} the sandbox IS BILLING until it is deleted (this verb never deletes on success):`,
            `${PREFIX}   yarn workspace @services/daytona sandbox down -- --sandbox sb-1`,
            `${PREFIX}   (a private sandbox also loses its preview tokens when it goes)`,
        ]);
    });

    it('says out loud when a delete fails, because that is still money', async () => {
        const { daytona, deploy } = flagsFor([]);
        const hostWorld = daytonaHost({ result: failedResult(), deleteFails: 'daytona api 503' });
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(1);
        expect(hostWorld.err).toContain(`${PREFIX} ✗ sandbox delete failed (daytona api 503) — delete it yourself:`);
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toMatchObject({
            status: 'failed',
            notes: ['the runtime never reported ready', 'sandbox.delete() failed — the sandbox is still billing'],
        });
    });

    it('reports a mint failure honestly: the payload IS serving, and no URL is invented', async () => {
        const { daytona, deploy } = flagsFor([]);
        const hostWorld = daytonaHost({ previewFails: 'preview proxy 403' });
        expect(await runStartDaytona(daytona, deploy, hostWorld.deps)).toBe(0);
        expect(hostWorld.sandbox.minted).toEqual([8800]);
        expect(hostWorld.sandbox.deletions).toEqual([]);
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toMatchObject({
            status: 'ready',
            providerUrl: null,
            notes: ['getPreviewLink(8800) failed (preview proxy 403) — the payload IS serving; mint the link from the Daytona console'],
        });
        // `providerToken` is absent rather than empty when there is no token.
        expect('providerToken' in (JSON.parse(hostWorld.out[hostWorld.out.length - 1]!) as Record<string, unknown>)).toBe(false);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  mainDaytona — parse first, pay later
// ══════════════════════════════════════════════════════════════════════════════

describe('mainDaytona', () => {
    it('prints its own usage for --help, and calls Daytona for nothing', async () => {
        const hostWorld = daytonaHost({ createFails: 'Daytona was never supposed to be called' });
        expect(await mainDaytona(['--help'], hostWorld.deps, {})).toBe(0);
        expect(hostWorld.out).toEqual([USAGE_DAYTONA]);
        expect(hostWorld.creates).toEqual([]);
        expect(hostWorld.cargo).toEqual([]);
    });

    it('refuses a contradiction without spending money on it', async () => {
        const hostWorld = daytonaHost({ createFails: 'Daytona was never supposed to be called' });
        expect(await mainDaytona(['--teardown'], hostWorld.deps, {})).toBe(1);
        expect(hostWorld.err).toEqual([
            `${PREFIX} ✗ start:daytona always creates a sandbox — --teardown (free a slot, ship nothing) is deploy:endpoint's verb`,
            '',
            USAGE_DAYTONA,
        ]);
        expect(hostWorld.creates).toEqual([]);
    });

    it('reaches Daytona only past the parser, and reports a create failure as one line', async () => {
        const hostWorld = daytonaHost({ createFails: 'DAYTONA_API_KEY is not set' });
        // A bare run is a REAL run (no --dry-run), so createSandbox is reached — and its
        // failure must surface as a message, not as an unhandled rejection.
        expect(await mainDaytona([], hostWorld.deps, {})).toBe(1);
        expect(hostWorld.creates).toEqual([{ public: true }]);
        expect(hostWorld.err).toEqual([
            `${PREFIX} ✗ createSandbox failed: DAYTONA_API_KEY is not set`,
            `${PREFIX} no sandbox was created, so nothing is billing (a half-built sandbox is deleted by createSandbox itself)`,
        ]);
        expect(JSON.parse(hostWorld.out[hostWorld.out.length - 1]!)).toEqual({
            mode: 'create+deploy',
            sandboxId: '<not created>',
            name: '<not created>',
            endpoint: '<Daytona preview link, runner port 7777>',
            status: 'failed',
            port: 8800,
            public: true,
            providerUrl: null,
            notes: ['createSandbox failed: DAYTONA_API_KEY is not set'],
        });
    });

    it('names the usage screen it documents, so --help and the README cannot drift apart', () => {
        expect(USAGE_DAYTONA.split('\n')[0]).toBe('usage: yarn workspace @typecrate/provider start:daytona [flags]');
        expect(USAGE_DAYTONA).toContain('the management endpoint stays on');
        expect(USAGE_DAYTONA).toContain("--ttl <minutes>     Daytona's kill fence; default NONE, so the sandbox bills until deleted");
    });
});
