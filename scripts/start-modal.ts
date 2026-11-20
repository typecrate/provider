// ══════════════════════════════════════════════════════════════════════════════
//  start-modal.ts — `start:modal`: the generic verb, aimed at a Modal box the
//  OPERATOR names, with the vault's box URL as the last-resort default
//
//      yarn workspace @typecrate/provider start:modal                    # the vault's box
//      yarn workspace @typecrate/provider start:modal -- --dry-run       # the plan, offline
//      yarn workspace @typecrate/provider start:modal -- --teardown      # free the slot
//      yarn workspace @typecrate/provider start:modal -- --endpoint https://other--my-box.us-east.modal.run
//      TYPECRATE_PROVIDER_ENDPOINT=https://… yarn workspace @typecrate/provider start:modal
// ══════════════════════════════════════════════════════════════════════════════
//
//  WHY THIS PACKAGE DOES NOT JUST COPY nexus's start-modal.ts. nexus treats the box URL
//  as fixed configuration and reads it from the vault with no override
//  (`packages/typecrate/nexus/scripts/start-modal.ts:25-26`: `main([Object.values(modalContainer)[0],
//  ...argv])` — the vault value always wins, and a positional URL has to AGREE with it).
//  That works for one workload on one box, but the box this repo owns is SHARED: under
//  runtime v2 it is a multi-app registry, so deploying the provider as the app "provider"
//  coexists with whatever else the box serves (/app/nexus/, /app/dashboard/, …) and no
//  peer is displaced. The URL is still a first-class parameter, because WHICH box a
//  workload lands on is an operator decision (resources, admin auth, blast radius), not
//  package identity — a blanket default would put the provider on someone else's origin
//  with no chance to say no. The precedence is, in this order:
//
//      1. --endpoint <url>            (or the bare positional URL)   — explicit per run
//      2. TYPECRATE_PROVIDER_ENDPOINT                                 — CI / a second box,
//                                                                      without editing the repo
//      3. @config/vault modalContainer                                — the repo's own box
//
//  A SECOND BOX gets its own URL, and it is DERIVED, not typed out of a hat: Modal names
//  a deployment `https://<source>--<BOX_LABEL>.modal.run` (the file that defines the box
//  says so in the `BOX_LABEL` comment at services/modal/deploy/sandbox/modal-sandbox-container.py
//  `:343-345`, and the `DRIVER CONFIGURATION` block at `:164-191` lists the four env names that
//  move it — search `SANDBOX_BOX_LABEL` rather than trusting a line number, that file is edited
//  concurrently).
//  The provider's internal port is the runner's choice now (v2 auto-allocates unless
//  --port names one), and it is never public: clients always use `/app/provider/…` on the
//  box's ONE origin (see the README runbook "Getting a dedicated box").
//
//  Everything except the URL is the generic verb: scripts/deploy-endpoint.ts, with every
//  flag forwarded verbatim. Nothing here re-implements cargo, packaging, the app policy
//  or the readiness poll — those live in @services/daytona/sandbox (runtime-deploy.ts),
//  which is the client BOTH this package and nexus share.
//
//  IMPORT IS A NO-OP: unlike nexus's equivalent, this file is guarded (entry-guard.ts),
//  so start-modal.test.ts can import resolveEndpoint() without deploying anything.
// ══════════════════════════════════════════════════════════════════════════════

import { modalContainer } from '@config/vault';

import { ENDPOINT_ENV, PREFIX, createDefaultDeps, main, parseArgs } from './deploy-endpoint';
import type { ProviderDeps } from './deploy-endpoint';
import { entrySignals, isCliEntryPoint, modulePathOf, scriptsDirOf } from './entry-guard';

/** This verb's own file name (the entry guard's identity). */
export const ENTRY_SCRIPT_NAME_MODAL = 'start-modal.ts';
const SCRIPTS_DIR = scriptsDirOf(import.meta.url);
const MODULE_FILE_PATH = modulePathOf(import.meta.url, SCRIPTS_DIR, ENTRY_SCRIPT_NAME_MODAL);

/** Where the endpoint for this run came from — printed once, because "which box?" must never be a guess. */
export type EndpointSource = 'argument' | 'environment' | 'vault';

/** The vault's box map, as the single default (`config/vault/src/container/modal-container.ts:1-3`). */
export const VAULT_BOXES: Record<string, string> = modalContainer as Record<string, string>;

/** `[label, url]` of the vault's first box, or `['', '']` when the vault names none. */
export const vaultBox = (boxes: Record<string, string> = VAULT_BOXES): [string, string] => {
    const [label = '', url = ''] = Object.entries(boxes)[0] ?? [];
    return [label, url];
};

/** Does the command line itself name this exact endpoint (as `--endpoint <url>` or as the positional)? */
const namedOnCommandLine = (argv: readonly string[], endpoint: string): boolean => {
    const flag = argv.indexOf('--endpoint');
    if (flag !== -1) return argv[flag + 1] === endpoint;
    return argv.some((token) => token === endpoint && !token.startsWith('-'));
};

/**
 * The precedence rule, as data: returns the argv to hand the verb (with `--endpoint`
 * prepended only when NEITHER the command line NOR the environment supplied one) plus
 * the source it resolved, for the one line it prints.
 *
 * It deliberately does NOT inject anything when the command line is already broken
 * (parse errors) or when `--help` was asked for: the verb reports those, and inventing an
 * endpoint for a mistyped flag hides the typo behind a deploy.
 */
export const resolveEndpoint = (
    argv: readonly string[],
    env: Readonly<Record<string, string | undefined>> = process.env,
    boxes: Record<string, string> = VAULT_BOXES,
): { argv: string[]; source: EndpointSource; endpoint: string; label: string } => {
    const { flags } = parseArgs(argv, env);
    const [label, url] = vaultBox(boxes);
    // The command line already answered the question, in one of two ways.
    if (flags.endpoint) return { argv: [...argv], source: namedOnCommandLine(argv, flags.endpoint) ? 'argument' : 'environment', endpoint: flags.endpoint, label };
    if (flags.help || !url) return { argv: [...argv], source: 'argument', endpoint: '', label };
    // No endpoint yet, so the ONLY thing the verb would complain about is the missing
    // endpoint itself. Re-parse the line AS IT WOULD BE AFTER the injection: any error
    // that survives is a real defect in the operator's command line, and inventing an
    // endpoint for it would hide a typo behind a deploy to someone else's box.
    const injected = ['--endpoint', url, ...argv];
    if (parseArgs(injected, env).errors.length) return { argv: [...argv], source: 'argument', endpoint: '', label };
    return { argv: injected, source: 'vault', endpoint: url, label };
};

/** The printed provenance line. One line, at the top, before any cargo or network output. */
const describeSource = (source: EndpointSource, endpoint: string, label: string): string =>
    `${PREFIX} endpoint: ${endpoint || '(none)'}\n${PREFIX}   source: ${
        source === 'argument'
            ? 'the command line (--endpoint / the positional URL)'
            : source === 'environment'
              ? `$${ENDPOINT_ENV}`
              : `@config/vault modalContainer${label ? `.${label}` : ''} — override it with --endpoint or ${ENDPOINT_ENV} to place the provider app on a different origin`
    }`;

/**
 * `start:modal` end to end: resolve the URL, then run the verb with every flag forwarded.
 * Returns the exit code (never calls process.exit), so a caller can wrap it.
 */
export const mainModal = async (
    argv: readonly string[] = process.argv.slice(2),
    options: { env?: Record<string, string | undefined>; boxes?: Record<string, string>; deps?: ProviderDeps } = {},
): Promise<number> => {
    const env = options.env ?? process.env;
    // ONE env source: the resolver and the verb must read the same TYPECRATE_PROVIDER_*
    // values, otherwise an injected env would resolve an endpoint in this file that the
    // verb then re-derives differently (and a test would have to set it twice).
    const deps: ProviderDeps = { ...(options.deps ?? createDefaultDeps()), env };
    const log = deps.log ?? ((text: string) => console.log(text));
    const resolved = resolveEndpoint(argv, env, options.boxes ?? VAULT_BOXES);
    if (resolved.endpoint) log(describeSource(resolved.source, resolved.endpoint, resolved.label));
    return main(resolved.argv, deps);
};

// ── the entry point (guarded: importing this file must never deploy) ──
if (isCliEntryPoint(entrySignals(MODULE_FILE_PATH))) {
    mainModal().then(
        (code) => {
            process.exitCode = code;
        },
        (failure) => {
            console.error(`${PREFIX} ✗ unexpected failure: ${failure instanceof Error ? (failure.stack ?? String(failure)) : String(failure)}`);
            process.exitCode = 1;
        },
    );
}
