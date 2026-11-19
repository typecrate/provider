// ══════════════════════════════════════════════════════════════════════════════
//  entry-guard.ts — the ONE entry detector this package's three verb scripts share
//
//  WHY IT IS A SEPARATE FILE (and not three copies, as the reference package has):
//  `packages/typecrate/nexus/scripts/deploy-endpoint.ts:1034-1100` carries this
//  detector inline, and its sibling `start-modal.ts` has NO guard at all — importing
//  that file runs a deploy (start-modal.ts:25-26). This package has THREE entry files
//  (deploy-endpoint / start-modal / start-daytona) and the two wrappers import `main`
//  from the verb, so an unguarded wrapper would mean an importing test file deploys.
//  One parameterised implementation keeps the three rules from drifting apart; the
//  only per-file input is the module's OWN PATH, which the caller passes.
//
//  THE THREE RULES (unchanged from the reference, cited for the reasoning):
//    1. argv's SCRIPT POSITIONAL is this file — `vite-node scripts/x.ts …` when the
//       runner keeps the file on the command line.
//    2. the npm/yarn lifecycle line names it — `yarn <verb>` runs
//       "vite-node scripts/x.ts", and that string is in `npm_lifecycle_script`.
//    3. vite-node evaluated THIS file as its entry — a plain `vite-node <file>`
//       REWRITES process.argv and DROPS the file, so rules 1-2 are blind there, and
//       the only remaining evidence is the module-runner stack: this file's own frame
//       with nothing but runner frames above it.
//  Rule 3 is why the caller must capture the stack DURING module evaluation: inside
//  `main()` the evidence is already gone. `vitest` is checked first and wins, so
//  importing a verb from a test never starts a deploy.
// ══════════════════════════════════════════════════════════════════════════════

import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/** Signals the detector reads — every field is process state, gathered by the caller at import time. */
export type EntryPointSignals = {
    /**
     * This module's ABSOLUTE path on disk, from `modulePathOf(import.meta.url, …)`. It
     * is the only identity the detector needs: rules 1-2 match a token against it, and
     * rule 3 identifies its own stack frames.
     * (The reference package's inline copy passes a bare script NAME and rebuilds a
     * `<name>\.[cm]?[jt]sx?` regex — which silently never matches once the caller passes
     * the name WITH its extension, i.e. `deploy-endpoint.ts.ts`. Path matching has no
     * such trap, so this API takes the path and no name.)
     */
    modulePath: string;
    argv?: readonly string[];
    env?: Readonly<Record<string, string | undefined>>;
    stack?: string;
    /** True under vitest — the override that keeps an import side-effect free. */
    vitest?: boolean;
};

/** vite/vite-node append module ids and query strings; strip them before matching. */
const stripModuleQuery = (token: string): string => String(token ?? '').split('?')[0].split('#')[0];

/** Backslashes travel as forward slashes so the comparison has one shape. */
const asPath = (value: string): string => value.split('\\').join('/');

/**
 * Is `rawToken` (an argv entry or one word of a lifecycle line) THIS module?
 * An absolute token is compared after resolving `..`/symlink-free segments; a bare or
 * relative token (`scripts/deploy-endpoint.ts`, exactly what a lifecycle line holds)
 * must be a path-segment SUFFIX of the module path. A suffix match is used instead of
 * `endsWith(name)` so `deploy-endpoint.test.ts` can never match `deploy-endpoint.ts`.
 */
const matchesModule = (modulePath: string, rawToken: unknown): boolean => {
    const token = stripModuleQuery(String(rawToken ?? ''));
    if (!token) return false;
    if (token.startsWith('/') || /^[a-zA-Z]:[\\/]/.test(token)) return resolve(asPath(token)) === resolve(asPath(modulePath));
    return asPath(modulePath).endsWith(`/${asPath(token)}`);
};

/** Rule 1: argv's script positional is this file, either bare or after `--script`. */
const namesScriptPositional = (modulePath: string, argv: readonly string[] | undefined): boolean => {
    const tokens = argv ?? [];
    if (matchesModule(modulePath, tokens[1])) return true;
    const flag = tokens.indexOf('--script');
    return flag !== -1 && matchesModule(modulePath, tokens[flag + 1]);
};

/** Rule 2: the lifecycle line (`"start:modal": "vite-node scripts/start-modal.ts"`) names it. */
const namesLifecycleEntryPoint = (modulePath: string, env: Readonly<Record<string, string | undefined>> | undefined): boolean =>
    String(env?.npm_lifecycle_script ?? '')
        .split(/\s+/)
        .some((token) => matchesModule(modulePath, token));

/**
 * Rule 3: vite-node evaluated THIS file as its entry — only runner frames sit above
 * it. A consumer module in between (start-modal.ts importing deploy-endpoint.ts) is
 * exactly what this rejects, which is how the wrappers stay importable.
 */
const isRunnerEntryPoint = (stack: string | undefined, modulePath: string | undefined): boolean => {
    if (!stack || !modulePath) return false;
    const frames = stack.split('\n').map((frame) => frame.trim().replace('file://', ''));
    if (!frames.some((frame) => frame.includes('ViteNodeRunner'))) return false;
    const own = frames.reduce((found, frame, index) => (frame.includes(modulePath) ? index : found), -1);
    if (own < 0) return false;
    const callers = frames.slice(own + 1);
    const executeFile = callers.findIndex((frame) => frame.includes('ViteNodeRunner.executeFile'));
    const upTo = executeFile === -1 ? callers.length : executeFile;
    return callers.slice(0, upTo).every((frame) => frame.includes('ViteNodeRunner'));
};

/** True when — and only when — the calling module is the process entry point. */
export const isCliEntryPoint = (signals: EntryPointSignals): boolean => {
    if (signals.vitest) return false;
    if (!signals.modulePath) return false;
    if (namesScriptPositional(signals.modulePath, signals.argv)) return true;
    if (namesLifecycleEntryPoint(signals.modulePath, signals.env)) return true;
    return isRunnerEntryPoint(signals.stack, signals.modulePath);
};

/**
 * The module's own path, for rule 3. `import.meta.url` is not always a file:// URL
 * under this repo's vitest env (vite-node, the real runner, always gives one), so the
 * fallback is the documented on-disk location rather than a guess.
 */
export const modulePathOf = (importMetaUrl: string, scriptsDir: string, script: string): string => {
    try {
        return fileURLToPath(importMetaUrl);
    } catch {
        return join(scriptsDir, script);
    }
};

/** The scripts directory of THIS package, resolved from a module URL (never from cwd). */
export const scriptsDirOf = (importMetaUrl: string): string => {
    try {
        return dirname(fileURLToPath(importMetaUrl));
    } catch {
        // vitest may hand a non-file URL; the package root is then the cwd the verb
        // ran from (yarn workspace … runs it from the package directory).
        return join(process.cwd(), 'scripts');
    }
};

/**
 * The live signals, gathered in ONE call at module scope. The stack must be captured
 * here, not inside `main()`.
 */
export const entrySignals = (modulePath: string): EntryPointSignals => ({
    modulePath,
    argv: process.argv,
    env: process.env,
    stack: new Error().stack,
    vitest: Boolean(process.env.VITEST),
});
