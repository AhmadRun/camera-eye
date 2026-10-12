#!/usr/bin/env node
/**
 * Export the Realtime voice session's instructions and tool definitions for
 * the Rust server (server-rs/src/providers/openai).
 *
 * The tools are built from the same modules the browser runs
 * (src/voice/actionSchemas.js, src/tools/*, server/providers/openai/*), so the
 * Rust server loads this generated file instead of duplicating them by hand.
 * Run it after `npm ci` and whenever those modules change:
 *
 *   node scripts/export-voice-tools.mjs [output.json]
 *
 * Default output: server-rs/generated/realtime-session.json, the Rust
 * server's default path (relative to CE_ROOT); point the server elsewhere with
 * CE_REALTIME_SESSION_FILE.
 */
import { mkdirSync, renameSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { realtimeInstructions } from '../server/providers/openai/instructions.js';
import { standaloneVoiceTools } from '../server/standalone/voiceTools.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const defaultDir = path.join(root, 'server-rs', 'generated');
const output = path.resolve(
  process.argv[2] || path.join(defaultDir, 'realtime-session.json'),
);

// Exactly what server/standalone/vite.config.js configures: the default
// annotation guidance and the standalone voice tool set.
const payload = {
  instructions: realtimeInstructions(),
  tools: standaloneVoiceTools(),
};

mkdirSync(path.dirname(output), { recursive: true });
// The default directory holds generated output only; keep it out of git.
if (path.dirname(output) === defaultDir) {
  writeFileSync(path.join(defaultDir, '.gitignore'), '*\n');
}
const tmp = `${output}.${process.pid}.tmp`;
writeFileSync(tmp, `${JSON.stringify(payload, null, 2)}\n`);
renameSync(tmp, output);
console.log(
  `Wrote ${payload.tools.length} voice tools and the session instructions to ${path.relative(root, output) || output}`,
);
