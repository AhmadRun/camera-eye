/**
 * Export the MCP server's static data for the Rust server (server-rs).
 *
 *   node scripts/export-mcp-tools.mjs [--check]
 *
 * Writes server-rs/src/mcp/data/tools.json: Core's tool definitions (names,
 * titles, descriptions, input schemas, annotations, required services and
 * MCP Apps metadata) in composition order, the surface table, the panel's
 * runtime script, and the data the tools embed. The Rust server includes
 * the file at build time, so the browser and both servers describe the same
 * tools. `--check` exits non-zero when the file is out of date.
 */

import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { coreTools, TOOL_SURFACES } from '../src/tools/index.js';
import { panelRuntime } from '../src/tools/panel.js';
import { BHOTE_KOSHI_FLOOD_PATH } from '../src/data/bhoteKoshiFloodPath.js';
import { LAYER_STATE_REGISTRY } from '../src/data/layerState.js';

const target = new URL('../server-rs/src/mcp/data/tools.json', import.meta.url);
const { version } = JSON.parse(
  readFileSync(new URL('../package.json', import.meta.url), 'utf8'),
);

const exported = {
  version,
  tools: coreTools.map((tool) => ({
    name: tool.name,
    kind: tool.kind,
    title: tool.title,
    description: tool.description,
    inputSchema: tool.inputSchema,
    annotations: tool.annotations,
    requires: tool.requires,
    ui: tool.ui,
  })),
  surfaces: TOOL_SURFACES,
  layerRegistry: LAYER_STATE_REGISTRY.map(({ id, token }) => ({ id, token })),
  bhoteKoshiFloodPath: BHOTE_KOSHI_FLOOD_PATH,
  panelRuntime: String(panelRuntime),
};

const text = `${JSON.stringify(exported, null, 2)}\n`;
if (process.argv.includes('--check')) {
  let current = '';
  try {
    current = readFileSync(target, 'utf8');
  } catch {
    /* missing counts as out of date */
  }
  if (current !== text) {
    console.error(
      `${fileURLToPath(target)} is out of date; run node scripts/export-mcp-tools.mjs`,
    );
    process.exit(1);
  }
} else {
  writeFileSync(target, text);
  console.log(`Wrote ${fileURLToPath(target)}`);
}
