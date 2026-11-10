import { readdir, readFile, writeFile, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Builds a self-contained static site in dist-static/ and checks its links.
// Usage: npm run build:static [-- /path/prefix/]
const root = fileURLToPath(new URL('../', import.meta.url));
const output = path.join(root, 'dist-static');
const prefix = process.argv[2] ?? '/';
if (process.argv.length > 3 || !/^\/([a-zA-Z0-9._-]+\/)*$/.test(prefix)) {
  throw new Error('Supply at most one path prefix that starts and ends with "/", such as /pg-proxy/.');
}
const base = prefix;
const origin = 'https://site.invalid';

function run(args) {
  const result = spawnSync('npm', args, {
    cwd: root,
    stdio: 'inherit',
    env: { ...process.env, ASTRO_TELEMETRY_DISABLED: '1' },
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`npm ${args.join(' ')} failed`);
}

async function listFiles(dir, prefix = '') {
  const files = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const name = prefix + entry.name;
    if (entry.isDirectory()) files.push(...await listFiles(path.join(dir, entry.name), `${name}/`));
    else if (entry.isFile()) files.push(name);
  }
  return files;
}

// Some static hosts serve exact filenames, not nested directory indexes. Astro's
// base handles generated assets; Markdown links are relative to their page, so
// resolve every local URL against the page and point it at an explicit file.
function staticURL(value, file, files) {
  if (value.startsWith('#') || value.startsWith('//') || /^[a-z][a-z0-9+.-]*:/i.test(value)) return value;
  const url = new URL(decodeHTML(value), `${origin}${base}${file}`);
  if (url.origin !== origin) return value;
  // Astro does not prefix redirect destinations with the base.
  let relative;
  if (url.pathname.startsWith(base)) relative = url.pathname.slice(base.length);
  else if (value.startsWith('/')) relative = url.pathname.slice(1);
  else return value;
  const index = relative ? `${relative.replace(/\/$/, '')}/index.html` : 'index.html';
  if (files.has(decodeURIComponent(index))) relative = index;
  return `${base}${relative}${url.search}${url.hash}`;
}

const urlAttribute = /\b(href|src|action)=(['"])(.*?)\2/gs;
const refreshAttribute = /\bcontent=(['"])(\d+;\s*url=)(.*?)\1/gi;
const srcsetAttribute = /\bsrcset=(['"])(.*?)\1/gs;
const decodeHTML = (value) => value.replace(/&amp;/g, '&').replace(/&quot;/g, '"')
  .replace(/&#39;|&apos;/g, "'").replace(/&#(x[0-9a-f]+|\d+);/gi,
    (_, number) => String.fromCodePoint(number[0].toLowerCase() === 'x' ? parseInt(number.slice(1), 16) : Number(number)));

run(['run', 'build', '--', '--base', base, '--outDir', output]);
const files = new Set(await listFiles(output));
const pages = new Map();
for (const file of files) {
  if (!file.endsWith('.html')) continue;
  let html = await readFile(path.join(output, file), 'utf8');
  html = html.replace(urlAttribute, (_, key, quote, value) => `${key}=${quote}${staticURL(value, file, files)}${quote}`);
  html = html.replace(refreshAttribute, (_, quote, prefix, value) => `content=${quote}${prefix}${staticURL(value, file, files)}${quote}`);
  html = html.replace(srcsetAttribute, (_, quote, value) => `srcset=${quote}${value.split(',').map((candidate) =>
    candidate.trim().replace(/^\S+/, (url) => staticURL(url, file, files))).join(', ')}${quote}`);
  await writeFile(path.join(output, file), html);
  pages.set(file, {
    html,
    ids: new Set([...html.matchAll(/\bid=(['"])(.*?)\1/gs)].map((match) => decodeHTML(match[2]))),
  });
}

// Fail the build on broken page links, anchors, redirects, or asset URLs.
let checked = 0;
for (const [file, page] of pages) {
  const urls = [...page.html.matchAll(urlAttribute)].map((match) => match[3]);
  urls.push(...[...page.html.matchAll(refreshAttribute)].map((match) => match[3]));
  for (const match of page.html.matchAll(srcsetAttribute)) {
    urls.push(...match[2].split(',').map((candidate) => candidate.trim().split(/\s+/)[0]));
  }
  for (const value of urls) {
    const url = new URL(decodeHTML(value), `${origin}${base}${file}`);
    if (url.origin !== origin) continue;
    if (!url.pathname.startsWith(base)) throw new Error(`${file}: URL escapes site prefix: ${value}`);
    const target = decodeURIComponent(url.pathname.slice(base.length));
    if (!files.has(target)) throw new Error(`${file}: missing file for ${value}`);
    const targetPage = pages.get(target);
    if (url.hash && targetPage && !targetPage.ids.has(decodeURIComponent(url.hash.slice(1)))) {
      throw new Error(`${file}: missing anchor for ${value}`);
    }
    checked++;
  }
}

// Discard the original search index so stale fragments cannot survive a rebuild.
// Pagefind is installed by Starlight and pinned in package-lock.json.
await rm(path.join(output, 'pagefind'), { recursive: true, force: true });
run(['exec', '--no', '--', 'pagefind', '--site', output, '--keep-index-url', '--quiet']);
console.log(`Checked ${checked} local links and assets. Upload the contents of ${output} with index.html at ${base}.`);
console.log('This command only builds files; it does not publish them.');
