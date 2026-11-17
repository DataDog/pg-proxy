<!--
Unless explicitly stated otherwise all files in this repository are licensed
under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/)
Copyright 2026-Present Datadog, Inc.
-->

# Documentation

This directory holds the PG Proxy documentation site. It uses [starlight as the framework](https://starlight.astro.build/). The homepage and guides are ordinary Markdown in `src/content/docs`.

## Local preview

From the repository root:

```bash
cd docs/site
npm ci
npm run dev -- --host 127.0.0.1 --port 4321 --background
```

Use background mode when starting the development server. Manage it from the docs project directory:

```bash
npm run astro -- dev status
npm run astro -- dev logs
npm run astro -- dev stop
```

## Production build

From the docs project directory:

```bash
npm run build
npm run preview -- --host 127.0.0.1 --port 4321
```

## Documentation structure

We follow the [Divio documentation system](https://docs.divio.com/documentation-system/). Choose a page's purpose before writing it:

| Directory in `src/content/docs` | Reader's need | Write |
| --- | --- | --- |
| `tutorials/` (deferred) | Learn through practice | A guided exercise with prerequisites, a fixed path, expected results, and cleanup. |
| `how-to/` | Complete a known task | Focused steps with an outcome and enough flexibility to use existing settings. |
| `reference/` | Look up facts | Precise options, defaults, constraints, APIs, and package responsibilities. |
| `explanation/` | Understand the design | Concepts, context, reasons, and tradeoffs. |

Keep each page focused on one purpose. Link to the other types when needed:
a tutorial can link to an explanation, but should not pause for a design essay.
Put option tables in reference rather than repeating them across guides.
The homepage remains a documentation index. Tutorials are deferred for now; show only sections with content.

For the complete website workflow, see [Contribute to the documentation](src/content/docs/how-to/documentation-contributing.md).

## Draft pages

Set `draft: true` in a page's frontmatter to keep it viewable with `npm run dev`
but excluded from production builds and search. Open its URL directly during
development. Do not add a draft to the explicit sidebar or link to it from
published pages. Remove the flag and add navigation links when it is ready.

## Add or edit a page

1. Edit a Markdown file in the appropriate directory. Start a new page with:

   ```yaml
   ---
   # Unless explicitly stated otherwise all files in this repository are licensed
   # under the Apache-2.0 License.
   # This product includes software developed at Datadog (https://www.datadoghq.com/)
   # Copyright 2026-Present Datadog, Inc.
   title: "A specific page title"
   description: "What the reader will learn or accomplish."
   ---
   ```

   Keep the license header as YAML comments inside the frontmatter, not as an
   HTML comment, so it does not reach the built page. CI checks both.

2. For a published page, add its slug to the matching sidebar section in `astro.config.mjs` and its
   link to `src/content/docs/index.md`. For example, `how-to/set-pool-limits.md`
   has slug `how-to/set-pool-limits` and URL `/how-to/set-pool-limits/`.
3. Use ordinary Markdown. Keep appearance in `src/styles/custom.css`. Link to
   other pages with relative URLs (for example `../pooling/`) so the site works
   at any path prefix, and to repository files with full GitHub URLs.
4. Preview the page, run `npm run build`, and check its links and navigation.
   Search is indexed during the production build, so check it with the production
   preview. Stop the dev server before using the same port for preview:
   `npm run astro -- dev stop`.
5. Verify executable steps against the current library. A site build validates
   rendering, not the behavior of the proxy. Keep expected output current and
   clearly identify prerequisites and local-only examples.

When moving a published page, update inbound links and add a redirect in
`astro.config.mjs`. Keep generated `dist/`, `dist-static/`, `.astro/`, and `node_modules/` out of Git.

## Static site

From this directory, after `npm ci`:

```bash
npm run build:static
```

This builds into `dist-static/`, changes directory links to explicit `index.html`
filenames, and rebuilds search with those filenames. It checks local links,
anchors, redirects, and assets before finishing. Draft pages stay excluded.
Nothing is uploaded.

To host the site below a path prefix, pass it: `npm run build:static -- /pg-proxy/`.
The prefix is part of the generated URLs, so rebuild when changing it. Upload the
contents of `dist-static/` with `index.html` at the site root. The ordinary
`dist/` output assumes hosting at `/` with directory-index routing.

## Deployment

[`.github/workflows/docs-pages.yml`](../../.github/workflows/docs-pages.yml)
runs `npm run build:static` and deploys `dist-static/` to GitHub Pages on each
push to `main` that changes `docs/site/`. It reads the path prefix from the
Pages settings, so the same workflow works for a private site, a public
project site (`/pg-proxy/`), or a custom domain. Run it by hand from the
Actions tab with **Run workflow**.

## Astro guides

Consult the relevant [Astro documentation](https://docs.astro.build) before working on these tasks:

- [Adding pages, dynamic routes, or middleware](https://docs.astro.build/en/guides/routing/)
- [Working with Astro components](https://docs.astro.build/en/basics/astro-components/)
- [Using React, Vue, Svelte, or other framework components](https://docs.astro.build/en/guides/framework-components/)
- [Adding or managing content](https://docs.astro.build/en/guides/content-collections/)
- [Adding styles or using Tailwind](https://docs.astro.build/en/guides/styling/)
- [Supporting multiple languages](https://docs.astro.build/en/guides/internationalization/)

Use **PG Proxy** in prose; preserve exact command, package, path, and option names in code.
