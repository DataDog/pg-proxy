---
# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/)
# Copyright 2026-Present Datadog, Inc.
title: "Contribute to the documentation"
description: "Edit, preview, and validate the Astro documentation site."
---

Use this guide to change the documentation website. It assumes you have a `pg-proxy` checkout, Node.js 22 or newer, and npm available.

## 1. Install the site dependencies

From the repository root:

```sh
cd docs/site
npm ci
```

The site is a standalone npm project with its own lockfile.

## 2. Choose the page type

Follow the [Divio documentation system](https://docs.divio.com/documentation-system/):

| Directory under `src/content/docs` | Purpose |
| --- | --- |
| `tutorials/` (deferred) | Guide a learner through a fixed exercise with expected results. |
| `how-to/` | Help an experienced reader complete a specific task. |
| `reference/` | Describe settings, interfaces, defaults, and constraints. |
| `explanation/` | Explain concepts, design choices, and tradeoffs. |

Choose one purpose per page and link to other types when needed.

## 3. Edit or add Markdown

Start a new page with:

```yaml
---
title: "A specific page title"
description: "What the reader will learn or accomplish."
---
```

When the page is ready to publish, add it to the relevant sidebar group in `astro.config.mjs` and to `src/content/docs/index.md`. A file at `how-to/local-development.md` has slug `how-to/local-development` and URL `/how-to/local-development/`.

For unfinished pages, add `draft: true` to the frontmatter. View the draft at its URL with the development server; production builds and search exclude it. Keep it out of the explicit sidebar and published page links until the draft flag is removed.

Link to other pages with relative URLs, such as `../../how-to/local-development/` from `explanation/pooling.md`, so the site works at any path prefix. Link to repository files with full `https://github.com/DataDog/pg-proxy/blob/main/...` URLs.

Sidebar page entries use `{ slug: "path/to/page" }` so their labels match the page title. Set the title in the page frontmatter rather than adding a separate navigation label.

Keep the homepage as an index. Put appearance changes in `src/styles/custom.css`. Use ordinary Markdown for content.

## 4. Preview edits

```sh
npm run dev -- --host 127.0.0.1 --port 4321 --background
```

Open the local address printed by Astro. Check the page, navigation, and links. If another server already uses port 4321, choose an unused port or stop the server you started.

## 5. Validate the production output

Stop the development server before reusing its port:

```sh
npm run astro -- dev stop
npm run build
npm run preview -- --host 127.0.0.1 --port 4321
```

Check search in the production preview because its index is generated during the build. A successful site build checks rendering, not whether proxy commands work; verify executable examples separately.

To produce a self-contained static site and check every local link, anchor, redirect, and asset, run:

```sh
npm run build:static
```

See [Publish the static site](#7-publish-the-static-site).

## 6. Prepare the change

Include Markdown, navigation, and any necessary redirects. When moving a page, update inbound links and add its old URL to `redirects` in `astro.config.mjs`. Keep `dist/`, `dist-static/`, `.astro/`, and `node_modules/` out of Git.

## 7. Publish the static site

`npm run build:static` writes plain HTML to `dist-static/`. Directory links point at explicit `index.html` files, so the output works on hosts without directory-index routing, and the search index is rebuilt to match. To host the site below a path prefix, pass the prefix:

```sh
npm run build:static -- /pg-proxy/
```

Upload the contents of `dist-static/` with `index.html` at the site root. Rebuild when changing the prefix; it is part of the generated URLs.
