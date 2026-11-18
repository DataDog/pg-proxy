// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  redirects: {
    '/reference/local-command': '/reference/configuration/configuration-file/',
    '/reference/configuration/local-development': '/reference/configuration/configuration-file/',
    '/guides/run-locally': '/how-to/local-development/',
    '/tutorials/first-query': '/how-to/local-development/',
    '/concepts/architecture': '/',
    '/explanation/architecture': '/',
    '/concepts/pooling': '/explanation/pooling/',
    '/explanation/observability': '/reference/observability/',
    '/introduction': '/',
    '/reference/configuration/yaml': '/reference/configuration/configuration-file/',
    '/explanation/introduction': '/',
  },
  integrations: [starlight({
    title: 'PG Proxy',
    description: 'A PostgreSQL wire-protocol proxy with transaction-level connection pooling.',
    favicon: '/pg-proxy-favicon.png',
    logo: { src: './src/assets/pg-proxy-logo.webp' },
    social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/DataDog/pg-proxy' }],
    editLink: { baseUrl: 'https://github.com/DataDog/pg-proxy/edit/main/docs/site/' },
    customCss: ['./src/styles/custom.css'],
    sidebar: [
      { label: 'Overview', items: [
        { slug: '' },
        { slug: 'features' },
        { slug: 'reference/limitations' },
        { slug: 'explanation/pooler-comparison' },
      ] },
      { label: 'How-to guides', items: [
        { slug: 'how-to/local-development' },
        { slug: 'how-to/run-in-production' },
        { slug: 'how-to/set-pool-limits' },
        { slug: 'how-to/otel-metrics-to-datadog' },
      ] },
      { label: 'Reference', items: [
        { label: 'Configuration', items: [
          { slug: 'reference/configuration/configuration-file' },
          { slug: 'reference/configuration/command-line' },
          { slug: 'reference/configuration/timeouts' },
          { slug: 'reference/configuration/client-connections' },
          { slug: 'reference/configuration/client-tls' },
          { slug: 'reference/configuration/client-authentication' },
        ] },
        { label: 'Observability', items: [
          { slug: 'reference/observability' },
          { slug: 'reference/observability/metrics' },
          { slug: 'reference/observability/tracing' },
        ] },
        { slug: 'reference/requirements' },
        { slug: 'reference/compatibility' },
        { slug: 'reference/troubleshooting' },
        { label: 'Go library', items: [
          { slug: 'reference/configuration/library' },
          { slug: 'reference/configuration/adapting-configuration' },
          { slug: 'reference/packages' },
        ] },
      ] },
      { label: 'Explanation', items: [
        { slug: 'explanation/authentication' },
        { slug: 'explanation/pooling' },
        { slug: 'explanation/prepared-statements' },
        { slug: 'explanation/read-routing' },
        { slug: 'explanation/traffic-mirroring' },
        { slug: 'explanation/graceful-shutdown' },
      ] },
      { label: 'Contribute', items: [
        { slug: 'how-to/contributing' },
        { slug: 'how-to/documentation-contributing' },
      ] },
    ],
  })],
});
