import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  redirects: {
    '/reference/local-command': '/how-to/local-development/#settings',
    '/reference/configuration/local-development': '/how-to/local-development/#settings',
    '/guides/run-locally': '/how-to/local-development/',
    '/tutorials/first-query': '/how-to/local-development/',
    '/concepts/architecture': '/',
    '/explanation/architecture': '/',
    '/concepts/pooling': '/explanation/pooling/',
    '/explanation/observability': '/reference/observability/',
    '/introduction': '/',
    '/reference/configuration/yaml': '/how-to/local-development/#settings',
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
        { slug: 'how-to/otel-metrics-to-datadog' },
        { slug: 'how-to/documentation-contributing' },
        { slug: 'how-to/set-pool-limits' },
      ] },
      { label: 'Reference', items: [
        { label: 'Configuration', items: [
          { slug: 'reference/configuration/timeouts' },
          { slug: 'reference/configuration/client-connections' },
        ] },
        { label: 'Observability', items: [
          { slug: 'reference/observability' },
          { slug: 'reference/observability/metrics' },
          { slug: 'reference/observability/tracing' },
        ] },
        { slug: 'reference/compatibility' },
        { label: 'Go library', items: [
          { slug: 'reference/configuration/library' },
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
    ],
  })],
});
