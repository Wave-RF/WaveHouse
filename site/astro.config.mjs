// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLlmsTxt from 'starlight-llms-txt';

// https://astro.build/config
export default defineConfig({
	site: 'https://docs.wavehouse.dev',
	integrations: [
		starlight({
			title: 'WaveHouse',
			lastUpdated: true,
			social: [
				{
					icon: 'github',
					label: 'GitHub',
					href: 'https://github.com/Wave-RF/WaveHouse',
				},
			],
			head: [
				{
					tag: 'script',
					attrs: { type: 'module' },
					content: `
import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
const theme = document.documentElement.dataset.theme === 'light' ? 'default' : 'dark';
mermaid.initialize({ startOnLoad: true, theme });
`,
				},
			],
			customCss: ['./src/styles/custom.css'],
			sidebar: [
				{ label: 'Home', link: '/' },
				{ label: 'Getting Started', slug: 'getting-started' },
				{
					label: 'Guides',
					items: [
						{ label: 'Architecture', slug: 'architecture' },
						{ label: 'API Reference', slug: 'api' },
						{ label: 'TypeScript SDK', slug: 'sdk' },
					],
				},
				{
					label: 'Operations',
					items: [
						{ label: 'Configuration', slug: 'configuration' },
						{ label: 'Deployment', slug: 'deployment' },
					],
				},
				{
					label: 'Contributing',
					items: [
						{ label: 'Development', slug: 'development' },
						{
							label: 'Contributing Guide',
							link: 'https://github.com/Wave-RF/WaveHouse/blob/main/CONTRIBUTING.md',
							attrs: { target: '_blank', rel: 'noopener' },
						},
						{
							label: 'Security Policy',
							link: 'https://github.com/Wave-RF/WaveHouse/blob/main/SECURITY.md',
							attrs: { target: '_blank', rel: 'noopener' },
						},
						{
							label: 'Changelog',
							link: 'https://github.com/Wave-RF/WaveHouse/blob/main/CHANGELOG.md',
							attrs: { target: '_blank', rel: 'noopener' },
						},
					],
				},
			],
			components: {
				PageSidebar: './src/components/overrides/PageSidebar.astro',
			},
			plugins: [starlightLlmsTxt()],
		}),
	],
});
