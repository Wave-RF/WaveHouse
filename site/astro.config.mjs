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
				{ label: 'Architecture', slug: 'architecture' },
				{ label: 'API Reference', slug: 'api' },
				{ label: 'Configuration', slug: 'configuration' },
				{ label: 'Deployment', slug: 'deployment' },
				{ label: 'Development', slug: 'development' },
				{ label: 'TypeScript SDK', slug: 'sdk' },
			],
			components: {
				PageSidebar: './src/components/overrides/PageSidebar.astro',
			},
			plugins: [starlightLlmsTxt()],
		}),
	],
});
