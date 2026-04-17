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
const isLight = document.documentElement.dataset.theme === 'light';
mermaid.initialize({
  startOnLoad: true,
  theme: 'base',
  securityLevel: 'loose',
  themeVariables: {
    fontFamily: 'Helvetica Neue, Helvetica, Arial, sans-serif',
    fontSize: '15px',
    primaryColor: '#0e7f8f',
    primaryTextColor: '#ffffff',
    primaryBorderColor: '#5bbfcf',
    lineColor: isLight ? '#576270' : '#c2c9d1',
    secondaryColor: '#475569',
    tertiaryColor: isLight ? '#f8f9fa' : '#1a1a1a',
    clusterBkg: isLight ? '#f1f5f9' : '#242a33',
    clusterBorder: isLight ? '#cbd5e1' : '#3d4752',
    edgeLabelBackground: isLight ? '#ffffff' : '#1a1a1a',
    titleColor: isLight ? '#1a1a1a' : '#f8f9fa',
    labelBoxBorderColor: '#5bbfcf',
    noteBkgColor: isLight ? '#fef3c7' : '#422006',
    noteBorderColor: '#f59e0b',
  },
  flowchart: {
    padding: 24,
    nodeSpacing: 48,
    rankSpacing: 60,
    htmlLabels: true,
    curve: 'basis',
    useMaxWidth: true,
  },
});
`,
				},
			],
			customCss: ['./src/styles/custom.css'],
			sidebar: [
				{ label: 'Home', link: '/' },
				{ label: 'Getting Started', slug: 'getting-started' },
				{ label: 'Why WaveHouse?', slug: 'why-wavehouse' },
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
