/**
 * Converts Markdown files from /docs/ into Starlight-compatible .mdx files.
 *
 * - Injects YAML frontmatter (title, description, sidebar order)
 * - Strips the first H1 heading (Starlight renders title from frontmatter)
 * - Escapes MDX-hostile syntax ({curly braces}) outside code blocks/inline code
 * - Writes .mdx files to src/content/docs/
 * - Copies clean .md originals to public/md/ for the copy-as-markdown feature
 */

import { readFileSync, writeFileSync, mkdirSync, copyFileSync } from 'node:fs';
import { execSync } from 'node:child_process';
import { join, basename } from 'node:path';

const DOCS_DIR = join(import.meta.dirname, '..', '..', 'docs');
const CONTENT_DIR = join(import.meta.dirname, '..', 'src', 'content', 'docs');
const PUBLIC_MD_DIR = join(import.meta.dirname, '..', 'public', 'md');

const DOC_META = {
	'getting-started.md': {
		title: 'Getting Started',
		description:
			'Run WaveHouse locally in five minutes — ingest, query, and subscribe to real-time events.',
		order: 2,
	},
	'architecture.md': {
		title: 'Architecture',
		description:
			'System design, data flows, internal packages, and technology stack.',
		order: 3,
	},
	'api.md': {
		title: 'API Reference',
		description:
			'All endpoints, authentication, request/response formats for the WaveHouse API.',
		order: 4,
	},
	'sdk.md': {
		title: 'TypeScript SDK',
		description:
			'Zero-dependency client SDK — query builder, real-time streaming, codegen.',
		order: 5,
	},
	'configuration.md': {
		title: 'Configuration',
		description:
			'Full configuration reference — YAML settings and environment variables.',
		order: 6,
	},
	'deployment.md': {
		title: 'Deployment',
		description:
			'Standalone, clustered, Docker, releases, health checks, and schema setup.',
		order: 7,
	},
	'development.md': {
		title: 'Development',
		description:
			'Building, testing, linting, project structure, and contribution workflow.',
		order: 8,
	},
};

/**
 * Convert ```mermaid fenced blocks to raw <pre class="mermaid"> HTML.
 * This prevents Expressive Code from processing them and allows
 * client-side Mermaid.js to render them as diagrams.
 */
function convertMermaidBlocks(content) {
	return content.replace(
		/^```mermaid\n([\s\S]*?)^```/gm,
		(_, code) => {
			// Use Astro's set:html to inject the mermaid block as raw HTML,
			// bypassing MDX markdown processing (which would mangle arrows, wrap in <p>, etc.).
			// Escape backticks and ${} for the JS template literal.
			const escaped = code.replace(/`/g, '\\`').replace(/\$\{/g, '\\${');
			return `<Fragment set:html={\`<pre class="mermaid">${escaped}</pre>\`} />`;
		}
	);
}

/**
 * Escape curly braces outside fenced code blocks and inline code spans.
 * MDX interprets bare {word} as JSX expressions, causing build errors.
 */
function escapeMdxSyntax(content) {
	const lines = content.split('\n');
	let inFencedBlock = false;
	const result = [];

	for (const line of lines) {
		// Track fenced code block boundaries
		if (/^(`{3,}|~{3,})/.test(line.trimStart())) {
			inFencedBlock = !inFencedBlock;
			result.push(line);
			continue;
		}

		if (inFencedBlock) {
			result.push(line);
			continue;
		}

		// Outside fenced blocks: escape {word} patterns not inside inline code
		result.push(escapeLineOutsideInlineCode(line));
	}

	return result.join('\n');
}

/**
 * Process a single line, escaping {braces} only in non-code segments.
 */
function escapeLineOutsideInlineCode(line) {
	// Split on inline code spans (backtick-delimited)
	const parts = line.split(/(`[^`]*`)/);
	return parts
		.map((part, i) => {
			// Odd indices are inside backticks — leave untouched
			if (i % 2 === 1) return part;
			// Even indices are outside backticks — escape curly braces
			return part.replace(/\{([^}]+)\}/g, '\\{$1\\}');
		})
		.join('');
}

/**
 * Strip the first H1 heading line from content.
 * Starlight renders the title from frontmatter, so keeping the H1 duplicates it.
 */
function stripFirstH1(content) {
	const lines = content.split('\n');
	const h1Index = lines.findIndex((line) => /^#\s+/.test(line));
	if (h1Index !== -1) {
		lines.splice(h1Index, 1);
		// Also remove a blank line immediately after the heading if present
		if (h1Index < lines.length && lines[h1Index].trim() === '') {
			lines.splice(h1Index, 1);
		}
	}
	return lines.join('\n');
}

/**
 * Get the ISO date of the last git commit that touched a file.
 * Returns null if the file has no git history.
 */
function getLastUpdated(filePath) {
	try {
		const date = execSync(`git log -1 --format=%cI -- "${filePath}"`, {
			encoding: 'utf-8',
			stdio: ['pipe', 'pipe', 'ignore'],
		}).trim();
		return date || null;
	} catch {
		return null;
	}
}

/**
 * Generate YAML frontmatter block.
 */
function buildFrontmatter(meta, lastUpdated) {
	const lines = [
		'---',
		`title: "${meta.title}"`,
		`description: "${meta.description}"`,
		'sidebar:',
		`  order: ${meta.order}`,
	];
	if (lastUpdated) {
		lines.push(`lastUpdated: ${lastUpdated}`);
	}
	lines.push('---', '');
	return lines.join('\n');
}

// Ensure output directories exist
mkdirSync(CONTENT_DIR, { recursive: true });
mkdirSync(PUBLIC_MD_DIR, { recursive: true });

let converted = 0;

for (const [filename, meta] of Object.entries(DOC_META)) {
	const srcPath = join(DOCS_DIR, filename);
	const slug = basename(filename, '.md');

	let content;
	try {
		content = readFileSync(srcPath, 'utf-8');
	} catch {
		console.warn(`  SKIP ${filename} (not found)`);
		continue;
	}

	// Copy clean original to public/md/ for the copy-as-markdown feature
	copyFileSync(srcPath, join(PUBLIC_MD_DIR, filename));

	// Transform for Starlight
	const lastUpdated = getLastUpdated(srcPath);
	let mdx = stripFirstH1(content);
	mdx = convertMermaidBlocks(mdx);
	mdx = escapeMdxSyntax(mdx);
	mdx = buildFrontmatter(meta, lastUpdated) + mdx;

	writeFileSync(join(CONTENT_DIR, `${slug}.mdx`), mdx);
	converted++;
	console.log(`  OK   ${filename} → ${slug}.mdx`);
}

console.log(`\nConverted ${converted} doc(s).`);
