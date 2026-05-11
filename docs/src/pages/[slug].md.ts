import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';

export async function getStaticPaths() {
	const docs = await getCollection('docs');
	return docs
		.filter((doc) => doc.id !== 'index')
		.map((doc) => ({ params: { slug: doc.id }, props: { body: doc.body ?? '' } }));
}

export const GET: APIRoute = ({ props }) => {
	return new Response(props.body, {
		headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
	});
};
