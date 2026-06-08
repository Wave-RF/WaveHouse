// Cloudflare Worker entrypoint. Delegates to the cloudflare-md-router
// package, which serves the `.md` twin of any static page when the
// requester is a known LLM bot or explicitly asks for `text/markdown`,
// and falls back to the HTML response otherwise.
export { default } from "cloudflare-md-router/worker";
