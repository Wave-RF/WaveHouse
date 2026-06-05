<wizard-report>
# PostHog post-wizard report

The wizard has completed a deep integration of PostHog analytics into the WaveHouse documentation site. PostHog is initialized via a browser snippet in a new `src/components/PostHog.astro` component, imported into the existing `src/components/Head.astro` override. The initialization is wrapped in a `window.__posthog_initialized` guard to prevent stack overflow during Astro View Transitions (ClientRouter) soft navigation. Pageviews are tracked automatically via `capture_pageview: 'history_change'`. Event capture scripts in Hero.astro, Footer.astro, and HomepageCtaTracking.astro (imported by index.mdx) attach click listeners using the `astro:page-load` event pattern to work correctly across both hard and soft navigations.

| Event | Description | File |
|-------|-------------|------|
| `hero_cta_clicked` | User clicked a CTA button in the homepage hero section (Get Started or View on GitHub). Properties: `cta_text`, `cta_href`, `cta_variant`. | `src/components/Hero.astro` |
| `footer_link_clicked` | User clicked an external link in the site footer (GitHub, Discussions, Changelog, Contributing, Support, Security, License). Properties: `link_name`, `link_href`. | `src/components/Footer.astro` |
| `homepage_cta_clicked` | User clicked a primary CTA in the homepage closing section (Get started in five minutes, Star on GitHub). Properties: `cta_text`, `cta_href`. | `src/components/HomepageCtaTracking.astro` |

## Next steps

We've built some insights and a dashboard for you to keep an eye on user behavior, based on the events we just instrumented:

- [Analytics basics (wizard) — Dashboard](https://us.posthog.com/project/419725/dashboard/1675839)
- [Total CTA Clicks (Last 30 Days)](https://us.posthog.com/project/419725/insights/9PDvsQMn)
- [Hero CTA Clicks by Button](https://us.posthog.com/project/419725/insights/fN5rr2n7)
- [Footer Link Clicks by Destination](https://us.posthog.com/project/419725/insights/qPUrkXKi)
- [Homepage CTA Clicks Over Time](https://us.posthog.com/project/419725/insights/SkpKT5uj)
- [Engagement Events Compared](https://us.posthog.com/project/419725/insights/YBywb6Ze)

### Agent skill

We've left an agent skill folder in your project. You can use this context for further agent development when using Claude Code. This will help ensure the model provides the most up-to-date approaches for integrating PostHog.

</wizard-report>
