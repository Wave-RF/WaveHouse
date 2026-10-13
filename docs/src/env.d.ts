/// <reference path="../.astro/types.d.ts" />

// Starlight exposes its built-in components to overrides through these virtual
// modules (Vite resolves them at build time), but only ships their TYPES in an
// internal declaration file that isn't visible to user code — so `astro check`
// can't find them. Declare the ones the Footer and Header overrides re-render.
// Starlight 0.42 no longer ships that declaration file at all, so the user
// config module (used by the Header and SiteTitle overrides) is typed here too.
declare module "virtual:starlight/components/EditLink" {
  const Component: import("astro").AstroComponentFactory;
  export default Component;
}
declare module "virtual:starlight/components/LastUpdated" {
  const Component: import("astro").AstroComponentFactory;
  export default Component;
}
declare module "virtual:starlight/components/Pagination" {
  const Component: import("astro").AstroComponentFactory;
  export default Component;
}
declare module "virtual:starlight/components/LanguageSelect" {
  const Component: import("astro").AstroComponentFactory;
  export default Component;
}
declare module "virtual:starlight/components/Search" {
  const Component: import("astro").AstroComponentFactory;
  export default Component;
}
declare module "virtual:starlight/components/SiteTitle" {
  const Component: import("astro").AstroComponentFactory;
  export default Component;
}
declare module "virtual:starlight/components/SocialIcons" {
  const Component: import("astro").AstroComponentFactory;
  export default Component;
}
declare module "virtual:starlight/components/ThemeSelect" {
  const Component: import("astro").AstroComponentFactory;
  export default Component;
}
declare module "virtual:starlight/user-config" {
  const config: import("@astrojs/starlight/types").StarlightConfig;
  export default config;
}
