/// <reference path="../.astro/types.d.ts" />

// Starlight exposes its built-in components to overrides through these virtual
// modules (Vite resolves them at build time), but only ships their TYPES in an
// internal declaration file that isn't visible to user code — so `astro check`
// can't find them. Declare the ones the Footer override re-renders. See
// node_modules/@astrojs/starlight/virtual-internal.d.ts for the canonical list.
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
