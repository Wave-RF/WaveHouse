import { defineCollection } from "astro:content";
import { docsLoader } from "@astrojs/starlight/loaders";
import { docsSchema } from "@astrojs/starlight/schema";
import { z } from "astro/zod";

export const collections = {
  docs: defineCollection({
    loader: docsLoader(),
    schema: docsSchema({
      extend: z.object({
        // Opt a page into the "WaveHouse Cloud will run this for you" callout,
        // rendered at the end of the content by Footer.astro. Driving it from
        // frontmatter (rather than importing <CloudCta/> per page) is what lets
        // the plain-.md ops pages carry it without being converted to .mdx.
        //
        //   cloudCta: true                 → default copy
        //   cloudCta: { body: "…" }        → page-specific copy, which is the
        //                                    point: the CTA lands hardest when
        //                                    it names the work THIS page just
        //                                    finished describing.
        cloudCta: z
          .union([
            z.boolean(),
            z.object({
              title: z.string().optional(),
              body: z.string().optional(),
            }),
          ])
          .optional(),
      }),
    }),
  }),
};
