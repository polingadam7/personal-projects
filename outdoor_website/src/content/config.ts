import { defineCollection, z } from 'astro:content';

const base = {
  title: z.string(),
  date: z.coerce.date(),
  description: z.string(),
  draft: z.boolean().default(false),
};

export const collections = {
  'trip-reports': defineCollection({
    type: 'content',
    schema: z.object({
      ...base,
      location: z.string().optional(),
      days: z.number().optional(),
      miles: z.number().optional(),
      difficulty: z.enum(['easy', 'moderate', 'strenuous']).optional(),
    }),
  }),
  'recipes': defineCollection({
    type: 'content',
    schema: z.object({
      ...base,
      calories: z.number().optional(),
      prepTime: z.string().optional(),
      servings: z.number().optional(),
    }),
  }),
  'meal-reviews': defineCollection({
    type: 'content',
    schema: z.object({
      ...base,
      product: z.string(),
      brand: z.string().optional(),
      rating: z.number().min(1).max(5),
      calories: z.number().optional(),
      price: z.number().optional(),
    }),
  }),
  'gear-reviews': defineCollection({
    type: 'content',
    schema: z.object({
      ...base,
      product: z.string(),
      brand: z.string().optional(),
      category: z.string().optional(),
      rating: z.number().min(1).max(5),
      price: z.number().optional(),
    }),
  }),
  'misc': defineCollection({
    type: 'content',
    schema: z.object(base),
  }),
};
