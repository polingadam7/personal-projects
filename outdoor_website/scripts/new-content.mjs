#!/usr/bin/env node
import readline from 'readline';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { execSync } from 'child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CONTENT_DIR = path.join(__dirname, '../src/content');

const RESET  = '\x1b[0m';
const GREEN  = '\x1b[32m';
const CYAN   = '\x1b[36m';
const YELLOW = '\x1b[33m';
const BOLD   = '\x1b[1m';
const DIM    = '\x1b[2m';

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

const ask = (question, fallback = '') => new Promise(resolve => {
  const hint = fallback ? ` ${DIM}(${fallback})${RESET}` : '';
  rl.question(`${CYAN}?${RESET} ${question}${hint}: `, answer => {
    resolve(answer.trim() || fallback);
  });
});

const askRequired = async (question) => {
  let answer = '';
  while (!answer) {
    answer = await ask(question);
    if (!answer) console.log(`${YELLOW}  Required.${RESET}`);
  }
  return answer;
};

const choose = async (question, options) => {
  console.log(`\n${CYAN}?${RESET} ${question}`);
  options.forEach((o, i) => console.log(`  ${DIM}${i + 1}.${RESET} ${o}`));
  let choice = '';
  while (!choice) {
    const raw = await ask(`Pick 1-${options.length}`);
    const n = parseInt(raw);
    if (n >= 1 && n <= options.length) choice = options[n - 1];
    else console.log(`${YELLOW}  Enter a number between 1 and ${options.length}.${RESET}`);
  }
  return choice;
};

const slugify = str =>
  str.toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .trim();

const today = () => new Date().toISOString().split('T')[0];

// ── Templates ────────────────────────────────────────────────────────────────

function tripReportTemplate(fields) {
  const { title, description, location, days, miles, difficulty } = fields;
  return `---
title: "${title}"
date: ${today()}
description: "${description}"
location: "${location}"
days: ${days || 1}
miles: ${miles || 0}
difficulty: ${difficulty || 'moderate'}
---

## The Route

[Overview of the route, trailhead, total distance and gain.]

${Array.from({ length: parseInt(days) || 1 }, (_, i) => `## Day ${i + 1}

[What happened, conditions, highlights.]

`).join('')}
## Food Notes

[What you ate, what worked, calorie targets vs. reality.]

## Gear Notes

[What performed well, what you'd change next time.]
`;
}

function recipeTemplate(fields) {
  const { title, description, calories, prepTime, servings } = fields;
  return `---
title: "${title}"
date: ${today()}
description: "${description}"
calories: ${calories || 0}
prepTime: "${prepTime || '10 minutes'}"
servings: ${servings || 1}
---

## Why This Works

[What makes this a good trail meal.]

## Ingredients

- [item] — [X] cal, [X] oz
- [item] — [X] cal, [X] oz

**Total: ~${calories || '?'} cal, ~[X] oz**

## Instructions

1. [Step one]
2. [Step two]
3. [Step three]

## Calorie Density

[X] cal for [X] oz = [X] cal/oz. [Context on how this compares to the 100 cal/oz benchmark.]

## Variations

[Optional swaps or add-ins.]
`;
}

function gearReviewTemplate(fields) {
  const { title, description, product, brand, category, rating, price } = fields;
  return `---
title: "${title}"
date: ${today()}
description: "${description}"
product: "${product}"
brand: "${brand}"
category: "${category}"
rating: ${rating || 3}
price: ${price || 0}
---

## Bottom Line

[One paragraph verdict. Who should buy this and why.]

## Specs

- Weight: [X oz]
- Price: $${price || '?'}
- [Other key spec]: [value]

## What Works

[Specific things it does well with real context.]

## What Doesn't

[Honest caveats. Nothing is perfect.]

## Who It's For

[The right buyer and the wrong buyer.]

## Rating

- [Category]: [X]/5
- [Category]: [X]/5
- **Overall: ${rating || '?'}/5**
`;
}

function mealReviewTemplate(fields) {
  const { title, description, product, brand, rating, calories, price } = fields;
  return `---
title: "${title}"
date: ${today()}
description: "${description}"
product: "${product}"
brand: "${brand}"
rating: ${rating || 3}
calories: ${calories || 0}
price: ${price || 0}
---

## Bottom Line

[One paragraph verdict.]

## Taste

[What it actually tastes like on trail, not in a kitchen.]

## Nutrition

- Calories: ${calories || '?'}
- Sodium: [X mg]
- Protein: [X g]
- Weight: [X oz]

At [X] cal/oz it's [above/below] the 100 cal/oz benchmark.

## Rehydration

[Minutes, water temp, tips.]

## Value

[$${price || '?'} per meal. How it compares to alternatives.]

## Rating

- Taste: [X]/5
- Nutrition: [X]/5
- Value: [X]/5
- Ease of prep: [X]/5
- **Overall: ${rating || '?'}/5**
`;
}

function miscTemplate(fields) {
  const { title, description } = fields;
  return `---
title: "${title}"
date: ${today()}
description: "${description}"
---

[Write your content here.]
`;
}

// ── Main flow ─────────────────────────────────────────────────────────────────

const TYPES = ['Trip Report', 'Recipe', 'Gear Review', 'Meal Review', 'Misc'];
const TYPE_DIRS = {
  'Trip Report': 'trip-reports',
  'Recipe':      'recipes',
  'Gear Review': 'gear-reviews',
  'Meal Review': 'meal-reviews',
  'Misc':        'misc',
};

console.log(`\n${BOLD}${GREEN}Toward Outdoors — New Content${RESET}\n`);

const type  = await choose('What are you writing?', TYPES);
const title = await askRequired('Title');
const desc  = await askRequired('Description (one sentence)');

let template = '';
let slug = slugify(title);

if (type === 'Trip Report') {
  const location   = await askRequired('Location');
  const days       = await ask('Days', '1');
  const miles      = await ask('Total miles', '0');
  const difficulty = await choose('Difficulty', ['easy', 'moderate', 'strenuous']);
  template = tripReportTemplate({ title, description: desc, location, days, miles, difficulty });

} else if (type === 'Recipe') {
  const calories = await ask('Calories (total)', '0');
  const prepTime = await ask('Prep time', '10 minutes');
  const servings = await ask('Servings', '1');
  template = recipeTemplate({ title, description: desc, calories, prepTime, servings });

} else if (type === 'Gear Review') {
  const product  = await askRequired('Product name');
  const brand    = await ask('Brand');
  const category = await ask('Category (e.g. Water Filtration)');
  const rating   = await ask('Rating (1-5)', '3');
  const price    = await ask('Price (numbers only)', '0');
  template = gearReviewTemplate({ title, description: desc, product, brand, category, rating, price });

} else if (type === 'Meal Review') {
  const product  = await askRequired('Product name');
  const brand    = await ask('Brand');
  const rating   = await ask('Rating (1-5)', '3');
  const calories = await ask('Calories', '0');
  const price    = await ask('Price (numbers only)', '0');
  template = mealReviewTemplate({ title, description: desc, product, brand, rating, calories, price });

} else {
  template = miscTemplate({ title, description: desc });
}

const dir      = path.join(CONTENT_DIR, TYPE_DIRS[type]);
const filePath = path.join(dir, `${slug}.md`);

if (fs.existsSync(filePath)) {
  console.log(`\n${YELLOW}File already exists: ${filePath}${RESET}`);
  rl.close();
  process.exit(1);
}

fs.writeFileSync(filePath, template, 'utf8');

console.log(`\n${GREEN}${BOLD}Created:${RESET} ${path.relative(process.cwd(), filePath)}`);

const open = await ask('\nOpen in Obsidian? (y/n)', 'y');
if (open.toLowerCase() !== 'n') {
  try {
    const obsidianUri = `obsidian://open?path=${encodeURIComponent(filePath)}`;
    execSync(`start "" "${obsidianUri}"`, { stdio: 'ignore', shell: true });
  } catch {
    console.log(`${YELLOW}Could not open Obsidian. Find the file at: ${filePath}${RESET}`);
  }
}

console.log(`\n${DIM}Done. Commit and push when you're ready to publish.${RESET}\n`);
rl.close();
