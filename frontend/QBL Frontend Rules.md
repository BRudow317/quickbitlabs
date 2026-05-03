# QuickBitLabs Frontend Operating Rules

## Core Architectural Hierarchy
Structure and styling must flow from the outside in:
**Layout -> Pages -> Sections -> Components**

### The Flex-Standard
To maintain consistent density and readability, we follow the **Col-Row-Col** pattern:
1.  **Pages (Column):** `Flex direction="column"`. Handles the vertical stack of sections.
2.  **Sections (Row):** `Flex direction={{ initial: 'column', md: 'row' }}`. Distributes major functional blocks horizontally on desktop, stacks them on mobile.
3.  **Components (Column):** `Flex direction="column"`. Optimizes for vertical readability of content (Title -> Body -> Action).

---

## 1. Layout & Responsiveness
- **Breakpoint Context:** ALL layout decisions must use the `useBreakpoint` hook.
- **The Three Pillars:**
    - `SmallLayout` (xsm, sm): Mobile-first, tight padding, fluid containers.
    - `MediumLayout` (md, lg): Tablet/Small desktop, balanced spacing.
    - `LargeLayout` (xl, xxl): Large display, generous padding, max-width containers.
- **Rule:** Pages should never set their own global max-widths; they must defer to the active Layout container.

## 2. Sections Pattern (Domain-Aware UI)
- **Definition:** A Section is a reusable block that orchestrates components to fulfill a feature (e.g., `CatalogPreviewSection`).
- **Location:** `src/sections/MySectionName/Index.tsx`.
- **Context Wiring (MANDATORY):**
    - **Auth Guard:** Every section must check `useAuth()` if it handles user data or actions.
    - **Data Guard:** Sections should consume `useData()` for shared state.
- **Portability:** Sections must be "Drop-in Ready." They should wrap content in a Radix `<Section>` or `<Box>` and fill the width of their parent.

## 3. Styling & Theming
- **Radix UI Themes:** Shadcn should be used as the default for all components, radix is a fall back but will ideally be removed.
- **Color Tokens:** Use Shadcn theme tokens for coloring and styling.

## 4. Security & Safety
- **Auth Interceptors:** All API calls via `client.gen` must include the bearer token from `localStorage`.
- **Gating:** Use `requireAuth` prop in `App.tsx` routes to trigger the `Layout` auth check.
- **Section Safety:** If a section is dropped into a public page but requires data, it MUST render a fallback (e.g., "Authentication Required" card) rather than crashing or leaking state.

## 5. Build & Performance
- **Vite Config:** `chunkSizeWarningLimit` is set to 10MB to accommodate large library bundles (like Apache Arrow).
- **Zero-Copy Data:** When handling data from the backend, maintain the `ArrowReader` / IPC stream integrity as long as possible before converting to UI-ready objects.

## 6. Code Quality & Maintenance
- **TypeScript Strictness:** All code must be written with `strict` mode enabled.
- **Component Reusability:** If a component is used in more than one place, it should be moved to `src/components` and made generic.
- **Documentation:** All sections and components must have JSDoc comments explaining their purpose, props, and any important implementation details.

## 7. Building Rules
- **Build Command:** Use `npm run build` after every change to ensure the frontend compiles correctly.
- **Linting:** Run `npm run lint` to catch any code style issues before committing.
