/**
 * Gets the computed height of an element by its ID.
 * @param elementId - The ID of the element (without the # prefix)
 * @returns The height in pixels, or null if element not found
 */
export function getComponentHeight(elementId: string): number | null {
  const element = document.getElementById(elementId);
  if (!element) return null;
  return element.getBoundingClientRect().height;
}
