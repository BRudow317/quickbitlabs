/**
 * getFromClipboard - Read text from clipboard
 * @returns {Promise<string>} - Clipboard content
 * 
 * @example
 * const text = await getFromClipboard();
 */
const getFromClipboard = async () => {
  try {
    return await navigator.clipboard.readText();
  } catch (err) {
    console.error('Paste failed:', err);
    return '';
  }
};

export default getFromClipboard;
export {getFromClipboard};