/**
 * readFileAsText - Read file input as text
 * @param {File} file - File object from input
 * @returns {Promise<string>} - File contents
 * 
 * @example
 * const text = await readFileAsText(inputElement.files[0]);
 */
export default readFileAsText;
export {readFileAsText};
const readFileAsText = (file) => {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => resolve(e.target.result);
    reader.onerror = (e) => reject(e);
    reader.readAsText(file);
  });
};