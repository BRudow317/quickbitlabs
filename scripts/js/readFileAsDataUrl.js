/**
 * readFileAsDataUrl - Read file as base64 data URL
 * @param {File} file - File object
 * @returns {Promise<string>} - Data URL string
 * 
 * @example
 * const dataUrl = await readFileAsDataUrl(imageFile);
 * img.src = dataUrl;
 */
export default readFileAsDataUrl;
export {readFileAsDataUrl};

const readFileAsDataUrl = (file) => {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => resolve(e.target.result);
    reader.onerror = (e) => reject(e);
    reader.readAsDataURL(file);
  });
};