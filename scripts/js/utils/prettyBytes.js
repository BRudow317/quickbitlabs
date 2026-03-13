/**
 * prettyBytes - Convert bytes to human readable format
 * @param {number} bytes - File size in bytes
 * @param {number} decimals - Decimal places
 * @returns {string} - Formatted size
 * 
 * @example
 * prettyBytes(1024); // "1 KB"
 * prettyBytes(1234567); // "1.18 MB"
 */
export default prettyBytes;
export const prettyBytes = (bytes, decimals = 2) => {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(decimals))} ${sizes[i]}`;
};