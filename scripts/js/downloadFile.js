/**
 * downloadFile - Trigger file download from data
 * @param {string|Blob} content - File content or Blob
 * @param {string} filename - Desired filename
 * @param {string} mimeType - MIME type for string content
 * 
 * @example
 * downloadFile(JSON.stringify(data), 'data.json', 'application/json');
 * downloadFile(csvString, 'report.csv', 'text/csv');
 * downloadFile(blob, 'image.png');
 */
export default downloadFile;
export {downloadFile};

const downloadFile = (content, filename, mimeType = 'text/plain') => {
  const blob = content instanceof Blob ? content : new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
};