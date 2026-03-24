/**
 * Parse CSV string to array of objects
 * @param {string} csv - CSV string content
 * @param {string} delimiter - Column delimiter
 * @returns {Array<Object>} - Parsed data
 * 
 * @example
 * const data = csvToArrOfObj(csvString);
 * const data = csvToArrOfObj(tsvString, '\t');
 */
export default csvToArrOfObj;
export const csvToArrOfObj = (csv, delimiter = ',') => {
  const lines = csv.trim().split('\n');
  const headers = lines[0].split(delimiter).map((h) => h.trim().replace(/^["']|["']$/g, ''));
  
  return lines.slice(1).map((line) => {
    const values = line.split(delimiter).map((v) => v.trim().replace(/^["']|["']$/g, ''));
    return headers.reduce((obj, header, i) => {
      obj[header] = values[i] || '';
      return obj;
    }, {});
  });
};