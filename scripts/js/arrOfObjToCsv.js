/**
 * ArrOfObjToCsv - Convert array of objects to CSV string
 * @param {Array<Object>} data - Data to convert
 * @param {string} delimiter - Column delimiter
 * @returns {string} - CSV string
 * 
 * @example
 * const csv = ArrOfObjToCsv(users);
 * downloadFile(csv, 'users.csv', 'text/csv');
 */
export const ArrOfObjToCsv = (data, delimiter = ',') => {
  if (!data.length) return '';
  
  const headers = Object.keys(data[0]);
  const headerRow = headers.join(delimiter);
  
  const rows = data.map((obj) =>
    headers.map((header) => {
      const value = obj[header]?.toString() || '';
      // Escape quotes and wrap in quotes if contains delimiter or quotes
      return value.includes(delimiter) || value.includes('"')
        ? `"${value.replace(/"/g, '""')}"`
        : value;
    }).join(delimiter)
  );
  
  return [headerRow, ...rows].join('\n');
};