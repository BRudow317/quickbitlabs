/**
 * dateDiff - Get difference between two dates
 * @param {Date|string} date1 - First date
 * @param {Date|string} date2 - Second date
 * @param {string} unit - Return unit
 * @returns {number} - Difference in specified unit
 * 
 * @example
 * dateDiff('2024-01-01', '2024-12-31', 'days'); // 365
 * dateDiff(startDate, endDate, 'hours');
 */
export const dateDiff = (date1, date2, unit = 'days') => {
  const d1 = new Date(date1);
  const d2 = new Date(date2);
  const diffMs = d2 - d1;
  
  const divisors = {
    years: 31536000000,
    months: 2592000000,
    weeks: 604800000,
    days: 86400000,
    hours: 3600000,
    minutes: 60000,
    seconds: 1000,
  };
  
  return Math.floor(diffMs / divisors[unit]);
};
