/**
 * isValidDate - Check if date is valid
 * @param {any} date - Value to check
 * @returns {boolean} - True if valid date
 * 
 * @example
 * isValidDate('2024-01-15'); // true
 * isValidDate('invalid'); // false
 * isValidDate(new Date('nope')); // false
 */
export const isValidDate = (date) => {
  const d = new Date(date);
  return d instanceof Date && !isNaN(d);
};
