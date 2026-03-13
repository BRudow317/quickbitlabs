/**
 * addTime - Add time to a date
 * @param {Date|string|number} date - Starting date
 * @param {number} amount - Amount to add
 * @param {string} unit - Unit: 'days', 'hours', 'minutes', 'months', 'years'
 * @returns {Date} - New date
 * 
 * @example
 * addTime(new Date(), 7, 'days'); // 1 week from now
 * addTime(new Date(), -1, 'months'); // 1 month ago
 */
export const addTime = (date, amount, unit) => {
  const d = new Date(date);
  
  switch (unit) {
    case 'years':
      d.setFullYear(d.getFullYear() + amount);
      break;
    case 'months':
      d.setMonth(d.getMonth() + amount);
      break;
    case 'weeks':
      d.setDate(d.getDate() + amount * 7);
      break;
    case 'days':
      d.setDate(d.getDate() + amount);
      break;
    case 'hours':
      d.setHours(d.getHours() + amount);
      break;
    case 'minutes':
      d.setMinutes(d.getMinutes() + amount);
      break;
    case 'seconds':
      d.setSeconds(d.getSeconds() + amount);
      break;
  }
  
  return d;
};
