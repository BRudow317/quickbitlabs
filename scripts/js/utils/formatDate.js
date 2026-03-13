/**
 * formatDate - Format date with various patterns
 * @param {Date|string|number} date - Date to format
 * @param {string} format - Format pattern
 * @returns {string} - Formatted date string
 * 
 * Patterns: YYYY, MM, DD, HH, mm, ss, ddd (weekday), MMM (month name)
 * 
 * @example
 * formatDate(new Date(), 'YYYY-MM-DD'); // "2024-01-15"
 * formatDate(new Date(), 'MMM DD, YYYY'); // "Jan 15, 2024"
 * formatDate(new Date(), 'DD/MM/YYYY HH:mm'); // "15/01/2024 14:30"
 */
export const formatDate = (date, format = 'YYYY-MM-DD') => {
  const d = new Date(date);
  
  const pad = (n) => n.toString().padStart(2, '0');
  
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  
  const replacements = {
    YYYY: d.getFullYear(),
    MM: pad(d.getMonth() + 1),
    DD: pad(d.getDate()),
    HH: pad(d.getHours()),
    mm: pad(d.getMinutes()),
    ss: pad(d.getSeconds()),
    ddd: days[d.getDay()],
    MMM: months[d.getMonth()],
  };
  
  return format.replace(/YYYY|MM|DD|HH|mm|ss|ddd|MMM/g, (match) => replacements[match]);
};
