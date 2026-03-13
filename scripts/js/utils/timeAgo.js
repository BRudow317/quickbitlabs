/**
 * timeAgo - Convert date to relative time string
 * @param {Date|string|number} date - Date to convert
 * @returns {string} - Relative time string
 * 
 * @example
 * timeAgo(new Date(Date.now() - 60000)); // "1 minute ago"
 * timeAgo('2024-01-01'); // "2 months ago"
 */
export const timeAgo = (date) => {
  const seconds = Math.floor((Date.now() - new Date(date).getTime()) / 1000);
  
  const intervals = [
    { label: 'year', seconds: 31536000 },
    { label: 'month', seconds: 2592000 },
    { label: 'week', seconds: 604800 },
    { label: 'day', seconds: 86400 },
    { label: 'hour', seconds: 3600 },
    { label: 'minute', seconds: 60 },
  ];
  
  for (const interval of intervals) {
    const count = Math.floor(seconds / interval.seconds);
    if (count >= 1) {
      return `${count} ${interval.label}${count > 1 ? 's' : ''} ago`;
    }
  }
  
  return 'just now';
};
