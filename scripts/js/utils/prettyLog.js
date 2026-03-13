/**
 * prettyLog - Enhanced console.log with timestamps, labels, and styling
 * @param {string} label - Label for the log group
 * @param {any} data - Data to log
 * @param {string} level - Log level: 'log', 'warn', 'error', 'info'
 * 
 * @example
 * prettyLog('User Data', { id: 1, name: 'John' }, 'info');
 * prettyLog('API Error', error, 'error');
 */
export default prettyLog;
export {prettyLog};

const prettyLog = (label, data, level = 'log') => {
  if (process.env.NODE_ENV === 'production') return;

  const timestamp = new Date().toISOString().split('T')[1].slice(0, -1);
  const styles = {
    log: 'color: #3498db; font-weight: bold;',
    warn: 'color: #f39c12; font-weight: bold;',
    error: 'color: #e74c3c; font-weight: bold;',
    info: 'color: #2ecc71; font-weight: bold;',
  };

  console.groupCollapsed(`%c[${timestamp}] ${label}`, styles[level] || styles.log);
  console[level](data);
  console.trace('Stack trace');
  console.groupEnd();
};