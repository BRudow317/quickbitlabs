/**
 * Get differences between two objects
 * 
 * @param {Object} original - Original object
 * @param {Object} updated - Updated object
 * @returns {Object} - Object containing only changed values
 * 
 * @example
 * const changes = checkObjState(oldUser, newUser);
 * // { name: 'New Name' } - only changed fields
 */
export default checkObjState;
export const checkObjState = (original, updated) => {
  const changes = {};
  
  const allKeys = new Set([...Object.keys(original), ...Object.keys(updated)]);
  
  allKeys.forEach((key) => {
    const origVal = original[key];
    const newVal = updated[key];
    
    if (typeof origVal === 'object' && typeof newVal === 'object' && origVal && newVal) {
      const nestedDiff = checkObjState(origVal, newVal);
      if (Object.keys(nestedDiff).length > 0) {
        changes[key] = nestedDiff;
      }
    } else if (origVal !== newVal) {
      changes[key] = newVal;
    }
  });
  
  return changes;
};