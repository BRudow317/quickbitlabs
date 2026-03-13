/**
 * Simple state management store (Redux-like)
 * 
 * @param {Object} initialState - Initial state
 * @param {Object} reducers - Reducer functions
 * @returns {Object} - Store with getState, dispatch, subscribe
 * 
 * @example
 * const store = stateStore(
 *   { count: 0 },
 *   {
 *     increment: (state) => ({ ...state, count: state.count + 1 }),
 *     decrement: (state) => ({ ...state, count: state.count - 1 }),
 *     set: (state, payload) => ({ ...state, count: payload }),
 *   }
 * );
 * 
 * store.subscribe((state) => console.log('State:', state));
 * store.dispatch('increment');
 * store.dispatch('set', 10);
 */
export default stateStore;
export const stateStore = (initialState, reducers) => {
  let state = initialState;
  const listeners = new Set();
  
  return {
    getState: () => state,
    
    dispatch: (action, payload) => {
      if (reducers[action]) {
        state = reducers[action](state, payload);
        listeners.forEach((listener) => listener(state));
      } else {
        console.warn(`Unknown action: ${action}`);
      }
    },
    
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    
    reset: () => {
      state = initialState;
      listeners.forEach((listener) => listener(state));
    },
  };
};