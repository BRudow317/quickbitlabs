/**
 * This file defines constants related to environment variables, specifically the URL for the API endpoint. It retrieves the URL from the environment variables and provides a default empty string if it is not set. This allows the application to dynamically use the API endpoint URL without hardcoding it, making it easier to manage different environments (development, staging, production) where the URL may differ.
 *
 * @exports API_URL as a string containing the URL of the API endpoint.
 */

export const BASE_URL = window.location.origin;
export const API_URL: string = BASE_URL+"/api/";