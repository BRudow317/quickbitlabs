/**
 * 
 * export { createLead, getLeads, getUser, login, type Options, postUser } from './sdk.gen';
export type { BodyLogin, ClientOptions, CreateLeadData, CreateLeadError, CreateLeadErrors, CreateLeadResponse, CreateLeadResponses, GetLeadsData, GetLeadsResponse, GetLeadsResponses, GetUserData, GetUserError, GetUserErrors, GetUserResponses, HttpValidationError, Lead, LoginData, LoginError, LoginErrors, LoginResponses, PostUserData, PostUserError, PostUserErrors, PostUserResponses, User, ValidationError 
 */
import { 
  login, 
  getUser, 
  type LoginData, 
  //type LoginResponses, // The union of possible success responses
  type UserBase,
  type Token
} from '@/api/openapi';
import { client } from '@/api/openapi/client.gen';

// Perform Login
export const authenticate = async (credentials: LoginData['body']): Promise<Token> => {
  const { data, error } = await login({ body: credentials });

  if (error || !data) {
    // error matches the LoginErrors or ValidationError shapes
    throw new Error('Authentication failed');
  }

  // Set the token on the singleton client
  client.setConfig({
    headers: {
      Authorization: `Bearer ${data.access_token}`,
    },
  });

  return data;
};

/**
 * Fetch User
 * Returns a 'UserBase' model
 */
export const fetchCurrentUser = async (): Promise<UserBase> => {
  const { data, error } = await getUser();

  if (error || !data) {
    throw new Error('Could not retrieve user profile');
  }

  return data; 
};