/**
 * Required Setting Keys
 * `pendoTrackEventSecretKey`: Pendo_trackEventSecret_Key
 * `personasSpaceId`: Personas Space ID
 * `profileApiToken`: Profile API Token
 */

/**
 * Handle identify event
 * @param  {SegmentIdentifyEvent} event
 * @param  {FunctionSettings} settings
 */
async function onIdentify(event, settings) {
  /**
   * Basic Validation
   */
  if (!event.userId) {
    throw new ValidationError('userId is required')
  }

  /**
   * Fetch User Traits from Profiles API
   */
  let fetchedUser;
  try {
    fetchedUser = await queryProfileAPI(
      'users',
      'user_id',
      event.userId,
      settings.personasSpaceId,
      settings.profileApiToken
    );
  } catch (error) {
    // Retry on connection error'
    throw new RetryError(error.message);
  }

  /**
   * Prepare Track Object for Pendo API
   */
  const { userId, timestamp, context } = event;
  const { traits, groupId } = await fetchedUser.json();
  const pendoTrackObject = {
    type: 'track',
    event: 'User Traits Update',
    visitorId: userId,
    accountId: groupId,
    timestamp: timestamp,
    properties: traits,
    context: context
  };

  /**
   * Send Track Object to Pendo API
   */
  const endpoint = 'https://app.pendo.io/data/track';
  let response;
  try {
    response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'x-pendo-integration-key': settings.pendoTrackEventSecretKey,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(pendoTrackObject)
    });
  } catch (error) {
    // Retry on connection error
    throw new RetryError(error.message);
  }

  if (response.status >= 500 || response.status === 429) {
    // Retry on 5xx (server errors) and 429s (rate limits)
    throw new RetryError(`Failed with ${response.status}`);
  }
}

/**
 * Handle group event
 * @param  {SegmentGroupEvent} event
 * @param  {FunctionSettings} settings
 */
async function onGroup(event, settings) {
  /**
   * Basic Validation
   */
  if (!event.groupId) {
    throw new ValidationError('groupId is required')
  }

  /**
   * Get Group / Account Traits
   */
  let fetchedAccount;
  try {
    fetchedAccount = await queryProfileAPI(
      'accounts',
      'group_id',
      event.groupId,
      settings.personasSpaceId,
      settings.profileApiToken
    );
  } catch (error) {
    // Retry on connection error'
    throw new RetryError(error.message);
  }

  /**
   * Prepare Track Object for Pendo API
   */
  const { userId, groupId, timestamp, context } = event;
  const { traits } = await fetchedAccount.json();
  const pendoTrackObject = {
    type: 'track',
    event: 'Account Traits Update',
    visitorId: userId,
    accountId: groupId,
    timestamp: timestamp,
    properties: traits,
    context: context
  };

  /**
   * Send Track Object to Pendo API
   */
  let response;
  const endpoint = 'https://app.pendo.io/data/track';
  try {
    response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'x-pendo-integration-key': settings.pendoTrackEventSecretKey,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(pendoTrackObject)
    });
  } catch (error) {
    // Retry on connection error
    throw new RetryError(error.message);
  }

  // Retry on 5xx (server errors) and 429s (rate limits)
  if (response.status >= 500 || response.status === 429 || response.status === 400) {
    throw new RetryError(`Failed with ${response.status}`);
  }
}

/**
 * Utility Functions
 */

/**
 * Query Profile API
 * @param {entity} string users or accounts
 * @param {lookupKey} string user_id or group_id as the key
 * @param {lookupValue} string user_id or group_id value
 * @param {personasSpaceId} string value set in settings
 * @param {profileApiToken} string value set in settings
 */
async function queryProfileAPI(
  entity,
  lookupKey,
  lookupValue,
  personasSpaceId,
  profileApiToken
) {
  const baseUrl = `https://profiles.segment.com/v1/spaces/${personasSpaceId}/collections/${entity}/profiles`;
  const url = `${baseUrl}/${lookupKey}:${lookupValue}/traits?limit=200`;

  let response;
  try {
    response = await fetch(url, {
      method: 'GET',
      headers: {
        Authorization: `Basic ${btoa(profileApiToken + ':')}`,
        'Content-Type': 'application/json'
      }
    });
  } catch (error) {
    // Retry on connection error
    throw new RetryError(error.message);
  }

  if (
    response.status >= 500 ||
    response.status == 429 ||
    response.status == 401
  ) {
    // Retry on 5xx (server errors) and 429s (rate limits)
    throw new RetryError(`Failed with ${response.status}`);
  }

  if (response.status == 200) return response;
}

/**
 * Events Not Supported
 * - track
 * - page
 * - screen
 * - alias
 * - delete
 */

/**
 * Handle track event
 * @param  {SegmentTrackEvent} event
 * @param  {FunctionSettings} settings
 */
async function onTrack(event, settings) {
  throw new EventNotSupported('track is not supported');
}

/**
 * Handle page event
 * @param  {SegmentPageEvent} event
 * @param  {FunctionSettings} settings
 */
async function onPage(event, settings) {
  throw new EventNotSupported('page is not supported');
}

/**
 * Handle screen event
 * @param  {SegmentScreenEvent} event
 * @param  {FunctionSettings} settings
 */
async function onScreen(event, settings) {
  throw new EventNotSupported('screen is not supported');
}

/**
 * Handle alias event
 * @param  {SegmentAliasEvent} event
 * @param  {FunctionSettings} settings
 */
async function onAlias(event, settings) {
  throw new EventNotSupported('alias is not supported');
}

/**
 * Handle delete event
 * @param  {SegmentDeleteEvent} event
 * @param  {FunctionSettings} settings
 */
async function onDelete(event, settings) {
  throw new EventNotSupported('delete is not supported');
}