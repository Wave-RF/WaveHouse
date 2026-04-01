# WaveHouse Client SDK

## Permissions

Public permissions are most restricted.

Can control reads (query builder for AST) per-:

- table
- column
- row
- function/pipe
- aggregation (SUM, AVG, etc.)
- limit
- execution time
permissions by JWT, claims, etc. w/ ability to restrict by matching patterns on values, etc.
For example, could have a user_id in the JWT claims, and then restrict access to rows where user_id = the user_id in the JWT claims, etc.

Writing (ingest) method can also restrict access via table and permissions/claims.

Could also have more restricted/admin type methods like:

- Raw SQL queries (not via AST query builder)
- Custom writes to tables (not just via ingest method)
- Updates and deletes
- creating tables, updating schemas, etc.
- reading schemas, column types, etc.
- (TBD on if we want these to be directly supported via SDK, or if they would just be raw SQL queries)
- creating and managing custom functions/pipes, etc.
- managing users, permissions, etc.

Potentially the idea would be that public users should only be able to access pre-defined pipes and maybe have limited ingest access, and then more trusted users (like internal users) could have more access to query the database directly (read-only AST) and expanded ingest access, and then admin users could have full access to everything, including raw SQL queries, pipe management, user and permission management, etc.

## Features

### Setup and Authentication

```js
// what should the actual import(s) look like...? and be named?
import WaveHouseClient as WHClient from '@wavehouse/client-sdk';

// client can be used in both node and browser environments
// it needs a baseURL, and then can use an API key (only for public pages or something?) or a JWT token for authentication
const wavehouse = new WHClient({
  baseURL: 'https://api.wavehouse.cloud',
  apiKey: 'your-api-key',
});

// or should this just take them not as named args, like:
const wavehouse = new WHClient('https://api.wavehouse.cloud', 'your-api-key');

// or is there a benefit in doing it like firebase with separate functions, like:
const wavehouse = new WHClient({
  baseURL: 'https://api.wavehouse.cloud',
  apiKey: 'your-api-key',
});
const db = wavehouse.database(); // or something like that...? or without `new` for firebase style? What's the difference? Should there be a return or anything?

// TODO: how to handle JWT and token refresh, especially if issued by a separate auth service?

// TODO: does ws/sse support JWT or key in headers, or do we need the init to get a token for the streaming connection, etc.? How would that work? Do we need to support both API key and JWT for streaming connections, etc.? What about if the token expires during a streaming connection, how do we handle that? Do we need to automatically refresh the token and reconnect the stream, etc.?

// TODO: also need to handle connection hooks – should they be per thing we are subscribing to, or global for the whole client instance, etc.? For example, do we need a different SSE for each table we subscribe to changes on, or one for the client that includes all the tables? What is better?

```

### Ingest

```js

// Likely want separate ingest/insert and not allow it as part of chainable query builder

// Ingest data into a table, with an array of objects representing rows to insert
await wavehouse.ingest(
  "table_name",
  [
    { column1: 'value1', column2: 'value2' },
    { column1: 'value3', column2: 'value4' },
  ]
);

// Or just a single row
await wavehouse.ingest(
  "table_name",
  { column1: 'value1', column2: 'value2' }
);

// TODO: how to handle schema validation, errors, etc. during ingest? What does the response look like? What if some rows fail? Do we want options for batch size, retries, etc.? How would built-in batching work? Can we have compile-time checks for the shape of the data being ingested, or would that be too complex? What about buffering data in memory and flushing on intervals or when a certain size is reached, and doing so over multiple different tables, etc.?

// Should it look more like:
await wavehouse.table("table_name").ingest(
  [
    { column1: 'value1', column2: 'value2' },
    { column1: 'value3', column2: 'value4' },
  ]
);

// Or even:
const table = wavehouse.table("table_name");
await table.ingest(
  [
    { column1: 'value1', column2: 'value2' },
    { column1: 'value3', column2: 'value4' },
  ]
);

// or with data returned:
const { data, errors } = await table.ingest(
  [
    { column1: 'value1', column2: 'value2' },
    { column1: 'value3', column2: 'value4' },
  ]
);

// TODO: needs options for immediate vs buffered, as backend like lambda cannot buffer while client can...
// maybe .insert() and .buffer.insert() or something? With a flush() method? And then the init config could have options for the buffer, like max size, flush interval, etc.?
// buffer would likely then needs it own callback/hooks for flushes, errors, etc. and also some way to monitor the buffer size, pending data, etc.?
// either way, both should attempt to store persistently on the client side until it is successfully sent to the backend, with retries, etc. for any failed requests, etc.?

```

### SQL Querying

```js

// Likely build query chainable style, something like:
const results = await wavehouse
  .from('table_name')
  .select('column1', 'column2')
  .where('column1', 'value1')
  .limit(10);
// but is there a common interface for the query builder that we want to follow, or should it be more custom? What about joins, aggregations, etc.? How does this work specifically for clickhouse compared to postgres, with its unique features and syntax? How do we then restrict auth/access and functions, tables, rows, columns, execution time, etc. for different users and use cases? What about pagination, streaming results, etc.? How would joins work?

// or should the query be more like firebase:
const q = query(wavehouse.table('table_name'), where('column1', 'value1'), limit(10));
const results = await getDocs(q);

// Or if you have proper permissions and want to run a raw SQL query, maybe something like:
const results = await wavehouse.sql('SELECT column1, column2 FROM table_name WHERE column1 = "value1" LIMIT 10');
// primarily for backend use with admin credentials only

```

### Custom Functions

```js

// like TinyBird, we want the dev to be able to build custom sql functions and then call them from the client, so maybe something like:
const results = await wavehouse
  .function('custom_function_name')
  .args('arg1', 'arg2')
  .call();

// but this feels a bit clunky, maybe it should be more like:
const results = await wavehouse.callFunction('custom_function_name', 'arg1', 'arg2');

// or even:
const customFunction = wavehouse.function('custom_function_name');
const results = await customFunction('arg1', 'arg2');

// or if we want to support streaming results from functions, maybe something like:
const customFunction = wavehouse.function('custom_function_name');
for await (const result of customFunction('arg1', 'arg2')) {
  // process result
}

// but TBD on what to do...

// calling them pipes, so something like:
const { data, error } = await wavehouse
  .pipe('custom_function_name')
  .args('arg1', 'arg2') // either that or .args({ arg1: 'value1', arg2: 'value2' })?
  .fetch();

// 1. Single shot fetch
const { data } = await wavehouse.pipe('live_sales').fetch();

// 2. Continuous Stream (Backend handles the complex aggregate math and pushes updates)
const salesStream = wavehouse.pipe('live_sales').stream({ 
  updateInterval: 1000 // Tell backend to push updates every second
});

// Async iterator consumes the SSE stream effortlessly
for await (const update of salesStream) {
  console.log('New aggregated data:', update.data);
}

// Alternatively, event listener style for React/UI frameworks
const unsubscribe = salesStream.subscribe((update) => {
  setGraphData(update.data);
});

```

### Streaming

```js
// Ideally most things support streaming... so you could query via SQL or via a function, or (either via SQL or its own method) get all data (that you have permission to) from a table. This would normally just return right away, or return when you called `.fetch()` or something, but if you wanted to stream results, you could do something like:
const stream = await wavehouse
  .from('table_name')
  .select('column1', 'column2')
  .where('column1', 'value1')
  .stream(); // instead of .fetch() or something..., or defaults to .fetch() but .stream() returns the fetched live data and then continues to stream new data as it comes in, etc.?

// for table streams, it is really easy, we just get the data since whatever date they want from the API, also fill it in with the gap data from SSE, and subscribe to it over SSE – with the client SDK de-duplicating data and building the list for us, etc. TBD on how this implementation will work, but it should be heavily abstracted away. For long time periods or lots of data, it should be able to auto bin-bucket it.
// This binning/bucketing, though, starts to get complicated as we need to figure out how to ensure it caches well for everyone on different screen resolutions, etc, and also how to do this if they want sums vs avg vs min vs max, etc, as the bin/bucketing approach would need to be different for each, so them calculating this themselves with the data returned would break if we didn't return full data...

// ideally, they could then specify via, say, the query builder, they wanted to stream the results of an avg, etc, and it would automatically get the count and total sum for the time period, and then as new data comes in, it would update the count and total sum and return the new avg, etc

// for more complicated SQL queries, where we can't use data from the custom WAL before it enters CH, we'd need to have some sort of backend process to run this query for us (ideally as the data in the table changes) to stream back data when the query would change to anyone subscribed.

// this would operate similarly for custom functions, where if they wanted to stream results from a function, we would need to have some sort of backend process to run this function for us (ideally as the data in the table changes) to stream back data when the function results would change to anyone subscribed.

// alternatively, we could build that only for functions, where we know they are more likely to cache hit and be common interests, and have generic sql queries that can't be made live require some `.poll(1000)` or something to re-run the query every second, etc.?

// TODO: ideally support both callbacks/event listeners and async iterators. like:

const stream = wavehouse.from('live_metrics').stream();

// Pattern 1: Callback (Great for React)
const unsubscribe = stream.on('data', (payload) => {
  setChartData(payload);
});
stream.on('error', (err) => console.error(err));

// Pattern 2: Async Iterator (Great for Node/Scripts)
for await (const payload of stream) {
  console.log(payload);
}

// TODO: need hooks

const stream = wavehouse.from('events').stream({
  onReconnect: (attempt) => console.log(`Reconnecting... attempt ${attempt}`),
  onStatusChange: (status) => setIndicator(status), // 'connecting', 'live', 'offline'
});

// TODO: how to send last-event-id or last-event-timestamp to get missed events during disconnect, etc.? Ideally automatic in client, but then how done per multiple subscriptions? and id or timestamp? how should de-duping work?

// TODO: SSE w/ browser-managed reconnects, last id, etc are really nice and simple and stable, and scale much better for public-type pages.
// but for sending data back up it may make more sense to use a websocket connection. Additionally, the sse case would likely then mean it would need
// either per-route sse connections, or a custom multiplex for a random client id, with a separate post method to control what that sse connection was subscribed to.
// auth for sse would also require data to be in the cookie, not an auth header
// if we can mandate http/2, maybe we could just open separate sse connections for every subscription and not hit the 6 connection cap?

```

### Miscellaneous Utility Functions

```js

// TODO: health check endpoint for the WaveHouse API, TODO on verifying creds, what content returned, interface, etc.
wavehouse.sys.health()

wavehouse.sys.tables()

wavehouse.table('table_name').schema()

// TODO: utility function to verify API key or JWT token, what content returned, interface, etc.
wavehouse.authTest()

//

```

## Draft 2

```js

// TODO: import and how to init w/ tree shaking, etc
import { createClient } from '@wavehouse/client-sdk';

// ============================================================================
// 1. INITIALIZATION & CONFIGURATION
// ============================================================================
const wavehouse = createClient({
  // TODO: is there a scenario where they may have multiple different wavehouse instances?
  baseURL: 'https://api.wavehouse.cloud',

  // Async auth function: The SDK calls this before every HTTP request and
  // SSE connection. It guarantees token freshness without cookie nightmares.
  // TODO: is there any other way we should do this, it feels janky?
  auth: async () => {
    // Example: Integrating with a 3rd party auth provider
    return await myAuthProvider.getToken();
  },

  // Global options for networking and the background ingestion buffer
  // TODO: do we want retries and buffering handled ourselves in this SDK...?
  options: {
    maxRetries: 3,
  }
});

async function main() {
  // ============================================================================
  // 2. SYSTEM & METADATA UTILITIES
  // ============================================================================

  // Check API health and backend latency
  // TODO: define endpoint and format
  const health = await wavehouse.sys.health();
  console.log('System Status:', health.status); // e.g., 'ok'

  // Verify the current JWT token validity and role
  const authStatus = await wavehouse.sys.verifyAuth();
  console.log('Logged in as:', authStatus.role);

  // Get accessible tables (filtered by JWT RLS permissions)
  const tables = await wavehouse.sys.getTables();

  // Get schema for a specific table (useful for dashboard builders)
  // returns column names, types, and any metadata (like if it's a dimension or
  // metric, etc.) available for the table, filtered by JWT RLS permissions
  const eventSchema = await wavehouse.table('events').schema();
  console.log('Event columns:', eventSchema.columns);

  // ============================================================================
  // 3. DATA INGESTION (WRITING)
  // ============================================================================
  const eventsTable = wavehouse.table('events');

  // A. Immediate / Synchronous (Best for serverless/backend)
  // Sends an HTTP POST immediately. Throws an error if it fails.
  // TODO: should this allow non-array input for single rows, or require arrays?
  // What about the response, should it return the inserted data, or just a success status, etc.?
  // what happens if some rows fail and some succeed in a batch?
  const { insertedCount, error } = await eventsTable.insert([
    { user_id: 'user_1', event_type: 'signup', timestamp: Date.now() }
  ]);

  // ============================================================================
  // 4. FETCHING DATA (READING)
  // ============================================================================

  // A. Standard AST Query Builder (Single Fetch)
  // TODO: do we use .from('table_name') or .table('table_name') like we have been...?
  // should we nest this into like wavehouse.query.from('table_name') or wavehouse.sql or smth?
  const { data: recentClicks } = await wavehouse
    .from('events')
    .select('user_id', 'path')
    .where('event_type', '==', 'click')
    .limit(100)
    .fetch();

  // B. Pre-defined Backend Pipes/Functions (Tinybird style)
  // Executes parameterized SQL stored securely on the backend
  const { data: salesData } = await wavehouse
    .pipe('daily_sales_aggregation')
    .args({ region: 'NA', category: 'software' })
    .fetch();

  // TODO: why do we use `.fetch()` here but not when querying the schema, etc...?

  // ============================================================================
  // 5. STREAMING & LIVE DATA (SSE under the hood)
  // ============================================================================
  // TODO: should we not have a fallback for HTTP/1.x environments? Cloudflare's
  // 2025 report shows 29% of traffic is on HTTP/1/x...

  // A. Streaming a Table (with developer connection hooks)
  const eventStream = wavehouse.from('events')
    .where('event_type', '==', 'purchase')
    .stream({
      // The SDK handles HTTP/2 multiplexing, Last-Event-ID, and NATS cursors.
      // These hooks just let the UI react to the network state.
      // TODO: why not have new data here too...?
      onReconnect: (attempt) => console.warn(`Connection lost. Retrying... (${attempt})`),
      onStatusChange: (status) => updateUIIndicator(status), // 'connecting', 'live', 'offline'
      onError: (err) => console.error('Stream critical error:', err)
    });

  // Consuming the stream via Event Listeners (React/Vue friendly)
  const unsubscribe = eventStream.subscribe((payload) => {
    console.log('New purchase detected!', payload.data);
  });

  // ... later when the component unmounts:
  // unsubscribe();
  // TODO: what happens if they don't unsubscribe?

  // B. Streaming a Pipe/Function
  // The backend recalculates the aggregate and pushes updates
  const liveMetrics = wavehouse.pipe('live_active_users').stream();

  // C. Consuming via Async Iterator (Node.js / Background script friendly)
  // Automatically pauses and resumes seamlessly if the network drops
  for await (const update of liveMetrics) {
    console.log('Current Active Users:', update.data.count);

    // Break the loop to close the stream automatically
    if (update.data.count > 10000) break;
  }
  // TODO: does this mean a `.stream()` call can either be consumed via subscribe or async iterator?
  // does is matter which? Do both allow the same hooks? Does one need a cleanup compared to the other?

  // D. Polling an AST Query
  // For complex ad-hoc queries that can't easily be tailored to the NATS stream.
  // This executes a standard fetch every X milliseconds.
  const pollingQuery = wavehouse
    .from('events')
    .select('count()')
    .where('path', '==', '/checkout')
    .poll(5000); // 5000ms interval

  pollingQuery.subscribe((result) => {
    console.log('Checkout page views (updated every 5s):', result.data);
  });
  // TODO: same question with polling here, is it consumed via subscribe or async iterator?

  // pollingQuery.stop();
  // TODO: why `.stop()` here instead of `unsubscribe()` like with the stream? Should we unify this?

  // ============================================================================
  // 6. ADMIN OVERRIDES (Privileged execution)
  // ============================================================================

  // This requires a JWT with admin claims. If exposed in a browser without 
  // the right token, the backend strictly rejects it.
  try {
    const { data: rawData } = await wavehouse.admin.sql(`
      SELECT database, table, total_rows 
      FROM system.tables 
      WHERE database = 'default'
    `);
    console.log('Raw DB Stats:', rawData);
  } catch (err) {
    console.error('Admin query failed (likely insufficient permissions):', err);
  }
  // TODO: should all of our calls not be wrapped with try/catch like this...?
  // TODO: or is it better to do `const { data, error } = await wavehouse.admin.sql(...)`
  // and let the developer check for the error instead of throwing it...?
}

main();

```

## Draft 3

```js

import { createClient } from '@wavehouse/client';

// ============================================================================
// 1. INITIALIZATION & CONFIGURATION
// ============================================================================

// synchronous call just returns object, no {data, error} wrapping
const wavehouse = createClient({
  baseURL: 'https://api.wavehouse.cloud',

  // Async auth function: The SDK calls this before every HTTP request and
  // SSE connection. It guarantees token freshness without cookie nightmares.
  // TODO: is there any other way we should do this, it feels janky?
  auth: async () => {
    // Example: Integrating with a 3rd party auth provider
    return await myAuthProvider.getToken();
  },
  options: {
    maxRetries: 3,
    defaultStreamOptions: {
      onReconnect: (attempt) => console.log(`Reconnecting... attempt ${attempt}`),
      onStatusChange: (status) => setIndicator(status), // 'connecting', 'live', 'offline'
    },
    defaultPollInterval: 5000, // 5 seconds for any .poll() calls that don't specify an interval
  }
});

async function main() {
  // ============================================================================
  // 2. SYSTEM & METADATA UTILITIES
  // ============================================================================

  // Check API health and backend latency
  // TODO: define endpoint and format
  const health = await wavehouse.sys.health();
  console.log('System Status:', health.status); // e.g., 'ok'

  // Verify the current JWT token validity and role
  const authStatus = await wavehouse.sys.verifyAuth();
  console.log('Logged in as:', authStatus.role);

  // Get accessible tables (filtered by JWT RLS permissions)
  const tables = await wavehouse.sys.getTables();

  // Get schema for a specific table (useful for dashboard builders)
  // returns column names, types, and any metadata (like if it's a dimension or
  // metric, etc.) available for the table, filtered by JWT RLS permissions
  const eventSchema = await wavehouse.table('events').schema();
  console.log('Event columns:', eventSchema.columns);

  // ============================================================================
  // 3. DATA INGESTION (WRITING)
  // ============================================================================
  const eventsTable = wavehouse.table('events');

  // A. Immediate / Synchronous (Best for serverless/backend)
  // Sends an HTTP POST immediately. Throws an error if it fails.
  // TODO: should this allow non-array input for single rows, or require arrays?
  // What about the response, should it return the inserted data, or just a success status, etc.?
  // what happens if some rows fail and some succeed in a batch?
  const { insertedCount, error } = await eventsTable.insert([
    { user_id: 'user_1', event_type: 'signup', timestamp: Date.now() }
  ]);

  // ============================================================================
  // 4. FETCHING DATA (READING)
  // ============================================================================

  // A. Standard AST Query Builder (Single Fetch)
  // TODO: do we use .from('table_name') or .table('table_name') like we have been...?
  // should we nest this into like wavehouse.query.from('table_name') or wavehouse.sql or smth?
  const { data: recentClicks } = await wavehouse
    .from('events')
    .select('user_id', 'path')
    .where('event_type', '==', 'click')
    .limit(100)
    .fetch();

  // B. Pre-defined Backend Pipes/Functions (Tinybird style)
  // Executes parameterized SQL stored securely on the backend
  const sales = await wavehouse.pipe('daily_sales_aggregation', { region: 'NA', category: 'software' });
  const { data: salesData } = sales.fetch();
  // TODO: why do we use `.fetch()` here but not when querying the schema, etc...?

  // ============================================================================
  // 5. STREAMING & LIVE DATA (SSE under the hood)
  // ============================================================================
  // TODO: should we not have a fallback for HTTP/1.x environments? Cloudflare's
  // 2025 report shows 29% of traffic is on HTTP/1/x...

  // A. Streaming a Table (with developer connection hooks)
  const eventStream = wavehouse.from('events')
    .where('event_type', '==', 'purchase')
    .stream({
      // The SDK handles HTTP/2 multiplexing, Last-Event-ID, and NATS cursors.
      // These hooks just let the UI react to the network state.
      // TODO: why not have new data here too...?
      onReconnect: (attempt) => console.warn(`Connection lost. Retrying... (${attempt})`),
      onStatusChange: (status) => updateUIIndicator(status), // 'connecting', 'live', 'offline'
      onError: (err) => console.error('Stream critical error:', err)
    });

  // Consuming the stream via Event Listeners (React/Vue friendly)
  const unsubscribe = eventStream.subscribe((payload) => {
    console.log('New purchase detected!', payload.data);
  });

  // ... later when the component unmounts:
  unsubscribe();

  // B. Streaming a Pipe/Function
  // The backend recalculates the aggregate and pushes updates
  const liveMetrics = wavehouse.pipe('live_active_users').stream();

  // C. Consuming via Async Iterator (Node.js / Background script friendly)
  // Automatically pauses and resumes seamlessly if the network drops
  for await (const update of liveMetrics) {
    console.log('Current Active Users:', update.data.count);

    // Break the loop to close the stream automatically
    if (update.data.count > 10000) break;
  }

  // D. Polling an AST Query
  // For complex ad-hoc queries that can't easily be tailored to the NATS stream.
  // This executes a standard fetch every X milliseconds.
  const pollingQuery = wavehouse
    .from('events')
    .select('count()')
    .where('path', '==', '/checkout')
    .stream(5000); // 5000ms interval if it needs to fallback to polling

  const unsubscribePolling = pollingQuery.subscribe((result) => {
    console.log('Checkout page views (updated every 5s):', result.data);
  });
  unsubscribePolling();

  // ============================================================================
  // 6. ADMIN OVERRIDES (Privileged execution)
  // ============================================================================

  // This requires a JWT with admin claims. If exposed in a browser without 
  // the right token, the backend strictly rejects it.
  try {
    const { data: rawData } = await wavehouse.admin.sql(`
      SELECT database, table, total_rows 
      FROM system.tables 
      WHERE database = 'default'
    `);
    console.log('Raw DB Stats:', rawData);
  } catch (err) {
    console.error('Admin query failed (likely insufficient permissions):', err);
  }
  // TODO: should all of our calls not be wrapped with try/catch like this...?
  // TODO: or is it better to do `const { data, error } = await wavehouse.admin.sql(...)`
  // and let the developer check for the error instead of throwing it...?
}

main();

```

## Draft 4

```ts
import { createClient } from '@wavehouse/client';
// (Optional) Developers generate this file via your CLI: `npx wavehouse gen types`
import type { Database } from './wavehouse-types';

// ============================================================================
// 1. INITIALIZATION & CONFIGURATION
// ============================================================================

// Synchronous creation. Throws an immediate JS Error if `baseURL` is missing.
// Passing <Database> generic enables magic IDE autocomplete for all tables/pipes.
const wavehouse = createClient<Database>({
  baseURL: 'https://api.wavehouse.cloud',

  // The async auth function guarantees the SDK always has a fresh token.
  // It completely bypasses third-party cookie blocking issues for SSE/fetch.
  auth: async () => {
    return await myAuthProvider.getToken();
  },

  options: {
    maxRetries: 3, // For standard fetch/insert requests
  }
});

async function main() {
  // ============================================================================
  // 2. SYSTEM & METADATA UTILITIES
  // ============================================================================

  // All async calls strictly return { data, error } tuples. No try/catch needed!
  const { data: health, error: healthErr } = await wavehouse.sys.health();
  if (!healthErr) console.log('Latency:', health.latencyMs);

  const { data: authStatus } = await wavehouse.sys.verifyAuth();
  console.log('Role:', authStatus?.role);

  // Schema namespace for DB structural metadata
  const { data: tables } = await wavehouse.schema.tables();
  const { data: eventSchema } = await wavehouse.table('events').schema();

  // ============================================================================
  // 3. DATA INGESTION (WRITING)
  // ============================================================================

  // The SDK automatically detects if you pass an object or an array.
  // Partial failures are handled by returning an error with details.
  const { data: insertData, error: insertErr } = await wavehouse.table('events').insert({
    user_id: 'user_1',
    event_type: 'signup',
    timestamp: Date.now()
  });

  if (insertErr) console.error('Ingest failed:', insertErr.message)
  else console.log('Rows inserted:', insertData.insertedCount);

  // ============================================================================
  // 4. FETCHING DATA (READING)
  // ============================================================================

  // A. AST Query Builder
  // Builders use .fetch() to signal execution.
  const { data: recentClicks, error: fetchErr } = await wavehouse
    .table('events')
    .select('user_id', 'path')
    .where('event_type', '==', 'click')
    .limit(100)
    .fetch();

  // TODO: do we want that ^ or to make the query builder separate from the execution, maybe with:
  //  .table('events').query() or .table('events').sql() or .table('events').query or 
  //  .table('events').sql or something? unclear, need to think about what is cleanest and most intuitive, and also how to support streaming queries, etc. with the builder, etc.?

  // B. Pre-defined Pipes (Tinybird style)
  // Clean, inline arguments. Returns the execution builder.
  const { data: salesData } = await wavehouse
    .pipe('daily_sales_aggregation', { region: 'NA', category: 'software' })
    .fetch();

  // ============================================================================
  // 5. STREAMING & LIVE DATA (SSE / Polling under the hood)
  // ============================================================================

  // The .stream() method returns a StreamController. It doesn't connect until
  // someone actually subscribes. If the backend cannot stream this specific query
  // via NATS, the SDK seamlessly falls back to the provided pollInterval.
  const livePurchases = wavehouse
    .table('events')
    .where('event_type', '==', 'purchase')
    .stream({ fallbackPollInterval: 5000 });

  // TODO ^ raw streams ideally should just send all available data for a table, and let the
  // client handle filtering out data they don't want (like event_type == 'purchase'), so
  // that we'd be able to have separate backend logic for generic table streams that
  // just send all the data and are super easy, vs backend logic that needs to handle actual queries,
  // whether they be something simple like this where filter or something more advanced like an aggregate etc...

  // A. The UI / React Pattern (Observer Object)
  // Keeps component logic isolated. Distributed hooks mean multiple components
  // can subscribe to the same stream and manage their own UI loading states.
  const unsubscribe = livePurchases.subscribe({
    next: (payload) => console.log('New purchase!', payload),
    status: (state) => updateUIIndicator(state), // 'connecting', 'live', 'reconnecting', 'polling'
    error: (err) => console.error('Connection issue:', err)
  });

  // ... later when the React component unmounts:
  // Kills the network connection ONLY if no other components are still listening.
  unsubscribe();

  // B. The Node.js / Backend Pattern (Async Iterator)
  // Excellent for backend workers or simple scripts.
  const liveMetrics = wavehouse.pipe('live_active_users').stream();

  for await (const payload of liveMetrics) {
    console.log('Active Users:', payload.count);

    // Breaking the loop automatically fires the unsubscribe/cleanup logic!
    if (payload.count > 10000) break;
  }

  // ============================================================================
  // 6. ADMIN OVERRIDES (Privileged execution)
  // ============================================================================

  // Raw SQL is strictly for admin backends.
  // Evaluated exactly like everything else: with a tuple.
  // Ideally this would mean that the frontend client SDK can do tree-shaking type
  // optimization to remove this `.admin` namespace entirely from the bundle, right?
  // TODO: ideally, all of the different pieces (`.sys`, `.admin`, etc. could be
  // built to only be included in the shipped browser JS if used, but how would that
  // actually work and to what level can we take it, like would it be bad to have
  // `.schema()` not ship if not used, then preventing them from being able to run
  // `wavehouse.table('table_name').schema()` in the browser console to debug if needed, etc.?)
  const { data: rawData, error: adminErr } = await wavehouse.admin.sql(`
    SELECT database, table, total_rows
    FROM system.tables
  `);

  if (adminErr) {
    console.error('Admin query failed. Check JWT claims.', adminErr);
  } else {
    console.log('System Tables:', rawData);
  }
}

main();
```
