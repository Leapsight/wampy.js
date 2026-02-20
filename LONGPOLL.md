  node test-bondy-transports.mjs --url http://localhost:18080/ws --realm com.leapsight.test

  What it does for each transport (longpoll then sse):

  1. Connects to Bondy
  2. Registers an RPC (com.example.test.add2) that adds two numbers
  3. Subscribes to a topic (com.example.test.onhello)
  4. Publishes an event to that topic (with exclude_me: false so it receives its own event)
  5. Calls the RPC it registered
  6. Unsubscribes, unregisters, and disconnects
  7. Prints a pass/fail summary

  Adjust the URL and REALM defaults at the top if your Bondy instance uses different values.

