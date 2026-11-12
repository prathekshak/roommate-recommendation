Start the distributed API first:

    ./run.sh

Then start the static UI in another terminal:

    python3 -m http.server 8080 --directory ui

Open http://127.0.0.1:8080 in a browser. Select node A, B, or C from the
dropdown. The UI sends JSON requests to the selected node and displays health,
leader, events, profiles, listings, recommendations, subscriptions, and
reservations.
