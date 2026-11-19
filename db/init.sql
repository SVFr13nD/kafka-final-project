CREATE TABLE products (
 product_id text PRIMARY KEY, document jsonb NOT NULL,
 kafka_partition integer NOT NULL, kafka_offset bigint NOT NULL
);
CREATE TABLE product_offsets (
 product_id text PRIMARY KEY, kafka_partition integer NOT NULL, kafka_offset bigint NOT NULL
);
CREATE TABLE client_requests (
 request_id uuid PRIMARY KEY, user_id text NOT NULL, event_type text NOT NULL,
 document jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE outbox (
 request_id uuid PRIMARY KEY REFERENCES client_requests, document jsonb NOT NULL,
 published_at timestamptz
);
CREATE TABLE recommendations (
 user_id text PRIMARY KEY, document jsonb NOT NULL,
 kafka_partition integer NOT NULL, kafka_offset bigint NOT NULL
);
CREATE INDEX products_name ON products ((lower(document->>'name')));
CREATE INDEX outbox_pending ON outbox(request_id) WHERE published_at IS NULL;
