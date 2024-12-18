CREATE TABLE consumer_complaints
(
    date_received                DATE,
    product                      TEXT,
    sub_product                  TEXT,
    issue                        TEXT,
    sub_issue                    TEXT,
    consumer_complaint_narrative TEXT,
    company_public_response      TEXT,
    company                      TEXT,
    state                        TEXT,
    zip_code                     VARCHAR(200),
    tags                         TEXT,
    consumer_consent_provided    TEXT,
    submitted_via                TEXT,
    date_sent_to_company         DATE,
    company_response_to_consumer TEXT,
    timely_response              TEXT,
    consumer_disputed            TEXT,
    complaint_id                 BIGINT PRIMARY KEY
);

/*
COPY consumer_complaints(
    date_received,
    product,
    sub_product,
    issue,
    sub_issue,
    consumer_complaint_narrative,
    company_public_response,
    company,
    state,
    zip_code,
    tags,
    consumer_consent_provided,
    submitted_via,
    date_sent_to_company,
    company_response_to_consumer,
    timely_response,
    consumer_disputed,
    complaint_id
)
FROM '/tmp/data/local/complaints.csv'
WITH (
    FORMAT csv,
    HEADER true,
    QUOTE '"',
    DELIMITER ',',
    NULL ''
);
 */