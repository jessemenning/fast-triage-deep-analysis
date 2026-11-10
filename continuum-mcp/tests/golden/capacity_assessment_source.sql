-- ---------------------------------------------------------------------------
-- Source stream: capacity_assessment (derived) -> queue fl-cp-demo-capacity_assessment
-- Subscriptions: fl/faa/arrival-capacity/demand/>
-- Initial state: one-shot snapshot of arrival-capacity.capacity_assessment, then live changes.
-- ---------------------------------------------------------------------------
CREATE TABLE capacity_assessment_raw (
    airport VARCHAR,
    bin_start TIMESTAMP,
    bin_end TIMESTAMP,
    demand BIGINT,
    bin_capacity INTEGER,
    runway_config VARCHAR,
    overage BIGINT,
    eta_quality DOUBLE,
    exceeded BOOLEAN
) WITH ('connectors' = '[
    {
        "name": "cp-demo-capacity_assessment-snapshot",
        "transport": {
            "name": "url_input",
            "config": {
                "path": "http://feldera:8080/v0/pipelines/arrival-capacity/query?sql=SELECT%20%2A%20FROM%20capacity_assessment&format=json"
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw",
                "array": false
            }
        }
    },
    {
        "name": "cp-demo-capacity_assessment",
        "transport": {
            "name": "solace_input",
            "config": {
                "host": "solace",
                "port": 55555,
                "vpn": "default",
                "username": "default",
                "password": "default",
                "queue": "fl-cp-demo-capacity_assessment"
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "insert_delete",
                "array": false
            }
        }
    }
]');

-- 'capacity_assessment' mirrors the base pipeline's view: the live topic carries its full
-- changelog (inserts and deletes) and the engine applies both to the table above.
CREATE VIEW capacity_assessment AS
SELECT airport, bin_start, bin_end, demand, bin_capacity, runway_config, overage, eta_quality, exceeded FROM capacity_assessment_raw;
