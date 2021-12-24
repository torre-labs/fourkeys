# Incidents Table
SELECT
source,
incident_id,
MIN(IF(root.time_created < issue.time_created, root.time_created, issue.time_created)) as time_created,
MAX(time_resolved) as time_resolved,
ARRAY_AGG(root_cause IGNORE NULLS) changes,
FROM
(
    SELECT source,
       metadata,
       CASE
           WHEN source LIKE "github%" THEN JSON_EXTRACT_SCALAR(metadata, '$.issue.number')
           WHEN source LIKE "gitlab%" AND event_type = "note"
               THEN JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.noteable_id')
           WHEN source LIKE "gitlab%" AND event_type = "issue"
               THEN JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.id')
           WHEN source LIKE "incident-reporter" AND event_type = "incident"
               THEN JSON_EXTRACT_SCALAR(metadata, '$.id')
           END                                                                                   AS incident_id,
       TIMESTAMP(CASE
                     WHEN source LIKE "github%" THEN JSON_EXTRACT_SCALAR(metadata, '$.issue.created_at')
                     WHEN source LIKE "gitlab%"
                         THEN JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.created_at')
                     WHEN source LIKE "incident-reporter" AND event_type = "incident"
                         THEN JSON_EXTRACT_SCALAR(metadata, '$.created_time') END)               AS time_created,
       TIMESTAMP(CASE
                     WHEN source LIKE "github%" THEN JSON_EXTRACT_SCALAR(metadata, '$.issue.closed_at')
                     WHEN source LIKE "gitlab%"
                         THEN JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.closed_at')
                     WHEN source LIKE "incident-reporter" AND event_type = "incident"
                         THEN JSON_EXTRACT_SCALAR(metadata,
                                                  "$.properties['Solved time'].date.start") END) as time_resolved,
       CASE
           WHEN source LIKE "incident-reporter" AND event_type = "incident"
               THEN JSON_EXTRACT_SCALAR(metadata, "$.properties['Introduced at (commit)'].plain_text")
           ELSE REGEXP_EXTRACT(metadata, r"root cause: ([[:alnum:]]*)") END                      as root_cause,
       CASE
           WHEN source LIKE "github%" THEN REGEXP_CONTAINS(JSON_EXTRACT(metadata, '$.issue.labels'),
                                                           '"name":"Incident"')
           WHEN source LIKE "gitlab%" THEN REGEXP_CONTAINS(JSON_EXTRACT(metadata, '$.object_attributes.labels'),
                                                           '"title":"Incident"')
           WHEN source LIKE "incident-reporter" AND event_type = "incident" THEN
                   JSON_EXTRACT_SCALAR(metadata, '$.properties.Priority.select.name') IN ("High", "Highest")
           END                                                                                   as bug,
    FROM four_keys.events_raw
    WHERE event_type = "incident"
      AND source = "incident-reporter"
) issue
LEFT JOIN (SELECT time_created, changes FROM four_keys.deployments d, d.changes) root on root.changes = root_cause
GROUP BY 1,2
HAVING max(bug) is True
;
