# Changes Table
WITH `changes_made_via_pr` AS (
    SELECT `source`,
           `event_type`,
           JSON_EXTRACT_SCALAR(`metadata`, '$.pull_request.merge_commit_sha') AS `change_id`,
           JSON_EXTRACT_SCALAR(`metadata`, '$.repository.full_name')      AS `repository`,
           TIMESTAMP_TRUNC(TIMESTAMP(JSON_EXTRACT_SCALAR(`metadata`, '$.pull_request.created_at')),
                           SECOND)                                            AS `time_created`
    FROM `four_keys`.`events_raw`
    WHERE `event_type` = 'pull_request'
      AND JSON_EXTRACT_SCALAR(`metadata`, '$.pull_request.state') = 'closed'
    GROUP BY 1, 2, 3, 4, 5
),
     `changes_without_pr` AS (
         SELECT `source`,
                `event_type`,
                JSON_EXTRACT_SCALAR(`commit`, '$.id')                                               `change_id`,
                JSON_EXTRACT_SCALAR(`e`.`metadata`, '$.repository.full_name')      AS `repository`,
                TIMESTAMP_TRUNC(TIMESTAMP(JSON_EXTRACT_SCALAR(`commit`, '$.timestamp')), SECOND) AS `time_created`,
         FROM `four_keys`.`events_raw` `e`,
              UNNEST(JSON_EXTRACT_ARRAY(`e`.`metadata`, '$.commits')) AS `commit`
         WHERE `event_type` = "push"
         GROUP BY 1, 2, 3, 4, 5
     ),
     `all_changes` AS (
         SELECT *
         FROM `changes_made_via_pr`
         UNION ALL
         SELECT *
         FROM `changes_without_pr`
         WHERE `change_id` NOT IN (SELECT `change_id` FROM `changes_made_via_pr`))
SELECT *
FROM `all_changes`;