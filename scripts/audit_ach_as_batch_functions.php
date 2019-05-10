<?php

use Acquia\ContentHubClient\Entity as ContentHubEntity;
use Drupal\acquia_contenthub\ContentHubEntitiesTracking;
use Drupal\user\Entity\User;

/**
 * Compares entities in the tracking table with Content Hub.
 *
 * Imports entities that do not exist in the tracking table or Drupal, or
 * the modified date in the tracking table does not match the modified date
 * in Content Hub.
 *
 * @param array $entities
 *   An array of records from the tracking table.
 * @param bool $import
 *   1 to import entities, FALSE to just print.
 * @param int $operation_key
 *   The key of the batch operation.
 * @param mixed $context
 *   The context array.
 *
 * @return mixed|false
 *   Drush Output.
 */
function audit_acquia_contenthub_subscriber_audit_subscriber(array $entities, $import, $operation_key, $delete, &$context) {
  $time = time();
  \Drupal::state()->set('ach_as_running', $time);
  if (empty($context['sandbox'])) {
    $context['results']['total_processed'] = !empty($context['results']['total_processed']) ? $context['results']['total_processed'] : 0;
    $context['results']['deleted'] = !empty($context['results']['deleted']) ? $context['results']['deleted'] : 0;
    $context['results']['outdated'] = !empty($context['results']['outdated']) ? $context['results']['outdated'] : 0;
    $context['results']['missing'] = !empty($context['results']['missing']) ? $context['results']['missing'] : 0;
    $context['results']['start_date'] = !empty($context['results']['start_date']) ? $context['results']['start_date'] : date_create();
    $context['results']['print'] = !empty($context['results']['print']) ? $context['results']['print'] : FALSE;
  }
  /** @var \Drupal\acquia_contenthub\Client\ClientManager $client_manager */
  $client_manager = \Drupal::service('acquia_contenthub.client_manager');
  if (!$client_manager->isConnected()) {
    return drush_set_error(dt('The Content Hub client is not connected. No operations could be performed.'));
  }

  // Collect UUIDs.
  $uuids = [];
  foreach ($entities as $entity) {
    $uuids[] = $entity->entity_uuid;
  }

  /** @var \Acquia\ContentHubClient\Entity[] $ch_entities */
  $ch_entities = audit_acquia_contenthub_audit_subscriber_get_ch_entities($uuids, 10);

  if ($ch_entities === FALSE) {
    die('ERROR: Request to Content Hub failed.');
  }
  foreach ($entities as $entity) {
    $should_import = FALSE;
    $uuid = $entity->entity_uuid;
    $ch_entity = isset($ch_entities[$uuid]) ? $ch_entities[$uuid] : FALSE;
    if (!$ch_entity) {
      // Entity is in the tracking table, but does not exist in Content Hub. Delete it.
      $drupal_entity = \Drupal::service("entity.repository")
        ->loadEntityByUuid($entity->entity_type, $entity->entity_uuid);
      if ($drupal_entity) {
        drush_print(dt('Entity is in the tracking table, but does not exist in Content Hub. Deleting from Drupal and tracking table.: Entity Type = @type, UUID = @uuid, ID = @id, Modified = @modified', [
          '@type' => $entity->entity_type,
          '@uuid' => $entity->entity_uuid,
          '@id' => $entity->entity_id,
          '@modified' => $entity->modified,
        ]));
        if ($delete) {
          $drupal_entity->delete();
        }
      }
      else {
        drush_print(dt('Entity is in the tracking table, but does not exist in Content Hub or Drupal. Deleting from tracking table.: Entity Type = @type, UUID = @uuid, ID = @id, Modified = @modified', [
          '@type' => $entity->entity_type,
          '@uuid' => $entity->entity_uuid,
          '@id' => $entity->entity_id,
          '@modified' => $entity->modified,
        ]));
        // Delete entity from the tracking table.
        /** @var \Drupal\acquia_contenthub\ContentHubEntitiesTracking $entities_tracking */
        $entities_tracking = \Drupal::getContainer()->get('acquia_contenthub.acquia_contenthub_entities_tracking');
        if ($tracking_record = $entities_tracking->loadImportedByUuid($entity->entity_uuid)) {
          if ($delete) {
            $tracking_record->delete();
          }
        }
      }

      $context['results']['deleted']++;
    }
    else {
      // Entity exists in Content Hub.
      if ($entity->modified !== $ch_entity->getModified()) {
        // Entity exists in Content Hub but the modified date in the tracking table does not match.
        // Import the entity.
        drush_print(dt('Outdated entity: Entity Type = @type, UUID = @uuid, ID = @id, Modified (local) = @lmodified, Modified (remote) = @rmodified', [
          '@type' => $entity->entity_type,
          '@uuid' => $entity->entity_uuid,
          '@id' => $entity->entity_id,
          '@lmodified' => $entity->modified,
          '@rmodified' => $ch_entity->getModified(),
        ]));

        $context['results']['outdated']++;
        $should_import = TRUE;
      }
      else {
        // Entity exists in Content Hub, and the modified date in the tracking table matches.
        // Make sure the entity exists in Drupal.
        if (!audit_acquia_contenthub_audit_subscriber_entity_exists($entity)) {
          drush_print(dt('Entity exists in the tracking table, and the modified date matches in Content Hub, but Drupal entity does not exist. Importing.: Entity Type = @type, UUID = @uuid, ID = @id, Modified = @modified', [
            '@type' => $entity->entity_type,
            '@uuid' => $entity->entity_uuid,
            '@id' => $entity->entity_id,
            '@modified' => $entity->modified,
          ]));

          $should_import = TRUE;
          $context['results']['missing']++;
        }
      }

      if ($should_import && $import) {
        /** @var \Drupal\acquia_contenthub\Controller\ContentHubEntityExportController $export_controller */
        $import_manager = \Drupal::service("acquia_contenthub.import_entity_manager");
        // Import Entities.
        $import_manager->import($entity->entity_uuid);
      }
    }
    $context['results']['total_processed']++;
  }

  // Print the status.
  $duration = date_diff($context['results']['start_date'], date_create());
  audit_acquia_contenthub_audit_subscriber_print_status($context['results']['total_processed'], $duration, 2, $context['results']['print']);
  // Update the last processed operation state variable.
  \Drupal::state()->set(ACH_AS_LAST_PROCESSED_OPERATION, $operation_key);
}

function audit_acquia_contenthub_subscriber_audit_subscriber_find_missing_imports($contenthub_filter_id, $start, $size, $reimport, $operation_key, &$context) {
  \Drupal::state()->set('ach_as_running', time());
  if (empty($context['sandbox'])) {
    $context['results']['ch_total_processed'] = !empty($context['results']['ch_total_processed']) ? $context['results']['ch_total_processed'] : 0;
    $context['results']['ch_missing'] = !empty($context['results']['ch_missing']) ? $context['results']['ch_missing'] : 0;
    $context['results']['ch_start_date'] = !empty($context['results']['ch_start_date']) ? $context['results']['ch_start_date'] : date_create();
    $context['results']['print'] = !empty($context['results']['print']) ? $context['results']['print'] : FALSE;
  }

  $contenthub_filter = \Drupal::entityTypeManager()->getStorage('contenthub_filter')->load($contenthub_filter_id);

  /* @var \Drupal\acquia_contenthub\ContentHubSearch $contenthub_search */
  $contenthub_search = \Drupal::service('acquia_contenthub.acquia_contenthub_search');
  $entities = $contenthub_search->getContentHubFilteredEntities($contenthub_filter, $start, $size);
  $context['results']['ch_total_processed'] += count($entities);

  unset($entities['total']);
  $uuids = array_keys($entities);

  $uuids_chunk = array_chunk($uuids, 10);
  $uuids_found = [];
  foreach ($uuids_chunk as $uuids_list) {
    $new_uuids_found = \Drupal::database()
      ->select(ContentHubEntitiesTracking::TABLE, 'ci')
      ->fields('ci', ['entity_uuid'])
      ->condition('entity_uuid', $uuids_list, 'IN')
      ->execute()
      ->fetchAllAssoc('entity_uuid');
    $new_uuids_found = array_column($new_uuids_found, 'entity_uuid');
    //drush_print("UUIDS Found:\n");
    //drush_print_r($new_uuids_found);
    $uuids_found = array_merge($uuids_found, $new_uuids_found);
  }

  // These are the entities that exist in Content Hub but not in the local
  // tracking table for publishers.
  $uuids_missing = array_diff($uuids, $uuids_found);
  //drush_print("UUIDS Missing:\n");
  //drush_print_r($uuids_missing);
  foreach ($uuids_missing as $uuid) {
    $context['results']['ch_missing']++;
    if ($reimport) {
      // Re-import missing entities.
      /* @var \Drupal\acquia_contenthub\ImportEntityManager $import_entity_manager */
      $import_entity_manager = \Drupal::service('acquia_contenthub.import_entity_manager');

      // Determine the author UUID for the nodes to be created.
      // Assign the appropriate author for this filter (User UUID).
      $uid = $contenthub_filter->author;
      $user = User::load($uid);

      // If filter condition evaluates to TRUE, save entity with dependencies.
      // Get the Status from the Filter Information.
      $status = $contenthub_filter->getPublishStatus();

      // Re-importing or re-queuing entities matching the filter that were not
      // previously imported.
      $import_entity_manager->import($uuid, TRUE, $user->uuid(), $status ? $status : NULL);
    }

    drush_print(dt('Importing entity with UUID = !uuid and type = !type that matches filter "!filter" and was not previously imported.', [
      '!type' => $entities[$uuid]->getType(),
      '!uuid' => $uuid,
      '!filter' => $contenthub_filter_id,
    ]));

  }

  $duration = date_diff($context['results']['ch_start_date'], date_create());
  audit_acquia_contenthub_audit_subscriber_print_status($context['results']['ch_total_processed'], $duration, 2, $context['results']['print']);
  \Drupal::state()->set('ach_as_last_processed_operation', $operation_key);
}

/**
 * Prints results from the comparison of the tracking table with Content Hub.
 *
 * @param bool $success
 *   TRUE if there were not PHP fatal errors, FALSE otherwise.
 * @param mixed $results
 *   An array of results.
 * @param mixed $operations
 *   The operations array.
 */
function audit_acquia_contenthub_subscriber_audit_subscriber_finished($success, $results, $operations) {
  // The 'success' parameter means no fatal PHP errors were detected. All
  // other error management should be handled using 'results'.
  if ($success) {
    drush_print(dt(str_repeat('-', 80)));
    drush_print(dt('Batch completed successfully. The results are:'));
    $duration = date_diff($results['start_date'], date_create());
    drush_print(dt('@total entities processed from the tracking table within @duration.', [
      '@total' => number_format($results['total_processed']),
      '@duration' => audit_acquia_contenthub_audit_subscriber_generate_duration_message($duration),
    ]));
    drush_print(dt(' - @total entities deleted from Drupal and tracking table.', [
      '@total' => number_format($results['deleted']),
    ]));
    drush_print(dt(' - @total entities missing from Drupal.', [
      '@total' => number_format($results['missing']),
    ]));
    drush_print(dt(' - @total entities outdated in tracking table.', [
      '@total' => number_format($results['outdated']),
    ]));

    $duration = date_diff($results['ch_start_date'], date_create());
    drush_print(dt('@total entities processed from the tracking table within @duration.', [
      '@total' => number_format($results['ch_total_processed']),
      '@duration' => audit_acquia_contenthub_audit_subscriber_generate_duration_message($duration),
    ]));
    drush_print(dt('Total number of Content Hub entities not found in tracking table: @total', [
      '@total' => number_format($results['ch_missing']),
    ]));
  }
  else {
    drush_print(dt('Finished with a PHP fatal error.'));
  }
}

function audit_acquia_contenthub_audit_subscriber_entity_exists($entity) {
  // Try to find the entity locally.
  $entity_type = \Drupal::entityTypeManager()->getStorage($entity->entity_type)->getEntityType();
  $table = $entity_type->getBaseTable();
  $id_col = $entity_type->getKey("id");
  $entity_id = \Drupal::database()->select($table)
    ->fields($table, [$id_col])
    ->condition("$table.$id_col", $entity->entity_id)
    ->execute()
    ->fetchField();

  if ($entity_id) {
    return TRUE;
  }

  return FALSE;
}

/**
 * Prints the total number of processed entities, and the duration.
 *
 * @param $total
 * @param \DateInterval $duration
 * @param int $interval
 * @param bool $print
 */
function audit_acquia_contenthub_audit_subscriber_print_status($total, DateInterval $duration, int $interval = 0, bool &$print = FALSE) {
  $duration_hours = $duration->h;
  $duration_minutes = $duration->i;

  if (($duration_hours !== 0 || $duration_minutes !== 0 ) && $duration_minutes % $interval === 0 && $print === TRUE) {
    // The interval has passed.
    // Print message.
    $message = dt('@total entities processed within @duration.', [
      '@total' => number_format($total),
      '@duration' => audit_acquia_contenthub_audit_subscriber_generate_duration_message($duration),
    ]);
    drush_print($message);
    // Only print the duration once within the interval.
    $print = FALSE;
  }
  elseif ($duration->i % $interval === 1) {
    // One minute has passed since the last print.
    // Enable print flag.
    $print = TRUE;
  }
}

function audit_acquia_contenthub_audit_subscriber_get_ch_entities($uuids, $size = 25) {
  /** @var \Drupal\acquia_contenthub\Client\ClientManager $client_manager */
  $client_manager = \Drupal::service('acquia_contenthub.client_manager');

  $chunks = array_chunk($uuids, $size);
  $objects = [];
  foreach ($chunks as $chunk) {
    $query = [
      'size' => $size,
      'fields' => [
        'uuid', 'data.type', 'data.modified',
      ],
      'query' => [
        'constant_score' => [
          'filter' => [
            'terms' => [
              'uuid' => $chunk,
            ],
          ],
        ],
      ],
    ];
    $results = $client_manager->createRequest('searchEntity', [$query]);
    if ($results === FALSE) {
      return FALSE;
    }
    if (is_array($results) && isset($results['hits']['total']) && $results['hits']['total'] > 0) {
      foreach ($results['hits']['hits'] as $key => $item) {
        $uuid = $item['fields']['uuid'][0];
        $entity = [
          'uuid' => $uuid,
          'type' => $item['fields']['data.type'][0],
          'modified' => $item['fields']['data.modified'][0],
        ];
        $objects[$uuid] = new ContentHubEntity($entity);
      }
    }
  }
  return $objects;
}

//function audit_acquia_contenthub_audit_subscriber_generate_duration_message(DateTimeInterface $start_date, DateTimeInterface $end_date, int $interval = 0, bool &$print) {
//  if (empty($end_date)) {
//    $end_date = date_create();
//  }
//  $duration = date_diff($start_date, $end_date);
//  if ($duration === FALSE) {
//    return '';
//  }
//  $duration_hours = $duration->h;
//  $duration_minutes = $duration->i;
//  $duration_message = '';
//  $duration_message_parts = [];
//
//  if (($duration_hours !== 0 || $duration_minutes !== 0 ) && ($duration_minutes % $interval === 0) && $print === TRUE) {
//    // Hours.
//    if ($duration_hours > 0) {
//      $duration_message_parts[0] = $duration_hours . ' hour';
//      if ($duration_hours > 1) {
//        $duration_message_parts[0] .= 's';
//      }
//    }
//
//    // Minutes.
//    if ($duration_minutes > 0) {
//      $duration_message_parts[2] = $duration_minutes . ' minutes';
//      if ($duration_minutes > 1) {
//        $duration_message_parts[2] .= 's';
//      }
//    }
//
//    $duration_message = implode(' ', $duration_message_parts);
//  }
//
//  return $duration_message;
//}

function audit_acquia_contenthub_audit_subscriber_generate_duration_message(DateInterval $duration) {
  $duration_hours = $duration->h;
  $duration_minutes = $duration->i;
  $duration_message_parts = [];

  // Hours.
  if ($duration_hours > 0) {
    $duration_message_parts[0] = $duration_hours . ' hour';
    if ($duration_hours > 1) {
      $duration_message_parts[0] .= 's';
    }
  }

  // Minutes.
  if ($duration_minutes > 0) {
    $duration_message_parts[1] = $duration_minutes . ' minute';
    if ($duration_minutes > 1) {
      $duration_message_parts[1] .= 's';
    }
  }

  return implode(' ', $duration_message_parts);
}
