<?php

use Acquia\ContentHubClient\Entity as ContentHubEntity;
use Drupal\acquia_contenthub\ContentHubEntitiesTracking;
use Drupal\node\NodeInterface;
use Drupal\Component\Serialization\Json;
use Drupal\acquia_contenthub\Normalizer\NormalizerWrapper;

const ACH_AP_LAST_PROCESSED_OPERATION = 'ach_ap_last_processed_operation';

/**
 * Checks published entities and compares them with Content Hub.
 *
 * This method also republishes entities if they are not in sync with what
 * exists currently in Content Hub.
 *
 * @param array $entities
 *   An array of records from the tracking table.
 * @param bool $republish
 *   1 to republish entities, FALSE to just print.
 * @param bool $delete
 *   TRUE to delete the entities, FALSE or NULL otherwise.
 * @param int $operation_key
 *   The key of the batch operation.
 * @param mixed $context
 *   The context array.
 *
 * @return mixed|false
 *   Drush Output.
 */
function audit_acquia_contenthub_audit_publisher(array $entities, $republish, $delete, $operation_key, &$context) {
  $time = time();
  \Drupal::state()->set('ach_ap_running', $time);
  if (empty($context['sandbox'])) {
    $context['results']['published'] = !empty($context['results']['published']) ? $context['results']['published'] : 0;
    $context['results']['not_published'] = !empty($context['results']['not_published']) ? $context['results']['not_published'] : 0;
    $context['results']['status_updated'] = !empty($context['results']['status_updated']) ? $context['results']['status_updated'] : 0;
    $context['results']['in_sync'] = !empty($context['results']['in_sync']) ? $context['results']['in_sync'] : 0;
    $context['results']['deleted'] = !empty($context['results']['deleted']) ? $context['results']['deleted'] : 0;
    $context['results']['outdated'] = !empty($context['results']['outdated']) ? $context['results']['outdated'] : 0;
    $context['results']['total_processed'] = !empty($context['results']['total_processed']) ? $context['results']['total_processed'] : 0;
    $context['results']['start_time'] = !empty($context['results']['start_time']) ? $context['results']['start_time'] : date_create();
    $context['results']['print_duration'] = !empty($context['results']['print_duration']) ? $context['results']['print_duration'] : FALSE;
  }
  $connection = \Drupal\Core\Database\Database::getConnection('default');

  /** @var \Drupal\acquia_contenthub\Client\ClientManager $client_manager */
  $client_manager = \Drupal::service('acquia_contenthub.client_manager');
  if (!$client_manager->isConnected()) {
    return drush_set_error(dt('The Content Hub client is not connected so no operations could be performed.'));
  }

  // Collect UUIDs.
  $uuids = [];
  foreach ($entities as $entity) {
    $uuids[] = $entity->entity_uuid;
  }

  /** @var \Drupal\acquia_contenthub\EntityManager $entity_manager */
  $entity_manager = \Drupal::service('acquia_contenthub.entity_manager');
  /** @var \Acquia\ContentHubClient\Entity[] $ch_entities */
//  $ch_entities = audit_acquia_contenthub_audit_publisher_create_request($client_manager, 'readEntities', [$uuids]);
  $ch_entities = audit_acquia_contenthub_audit_publisher_get_ch_entities($uuids, 10);
  foreach ($entities as $entity) {
    $out_of_sync = FALSE;
    $uuid = $entity->entity_uuid;
    $ch_entity = isset($ch_entities[$uuid]) ? $ch_entities[$uuid] : FALSE;
    if (!$ch_entity) {
      // Entity exists in tracking table, but does not exist in Content Hub.
      drush_print(dt('Entity in tracking table does not exist in Content Hub: Entity Type = @type, UUID = @uuid, ID = @id, Modified = @modified', [
        '@type' => $entity->entity_type,
        '@uuid' => $entity->entity_uuid,
        '@id' => $entity->entity_id,
        '@modified' => $entity->modified,
      ]));
      $out_of_sync = TRUE;
      $context['results']['not_published']++;
    }
    elseif ($ch_entity && $entity->modified !== $ch_entity->getModified()) {
      // Entity exists in Content Hub but the modified flag does not match.
      drush_print(dt('Outdated entity: Entity Type = @type, UUID = @uuid, ID = @id, Modified (local) = @lmodified, Modified (remote) = @rmodified', [
        '@type' => $entity->entity_type,
        '@uuid' => $entity->entity_uuid,
        '@id' => $entity->entity_id,
        '@lmodified' => $entity->modified,
        '@rmodified' => $ch_entity->getModified(),
      ]));
      $out_of_sync = TRUE;
      $context['results']['outdated']++;
    }
    else {
      // These entities are already in sync with Content Hub, but we still need
      // to evaluate their eligibility.
      $drupal_entity = \Drupal::entityTypeManager()->getStorage($entity->entity_type)->load($entity->entity_id);
      if ($drupal_entity) {
        if (isEligible($entity_manager, $drupal_entity)) {
          if (!audit_acquia_contenthub_audit_publisher_is_exported($connection, $entity)) {
            // These entities have been published and are in sync with Content Hub
            // so no need to republish but might need to change the status in the
            // tracking table.
            drush_print(dt('Entity is in tracking, in sync with Content Hub and Drupal. Make status exported: Entity Type = @type, UUID = @uuid, ID = @id, Modified (local) = @lmodified, Modified (remote) = @rmodified', [
              '@type' => $entity->entity_type,
              '@uuid' => $entity->entity_uuid,
              '@id' => $entity->entity_id,
              '@lmodified' => $entity->modified,
              '@rmodified' => $ch_entity->getModified(),
            ]));
            $connection->update(ContentHubEntitiesTracking::TABLE)
              ->condition('entity_type', $entity->entity_type)
              ->condition('entity_id', $entity->entity_id)
              ->fields([
                'status_export' => ContentHubEntitiesTracking::EXPORTED,
              ])
              ->execute();
            $context['results']['status_updated']++;
          }
          $context['results']['in_sync']++;
        }
        else {
          // These entities have been published and are in sync with Content hub
          // but they are NOT eligible to be published anymore. Delete it by using
          // delete option.
          $msg = '';
          if ($delete) {
            audit_acquia_contenthub_audit_publisher_delete_remote_entity($client_manager, $connection, $entity->entity_type, $entity->entity_uuid);
            $msg = t('. Deleting');
          }
          drush_print(dt('Entity exists in the tracking table and its in sync with Content Hub but is no longer eligible for exporting. Check entity configuration@deleting: Entity Type = @type, UUID = @uuid, ID = @id, Modified (local) = @lmodified, Modified (remote) = @rmodified', [
            '@deleting' => $msg,
            '@type' => $entity->entity_type,
            '@uuid' => $entity->entity_uuid,
            '@id' => $entity->entity_id,
            '@lmodified' => $entity->modified,
            '@rmodified' => $ch_entity ? $ch_entity->getModified() : dt('Not found in Content Hub'),
          ]));
          $context['results']['deleted']++;
        }
      }
      else {
        // These entities have been published and are in sync with Content Hub
        // but the drupal entity does not exist anymore. Delete automatically.
        audit_acquia_contenthub_audit_publisher_delete_remote_entity($client_manager, $connection, $entity->entity_type, $entity->entity_uuid);
        drush_print(dt('Entity exists in the tracking table and its in sync with Content Hub but the entity does not exist in Drupal. Deleting: Entity Type = @type, UUID = @uuid, ID = @id, Modified (local) = @lmodified, Modified (remote) = @rmodified', [
          '@type' => $entity->entity_type,
          '@uuid' => $entity->entity_uuid,
          '@id' => $entity->entity_id,
          '@lmodified' => $entity->modified,
          '@rmodified' => $ch_entity ? $ch_entity->getModified() : dt('Not found in Content Hub'),
        ]));
        $context['results']['deleted']++;
      }
    }
    if ($out_of_sync) {
      $entity_id = FALSE;
      if ($republish) {
        $drupal_entity = \Drupal::entityTypeManager()->getStorage($entity->entity_type)->load($entity->entity_id);
        if ($drupal_entity) {
          $entity_id = $drupal_entity->id();
          if (isEligible($entity_manager, $drupal_entity)) {
            /** @var \Drupal\acquia_contenthub\Controller\ContentHubEntityExportController $export_controller */
            $export_controller = \Drupal::service('acquia_contenthub.acquia_contenthub_export_entities');
            // Export Entities.
//            $export_controller->exportEntities([$drupal_entity]);
            temporal_directly_export_entity_without_export_queue($entity_manager, $export_controller, $drupal_entity);
            drush_print(dt('Exporting Entity Type = @type, UUID = @uuid, ID = @id', [
              '@type' => $entity->entity_type,
              '@uuid' => $entity->entity_uuid,
              '@id' => $entity->entity_id,
            ]));
            $context['results']['published']++;
          }
          else {
            $msg = '';
            if ($delete) {
              audit_acquia_contenthub_audit_publisher_delete_remote_entity($client_manager, $connection, $entity->entity_type, $entity->entity_uuid);
              $msg = t('. Deleting');
            }
            // Entity is not eligible for exporting anymore.
            drush_print(dt('Entity exists in the tracking table but is no longer eligible for exporting@deleting: Entity Type = @type, UUID = @uuid, ID = @id, Modified (local) = @lmodified, Modified (remote) = @rmodified', [
              '@deleting' => $msg,
              '@type' => $entity->entity_type,
              '@uuid' => $entity->entity_uuid,
              '@id' => $entity->entity_id,
              '@lmodified' => $entity->modified,
              '@rmodified' => $ch_entity ? $ch_entity->getModified() : dt('Not found in Content Hub'),
            ]));
            $context['results']['deleted']++;
          }
        }
      }
      else {
        // Get the Drupal entity ID.
        $entity_type = \Drupal::entityTypeManager()->getStorage($entity->entity_type)->getEntityType();
        $table = $entity_type->getBaseTable();
        $id_col = $entity_type->getKey("id");
        $query = $connection->select($table)
          ->fields($table, [$id_col]);
        $query->condition("$table.$id_col", $entity->entity_id);
        $entity_id = $query->execute()->fetchField();
      }

      if (empty($entity_id)) {
        // Entity exists in Content Hub and the tracking table, but does not exist in Drupal.
        // Delete from tracking table and Content Hub.
        audit_acquia_contenthub_audit_publisher_delete_remote_entity($client_manager, $connection, $entity->entity_type, $entity->entity_uuid);
        // The drupal entity could not be loaded.
        drush_set_error(dt('This entity exists in the tracking table but could not be loaded in Drupal. Deleting.: Entity Type = @type, UUID = @uuid, ID = @id, Modified = @modified', [
          '@type' => $entity->entity_type,
          '@uuid' => $entity->entity_uuid,
          '@id' => $entity->entity_id,
          '@modified' => $entity->modified,
        ]));
        $context['results']['deleted']++;
      }
    }
  }

  audit_acquia_contenthub_audit_publisher_print_duration($context, $entities);
  // Update the last processed operation state variable.
  \Drupal::state()->set(ACH_AP_LAST_PROCESSED_OPERATION, $operation_key);
}

/**
 * Checks all entities from Content Hub that do not exist in the tracking table.
 *
 * @param int $start
 *   The offset for the listEntities in Content Hub.
 * @param bool $delete
 *   TRUE to delete the entities, FALSE or NULL otherwise.
 * @param int $operation_key
 *   The key of the batch operation.
 * @param array $context
 *   The context array.
 */
function audit_acquia_contenthub_audit_publisher_delete($start, $delete, $operation_key, &$context) {
  \Drupal::state()->set('ach_ap_running', time());
  if (empty($context['sandbox'])) {
    $context['results']['ch_exported'] = !empty($context['results']['ch_exported']) ? $context['results']['ch_exported'] : 0;
    $context['results']['ch_deleted'] = !empty($context['results']['ch_deleted']) ? $context['results']['ch_deleted'] : 0;
    $context['results']['ch_total_processed'] = !empty($context['results']['ch_total_processed']) ? $context['results']['ch_total_processed'] : 0;
    $context['results']['ch_start_time'] = !empty($context['results']['ch_start_time']) ? $context['results']['ch_start_time'] : date_create();
    $context['results']['print_duration'] = !empty($context['results']['print_duration']) ? $context['results']['print_duration'] : FALSE;
  }

  drush_print("--------------------------------------------------------------------------------------\n");
  drush_print("Checking all entities from Content Hub that do not exist in the tracking table.\n");

  /** @var \Drupal\acquia_contenthub\Client\ClientManager $client_manager */
  $client_manager = \Drupal::service('acquia_contenthub.client_manager');
  /** @var \Drupal\acquia_contenthub\EntityManager $entity_manager */
  $entity_manager = \Drupal::service('acquia_contenthub.entity_manager');
  $options = [
    'start' => $start,
  ];
  $list = audit_acquia_contenthub_audit_publisher_create_request($client_manager, 'listEntities', [$options]);
  $entities = [];
  if (is_array($list) && isset($list['data'])) {
    $uuids = [];
    foreach ($list['data'] as $entity) {
      $uuids[] = $entity['uuid'];
      $entities[$entity['uuid']] = $entity;
    }

    //drush_print("--------------------------------------------------------------------------------------\n");

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
      $entity = $entities[$uuid];
      // Trying to see if this is an eligible export entity and if so, then
      // export it.
      $entity_repository = \Drupal::service("entity.repository");
      $drupal_entity = $entity_repository->loadEntityByUuid($entity['type'], $entity['uuid']);
      if ($drupal_entity) {
        $entity_id = $drupal_entity->id();
        if (isEligible($entity_manager, $drupal_entity)) {
          drush_print(dt('Entity exists in Content Hub but not in the tracking table. Re-exporting eligible entity type = @type, UUID = @uuid, ID = @id.', [
            '@type' => $entity['type'],
            '@uuid' => $entity['uuid'],
            '@id' => $entity_id,
          ]));
          // Entity is eligible, then re-export.
          /** @var \Drupal\acquia_contenthub\Controller\ContentHubEntityExportController $export_controller */
          $export_controller = \Drupal::service('acquia_contenthub.acquia_contenthub_export_entities');
          // Export Entities.
          temporal_directly_export_entity_without_export_queue($entity_manager, $export_controller, $drupal_entity);
//          $export_controller->exportEntities([$drupal_entity]);
          $context['results']['ch_exported']++;
        }
        else {
          // Entity is not eligible anymore. Check configurations.
          drush_set_error(dt('This entity exists in Content Hub and Drupal but is not eligible to be exported anymore. Check configurations or use delete option: Entity Type = @type, UUID = @uuid, ID = @id.', [
            '@type' => $entity['type'],
            '@uuid' => $entity['uuid'],
            '@id' => $entity_id,
          ]));
          if ($delete) {
            // Entity is not in the tracking table, and the delete option was used. Delete the entity from Content Hub.
            $client_manager->createRequest('deleteEntity', [$entity['uuid']]);
            $context['results']['ch_deleted']++;
          }
        }
      }
      else {
        // Drupal entity does not exist (cannot be loaded).
        drush_set_error(dt('Entity exists in Content Hub, but it is not in the tracking table and could not be loaded in Drupal. Deleting it: Entity Type = @type, UUID = @uuid.', [
          '@type' => $entity['type'],
          '@uuid' => $entity['uuid'],
        ]));
        // Delete the entity from Content hub.
        if ($client_manager->createRequest('deleteEntity', [$entity['uuid']])) {
          $context['results']['ch_deleted']++;
        }
      }
    }

    audit_acquia_contenthub_audit_publisher_print_duration($context, $entities);
  }
  \Drupal::state()->set(ACH_AP_LAST_PROCESSED_OPERATION, $operation_key);
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
function audit_acquia_contenthub_audit_publisher_finished($success, $results, $operations) {
  // The 'success' parameter means no fatal PHP errors were detected. All
  // other error management should be handled using 'results'.
  if ($success) {
    drush_print(dt('Total number of entities not found in Content Hub: @total', [
      '@total' => number_format($results['not_published']),
    ]));
    drush_print(dt('Total number of entities outdated in Content Hub: @total', [
      '@total' => number_format($results['outdated']),
    ]));
    drush_print(dt('Total number of entities deleted from Content Hub: @total', [
      '@total' => number_format($results['deleted']),
    ]));
    drush_print(dt('Total number of entities with status updated in the tracking table: @total', [
      '@total' => number_format($results['status_updated']),
    ]));
    drush_print(dt('Total number of entities in sync with Content Hub: @total', [
      '@total' => number_format($results['in_sync']),
    ]));

    $duration = date_diff($results['start_time'], date_create());
    $duration_hours = $duration->h;
    $duration_minutes = $duration->i;
    $duration_message = '';
    if ($duration_hours == 1) {
      $duration_message .= $duration_hours . ' hour ';
    }
    elseif ($duration_hours > 1) {
      $duration_message .= $duration_hours . ' hours ';
    }

    if ($duration_minutes == 1) {
      $duration_message .= $duration_minutes . ' minute';
    }
    elseif ($duration_minutes > 1) {
      $duration_message .= $duration_minutes . ' minutes';
    }
    drush_print(dt('Total number of entities processed: @total within @duration.', [
      '@total' => number_format($results['total_processed']),
      '@duration' => $duration_message,
    ]));


    drush_print(dt('Total number of Content Hub entities not found in tracking table: @total', [
      '@total' => number_format($results['ch_exported']),
    ]));
    drush_print(dt('Total number of Content Hub entities deleted from Content Hub: @total', [
      '@total' => number_format($results['ch_deleted']),
    ]));
    drush_print(dt('Total number of Content Hub entities processed: @total', [
      '@total' => number_format($results['ch_total_processed']),
    ]));
  }
  else {
    drush_print(dt('Finished with a PHP fatal error.'));
  }
}

function audit_acquia_contenthub_audit_publisher_create_request($client_manager, $name, $arguments) {
  $i = 0;
  do {
    /** @var \Acquia\ContentHubClient\Entity[] $ch_entities */
    $ch_entities = $client_manager->createRequest($name, $arguments);
    $i++;
  } while (!is_array($ch_entities) || $i < 5);
  return is_array($ch_entities) ? $ch_entities : [];
}

function audit_acquia_contenthub_audit_publisher_delete_remote_entity($client_manager, $connection, $type, $uuid) {
  $client_manager->createRequest('deleteEntity', [$uuid]);
  $connection->delete(ContentHubEntitiesTracking::TABLE)
    ->condition('entity_uuid', $uuid, '=')
    ->condition('entity_type', $type, '=')
    ->execute();
}

function audit_acquia_contenthub_audit_publisher_is_exported($connection, $entity) {
  $status_export = $connection->select(ContentHubEntitiesTracking::TABLE, 'chet')
    ->fields('chet' , ['status_export'])
    ->condition('chet.entity_type', $entity->entity_type)
    ->condition('chet.entity_id', $entity->entity_id)
    ->execute()
    ->fetchField();
  
  if ($status_export === ContentHubEntitiesTracking::EXPORTED) {
    return TRUE;
  }
  
  return FALSE;
}



function audit_acquia_contenthub_audit_publisher_print_duration(&$context, $entities) {
  $context['results']['total_processed'] += count($entities);
  if (empty($context['results']['start_time'])) {
    $context['results']['start_time'] = date_create();
  }
  $duration = date_diff($context['results']['start_time'], date_create());
  $duration_hours = $duration->h;
  $duration_minutes = $duration->i;

  if (($duration_hours !== 0 || $duration_minutes !== 0 ) && $duration_minutes % 2 === 0 && $context['results']['print_duration'] === TRUE) {
    // 2 minutes have passed.
    // Print message to keep SSH alive.
    $duration_message = '';
    if ($duration_hours == 1) {
      $duration_message .= $duration_hours . ' hour';
    }
    elseif ($duration_hours > 1) {
      $duration_message .= $duration_hours . ' hours';
    }

    if ($duration_hours > 0 && $duration_minutes > 0) {
      $duration_message .= ' ';
    }

    if ($duration_minutes == 1) {
      $duration_message .= $duration_minutes . ' minute';
    }
    elseif ($duration_minutes > 1) {
      $duration_message .= $duration_minutes . ' minutes';
    }

    $message = dt('@total_processed entities processed within @duration.', [
      '@total_processed' => number_format($context['results']['total_processed']),
      '@duration' => $duration_message,
    ]);
    drush_print($message);
    // Only print the duration once within a 5 minute span.
    $context['results']['print_duration'] = FALSE;
  }
  elseif ($duration->i % 2 === 1) {
    // One minute has passed since the last duration print.
    // Enable print duration flag.
    $context['results']['print_duration'] = TRUE;
  }
}

function audit_acquia_contenthub_audit_publisher_get_ch_entities($uuids, $size = 10) {
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

/**
 * @param \Drupal\acquia_contenthub\EntityManager $entity_manager
 * @param \Drupal\Core\Entity\EntityInterface $drupal_entity
 * @return bool
 */
function isEligible($entity_manager, $drupal_entity) {
  if (!$entity_manager->isEligibleEntity($drupal_entity)) {
    return FALSE;
  }
  if ($drupal_entity instanceof NodeInterface) {
    return audit_ach_ap_entity_is_published($drupal_entity);
  }
  return TRUE;
}

/**
 * Exports entity.
 *
 * @param \Drupal\acquia_contenthub\EntityManager $entity_manager
 *   The Entity Manager.
 * @param \Drupal\acquia_contenthub\Controller\ContentHubEntityExportController $export_controller
 *   The Export Controller.
 * @param \Drupal\Core\Entity\EntityInterface $entity
 *   The drupal entity.
 *
 * @return int
 */
function temporal_directly_export_entity_without_export_queue($entity_manager, $export_controller, $entity) {
  $exported_entities = [];
  /** @var \Drupal\acquia_contenthub\Normalizer $normalizer */
  $cdf_normalizer = \Drupal::service('serializer.normalizer.acquia_contenthub_cdf.acquia_contenthub');
  $context['query_params']['include_references'] = 'true';
  $exported_entity = $cdf_normalizer->normalize($entity, 'acquia_contenthub_cdf', $context);
  foreach ($exported_entity['entities'] as $key => $ch_entity) {
    $exported_entities['entities'][$key] = Json::decode($ch_entity->json());
  }

  // Eliminate duplicates.
  $uuids = [];
  foreach ($exported_entities['entities'] as $cdf) {
    if (!empty($cdf)) {
      $uuids[] = $cdf['uuid'];
    }
  }
  $exported_cdfs = $exported_entities['entities'];
  drush_print("Exporting UUIDs: " . implode(': ', $uuids));

  // Publish entities.
  if (!empty($exported_cdfs)) {
    if ($entity_manager->putRemoteEntities($exported_cdfs)) {
      foreach ($exported_cdfs as $exported_entity) {
        // Obtaining the entity ID from the entity.
        $export_controller->trackExportedEntity($exported_entity, TRUE);
      }
      return count($exported_cdfs);
    }
    else {
      // Error, cannot put entities to Content Hub.
      \Drupal::logger('acquia_contenthub')->debug('PUT request to Content Hub failed for these UUIDs: @uuids', [
        '@uuids' => implode(', ', $uuids),
      ]);
    }
  }
  else {
    // Nothing to export.
    \Drupal::logger('acquia_contenthub')->debug('There was nothing to export. UUIDs: @uuids', [
      '@uuids' => implode(', ', $uuids),
    ]);
  }
}

function audit_ach_ap_entity_is_published($entity) {
  $translations = \Drupal::database()->select('node_field_data', 'nfd')
    ->fields('nfd' , ['status'])
    ->condition('nfd.status', 1)
    ->condition('nfd.nid', $entity->id())
    ->countQuery()
    ->execute()
    ->fetchField();
  return ($translations >= 1);
}
