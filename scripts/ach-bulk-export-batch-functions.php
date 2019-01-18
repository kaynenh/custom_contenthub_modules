<?php

use Drupal\acquia_contenthub\ContentHubEntitiesTracking;
use Drupal\node\NodeInterface;

const ENTITIES_PER_BATCH = 20;
const ACH_ENQUEUE_LAST_PROCESSED_OPERATION = 'ach_enqueue_last_processed_operation';

/**
 * Process a subset of all the entities to be enqueued in a single request.
 *
 * @param $entity_type
 *   The entity type.
 * @param $bundle
 *   The entity bundle.
 * @param $offset
 *   The offset to limit the range of entities to process in every batch.
 * @param bool $enqueue
 *   Enqueue entities.
 * @param bool $enqueue_all
 *   TRUE to enqueue all entities, FALSE to skip entities with status EXPORTED
 *   in the tracking table.
 * @param int $operation_key
 *   The operation id.
 */
function export_enqueue_entities($entity_type, $bundle, $offset, $enqueue, $enqueue_all, $operation_key, &$context) {
  /**
   * Number of entities per iteration. Decrease this number if your site has
   * too many dependencies per node.
   *
   * @var int $entities_per_iteration
   */
  $entities_per_iteration = 5;

  if (empty($context['sandbox'])) {
    // Get the IDs of the Entities to export.
    $entity_ids = get_entities_to_export($entity_type, $bundle, $offset, $enqueue_all);
    // @todo: Check the case when it is empty.

    $context['sandbox']['progress'] = 0;
    $context['sandbox']['max'] = count($entity_ids);
    $context['sandbox']['entity_ids'] = $entity_ids;
    $context['results']['total'] = !empty($context['results']['total']) ? $context['results']['total'] : 0;
    $context['results']['eligible'] = !empty($context['results']['eligible']) ? $context['results']['eligible'] : 0;
    $context['results']['ineligible'] = !empty($context['results']['ineligible']) ? $context['results']['ineligible'] : 0;
  }

  // Always obtain the list of entities from the sandbox.
  $entity_ids = $context['sandbox']['entity_ids'];

  /** @var \Drupal\acquia_contenthub\EntityManager $entity_manager */
  $entity_manager = \Drupal::service('acquia_contenthub.entity_manager');
  /** @var \Drupal\acquia_contenthub\Controller\ContentHubEntityExportController $export_controller */
  $export_controller = \Drupal::service('acquia_contenthub.acquia_contenthub_export_entities');

  $slice_entity_ids = array_slice($entity_ids, $context['sandbox']['progress'], $entities_per_iteration);
  $ids = array_values($slice_entity_ids);
  if (!empty($ids)) {
    $entities = \Drupal::entityTypeManager()
      ->getStorage($entity_type)
      ->loadMultiple($ids);
    foreach ($entities as $entity) {
      if (isEligible($entity_manager, $entity)) {
        if ($enqueue) {
          // Entity is eligible, then re-export.
          $export_controller->exportEntities([$entity]);
        }
        $context['results']['eligible']++;
      }
      else {
        drush_print(dt('Entity not eligible for export: entity type = @type, UUID = @uuid, ID = @id.', [
          '@type' => $entity->getEntityTypeId(),
          '@uuid' => $entity->uuid(),
          '@id' => $entity->id(),
        ]));
        $context['results']['ineligible']++;
      }
    }
  }
  $context['sandbox']['progress'] += count($ids);

  $enqueued = implode(',', $ids);
  $message = empty($enqueued) ? "Processing '$entity_type' ($bundle) entities: No entities to queue." :  "Processing '$entity_type' ($bundle) entities with IDs: " . $enqueued;

  $context['results']['total'] += count($ids);
  $context['message'] = $message;

  if ($context['sandbox']['progress'] != $context['sandbox']['max']) {
    $context['finished'] = $context['sandbox']['progress'] / $context['sandbox']['max'];
  }
  else {
    // Update the last processed operation state variable.
    \Drupal::state()->set(ACH_ENQUEUE_LAST_PROCESSED_OPERATION, $operation_key);
  }

}

function export_enqueue_finished($success, $results, $operations) {
  // The 'success' parameter means no fatal PHP errors were detected. All
  // other error management should be handled using 'results'.
  if ($success) {
    $message = "Total number of processed entities: " . $results['total'];
    $message .= "\nTotal number of eligible entities (enqueued): " . $results['eligible'];
    $message .= "\nTotal number of ineligible entities (not enqueued): " . $results['ineligible'];
  }
  else {
    $message = t('Finished with an error.');
  }
  drush_print($message);

  // If we finish execution normally, delete state variable.
  \Drupal::state()->delete(ACH_ENQUEUE_LAST_PROCESSED_OPERATION);
}


/**
 * Provided a list of entities to export, it filters to not exported ones.
 *
 * @param string $entity_type
 *   The entity type.
 * @param $bundle
 *   The entity bundle.
 * @param $offset
 *   The offset to limit the range of entities to process in every batch.
 * @param bool $enqueue_all
 *   TRUE to enqueue all entities, FALSE to skip entities with status EXPORTED
 *   in the tracking table.
 *
 * @return array
 *   An array of entities not previously exported.
 */
function get_entities_to_export($entity_type, $bundle, $offset, $enqueue_all) {
  // $drupal_bundles = \Drupal::entityManager()->getBundleInfo($entity_type);
  /** @var \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager */
  $entity_type_manager = \Drupal::entityTypeManager();
  $bundle_key = $entity_type_manager->getDefinition($entity_type)->getKey('bundle');

  /** @var \Drupal\Core\Entity\Query\QueryInterface $query */
  $query = \Drupal::entityQuery($entity_type);
  // If we do not have file bundles but files have been marked as "exportable"
  // then do not include a condition for bundle.
  if (!empty($bundle_key)) {
    $query->condition($bundle_key, $bundle);
  }

  // We are only obtaining a specific range of entities.
  $entity_ids = array_values($query->range($offset, ENTITIES_PER_BATCH)->execute());

  // If we do not find any entity, return empty array.
  if (empty($entity_ids)) {
    return [];
  }

  // If we are NOT ignoring exported entities, then return all found entities.
  if ($enqueue_all) {
    return $entity_ids;
  }

  /** @var \Drupal\Core\Database\Connection $database */
  $database = \Drupal::service('database');
  $query = $database->select(ContentHubEntitiesTracking::TABLE, 'acet');
  $query->addField('acet', 'entity_id');

  // Adding Conditions.
  $query->condition('entity_id', $entity_ids, 'IN')
    ->condition('entity_type', $entity_type, '=')
    ->condition('status_export', 'EXPORTED');
  $result = $query->execute()->fetchAllKeyed();
  $exported_entities = array_keys($result);
  return array_diff($entity_ids, $exported_entities);
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
    return $entity_manager->isPublished($drupal_entity);
  }
  return TRUE;
}
