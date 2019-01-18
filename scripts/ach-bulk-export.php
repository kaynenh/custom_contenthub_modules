<?php
/**
 * @file
 * Bulk Exports all entities to Content Hub.
 *
 * IMPORTANT: You should only use this script with the Export Queue Enabled.
 *
 * How to use:
 *
 * 1. Go to admin/config/services/acquia-contenthub/configuration and select
 *    all the entity types and bundles that you want to export to Content Hub.
 *
 * 2. Be gentle to the exporting site by increasing the export queue waiting
 *   time to 5 seconds and a queue batch size of 5 entities if your site is not
 *   packed with too many dependencies. You should decide according to your
 *   content structure how many entities per item and batch size would fit
 *   your situation better.
 *
 * 3. Execute this script in the following way:
 *    $drush scr ../scripts/ach-bulk-export.php
 *
 *    (Assuming the script files were installed in the '/script' directory).
 *
 *    This will queue all selected entity types and bundles in the export queue.
 *
 * 4. Go to admin/config/services/acquia-contenthub/export-queue and run the
 *    export queue.
 */
use Drupal\acquia_contenthub\ContentHubEntityDependency;
use Drupal\acquia_contenthub\ContentHubEntitiesTracking;

const ENTITIES_PER_BATCH = 20;
const ACH_ENQUEUE_LAST_PROCESSED_OPERATION = 'ach_enqueue_last_processed_operation';

$restart = drush_get_option("restart") ?: FALSE;
if ($restart) {
  \Drupal::state()->delete(ACH_ENQUEUE_LAST_PROCESSED_OPERATION);
}

$enqueue = drush_get_option("enqueue") ?: FALSE;
$node_only = drush_get_option("node_only") ?: FALSE;
$reverse = drush_get_option("reverse") ?: FALSE;
// $enqueue_all: TRUE to enqueue all entities, FALSE to skip entities with status EXPORTED
// in the tracking table.
$enqueue_all = drush_get_option("enqueue-all") ?: FALSE;

$operations = [];

$entity_manager = \Drupal::getContainer()->get('acquia_contenthub.entity_manager');

// Read all configured entity types and bundles from Content Hub.
/** @var  Drupal\acquia_contenthub\ContentHubEntityTypeConfigInterface[] $contenthub_types */
$contenthub_types = $entity_manager->getContentHubEntityTypeConfigurationEntities();

// Move nodes to the end of the array.
// The reason is because we want first to export all entity types that a node
// might have as dependencies. Then when we export nodes, those entities that
// could potentially be dependencies of those nodes won't be exported anymore,
// thus decreasing the export load.
if (isset($contenthub_types['node'])) {
  $node_types_config = $contenthub_types['node'];
  unset($contenthub_types['node']);
}

if (!$node_only) {
  // Obtain list of dependent entity types.
  $dependent_entity_type_ids = ContentHubEntityDependency::getPostDependencyEntityTypes();
  foreach ($contenthub_types as $entity_type => $entity_type_config) {
    // If it is a dependent entity, do not process because they will be
    // imported as dependencies.
    if (in_array($entity_type, $dependent_entity_type_ids)) {
      continue;
    }
    process_entity_type($entity_type, $entity_type_config, $enqueue, $reverse, $enqueue_all, $operations);
  }
  
  // Unsetting some variables not needed anymore.
  unset($dependent_entity_type_ids);
}

unset($entity_manager);
unset($contenthub_types);

// Finally process the node.
process_entity_type('node', $node_types_config, $enqueue, $reverse, $enqueue_all, $operations);

// Adding operation key to each operation.
foreach ($operations as $operation_key => $operation) {
  $args = $operation[1];
  $args[] = $operation_key;
  $operation[1] = $args;
  $operations[$operation_key] = $operation;
}

// If continuing from previous run...
$last_processed_operation = \Drupal::state()->get(ACH_ENQUEUE_LAST_PROCESSED_OPERATION);
if (!empty($last_processed_operation)) {
  $length = count($operations) - $last_processed_operation;
  $operations = array_slice($operations, $last_processed_operation + 1, $length, TRUE);
}

$batch = [
  'title' => "Enqueue entities for export",
  'file' => '../../../../../home/ibmcom/ach-bulk-export-batch-functions.php',
  'operations' => $operations,
  'finished' => 'export_enqueue_finished',
];

// Batch processing.
batch_set($batch);

// Start the batch process.
drush_backend_batch_process();


/**
 * Exports all entities for a specific entity type.
 *
 * @param string $entity_type
 *   The entity type.
 * @param \Drupal\acquia_contenthub\ContentHubEntityTypeConfigInterface $entity_type_config
 *   The Content Hub Entity Type Configuration Entity.
 * @param bool $enqueue
 *   Enqueue the entities.
 * @param bool $reverse
 *   Sort the list descending.
 * @param bool $ignore_exported
 *   TRUE to ignore Exported entities, FALSE otherwise (enqueue everything).
 * @param bool $enqueue_all
 *   TRUE to enqueue all entities, FALSE to skip entities with status EXPORTED 
 *   in the tracking table.
 */
function process_entity_type($entity_type, $entity_type_config, $enqueue, $reverse, $enqueue_all, &$operations) {
  $bundles = array_keys($entity_type_config->getBundles());

  if ($entity_type == 'node' && $reverse) {
    rsort($bundles);
  }
  
  // $drupal_bundles = \Drupal::entityManager()->getBundleInfo($entity_type);
  /** @var \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager */
  $entity_type_manager = \Drupal::entityTypeManager();
  $bundle_key = $entity_type_manager->getDefinition($entity_type)->getKey('bundle');
  foreach ($bundles as $bundle) {
    if ($entity_type_config->isEnableIndex($bundle) === FALSE) {
      // Do not process bundle if it is not enabled.
      continue;
    }

    print("Checking type = $entity_type, bundle = $bundle...");
    // For all enabled bundles.
    /** @var \Drupal\Core\Entity\Query\QueryInterface $query */
    $query = \Drupal::entityQuery($entity_type);
    // If we do not have file bundles but files have been marked as "exportable"
    // then do not include a condition for bundle.
    if (!empty($bundle_key)) {
      $query->condition($bundle_key, $bundle);
    }
    $total = $query->count()->execute();
    print "Found {$total} entities.\n";

    $steps = ceil($total / ENTITIES_PER_BATCH);
    for ($i = 0; $i < $steps; $i++) {
      $offset = $i * ENTITIES_PER_BATCH;
      $operations[] = [
        'export_enqueue_entities',
        [$entity_type, $bundle, $offset, $enqueue, $enqueue_all]
      ];
    }
  }
}
