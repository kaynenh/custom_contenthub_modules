<?php

use Drupal\acquia_contenthub\ContentHubEntitiesTracking;

const ACH_AS_LAST_PROCESSED_OPERATION = 'ach_as_last_processed_operation';

# Override to prevent memory exhaustion
ini_set('memory_limit', -1);

/*
 * Helper function to set the current session wait_timeout
 */
function set_db_wait_timeout($seconds) {
  $connection = \Drupal\Core\Database\Database::getConnection('default');
  $query = $connection->query("SET wait_timeout = $seconds");
}

/*
 * Helper function to read the current session wait_timeout
 */
function get_db_wait_timeout() {
  $connection = \Drupal\Core\Database\Database::getConnection('default');
  $result = $connection->query("SHOW SESSION VARIABLES LIKE 'wait_timeout'");
  if ($result) {
    drush_print("------------------------------------------------------------");
    drush_print("* MYSQL SESSION WAIT_TIMEOUT OVERRIDE *");
    while ($row = $result->fetchAssoc()) {
      drush_print("- " . $row['Variable_name'] . " is currently set to: " . $row['Value'] . " seconds.");
      //drush_print_r($row);
    }
  }
  drush_print("------------------------------------------------------------");
}

// Set MYSQL wait_timeout to 8 hours to avoid errors
$wait_timeout = 28800;
set_db_wait_timeout($wait_timeout);
// Print out the current session wait_timeout
get_db_wait_timeout();

//exit(0);

// Check if there is process already running this script by checking if the
// timestamp of the appropriate state variable is older that 1 minute.
$ach_as_running = \Drupal::state()->get('ach_as_running');
if ($ach_as_running && time() - $ach_as_running < 60) {
  return drush_set_error('Script already running. Check it out!');
}

\Drupal::state()->set('ach_as_running', time());

// Obtaining the query.
$reimport = drush_get_option("import") ?: FALSE;
if ($reimport) {
  $warning_message = dt('Are you sure you want to import outdated entities in this site?');
  if (drush_confirm($warning_message) == FALSE) {
    return drush_user_abort();
  }
}

$entity_type_id = drush_get_option("entity_type_id") ?: NULL;

$restart = drush_get_option("restart") ?: FALSE;
if ($restart) {
  \Drupal::state()->delete(ACH_AS_LAST_PROCESSED_OPERATION);
}

/** @var \Drupal\acquia_contenthub\ContentHubEntitiesTracking $entities_tracking */
$entities_tracking = \Drupal::getContainer()->get('acquia_contenthub.acquia_contenthub_entities_tracking');
$entities = $entities_tracking->getImportedEntities(ContentHubEntitiesTracking::AUTO_UPDATE_ENABLED, $entity_type_id);

$num_entities = number_format(count($entities));
drush_print(dt('Auditing @num_entities entities with import status = AUTO_UPDATE_ENABLED..', [
  '@num_entities' => $num_entities,
]));

unset($entities_tracking);

// Creating the batch process.
$operations = [];
$chunks = array_chunk($entities, 10);
foreach ($chunks as $operation_key => $chunk) {
  $operations[$operation_key] = [
    'ibm_acquia_contenthub_subscriber_audit_subscriber',
    [$chunk, $reimport, $operation_key],
  ];
}

/* @var \Drupal\acquia_contenthub\ContentHubSearch $contenthub_search */
$contenthub_search = \Drupal::service('acquia_contenthub.acquia_contenthub_search');

// Load all Content Hub Filters.
/** @var \Drupal\acquia_contenthub_subscriber\ContentHubFilterInterface[] $contenthub_filters */
$contenthub_filters = \Drupal::entityTypeManager()->getStorage('contenthub_filter')->loadMultiple();
foreach ($contenthub_filters as $contenthub_filter) {

  // Get the Status from the Filter Information.
  $status = $contenthub_filter->getPublishStatus();

  // If Publish Status is FALSE, stop processing this filter and jump to the
  // next one.
  if ($status === FALSE) {
    continue;
  }

  $entities = $contenthub_search->getContentHubFilteredEntities($contenthub_filter, 0);
  // Do something with the entities.
  $total = $entities['total'];
  // Dividing into batches of 1000 entities.
  $iterations = ceil($total / 1000);
  $operation_key = count($operations) - 1;
  for ($i = 0; $i < $iterations; $i++) {
    $operation_key++;
    $start = $i * 1000;
    $operations[$operation_key] = [
      'ibm_acquia_contenthub_subscriber_audit_subscriber_find_missing_imports',
      [$contenthub_filter->id(), $start, 1000, $reimport, $operation_key],
    ];
  }
}

$last_processed_operation = \Drupal::state()->get(ACH_AS_LAST_PROCESSED_OPERATION);
if (!empty($last_processed_operation)) {
  $length = count($operations) - ($last_processed_operation - 1);
  $operations = array_slice($operations, $last_processed_operation, $length, TRUE);
}

// Setting up batch process.
$batch = [
  'title' => dt("Checks imported entities and compares them with Content Hub"),
  'operations' => $operations,
  'finished' => 'ibm_acquia_contenthub_subscriber_audit_subscriber_finished',
];

$batch['file'] = '../../../../../home/ibmcom/ibm_ach_as_batch_functions.php';

$batch['file'] = '../ch-scripts/ibm_ach_as_batch_functions.php';

// Batch processing.
batch_set($batch);

// Start the batch process.
drush_backend_batch_process();
