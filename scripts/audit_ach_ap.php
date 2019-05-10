<?php

use Drupal\acquia_contenthub\ContentHubEntitiesTracking;

const ACH_AP_LAST_PROCESSED_OPERATION = 'ach_ap_last_processed_operation';

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
$ach_ap_running = \Drupal::state()->get('ach_ap_running');
if ($ach_ap_running && time() - $ach_ap_running < 60) {
  return drush_set_error('Script already running. Check it out!');
}

\Drupal::state()->set('ach_ap_running', time());

// Get all of the options passed to this command.
$delete = drush_get_option("delete") ?: FALSE;
$publish = drush_get_option("publish") ?: FALSE;
$restart = drush_get_option("restart") ?: FALSE;
$status = drush_get_option("status") ?: ContentHubEntitiesTracking::EXPORTED;

$entity_type_id = NULL;

if ($delete) {
  $warning_message = dt('Are you sure you want to delete entities from Content Hub if they no longer exist in Drupal?');
  if (drush_confirm($warning_message) == FALSE) {
    return drush_user_abort();
  }
}
if ($publish) {
  $warning_message = dt('Are you sure you want to republish entities to Content Hub?');
  if (drush_confirm($warning_message) == FALSE) {
    return drush_user_abort();
  }
}
if ($restart) {
  \Drupal::state()->delete(ACH_AP_LAST_PROCESSED_OPERATION);
}

/** @var \Drupal\acquia_contenthub\ContentHubEntitiesTracking $entities_tracking */
//$entities_tracking = \Drupal::getContainer()->get('acquia_contenthub.acquia_contenthub_entities_tracking');

switch ($status) {
  case ContentHubEntitiesTracking::EXPORTED:
//    $entities = $entities_tracking->getPublishedEntities($entity_type_id);
//    break;

  case ContentHubEntitiesTracking::INITIATED:
//    $entities = $entities_tracking->getInitiatedEntities($entity_type_id);
//    break;

  case ContentHubEntitiesTracking::REINDEX:
//    $entities = $entities_tracking->getEntitiesToReindex($entity_type_id);
    $entities = audit_ach_ap_get_entities_tracking($entity_type_id, $status);
    break;

  case ContentHubEntitiesTracking::QUEUED:
    // If we want to queue "queued" entities, then we have to make sure the
    // export queue is empty or we might be re-queuing entities that already
    // are in the queue.
    /** @var \Drupal\Core\Queue\QueueInterface $queue */
    $queue = \Drupal::getContainer()->get('queue')->get('acquia_contenthub_export_queue');
    if ($queue->numberOfItems() > 0) {
      \Drupal::state()->delete('ach_ap_running');
      return drush_set_error('You cannot audit entities with a status of QUEUED in the tracking table when the queue is not empty, because you run the risk of enqueuing entities that are already in the queue. Please retry when the queue is empty.');
    }

    // @todo: I think it makes more sense to ignore the last processed operation when using the status=QUEUED rather than deleting.
    // Delete the last processed operation when using the status=QUEUED option.
    // We cannot guarantee the operation number/page will be the same, because 
    // we may have enqueued more entities with the previous run.
    $last_processed_operation = \Drupal::state()->get(ACH_AP_LAST_PROCESSED_OPERATION);
    if (!empty($last_processed_operation)) {
      $warning_message = dt('Using the --status=QUEUED option will automatically delete the last processed operation state variable, which is set to @last_processed.', ['@last_processed' => $last_processed_operation]);
      drush_print($warning_message);
      $warning_message = dt('Are you sure you want to delete the last processed operation state variable?');
      if (drush_confirm($warning_message) == FALSE) {
        return drush_user_abort();
      }
      \Drupal::state()->delete(ACH_AP_LAST_PROCESSED_OPERATION);
    }

//    $entities = $entities_tracking->getQueuedEntities($entity_type_id);
    $entities = audit_ach_ap_get_entities_tracking($entity_type_id, $status);
    break;

  default:
    \Drupal::state()->delete('ach_ap_running');
    return drush_set_error('You can only use the following values for status: EXPORTED, INITIATED, REINDEX, QUEUED.');
}

$num_entities = number_format(count($entities));
drush_print(dt('Auditing @num_entities entities with export status = @status...', [
  '@num_entities' => $num_entities,
  '@status' => $status,
]));

unset($entities_tracking);

// Creating the batch process.
$operations = [];
$chunks = array_chunk($entities, 10);
foreach ($chunks as $operation_key =>$chunk) {
  $operations[] = [
    'audit_acquia_contenthub_audit_publisher',
    [$chunk, $publish, $delete, $operation_key],
  ];
}

// Check for all entities having Content Hub as a source.
$options = [
  'start' => 0,
];

$operation_key = count($operations) - 1;

/** @var \Drupal\acquia_contenthub\Client\ClientManager $client_manager */
$client_manager = \Drupal::service('acquia_contenthub.client_manager');
if ($list = $client_manager->createRequest('listEntities', [$options])) {
  $steps = floor($list['total'] / 1000);
  for ($i = 0; $i <= $steps; $i++) {
    $operation_key++;
    $start = $i * 1000;
    $operations[] = [
      'audit_acquia_contenthub_audit_publisher_delete',
      [$start, $delete, $operation_key]
    ];
  }
}

$last_processed_operation = \Drupal::state()->get(ACH_AP_LAST_PROCESSED_OPERATION);
if (!empty($last_processed_operation)) {
  $length = count($operations) - ($last_processed_operation - 1);
  $operations = array_slice($operations, $last_processed_operation, $length, TRUE);
}

// Setting up batch process.
$batch = [
  'title' => dt("Checks published entities with Content Hub for correct status"),
  'operations' => $operations,
  'finished' => 'audit_acquia_contenthub_audit_publisher_finished',
];

// @TODO: Change this directory to reflect your particular case.
$batch['file'] = '../../../../../home/auditcom/audit_ach_ap_batch_functions.php';

// Batch processing.
batch_set($batch);

// Start the batch process.
drush_backend_batch_process();

function audit_ach_ap_get_entities_tracking($entity_type_id, $status_export) {
  $connection = \Drupal\Core\Database\Database::getConnection('default');
  $query = $connection->select(ContentHubEntitiesTracking::TABLE, 'ci')
    ->fields('ci');
  if (!empty($status_export)) {
    $query = $query->condition('status_export', $status_export);
  }
  if (!empty($entity_type_id)) {
    $query = $query->condition('entity_type', $entity_type_id);
  }

  $results = $query->execute()->fetchAll();

  return $results;
}
