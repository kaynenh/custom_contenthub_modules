<?php
use Drupal\acquia_contenthub\ContentHubEntitiesTracking;
use Drupal\acquia_contenthub_subscriber\ContentHubFilterInterface;
use Acquia\ContentHubClient\Entity as ContentHubEntity;
use Drupal\Core\Language\LanguageManagerInterface;
use Drupal\Core\Language\LanguageInterface;
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
// Get all of the options passed to this command.
$delete = drush_get_option("delete") ?: FALSE;
if ($delete) {
  $warning_message = dt('Are you sure you want to delete entities from Content Hub if they no longer exist in Drupal?');
  if (drush_confirm($warning_message) == FALSE) {
    return drush_user_abort();
  }
}
// Obtaining the query.
$reimport = TRUE;//drush_get_option("import") ?: FALSE;
if (!$reimport) {
  $warning_message = dt('Are you sure you want to import outdated entities in this site?');
  if (drush_confirm($warning_message) == FALSE) {
    return drush_user_abort();
  }
}
$restart = drush_get_option("restart") ?: FALSE;
if ($restart) {
  \Drupal::state()->delete(ACH_AS_LAST_PROCESSED_OPERATION);
}
/** @var \Drupal\acquia_contenthub\ContentHubEntitiesTracking $entities_tracking */
$entities_tracking = \Drupal::getContainer()->get('acquia_contenthub.acquia_contenthub_entities_tracking');
$entities = getImportedEntities(ContentHubEntitiesTracking::AUTO_UPDATE_ENABLED, $entity_type_id);
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
    'audit_acquia_contenthub_subscriber_audit_subscriber',
    [$chunk, $reimport, $operation_key, $delete],
  ];
}
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
  $entities = getContentHubFilteredEntities($contenthub_filter, 0);
  // Do something with the entities.
  $total = $entities['total'];
  // Dividing into batches of 1000 entities.
  $iterations = ceil($total / 1000);
  $operation_key = count($operations) - 1;
  for ($i = 0; $i < $iterations; $i++) {
    $operation_key++;
    $start = $i * 1000;
    $operations[$operation_key] = [
      'audit_acquia_contenthub_subscriber_audit_subscriber_find_missing_imports',
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
  'finished' => 'audit_acquia_contenthub_subscriber_audit_subscriber_finished',
];
// @TODO: Change this directory to reflect your particular case.
$batch['file'] = '../../../../../home/customer/scripts/audit_ach_as_batch_functions.php';
// Batch processing.
batch_set($batch);
// Start the batch process.
drush_backend_batch_process();
function getImportedEntities($status_import = '', $entity_type_id = '') {
  if ($status_import) {
    /** @var \Drupal\Core\Database\Query\SelectInterface $query */
    $query = \Drupal::database()->select(ContentHubEntitiesTracking::TABLE, 'ci')
      ->fields('ci');
    $query = $query->condition('status_import', $status_import);
    if (!empty($entity_type_id)) {
      $query = $query->condition('entity_type', $entity_type_id);
    }
    return $query->execute()->fetchAll();
  }
  return [];
}
function getContentHubFilteredEntities(ContentHubFilterInterface $contenthub_filter, $start = 0, $size = 1000) {
  $options = [
    'start' => $start,
    'count' => $size,
  ];
  // Obtain the Filter conditions.
  $conditions = $contenthub_filter->getConditions();
  if (!empty($conditions)) {
    $items = getElasticSearchQueryResponse($conditions, NULL, NULL, $options);
    $entities = [
      'total' => $items['total']
    ];
    foreach ($items['hits'] as $item) {
      $entities[$item['_source']['data']['uuid']] = new  ContentHubEntity($item['_source']['data']);
    }
    return $entities;
  }
  // If we reach here, return empty array.
  return [];
}
function getElasticSearchQueryResponse(array $conditions, $asset_uuid, $asset_type, array $options = []) {
  $query = [
    'query' => [
      'bool' => [
        'must' => [],
        'should' => [],
      ],
    ],
    'size' => !empty($options['count']) ? $options['count'] : 10,
    'from' => !empty($options['start']) ? $options['start'] : 0,
    'highlight' => [
      'fields' => [
        '*' => new \stdClass(),
      ],
    ],
  ];
  // Supported Entity Types and Bundles.
  $supported_entity_types_bundles = acquia_contenthub_subscriber_supported_entity_types_and_bundles();
  // Iterating over each condition.
  foreach ($conditions as $condition) {
    list($filter, $value) = explode(':', $condition);
    // Tweak ES query for each filter condition.
    switch ($filter) {
      // For entity types.
      case 'entity_types':
        $query['query']['bool']['should'][] = [
          'terms' => [
            'data.type' => explode(',', $value),
          ],
        ];
        break;
      // For bundles.
      case 'bundle':
        // Obtaining bundle_key for this bundle.
        foreach ($supported_entity_types_bundles as $entity_type => $bundles) {
          if (in_array($value, $bundles['bundles'])) {
            $bundle_key = $bundles['bundle_key'];
            break;
          }
        }
        if (empty($bundle_key)) {
          break;
        }
        // Test all supported languages.
        $supported_languages = array_keys(\Drupal::languageManager()->getLanguages(LanguageInterface::STATE_ALL));
        foreach ($supported_languages as $supported_language) {
          $query['query']['bool']['should'][] = [
            'term' => [
              "data.attributes.{$bundle_key}.value.{$supported_language}" => $value,
            ],
          ];
        }
        break;
      // For Search Term (Keyword).
      case 'search_term':
        if (!empty($value)) {
          $query['query']['bool']['must'][] = [
            'match' => [
              "_all" => "*{$value}*",
            ],
          ];
        }
        break;
      // For Tags.
      case 'tags':
        $query['query']['bool']['must'][] = [
          'match' => [
            "_all" => $value,
          ],
        ];
        break;
      // For Origin / Source.
      case 'origins':
        $query['query']['bool']['must'][] = [
          'match' => [
            "_all" => $value,
          ],
        ];
        break;
      case 'modified':
        $dates = explode('to', $value);
        $from = isset($dates[0]) ? trim($dates[0]) : '';
        $to = isset($dates[1]) ? trim($dates[1]) : '';
        if (!empty($from)) {
          $date_modified['gte'] = $from;
        }
        if (!empty($to)) {
          $date_modified['lte'] = $to;
        }
        $date_modified['time_zone'] = '+1:00';
        $query['query']['bool']['must'][] = [
          'range' => [
            "data.modified" => $date_modified,
          ],
        ];
        break;
    }
  }
  if (!empty($options['sort']) && strtolower($options['sort']) !== 'relevance') {
    $query['sort']['data.modified'] = strtolower($options['sort']);
  }
  if (isset($asset_uuid)) {
    // This part of the query references the entity UUID and goes in its
    // separate "must" condition to only filter this single entity.
    $query_filter['query']['bool']['must'][] = [
      'term' => [
        '_id' => $asset_uuid,
      ],
    ];
    // This part of the query is to filter all entities according to the
    // content hub filter selection and it goes on its own "must" condition.
    $query_filter['query']['bool']['must'][] = $query['query'];
    // Together, they verify if a particular content hub filter applies to
    // an entity UUID or not.
    $query = $query_filter;
  }
  /* @var \Drupal\acquia_contenthub\ContentHubSearch $contenthub_search */
  $contenthub_search = \Drupal::service('acquia_contenthub.acquia_contenthub_search');
  return $contenthub_search->executeSearchQuery($query);
}
/**
 * Obtains a list of supported entity types and bundles by this site.
 *
 * This also includes the 'bundle' key field. If the bundle key is empty this
 * means that this entity does not have any bundle information.
 *
 * @return array
 *   An array of entity_types and bundles keyed by entity_type.
 */
function acquia_contenthub_subscriber_supported_entity_types_and_bundles() {
  /** @var \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager */
  $entity_manager = \Drupal::getContainer()->get('acquia_contenthub.entity_manager');
  /** @var \Drupal\acquia_contenthub\EntityManager $entity_manager */
  $entity_type_manager = \Drupal::entityTypeManager();
  $entity_types = $entity_manager->getAllowedEntityTypes();
  $entity_types_and_bundles = [];
  foreach ($entity_types as $entity_type => $bundles) {
    if ($entity_type === 'taxonomy_term') {
      $bundle_key = 'vocabulary';
    }
    else {
      $bundle_key = $entity_type_manager->getDefinition($entity_type)->getKey('bundle');
    }
    $entity_types_and_bundles[$entity_type] = [
      'bundle_key' => $bundle_key,
      'bundles' => array_keys($bundles),
    ];
  }
  return $entity_types_and_bundles;
}
