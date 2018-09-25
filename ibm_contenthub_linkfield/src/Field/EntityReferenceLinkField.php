<?php

namespace Drupal\ibm_contenthub_linkfield\Field;

use Drupal\Core\Session\AccountInterface;
use Drupal\Core\Field\EntityReferenceFieldItemList;

/**
 * Class EntityReferenceLinkField.
 *
 * @package Drupal\ibm_contenthub_linkfield\Field
 */
class EntityReferenceLinkField extends EntityReferenceFieldItemList {

  /**
   * Initialize the internal field list with the modified items.
   */
  protected function initList() {
    if ($this->list) {
      return;
    }
    $url_list = [];
    $nids = $this->generateLinkIds();
    // If we found an internal path that points to an existent entity.
    if (!empty($nids) && $this->getSetting('target_type') === 'node') {
      /** @var \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager */
      $entity_type_manager = \Drupal::entityTypeManager();
      if ($entities = $entity_type_manager->getStorage('node')->loadMultiple($nids)) {
        // Create an entity reference attribute so that the entity it
        // points to becomes a dependency of the "redirect" entity.
        $delta = 0;
        foreach ($entities as $entity) {
          $url_list[] = $this->createItem($delta++, $entity->id());
        }
      }
    }
    $this->list = $url_list;
  }

  /**
   * {@inheritdoc}
   */
  public function getValue($include_computed = FALSE) {
    $this->initList();
    return parent::getValue($include_computed);
  }

  /**
   * {@inheritdoc}
   */
  public function access($operation = 'view', AccountInterface $account = NULL, $return_as_object = FALSE) {
    $this->getFieldDefinition()->getName();

    // Here are only assigning the view permission to the field the same
    // permission as the first link field in the entity. If no link field with
    // appropriate values were found then FALSE is returned.
    $entity = $this->getEntity();
    $link_fields = ibm_contenthub_linkfield_extract_destination_entities($entity);
    $field_name = reset(array_keys($link_fields));
    // Taking the first link field.
    if ($field_name) {
      return $this->getEntity()
        ->get($field_name)
        ->access($operation, $account, $return_as_object);
    }
    return FALSE;
  }

  /**
   * {@inheritdoc}
   */
  public function isEmpty() {
    $link_fields = $this->generateLinkIds();
    return empty($link_fields);
  }

  /**
   * {@inheritdoc}
   */
  public function getIterator() {
    $this->initList();
    return parent::getIterator();
  }

  /**
   * {@inheritdoc}
   */
  public function get($index) {
    $this->initList();
    return parent::get($index);
  }

  /**
   * Generate the list of UUIDs for this field.
   *
   * @return array
   *   An array of UUIDs.
   */
  public function generateLinkUuids() {
    $uuids = [];
    $link_fields = ibm_contenthub_linkfield_extract_destination_entities($this->getEntity());
    switch ($this->getFieldDefinition()->getName()) {
      case IBM_CONTENTHUB_LINKFIELD_LINK_NODE_FRAGMENT:
        $item = 'node_fragment';
        break;

      case IBM_CONTENTHUB_LINKFIELD_LINK_NODE_PATH:
      default:
        $item = 'node_path';
        break;
    }
    foreach ($link_fields as $name => $data) {
      if (isset($data[$item]['uuid']) && count($data[$item]['uuid']) > 0) {
        $uuids = array_merge($uuids, $data[$item]['uuid']);
      }
    }
    return array_unique($uuids);
  }

  /**
   * Generate the list of Entity IDs for this field.
   *
   * @return array
   *   An array of NIDs.
   */
  public function generateLinkIds() {
    $nids = [];
    $link_fields = ibm_contenthub_linkfield_extract_destination_entities($this->getEntity());
    switch ($this->getFieldDefinition()->getName()) {
      case IBM_CONTENTHUB_LINKFIELD_LINK_NODE_FRAGMENT:
        $item = 'node_fragment';
        break;

      case IBM_CONTENTHUB_LINKFIELD_LINK_NODE_PATH:
      default:
        $item = 'node_path';
        break;
    }
    foreach ($link_fields as $name => $data) {
      if (isset($data[$item]['nid']) && count($data[$item]['nid']) > 0) {
        $nids = array_merge($nids, $data[$item]['nid']);
      }
    }
    return array_unique($nids);
  }

}
