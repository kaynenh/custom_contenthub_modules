<?php
namespace Drupal\ibm_contenthub_redirect\Field;

use Drupal\Core\Session\AccountInterface;
use Drupal\Core\Field\EntityReferenceFieldItemList;

/**
 * @internal
 */
class EntityReferenceRedirect extends EntityReferenceFieldItemList {

  /**
   * Initialize the internal field list with the modified items.
   */
  protected function initList() {
    if ($this->list) {
      return;
    }
    $url_list = [];
    $entity = ibm_contenthub_redirect_extract_destination_entity($this->getEntity());
    // If we found an internal path that points to an existent entity.
    if ($entity !== FALSE && $this->getSetting('target_type') === $entity['type']) {
      /** @var \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager */
      $entity_type_manager = \Drupal::entityTypeManager();
      if ($entity = $entity_type_manager->getStorage($entity['type'])->load($entity['id'])) {
        // Create an entity reference attribute so that the entity it
        // points to becomes a dependency of the "redirect" entity.
        $url_list[] = $this->createItem(0, $entity->id());
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
    return $this->getEntity()
      ->get('redirect_redirect')
      ->access($operation, $account, $return_as_object);
  }

  /**
   * {@inheritdoc}
   */
  public function isEmpty() {
    return $this->getEntity()->get('redirect_redirect')->isEmpty();
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

}