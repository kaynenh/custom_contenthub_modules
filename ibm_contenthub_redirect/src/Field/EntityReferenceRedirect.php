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
    foreach ($this->getEntity()->get('redirect_redirect')->getValue() as $delta => $redirect_item) {
      $uri = $redirect_item['uri'];
      $entity = FALSE;
      list($source, $url) = explode(':', $uri);
      if ($source === 'internal') {
        // Checking for node URL.
        $path = pathinfo($url);
        switch ($path['dirname']) {
          case '/node':
            $nid = $path['filename'];
            if (is_numeric($nid)) {
              $entity = [
                'type' => 'node',
                'id' => $nid,
              ];
            }
            break;

          case '/taxonomy/term':
            $tid = $path['filename'];
            if (is_numeric($tid)) {
              $entity = [
                'type' => 'taxonomy_term',
                'id' => $tid,
              ];
            }
            break;
        }
        // If we found an internal path.
        if ($entity !== FALSE) {
          /** @var \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager */
          $entity_type_manager = \Drupal::entityTypeManager();
          if ($entity = $entity_type_manager->getStorage($entity['type'])
            ->load($entity['id'])
          ) {
            // Create an entity reference attribute so that the entity it
            // points to becomes a dependency of the "redirect" entity.
            $url_list[$delta] = $this->createItem($delta, $entity->id());
          }
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