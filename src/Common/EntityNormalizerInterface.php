<?php
declare(strict_types=1);

namespace Zhulanov111\CodeceptionClickhouseDoctrine2\Common;

use Doctrine\ORM\Mapping\ClassMetadata;
use Doctrine\ORM\Mapping\MappingException;

interface EntityNormalizerInterface
{
    /**
     * @throws MappingException
     */
    public function toEntity(ClassMetadata $metadata, array $row): ?object;

    public function toArray(ClassMetadata $metadata, object $entity): array;
}
