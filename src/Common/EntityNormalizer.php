<?php
declare(strict_types=1);

namespace Zhulanov111\CodeceptionClickhouseDoctrine2\Common;

use Carbon\Carbon;
use Carbon\Exceptions\InvalidFormatException;
use Doctrine\ORM\Mapping\ClassMetadata;
use Doctrine\ORM\Mapping\MappingException;

final class EntityNormalizer implements EntityNormalizerInterface
{
    private const DATETIME_FORMAT = 'Y-m-d H:i:s';
    private const DATE_FORMAT = 'Y-m-d';
    private const DATETIME_TYPES = ['datetime'];

    /**
     * @param ClassMetadata $metadata
     * @param array $row
     * @return object|null
     * @throws MappingException
     */
    public function toEntity(ClassMetadata $metadata, array $row): ?object
    {
        $entityClass = $metadata->getName();

        if ($row === [] || class_exists($entityClass) === false) {
            return null;
        }

        $entity = new $entityClass();

        foreach ($row as $column => $value) {
            $field = $metadata->getFieldForColumn($column);
            $fieldMapping = $metadata->getFieldMapping($field);
            $type = $fieldMapping['type'] ?? null;

            if (in_array($type, self::DATETIME_TYPES, true)) {
                try {
                    $parsedDateValue = Carbon::createFromFormat(self::DATETIME_FORMAT, $value);
                } catch (InvalidFormatException $exception) {
                    $parsedDateValue = Carbon::createFromFormat(self::DATE_FORMAT, $value)->setTime(0, 0);
                }

                $value = $parsedDateValue;
            }

            $metadata->setFieldValue($entity, $field, $value);
        }

        return $entity;
    }

    public function toArray(ClassMetadata $metadata, object $entity): array
    {
        $data = [];
        $fields = $metadata->getFieldNames();
        $columnsFieldsAssociation = array_combine($metadata->getColumnNames($fields), $fields);

        foreach ($columnsFieldsAssociation as $column => $field) {
            $value = $metadata->getFieldValue($entity, $field);

            if ($value instanceof \DateTimeInterface) {
                $value = $value->format(self::DATETIME_FORMAT);
            }

            $data[$column] = $value;
        }

        return $data;
    }
}
